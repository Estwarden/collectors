#!/usr/bin/env python3
"""Summarize unsummarized event clusters in English via LLM.

Runs after clustering in the event detection pipeline.
Finds clusters without event_summary (or with non-English summary),
batch-summarizes via OpenRouter, writes back to DB.

Env:
    ESTWARDEN_API_URL, ESTWARDEN_API_KEY — Data API
    OPENROUTER_API_KEY — LLM
    LLM_MODEL — model (default: qwen/qwen3-235b-a22b-2507)
    DATABASE_URL — direct DB access for batch update
"""

import json
import os
import re
import sys
import urllib.request

DB_URL = os.environ.get("DATABASE_URL", "postgresql://estwarden:estwarden@postgres:5432/estwarden")
LLM_URL = "https://openrouter.ai/api/v1/chat/completions"
LLM_KEY = os.environ.get("OPENROUTER_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen/qwen3-235b-a22b-2507")
BATCH = 20
MAX_CLUSTERS = 80
CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")


def get_unsummarized(cur):
    cur.execute("""
        SELECT ec.id, ec.signal_count,
               array_agg(DISTINCT LEFT(s.title, 100) ORDER BY LEFT(s.title, 100))
                   FILTER (WHERE s.title IS NOT NULL AND s.title != '') as titles
        FROM event_clusters ec
        JOIN cluster_signals cs ON cs.cluster_id = ec.id
        JOIN signals s ON s.id = cs.signal_id
        WHERE ec.created_at > now() - interval '14 days'
          AND ec.signal_count >= 3
          AND (ec.event_summary IS NULL OR ec.event_summary = ''
               OR ec.event_summary ~ '[а-яА-ЯёЁ]')
          AND NOT (COALESCE(array_length(ec.regions, 1), 0) = 1 AND ec.regions[1] = 'global')
        GROUP BY ec.id
        ORDER BY ec.signal_count DESC
        LIMIT %s
    """ % MAX_CLUSTERS)
    return cur.fetchall()


def fallback_summary(titles):
    for title in titles or []:
        text = (title or "").strip()
        if text and not CYRILLIC_RE.search(text):
            if len(text) > 180:
                text = text[:177] + "..."
            return text
    return "Cluster of related security reports in monitored channels."


def normalize_summary(text, titles):
    if not isinstance(text, str):
        text = ""
    summary = (text or "").strip().strip('"')
    if not summary:
        return fallback_summary(titles)
    if len(summary) > 200:
        summary = summary[:197] + "..."
    if CYRILLIC_RE.search(summary):
        return fallback_summary(titles)
    return summary


def summarize_batch(clusters):
    fallback = {str(cid): fallback_summary(titles) for cid, _, titles in clusters}
    if not LLM_KEY:
        return fallback

    parts = []
    for cid, count, titles in clusters:
        sample = "\n".join(f"- {t}" for t in (titles or [])[:5])
        parts.append(f"CLUSTER {cid} ({count} signals):\n{sample}")

    prompt = (
        "Summarize each cluster in ONE clear English sentence (max 140 chars). "
        "Always write in English regardless of input language. Be factual and specific. "
        "Do not write words like 'cluster' or 'signals' in summaries. "
        'Return ONLY JSON: {"cluster_id": "summary", ...}\n\n'
        + "\n\n".join(parts)
    )

    req = urllib.request.Request(
        LLM_URL,
        data=json.dumps({
            "model": LLM_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 3000,
        }).encode(),
        headers={
            "Authorization": f"Bearer {LLM_KEY}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.load(resp)
        text = data["choices"][0]["message"]["content"]
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end])
            for key, value in parsed.items():
                fallback[str(key)] = normalize_summary(value, None)
            return fallback
    except Exception as e:
        print(f"LLM error: {e}", file=sys.stderr)
    return fallback


def main():
    try:
        import psycopg2
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
        import psycopg2

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    clusters = get_unsummarized(cur)
    print(f"Found {len(clusters)} clusters needing English summaries")

    if not clusters:
        cur.close()
        conn.close()
        return

    total = 0
    for i in range(0, len(clusters), BATCH):
        batch = clusters[i:i + BATCH]
        summaries = summarize_batch(batch)

        for cid, _, titles in batch:
            key = str(cid)
            summary = normalize_summary(summaries.get(key), titles)
            if not summary:
                continue
            cur.execute(
                "UPDATE event_clusters SET event_summary = %s WHERE id = %s",
                (summary, cid),
            )
            total += 1

        conn.commit()
        print(f"  Batch {i // BATCH + 1}: {len(batch)} summarized")

    print(f"Done: {total}/{len(clusters)} clusters summarized")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
