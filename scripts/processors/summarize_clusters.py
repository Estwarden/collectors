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
import sys
import urllib.request

DB_URL = os.environ.get("DATABASE_URL", "postgresql://estwarden:estwarden@postgres:5432/estwarden")
LLM_URL = "https://openrouter.ai/api/v1/chat/completions"
LLM_KEY = os.environ.get("OPENROUTER_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "qwen/qwen3-235b-a22b-2507")
BATCH = 15
MAX_CLUSTERS = 50


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
        GROUP BY ec.id
        ORDER BY ec.signal_count DESC
        LIMIT %s
    """ % MAX_CLUSTERS)
    return cur.fetchall()


def summarize_batch(clusters):
    if not LLM_KEY:
        return {}

    parts = []
    for cid, count, titles in clusters:
        sample = "\n".join(f"- {t}" for t in (titles or [])[:5])
        parts.append(f"CLUSTER {cid} ({count} signals):\n{sample}")

    prompt = (
        "Summarize each cluster in ONE English sentence (max 100 chars). "
        "Always write in English regardless of input language. Be factual. "
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
            return json.loads(text[start:end])
    except Exception as e:
        print(f"LLM error: {e}", file=sys.stderr)
    return {}


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

        for cid_str, summary in summaries.items():
            summary = summary.strip()[:200]
            if summary:
                cur.execute(
                    "UPDATE event_clusters SET event_summary = %s WHERE id = %s",
                    (summary, int(cid_str)),
                )
                total += 1

        conn.commit()
        print(f"  Batch {i // BATCH + 1}: {len(summaries)} summarized")

    print(f"Done: {total}/{len(clusters)} clusters summarized")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
