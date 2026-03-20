#!/usr/bin/env python3
"""Narrative amplification detector for EstWarden.

Checks if narratives from Russian state/proxy sources appear in
untrusted/commentator channels within a time window. Uses keyword
matching against the narrative taxonomy.

Phase 1: Taxonomy keyword matching (no LLM, no embeddings)
Phase 2 (future): Semantic embedding similarity

Usage:
    python3 detect_amplification.py [--hours 24] [--window 72]

Environment:
    ESTWARDEN_API_URL — Data API base URL
    ESTWARDEN_API_KEY — Pipeline API key
    DATABASE_URL      — Postgres for writing detections
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

import yaml

# ── Taxonomy loading ──

def load_taxonomy(path):
    """Load narrative taxonomy YAML. Returns list of narrative dicts."""
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("narratives", [])


def compile_patterns(narratives):
    """Pre-compile keyword regexes for each narrative across all languages."""
    compiled = []
    for n in narratives:
        patterns = []
        for lang_keywords in n.get("keywords", {}).values():
            for kw in lang_keywords:
                try:
                    patterns.append(re.compile(re.escape(kw), re.IGNORECASE))
                except re.error:
                    pass
        compiled.append({
            "id": n["id"],
            "theme": n["theme"],
            "patterns": patterns,
        })
    return compiled


def match_narratives(text, compiled_narratives):
    """Return list of matched narrative IDs for a given text."""
    if not text:
        return []
    matches = []
    for n in compiled_narratives:
        for p in n["patterns"]:
            if p.search(text):
                matches.append(n["id"])
                break  # one match per narrative is enough
    return matches


# ── Source classification ──

RU_CATEGORIES = {"ru_state", "ru_proxy"}
WATCH_CATEGORIES = {"untrusted", "unverified_anonymous", "unverified_commentator",
                    "unverified_media", "unverified_independent"}


def main():
    parser = argparse.ArgumentParser(description="Narrative amplification detector")
    parser.add_argument("--hours", type=int, default=24, help="Look back N hours for signals")
    parser.add_argument("--window", type=int, default=72, help="Amplification window in hours")
    parser.add_argument("--taxonomy", default="/dags/config/narrative_taxonomy.yaml")
    args = parser.parse_args()

    narratives = load_taxonomy(args.taxonomy)
    compiled = compile_patterns(narratives)
    print(f"Loaded {len(narratives)} narrative templates")

    client = EstWardenClient()

    # Fetch recent signals from both source types
    all_signals = []
    for st in ["telegram_channel", "youtube_transcript", "rss", "rss_security"]:
        try:
            sigs = client.query_signals(source_type=st, since=f"{args.hours}h", limit=5000)
            if sigs:
                all_signals.extend(sigs)
        except Exception:
            pass

    if not all_signals:
        print("No signals to analyze")
        return

    print(f"Analyzing {len(all_signals)} signals")

    # Classify signals by source category
    ru_signals = []     # from ru_state/ru_proxy
    watch_signals = []  # from untrusted/commentator channels

    for sig in all_signals:
        meta = sig.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        category = meta.get("category", "")
        if category in RU_CATEGORIES:
            ru_signals.append(sig)
        elif category in WATCH_CATEGORIES:
            watch_signals.append(sig)

    print(f"  RU origin signals: {len(ru_signals)}")
    print(f"  Watch target signals: {len(watch_signals)}")

    # Match narratives in RU signals (origin detection)
    ru_narrative_times = defaultdict(list)  # narrative_id → list of (timestamp, channel, text_snippet)
    for sig in ru_signals:
        content = (sig.get("content", "") or "") + " " + (sig.get("title", "") or "")
        matches = match_narratives(content, compiled)
        meta = sig.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        for nid in matches:
            ru_narrative_times[nid].append({
                "time": sig.get("published_at", ""),
                "channel": meta.get("channel", "unknown"),
                "snippet": content[:200],
            })

    if not ru_narrative_times:
        print("No RU narrative matches found")
        return

    print(f"\nRU origin narratives detected: {len(ru_narrative_times)}")
    for nid, items in ru_narrative_times.items():
        print(f"  {nid}: {len(items)} posts")

    # Match narratives in watch signals (amplification detection)
    amplifications = []
    for sig in watch_signals:
        content = (sig.get("content", "") or "") + " " + (sig.get("title", "") or "")
        matches = match_narratives(content, compiled)
        if not matches:
            continue

        meta = sig.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        for nid in matches:
            if nid not in ru_narrative_times:
                continue  # narrative wasn't in RU sources recently

            amplifications.append({
                "narrative_id": nid,
                "narrative_theme": next((n["theme"] for n in narratives if n["id"] == nid), ""),
                "amplifier_channel": meta.get("channel", "unknown"),
                "amplifier_name": meta.get("channel_name", ""),
                "amplifier_category": meta.get("category", ""),
                "signal_url": sig.get("url", ""),
                "signal_title": sig.get("title", ""),
                "signal_time": sig.get("published_at", ""),
                "ru_origin_count": len(ru_narrative_times[nid]),
                "ru_first_channel": ru_narrative_times[nid][0]["channel"] if ru_narrative_times[nid] else "",
            })

    print(f"\n{'='*60}")
    print(f"AMPLIFICATION DETECTIONS: {len(amplifications)}")
    print(f"{'='*60}")

    if not amplifications:
        print("No amplification detected")
        return

    # Group by narrative
    by_narrative = defaultdict(list)
    for a in amplifications:
        by_narrative[a["narrative_id"]].append(a)

    for nid, items in by_narrative.items():
        theme = items[0]["narrative_theme"]
        channels = set(a["amplifier_channel"] for a in items)
        print(f"\n  📡 {theme}")
        print(f"     RU origin: {items[0]['ru_origin_count']} posts, first in @{items[0]['ru_first_channel']}")
        print(f"     Amplified by {len(channels)} channels: {', '.join(sorted(channels))}")
        for a in items[:3]:
            print(f"       → @{a['amplifier_channel']}: {a['signal_title'][:80]}")

    # Write to database if available
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return

    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS narrative_amplifications (
                id              serial PRIMARY KEY,
                narrative_id    text NOT NULL,
                narrative_theme text,
                amplifier_handle text NOT NULL,
                amplifier_name  text,
                amplifier_category text,
                signal_url      text,
                signal_title    text,
                signal_time     timestamptz,
                ru_origin_count int,
                detected_at     timestamptz DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_narr_amp_detected ON narrative_amplifications (detected_at);
            CREATE INDEX IF NOT EXISTS idx_narr_amp_handle ON narrative_amplifications (amplifier_handle);
        """)

        inserted = 0
        for a in amplifications:
            cur.execute("""
                INSERT INTO narrative_amplifications
                    (narrative_id, narrative_theme, amplifier_handle, amplifier_name,
                     amplifier_category, signal_url, signal_title, signal_time, ru_origin_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (a["narrative_id"], a["narrative_theme"], a["amplifier_channel"],
                  a["amplifier_name"], a["amplifier_category"], a["signal_url"],
                  a["signal_title"], a["signal_time"] or None, a["ru_origin_count"]))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()
        print(f"\nStored {inserted} amplification events in database")
    except Exception as e:
        print(f"DB write error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
