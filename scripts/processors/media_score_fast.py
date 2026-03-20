#!/usr/bin/env python3
"""Fast media source scoring — keyword-based, no LLM required.

Scores two Yudkowsky rationality metrics from post content:
  - Evidence quality: URL citations, "according to" phrases, data references
  - Uncertainty expression: hedging vs. certainty language ratio

Reads recent signals from the Data API, scores each post,
updates rolling averages in the media_sources table.

Usage:
    python3 media_score_fast.py [--hours 24]

Environment:
    ESTWARDEN_API_URL — Data API base URL
    ESTWARDEN_API_KEY — Pipeline API key
    DATABASE_URL      — Direct Postgres connection for updates
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

# ── Evidence scoring ──

# Patterns that indicate evidence citation
EVIDENCE_PATTERNS = [
    re.compile(r'https?://\S+', re.IGNORECASE),                    # URL
    re.compile(r'according\s+to\b', re.IGNORECASE),                # EN
    re.compile(r'за\s+даними\b', re.IGNORECASE),                   # UK "according to data"
    re.compile(r'за\s+словами\b', re.IGNORECASE),                  # UK "according to words of"
    re.compile(r'повідомляє\b', re.IGNORECASE),                    # UK "reports"
    re.compile(r'як\s+повідомляє\b', re.IGNORECASE),               # UK "as reported by"
    re.compile(r'джерело\b', re.IGNORECASE),                       # UK "source"
    re.compile(r'по\s+данным\b', re.IGNORECASE),                   # RU "according to data"
    re.compile(r'сообщает\b', re.IGNORECASE),                      # RU "reports"
    re.compile(r'источник\b', re.IGNORECASE),                      # RU "source"
    re.compile(r'cited\b|citing\b', re.IGNORECASE),                # EN
    re.compile(r'report\s+by\b|data\s+from\b', re.IGNORECASE),    # EN
    re.compile(r'\b\d{1,3}[.,]\d+\s*%', re.IGNORECASE),           # percentage data
    re.compile(r'документ\b|document\b', re.IGNORECASE),           # document reference
]

def score_evidence(text):
    """Score evidence quality 0.0-1.0 based on citation patterns."""
    if not text or len(text) < 20:
        return 0.0
    matches = sum(1 for p in EVIDENCE_PATTERNS if p.search(text))
    # Normalize: 0 matches = 0.0, 4+ matches = 1.0
    return min(matches / 4.0, 1.0)


# ── Uncertainty scoring ──

# Hedging language (expressing appropriate uncertainty)
HEDGE_PATTERNS = [
    re.compile(r'\breportedly\b|\ballegedly\b|\bpurportedly\b', re.IGNORECASE),
    re.compile(r'\bif\s+confirmed\b|\bunconfirmed\b|\bunverified\b', re.IGNORECASE),
    re.compile(r'\bpossibly\b|\bprobably\b|\blikely\b|\bapparently\b', re.IGNORECASE),
    re.compile(r'\bmay\b|\bmight\b|\bcould\b', re.IGNORECASE),
    re.compile(r'\bнібито\b|\bімовірно\b|\bможливо\b', re.IGNORECASE),           # UK
    re.compile(r'\bза\s+непідтвердженими\b', re.IGNORECASE),                       # UK "unconfirmed"
    re.compile(r'\bкак\s+сообщается\b|\bвероятно\b|\bвозможно\b', re.IGNORECASE), # RU
    re.compile(r'\bпредположительно\b|\bякобы\b', re.IGNORECASE),                 # RU "allegedly"
    re.compile(r'\bestimated\b|\bapproximately\b|\babout\b', re.IGNORECASE),
]

# Certainty language (overconfident assertions)
CERTAINTY_PATTERNS = [
    re.compile(r'\bBREAKING\b|\bURGENT\b|\bTERMINOVO\b', re.IGNORECASE),
    re.compile(r'\bconfirmed\b|\b100%\b|\bguaranteed\b', re.IGNORECASE),
    re.compile(r'\bwill\s+definitely\b|\bwill\s+certainly\b', re.IGNORECASE),
    re.compile(r'\bБЛИСКАВКА\b|\bТЕРМІНОВО\b|\bСРОЧНО\b', re.IGNORECASE),      # UK/RU "URGENT"
    re.compile(r'\bточно\b|\bоднозначно\b|\bгарантовано\b', re.IGNORECASE),       # UK/RU "definitely"
    re.compile(r'!!!|⚡⚡⚡|🔴🔴🔴', re.IGNORECASE),                                # emoji urgency
    re.compile(r'[A-ZА-ЯІЇЄҐ]{10,}', re.IGNORECASE),                              # long ALL-CAPS
]

def score_uncertainty(text):
    """Score uncertainty expression 0.0-1.0. High = good (expresses uncertainty appropriately)."""
    if not text or len(text) < 20:
        return 0.5  # neutral for very short posts
    hedges = sum(1 for p in HEDGE_PATTERNS if p.search(text))
    certainties = sum(1 for p in CERTAINTY_PATTERNS if p.search(text))
    total = hedges + certainties
    if total == 0:
        return 0.5  # neutral — no strong signals either way
    # Ratio of hedging to total confidence markers
    return min(hedges / total, 1.0)


# ── Aggregation ──

def score_to_level(avg):
    """Convert 0.0-1.0 average to high/medium/low."""
    if avg >= 0.6:
        return "high"
    elif avg >= 0.3:
        return "medium"
    else:
        return "low"


def main():
    parser = argparse.ArgumentParser(description="Fast media scoring")
    parser.add_argument("--hours", type=int, default=24, help="Score signals from last N hours")
    args = parser.parse_args()

    client = EstWardenClient()

    # Fetch recent telegram + youtube signals
    channel_scores = defaultdict(lambda: {"evidence": [], "uncertainty": []})

    for source_type in ["telegram_channel", "youtube_transcript"]:
        try:
            signals = client.query_signals(source_type=source_type, since=f"{args.hours}h", limit=2000)
        except Exception as e:
            print(f"Query error for {source_type}: {e}", file=sys.stderr)
            continue

        if not signals:
            continue

        for sig in signals:
            content = sig.get("content", "") or sig.get("title", "")
            meta = sig.get("metadata", {})
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}

            handle = meta.get("channel", "")
            if not handle:
                continue

            ev = score_evidence(content)
            unc = score_uncertainty(content)
            channel_scores[handle]["evidence"].append(ev)
            channel_scores[handle]["uncertainty"].append(unc)

    if not channel_scores:
        print("No signals to score")
        return

    # Connect to Postgres directly for updates
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set, printing scores only", file=sys.stderr)
        for handle, scores in sorted(channel_scores.items()):
            ev_avg = sum(scores["evidence"]) / len(scores["evidence"])
            unc_avg = sum(scores["uncertainty"]) / len(scores["uncertainty"])
            print(f"  {handle:25s} evidence={score_to_level(ev_avg):6s} ({ev_avg:.2f})  uncertainty={score_to_level(unc_avg):6s} ({unc_avg:.2f})  posts={len(scores['evidence'])}")
        return

    try:
        import psycopg2
    except ImportError:
        # Fallback: use urllib to talk to ingest API (no direct DB)
        print("psycopg2 not available, printing scores", file=sys.stderr)
        for handle, scores in sorted(channel_scores.items()):
            ev_avg = sum(scores["evidence"]) / len(scores["evidence"])
            unc_avg = sum(scores["uncertainty"]) / len(scores["uncertainty"])
            print(f"  {handle:25s} evidence={score_to_level(ev_avg):6s} uncertainty={score_to_level(unc_avg):6s} posts={len(scores['evidence'])}")
        return

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    updated = 0

    for handle, scores in channel_scores.items():
        ev_avg = sum(scores["evidence"]) / len(scores["evidence"])
        unc_avg = sum(scores["uncertainty"]) / len(scores["uncertainty"])
        ev_level = score_to_level(ev_avg)
        unc_level = score_to_level(unc_avg)

        cur.execute("""
            UPDATE media_sources
            SET r_evidence = %s, r_uncertainty = %s,
                signal_count = signal_count + %s,
                last_collected = now(), updated_at = now()
            WHERE handle = %s
        """, (ev_level, unc_level, len(scores["evidence"]), handle))
        if cur.rowcount > 0:
            updated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Updated {updated} media sources from {sum(len(s['evidence']) for s in channel_scores.values())} posts")


if __name__ == "__main__":
    main()
