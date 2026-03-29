#!/usr/bin/env python3
"""Perplexity OSINT collector — AI-powered Baltic security research queries."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

QUERIES = [
    "Latest Russian military movements near Baltic states this week",
    "Recent NATO exercises in Estonia Latvia Lithuania",
    "Baltic Sea security incidents this week shipping cables",
    "Russian disinformation campaigns targeting Estonia 2026",
    "Kaliningrad military base activity this month",
]

def main():
    # Using flat API
    api_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        print("PERPLEXITY_API_KEY not set"); return

    signals = []
    for query in QUERIES:
        try:
            body = json.dumps({
                "model": "sonar",
                "messages": [{"role": "user", "content": query}],
            }).encode()
            req = urllib.request.Request(
                "https://api.perplexity.ai/chat/completions",
                data=body,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
            content = data["choices"][0]["message"]["content"]
            qhash = hashlib.sha256(query.encode()).hexdigest()[:8]
            signals.append({
                "source_type": "osint_perplexity",
                "source_id": f"pplx:{qhash}:{datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                "title": query,
                "content": content[:5000],
                "published_at": datetime.now(timezone.utc).isoformat(),
                "metadata": {"query": query, "model": "sonar"},
            })
        except Exception as e:
            print(f"  Query failed: {e}", file=sys.stderr)
    if signals:
        result = ingest_signals(signals)
        print(f"Perplexity: {result['inserted']} research results")

if __name__ == "__main__": main()
