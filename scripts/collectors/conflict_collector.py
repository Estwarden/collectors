#!/usr/bin/env python3
"""Conflict event collector — aggregates from multiple sources as ACLED API is down.
Uses GDELT GKG for protest/conflict events + Uppsala UCDP API."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals
from lib.ua import random_ua

BALTIC_COUNTRIES = ["Estonia", "Latvia", "Lithuania", "Russia", "Belarus", "Ukraine", "Finland", "Poland"]

def fetch_ucdp():
    """Uppsala Conflict Data Program — academic conflict event database."""
    now = datetime.now(timezone.utc)
    year = now.year
    signals = []
    url = f"https://ucdpapi.pcr.uu.se/api/gedevents/25.0.1?pagesize=100&Country={','.join(['Russia','Ukraine','Belarus'])}&StartDate={year}-01-01"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": random_ua()})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        for ev in data.get("Result", []):
            eid = ev.get("id", "")
            signals.append({
                "source_type": "conflict",
                "source_id": f"ucdp:{eid}",
                "title": f"UCDP: {ev.get('side_a', '?')} vs {ev.get('side_b', '?')} ({ev.get('country', '?')})",
                "content": f"Conflict event in {ev.get('country', '?')}, {ev.get('region', '?')}. "
                           f"Deaths: {ev.get('best', 0)}. Source: {ev.get('source_article', '')}",
                "url": ev.get("source_article", ""),
                "published_at": ev.get("date_start", now.isoformat()),
                "severity": "HIGH" if (ev.get("best", 0) or 0) > 10 else "MODERATE",
                "latitude": float(ev["latitude"]) if ev.get("latitude") else None,
                "longitude": float(ev["longitude"]) if ev.get("longitude") else None,
                "metadata": {"source": "ucdp", "deaths": ev.get("best", 0), "country": ev.get("country", "")},
            })
    except Exception as e:
        print(f"  UCDP: {e}", file=sys.stderr)
    return signals

def fetch_crisis_group():
    """International Crisis Group — conflict tracker RSS."""
    signals = []
    try:
        import feedparser
        feed = feedparser.parse("https://www.crisisgroup.org/rss.xml")
        keywords = {"russia", "ukraine", "belarus", "baltic", "nato", "estonia", "latvia", "lithuania", "europe"}
        for entry in feed.entries[:30]:
            title = entry.get("title", "").lower()
            summary = entry.get("summary", "").lower()
            if any(kw in title or kw in summary for kw in keywords):
                sid = hashlib.sha256(entry.get("link", "").encode()).hexdigest()[:16]
                signals.append({
                    "source_type": "conflict",
                    "source_id": f"icg:{sid}",
                    "title": entry.get("title", ""),
                    "content": entry.get("summary", "")[:2000],
                    "url": entry.get("link", ""),
                    "published_at": entry.get("published", datetime.now(timezone.utc).isoformat()),
                    "metadata": {"source": "crisis_group"},
                })
    except Exception as e:
        print(f"  CrisisGroup: {e}", file=sys.stderr)
    return signals

def main():
    # Using flat API
    signals = fetch_ucdp() + fetch_crisis_group()
    if signals:
        result = ingest_signals(signals)
        print(f"Conflict: {result['inserted']} events from UCDP + Crisis Group")
    else:
        print("Conflict: no new events")

if __name__ == "__main__": main()
