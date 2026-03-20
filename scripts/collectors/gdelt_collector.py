#!/usr/bin/env python3
"""GDELT news collector. Fetches military-relevant news near Baltic/Russian bases."""
import json, os, sys, urllib.request, urllib.parse, hashlib, time
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

SITES = [
    ("Kaliningrad", "Kaliningrad military"), ("Pskov", "Pskov airborne division"),
    ("Ostrov", "Ostrov air base Russia"), ("Luga", "Luga naval base Russia"),
    ("Baltiysk", "Baltiysk naval fleet"), ("Machulishchy", "Machulishchy air base Belarus"),
]
MIL_KEYWORDS = {"military", "army", "navy", "airbase", "missile", "troops", "exercise", "deployment", "brigade", "regiment", "fleet", "submarine", "nuclear", "drone", "UAV"}

def is_military(title):
    tl = title.lower()
    return any(k in tl for k in MIL_KEYWORDS)

def main():
    client = EstWardenClient()
    signals = []
    for site_name, query in SITES:
        q = urllib.parse.quote(query)
        url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={q}&mode=artlist&maxrecords=30&format=json&timespan=7d"
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                data = json.loads(r.read())
        except:
            time.sleep(15); continue  # GDELT rate-limits hard

        for art in data.get("articles", []):
            if not is_military(art.get("title", "")): continue
            url_hash = hashlib.sha256(art["url"].encode()).hexdigest()[:16]
            signals.append({
                "source_type": "gdelt", "source_id": f"gdelt:{url_hash}",
                "title": f"GDELT [{site_name}]: {art.get('title', '')}",
                "content": art.get("title", ""), "url": art.get("url", ""),
                "published_at": art.get("seendate", "")[:19].replace(" ", "T") + "Z" if art.get("seendate") else None,
                "metadata": {"site": site_name, "domain": art.get("domain", ""), "language": art.get("language", "")},
            })
        time.sleep(15)

    if signals:
        result = client.ingest_signals(signals[:500])
        print(f"GDELT: {result['inserted']} military articles from {len(SITES)} sites")

if __name__ == "__main__": main()
