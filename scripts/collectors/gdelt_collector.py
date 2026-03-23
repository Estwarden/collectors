#!/usr/bin/env python3
"""GDELT news collector. Fetches military-relevant news for Baltic/Russian military zones.

Queries GDELT for military activity near tracked sites, assigns site coordinates
to signals for intelligence map display. Filters for actual military content.
"""
import json, os, sys, urllib.request, urllib.parse, hashlib, time, yaml
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

# Strict military keywords — reduces noise
MIL_KEYWORDS = {
    "troops", "exercise", "deployment", "brigade", "regiment", "fleet",
    "submarine", "missile", "drone", "uav", "radar", "artillery",
    "warship", "bomber", "fighter", "airborne", "garrison", "mobilization",
    "nato", "nuclear", "anti-aircraft", "air defense", "naval base",
    "military base", "air base", "ground forces", "tank", "armored",
}

def is_relevant(title):
    """Strict filter: title must contain military activity terms."""
    tl = title.lower()
    return sum(1 for k in MIL_KEYWORDS if k in tl) >= 1

def load_sites(path="/dags/config/military_sites.yaml"):
    try:
        with open(path) as f:
            return {s["id"]: s for s in yaml.safe_load(f).get("sites", [])}
    except:
        return {}

# Consolidated into 2 broad queries instead of 8 per-site queries
# to stay under GDELT rate limits (~6 req/min).
QUERIES = [
    ("baltic-mil", "Baltic military OR Kaliningrad troops OR Belarus military exercises"),
    ("russia-near", "Pskov airborne OR Kronstadt naval OR Murmansk fleet OR Leningrad military"),
]

def main():
    client = EstWardenClient()
    sites = load_sites()
    signals = []

    for site_id, query in QUERIES:
        site = sites.get(site_id, {})
        lat = site.get("lat")
        lon = site.get("lon")

        q = urllib.parse.quote(query)
        url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={q}&mode=artlist&maxrecords=15&format=json&timespan=2d"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"  {site_id}: rate limited, backing off 60s")
                time.sleep(60)
            else:
                print(f"  {site_id}: HTTP {e.code}")
                time.sleep(5)
            continue
        except Exception as e:
            print(f"  {site_id}: {e}")
            time.sleep(5)
            continue

        time.sleep(10)  # rate limit: ~6 queries/min max

        for art in data.get("articles", []):
            title = art.get("title", "")
            if not is_relevant(title):
                continue

            url_hash = hashlib.sha256(art["url"].encode()).hexdigest()[:16]
            seen = art.get("seendate", "")
            pub_at = seen[:19].replace(" ", "T") + "Z" if seen else None

            sig = {
                "source_type": "gdelt",
                "source_id": f"gdelt:{url_hash}",
                "title": title,  # Clean title, no [Site] prefix
                "content": title,
                "url": art.get("url", ""),
                "published_at": pub_at,
                "site_id": site_id,
                "metadata": {
                    "site": site_id,
                    "domain": art.get("domain", ""),
                    "language": art.get("language", ""),
                },
            }
            if lat and lon:
                sig["latitude"] = lat
                sig["longitude"] = lon
            signals.append(sig)
        time.sleep(12)

    if signals:
        result = client.ingest_signals(signals[:200])
        print(f"GDELT: {result.get('inserted', 0)} articles from {len(QUERIES)} site queries")
    else:
        print("GDELT: 0 relevant articles found")

if __name__ == "__main__":
    main()
