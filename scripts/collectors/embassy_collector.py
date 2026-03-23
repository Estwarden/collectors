#!/usr/bin/env python3
"""Embassy travel advisory collector — US, UK, DE, FI, SE for Baltic states."""
import json, os, sys, urllib.request, re, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

ADVISORIES = [
    ("US", "EE", "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/estonia-travel-advisory.html"),
    ("US", "LV", "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/latvia-travel-advisory.html"),
    ("US", "LT", "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/lithuania-travel-advisory.html"),
    ("UK", "EE", "https://www.gov.uk/foreign-travel-advice/estonia"),
    ("UK", "LV", "https://www.gov.uk/foreign-travel-advice/latvia"),
    ("UK", "LT", "https://www.gov.uk/foreign-travel-advice/lithuania"),
    # FI um.fi blocks automated requests (Cloudflare), removed
]

def main():
    client = EstWardenClient()
    signals = []
    for country, target, url in ADVISORIES:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; EstWarden/1.0)"})
            with urllib.request.urlopen(req, timeout=15) as r:
                html = r.read().decode("utf-8", errors="replace")
            text = re.sub(r'<[^>]+>', ' ', html)
            text = re.sub(r'\s+', ' ', text).strip()[:1000]
            content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
            signals.append({
                "source_type": "embassy",
                "source_id": f"embassy:{country}:{target}:{content_hash}",
                "title": f"{country} travel advisory for {target}",
                "content": text[:500],
                "url": url,
                "published_at": datetime.now(timezone.utc).isoformat(),
                "metadata": {"issuing_country": country, "target_country": target},
            })
        except Exception as e:
            print(f"  {country}→{target}: {e}", file=sys.stderr)
    if signals:
        result = client.ingest_signals(signals)
        print(f"Embassy: {result.get('inserted', 0)} advisories")
    else:
        print("Embassy: 0 advisories fetched")

if __name__ == "__main__": main()
