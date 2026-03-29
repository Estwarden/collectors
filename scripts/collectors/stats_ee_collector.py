#!/usr/bin/env python3
"""Statistics Estonia collector — CPI, unemployment, wages."""
import json, os, sys, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

INDICATORS = [
    ("CPI", "https://andmed.stat.ee/api/v1/en/stat/IA001"),
    ("Unemployment", "https://andmed.stat.ee/api/v1/en/stat/TT466"),
]

def main():
    # Using flat API
    signals = []
    for name, url in INDICATORS:
        try:
            # stat.ee uses POST with JSON query for specific data
            req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            title = data.get("title", name)
            signals.append({
                "source_type": "stats",
                "source_id": f"statee:{name.lower()}:{datetime.now(timezone.utc).strftime('%Y-%m')}",
                "title": f"Statistics Estonia: {title}",
                "content": json.dumps(data.get("variables", []))[:2000],
                "url": f"https://andmed.stat.ee/en/stat/{name}",
                "published_at": datetime.now(timezone.utc).isoformat(),
                "metadata": {"indicator": name, "source": "stat.ee"},
            })
        except Exception as e:
            print(f"  {name}: {e}", file=sys.stderr)
    if signals:
        result = ingest_signals(signals)
        print(f"Stats.ee: {result['inserted']} indicators")

if __name__ == "__main__": main()
