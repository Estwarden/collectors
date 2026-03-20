#!/usr/bin/env python3
"""ACLED conflict events collector. Fetches European conflict data."""
import json, os, sys, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    params = urllib.parse.urlencode({"event_date": f"{from_date}|{now.strftime('%Y-%m-%d')}",
                                     "event_date_where": "BETWEEN", "region": "1", "limit": "500"})
    url = f"https://api.acleddata.com/acled/read?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())

    signals = []
    for ev in data.get("data", []):
        lat = float(ev.get("latitude", 0) or 0)
        lon = float(ev.get("longitude", 0) or 0)
        signals.append({
            "source_type": "acled", "source_id": f"acled:{ev.get('data_id', '')}",
            "title": f"ACLED: {ev.get('event_type', '')} in {ev.get('country', '')}",
            "content": f"{ev.get('event_type', '')} / {ev.get('sub_event_type', '')} at {ev.get('location', '')}, {ev.get('country', '')}. {ev.get('notes', '')[:300]}",
            "url": ev.get("source_url", ""), "published_at": ev.get("event_date", ""),
            "latitude": lat if lat else None, "longitude": lon if lon else None,
            "metadata": {"event_type": ev.get("event_type"), "sub_type": ev.get("sub_event_type"),
                         "country": ev.get("country"), "fatalities": ev.get("fatalities", 0)},
        })
    if signals:
        result = client.ingest_signals(signals[:500])
        print(f"ACLED: {result['inserted']} events")

if __name__ == "__main__": main()
