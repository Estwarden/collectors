#!/usr/bin/env python3
"""USGS Earthquake collector — seismic events in/near Baltic region.
Natural events + potential underground test detection."""
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

# Extended Baltic + Fennoscandia + NW Russia
BBOX = {"minlatitude": 53, "maxlatitude": 72, "minlongitude": 18, "maxlongitude": 45}

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    params = "&".join(f"{k}={v}" for k, v in {**BBOX, "starttime": start, "format": "geojson", "orderby": "time"}.items())
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?{params}"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"USGS: {e}"); return

    signals = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        geo = feat.get("geometry", {})
        coords = geo.get("coordinates", [0, 0, 0])
        mag = props.get("mag", 0) or 0
        place = props.get("place", "Unknown")
        etime = props.get("time", 0)
        eid = feat.get("id", "")

        sev = "HIGH" if mag >= 4.0 else "MODERATE" if mag >= 2.5 else "LOW"
        ts = datetime.fromtimestamp(etime / 1000, tz=timezone.utc).isoformat() if etime else now.isoformat()

        signals.append({
            "source_type": "seismic",
            "source_id": f"usgs:{eid}",
            "title": f"M{mag:.1f} earthquake: {place}",
            "content": f"Magnitude {mag:.1f} at depth {coords[2]:.1f}km. {place}. Type: {props.get('type', 'earthquake')}",
            "url": props.get("url", ""),
            "published_at": ts,
            "severity": sev,
            "latitude": coords[1],
            "longitude": coords[0],
            "metadata": {"magnitude": mag, "depth_km": coords[2], "type": props.get("type", ""), "place": place},
        })

    if signals:
        result = client.ingest_signals(signals)
        print(f"Seismic: {result['inserted']} events (7d)")
    else:
        print("Seismic: no events in Baltic region")

if __name__ == "__main__": main()
