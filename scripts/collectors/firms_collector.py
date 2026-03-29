#!/usr/bin/env python3
"""NASA FIRMS thermal anomaly collector. Fetches fire hotspots, filters to Baltic/Russia region."""
import csv, io, os, sys, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

LAT_MIN, LAT_MAX = 50.0, 70.0
LON_MIN, LON_MAX = 20.0, 45.0

def main():
    # Using flat API
    map_key = os.environ.get("FIRMS_MAP_KEY", "")
    if not map_key:
        print("FIRMS_MAP_KEY not set"); return

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/VIIRS_SNPP_NRT/world/2/{date}"
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    signals = []
    for row in reader:
        try:
            lat = float(row.get("latitude", 0))
            lon = float(row.get("longitude", 0))
        except (ValueError, TypeError):
            continue
        if lat < LAT_MIN or lat > LAT_MAX or lon < LON_MIN or lon > LON_MAX:
            continue

        acq = row.get("acq_date", date)
        acq_time = row.get("acq_time", "0000")
        frp = row.get("frp", "0")
        conf = row.get("confidence", "nominal")
        signals.append({
            "source_type": "firms",
            "source_id": f"firms:{lat:.4f}:{lon:.4f}:{acq}:{acq_time}",
            "title": f"VIIRS hotspot ({conf}) FRP={frp}",
            "content": f"Thermal anomaly at {lat:.4f},{lon:.4f}. FRP={frp} MW. Confidence: {conf}",
            "published_at": f"{acq}T{acq_time[:2]}:{acq_time[2:]}:00Z",
            "latitude": lat, "longitude": lon,
            "severity": "HIGH" if float(frp or 0) > 50 else "MODERATE" if float(frp or 0) > 10 else "LOW",
            "metadata": {"frp": frp, "confidence": conf, "satellite": "VIIRS_SNPP"},
        })

    if signals:
        result = ingest_signals(signals[:500])
        print(f"FIRMS: {result['inserted']} new from {len(signals)} hotspots")
    else:
        print("FIRMS: no hotspots in region")

if __name__ == "__main__":
    main()
