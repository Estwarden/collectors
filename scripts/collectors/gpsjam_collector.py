#!/usr/bin/env python3
"""GPS jamming collector. Fetches gpsjam.org daily H3 hex CSV, filters to Baltic zones."""
import csv, io, os, sys, urllib.request, hashlib
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

ZONES = {
    "Estonia":     (57.5, 59.7, 21.8, 28.2),
    "Latvia":      (55.7, 58.1, 20.9, 28.2),
    "Lithuania":   (53.9, 56.5, 20.9, 26.8),
    "Kaliningrad": (54.2, 55.4, 19.5, 22.8),
    "Finland_S":   (59.7, 61.5, 22.0, 30.5),
    "Pskov":       (56.0, 59.0, 27.5, 32.0),
    "StPete":      (59.5, 60.5, 29.0, 31.5),
    "Belarus_N":   (53.5, 56.5, 23.0, 30.5),
}

def severity(rate):
    if rate > 0.3: return "HIGH"
    if rate > 0.1: return "MODERATE"
    return "LOW"

def main():
    client = EstWardenClient()
    # Try last 3 days (gpsjam has 1-2 day delay)
    for days_ago in range(1, 4):
        date = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        url = f"https://gpsjam.org/data/{date}-h3_4.csv"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                text = r.read().decode()
            if len(text) > 100: break
        except: continue
    else:
        print("GPSJam: no data available"); return

    # Parse CSV, aggregate by zone
    zone_data = {z: {"count": 0, "total": 0} for z in ZONES}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        try:
            lat = float(row.get("lat", row.get("latitude", 0)))
            lon = float(row.get("lng", row.get("lon", row.get("longitude", 0))))
            rate = float(row.get("interference_rate", row.get("rate", 0)))
        except: continue
        for zone, (la_min, la_max, lo_min, lo_max) in ZONES.items():
            if la_min <= lat <= la_max and lo_min <= lon <= lo_max:
                zone_data[zone]["count"] += 1
                zone_data[zone]["total"] += rate

    signals = []
    for zone, d in zone_data.items():
        if d["count"] == 0: continue
        avg_rate = d["total"] / d["count"]
        bb = ZONES[zone]
        signals.append({
            "source_type": "gpsjam", "source_id": f"gpsjam:{zone}:{date}",
            "title": f"GPS jamming {zone}: {severity(avg_rate)} ({avg_rate:.1%})",
            "content": f"GPS interference in {zone} zone on {date}: avg rate {avg_rate:.1%} from {d['count']} hexes",
            "published_at": f"{date}T12:00:00Z", "severity": severity(avg_rate),
            "latitude": (bb[0]+bb[1])/2, "longitude": (bb[2]+bb[3])/2,
            "metadata": {"zone": zone, "hex_count": d["count"], "avg_rate": avg_rate, "date": date},
        })
    if signals:
        result = client.ingest_signals(signals)
        print(f"GPSJam: {result['inserted']} zones, date={date}")

if __name__ == "__main__": main()
