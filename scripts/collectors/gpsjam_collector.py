#!/usr/bin/env python3
"""GPS jamming collector. Fetches gpsjam.org daily H3 hex CSV, decodes to lat/lng,
filters to Baltic/Nordic zones, and computes interference rates per zone."""
import csv, io, os, sys, urllib.request, hashlib
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient
import h3

ZONES = {
    "Estonia":       (57.5, 59.7, 21.8, 28.2),
    "Latvia":        (55.7, 58.1, 20.9, 28.2),
    "Lithuania":     (53.9, 56.5, 20.9, 26.8),
    "Kaliningrad":   (54.2, 55.4, 19.5, 22.8),
    "Finland_S":     (59.7, 61.5, 22.0, 30.5),
    "Poland_NE":     (53.5, 54.8, 19.5, 23.5),
    "Pskov":         (56.0, 59.0, 27.5, 32.0),
    "StPete":        (59.5, 60.5, 29.0, 31.5),
    "Belarus_N":     (53.5, 56.5, 23.0, 30.5),
    "Gulf-Finland":  (59.0, 60.5, 22.0, 30.0),
    "Baltic-Sea":    (54.0, 59.0, 13.0, 22.0),
}

def severity(rate):
    if rate > 0.3:  return "HIGH"
    if rate > 0.1:  return "MODERATE"
    return "LOW"

def main():
    client = EstWardenClient()
    text = None
    date = None

    # Try last 3 days (gpsjam has 1-2 day delay)
    for days_ago in range(1, 4):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        url = f"https://gpsjam.org/data/{d}-h3_4.csv"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                text = r.read().decode()
            if len(text) > 100:
                date = d
                break
        except:
            continue

    if not text or not date:
        print("GPSJam: no data available")
        return

    # Parse CSV: columns are hex, count_good_aircraft, count_bad_aircraft
    zone_data = {z: {"count": 0, "bad": 0, "good": 0, "total_rate": 0.0} for z in ZONES}
    total_hexes = 0

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        try:
            hex_id = row.get("hex", "")
            good = int(row.get("count_good_aircraft", 0))
            bad = int(row.get("count_bad_aircraft", 0))
        except (ValueError, TypeError):
            continue

        if good + bad < 5:  # Skip hexes with too few observations
            continue

        # Decode H3 hex to lat/lng
        try:
            lat, lon = h3.cell_to_latlng(hex_id)
        except Exception:
            continue

        total_hexes += 1
        rate = bad / (good + bad) if (good + bad) > 0 else 0

        # Check which zone this hex falls in
        for zone, (la_min, la_max, lo_min, lo_max) in ZONES.items():
            if la_min <= lat <= la_max and lo_min <= lon <= lo_max:
                zone_data[zone]["count"] += 1
                zone_data[zone]["bad"] += bad
                zone_data[zone]["good"] += good
                zone_data[zone]["total_rate"] += rate
                break  # Each hex belongs to at most one zone

    signals = []
    for zone, d in zone_data.items():
        if d["count"] == 0:
            continue
        avg_rate = d["total_rate"] / d["count"]
        bb = ZONES[zone]
        center_lat = (bb[0] + bb[1]) / 2
        center_lon = (bb[2] + bb[3]) / 2

        sev = severity(avg_rate)
        signals.append({
            "source_type": "gpsjam",
            "source_id": f"gpsjam:{zone}:{date}",
            "title": f"GPS jamming {zone}: {sev} ({avg_rate:.1%})",
            "content": (f"GPS interference in {zone} on {date}: "
                       f"avg rate {avg_rate:.1%} from {d['count']} H3 hexes. "
                       f"{d['bad']} bad / {d['good']+d['bad']} total aircraft observations."),
            "published_at": f"{date}T12:00:00Z",
            "severity": sev,
            "latitude": center_lat,
            "longitude": center_lon,
            "metadata": {
                "zone": zone, "hex_count": d["count"],
                "avg_rate": round(avg_rate, 4),
                "bad_aircraft": d["bad"], "good_aircraft": d["good"],
                "date": date,
            },
        })

    if signals:
        result = client.ingest_signals(signals)
        high = sum(1 for s in signals if s["severity"] == "HIGH")
        print(f"GPSJam: {result.get('inserted', 0)} zones (date={date}, "
              f"{high} HIGH, {len(signals)} total, {total_hexes} hexes scanned)")
    else:
        print(f"GPSJam: 0 zones with data (date={date}, {total_hexes} hexes scanned)")

if __name__ == "__main__":
    main()
