#!/usr/bin/env python3
"""GPS jamming collector.

Uses gpsjam.org's manifest as the source of truth for the latest available daily
H3 dataset, then decodes the published hex CSV to Baltic/Nordic zone summaries.
"""
import csv
import io
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient
import h3

MANIFEST_URL = "https://gpsjam.org/data/manifest.csv"
DATA_URL_TMPL = "https://gpsjam.org/data/{date}-h3_4.csv"

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
    if rate > 0.3:
        return "HIGH"
    if rate > 0.1:
        return "MODERATE"
    return "LOW"


def fetch_manifest():
    req = urllib.request.Request(MANIFEST_URL, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode()
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        raise RuntimeError("empty GPSJam manifest")
    return rows


def fetch_dataset(date):
    url = DATA_URL_TMPL.format(date=date)
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode()


def main():
    client = EstWardenClient()

    try:
        manifest = fetch_manifest()
    except Exception as e:
        print(f"GPSJam: failed to load manifest: {e}", file=sys.stderr)
        sys.exit(1)

    latest = manifest[-1]
    date = latest.get("date", "")
    suspect = str(latest.get("suspect", "false")).lower() == "true"
    num_bad_hexes = int(latest.get("num_bad_aircraft_hexes") or 0)
    if not date:
        print("GPSJam: manifest missing latest date", file=sys.stderr)
        sys.exit(1)

    try:
        text = fetch_dataset(date)
    except Exception as e:
        print(f"GPSJam: failed to fetch dataset for {date}: {e}", file=sys.stderr)
        sys.exit(1)

    if len(text) <= 100:
        print(f"GPSJam: dataset for {date} is unexpectedly small", file=sys.stderr)
        sys.exit(1)

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

        if good + bad < 5:
            continue

        try:
            lat, lon = h3.cell_to_latlng(hex_id)
        except (ValueError, TypeError):
            continue

        total_hexes += 1
        rate = bad / (good + bad) if (good + bad) > 0 else 0

        for zone, (la_min, la_max, lo_min, lo_max) in ZONES.items():
            if la_min <= lat <= la_max and lo_min <= lon <= lo_max:
                zone_data[zone]["count"] += 1
                zone_data[zone]["bad"] += bad
                zone_data[zone]["good"] += good
                zone_data[zone]["total_rate"] += rate
                break

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
            "content": (
                f"GPS interference in {zone} on {date}: avg rate {avg_rate:.1%} from {d['count']} H3 hexes. "
                f"{d['bad']} bad / {d['good'] + d['bad']} total aircraft observations."
            ),
            "published_at": f"{date}T12:00:00Z",
            "severity": sev,
            "latitude": center_lat,
            "longitude": center_lon,
            "metadata": {
                "zone": zone,
                "hex_count": d["count"],
                "avg_rate": round(avg_rate, 4),
                "bad_aircraft": d["bad"],
                "good_aircraft": d["good"],
                "date": date,
                "manifest_latest_date": date,
                "manifest_suspect": suspect,
                "manifest_bad_hexes": num_bad_hexes,
            },
        })

    if signals:
        result = client.ingest_signals(signals)
        high = sum(1 for s in signals if s["severity"] == "HIGH")
        print(
            f"GPSJam: {result.get('inserted', 0)} zones (date={date}, suspect={suspect}, "
            f"bad_hexes={num_bad_hexes}, {high} HIGH, {len(signals)} total, {total_hexes} hexes scanned)"
        )
    else:
        print(
            f"GPSJam: 0 zones with data (date={date}, suspect={suspect}, "
            f"bad_hexes={num_bad_hexes}, {total_hexes} hexes scanned)"
        )


if __name__ == "__main__":
    main()
