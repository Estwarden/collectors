#!/usr/bin/env python3
"""AIS vessel collector. Fetches Baltic vessel positions, flags shadow fleet."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

BBOX = {"lat_min": 53.0, "lat_max": 62.0, "lon_min": 18.0, "lon_max": 32.0}
# Shadow fleet indicators: old tankers, flag-hoppers, AIS gaps
SHADOW_FLAGS = {"CM", "GA", "TG", "TZ", "PW", "KM"}  # Cameroon, Gabon, Togo, Tanzania, Palau, Comoros

def main():
    # Using flat API
    # Use AISHub or MarineTraffic free API — simplified version
    url = "https://meri.digitraffic.fi/api/ais/v1/locations"
    from lib.ua import random_ua, jitter
    jitter(90)
    req = urllib.request.Request(url, headers={
        "User-Agent": random_ua(),
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    })
    try:
        import gzip
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            data = json.loads(raw)
    except Exception as e:
        print(f"AIS fetch error: {e}", file=sys.stderr); return

    now = datetime.now(timezone.utc)
    signals, shadow = [], 0
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        coords = feat.get("geometry", {}).get("coordinates", [0, 0])
        lon, lat = coords[0], coords[1]
        if not (BBOX["lat_min"] <= lat <= BBOX["lat_max"] and BBOX["lon_min"] <= lon <= BBOX["lon_max"]): continue

        mmsi = str(props.get("mmsi", ""))
        name = props.get("name", mmsi)
        sog = props.get("sog", 0) or 0
        cog = props.get("cog", 0) or 0
        flag = mmsi[:3] if len(mmsi) >= 3 else ""
        is_shadow = flag in SHADOW_FLAGS
        if is_shadow: shadow += 1

        signals.append({
            "source_type": "ais", "source_id": f"ais:{mmsi}:{int(now.timestamp())//300}",
            "title": f"{'⚠️ ' if is_shadow else ''}{name} ({mmsi})",
            "content": f"Vessel {name} MMSI={mmsi} at {lat:.4f},{lon:.4f} SOG={sog}kn COG={cog}°",
            "published_at": now.isoformat(), "latitude": lat, "longitude": lon,
            "severity": "MODERATE" if is_shadow else None,
            "metadata": {"mmsi": mmsi, "name": name, "sog": sog, "cog": cog,
                         "shadow_fleet": is_shadow, "vessel_type": props.get("shipType", "")},
        })
    if signals:
        result = ingest_signals(signals[:500])
        print(f"AIS: {result['inserted']} new, {len(signals)} vessels, {shadow} shadow fleet")

if __name__ == "__main__": main()
