#!/usr/bin/env python3
"""ADS-B aircraft collector. Polls adsb.lol API, filters Baltic region, classifies military."""
import json, os, sys, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

BBOX = {"lat_min": 53.0, "lat_max": 61.0, "lon_min": 18.0, "lon_max": 32.0}
THREAT_ICAO = [(0x140000, 0x15FFFF, "Russia"), (0x510000, 0x5103FF, "Belarus")]
MIL_PREFIXES = {"RFF": "Russian AF", "RSD": "Russian MoD", "RRR": "Russian Special"}

def classify(icao24, callsign):
    try:
        val = int(icao24.lower(), 16)
        for low, high, country in THREAT_ICAO:
            if low <= val <= high: return country, "threat_icao"
    except: pass
    cs = (callsign or "").strip().upper()
    for pfx, desc in MIL_PREFIXES.items():
        if cs.startswith(pfx): return desc, "mil_callsign"
    return "", ""

def main():
    client = EstWardenClient()
    lat_c = (BBOX["lat_min"] + BBOX["lat_max"]) / 2
    lon_c = (BBOX["lon_min"] + BBOX["lon_max"]) / 2
    url = f"https://api.adsb.lol/v2/lat/{lat_c}/lon/{lon_c}/dist/500"
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())

    now = datetime.now(timezone.utc)
    signals, mil_count = [], 0
    for ac in data.get("ac", []):
        lat, lon = ac.get("lat"), ac.get("lon")
        if not lat or not lon: continue
        if not (BBOX["lat_min"] <= lat <= BBOX["lat_max"] and BBOX["lon_min"] <= lon <= BBOX["lon_max"]): continue
        icao24 = ac.get("hex", "")
        callsign = (ac.get("flight", "") or "").strip()
        alt = ac.get("alt_baro", 0) or 0
        threat, reason = classify(icao24, callsign)
        if threat: mil_count += 1
        signals.append({
            "source_type": "adsb", "source_id": f"adsb:{icao24}:{int(now.timestamp())}",
            "title": f"{'🔴 ' if threat else ''}{callsign or icao24} alt={alt}",
            "content": f"Aircraft {callsign or icao24} at {lat:.4f},{lon:.4f} alt={alt}m. {threat}".strip(),
            "published_at": now.isoformat(), "latitude": lat, "longitude": lon,
            "severity": "HIGH" if threat else None,
            "metadata": {"icao24": icao24, "callsign": callsign, "altitude_m": alt,
                         "category": "military" if threat else "civilian", "threat": threat, "reason": reason},
        })
    if signals:
        result = client.ingest_signals(signals[:500])
        print(f"ADS-B: {result['inserted']} new, {len(signals)} total, {mil_count} military")

if __name__ == "__main__": main()
