#!/usr/bin/env python3
"""Radiation monitoring — STUK external radiation via FMI Open Data WFS."""
import os, sys, urllib.request, hashlib
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

NS = {"BsWfs": "http://xml.fmi.fi/schema/wfs/2.0", "gml": "http://www.opengis.net/gml/3.2"}

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)

    url = ("https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature"
           "&storedquery_id=stuk::observations::external-radiation::latest::simple")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            xml_data = r.read()
    except Exception as e:
        print(f"FMI/STUK: {e}"); return

    root = ET.fromstring(xml_data)
    stations = {}

    for elem in root.iter("{http://xml.fmi.fi/schema/wfs/2.0}BsWfsElement"):
        pos_elem = elem.find(".//gml:pos", NS)
        time_val = elem.findtext("BsWfs:Time", "", NS)
        value_str = elem.findtext("BsWfs:ParameterValue", "", NS)
        param = elem.findtext("BsWfs:ParameterName", "", NS)

        if pos_elem is None or pos_elem.text is None or not value_str:
            continue
        try:
            val = float(value_str)
        except:
            continue
        if val <= 0:
            continue
        # Only DR (dose rate), not DRS1 (spectral)
        if param and not param.startswith("DR_"):
            continue

        parts = pos_elem.text.strip().split()
        if len(parts) < 2:
            continue
        lat, lon = float(parts[0]), float(parts[1])
        key = f"{lat:.3f},{lon:.3f}"

        if key not in stations or time_val > stations[key].get("time", ""):
            stations[key] = {"lat": lat, "lon": lon, "value": val, "time": time_val}

    signals = []
    elevated = 0
    for key, s in stations.items():
        sev = "HIGH" if s["value"] > 0.5 else "MODERATE" if s["value"] > 0.3 else "LOW"
        if sev != "LOW":
            elevated += 1
        sid = hashlib.sha256(f"stuk:{key}:{s['time'][:13]}".encode()).hexdigest()[:12]
        signals.append({
            "source_type": "radiation",
            "source_id": f"stuk:{sid}",
            "title": f"Radiation: {s['value']:.3f} µSv/h ({s['lat']:.1f}N {s['lon']:.1f}E)",
            "content": f"STUK dose rate: {s['value']:.3f} µSv/h at ({s['lat']:.3f}, {s['lon']:.3f})",
            "published_at": s["time"] or now.isoformat(),
            "severity": sev,
            "latitude": s["lat"],
            "longitude": s["lon"],
            "metadata": {"value_usvh": s["value"], "source": "stuk_fmi"},
        })

    if signals:
        result = client.ingest_signals(signals)
        print(f"Radiation: {result['inserted']} stations ({elevated} elevated, {len(stations)} total)")
    else:
        print("Radiation: no STUK data")

if __name__ == "__main__": main()
