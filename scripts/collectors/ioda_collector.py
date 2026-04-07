#!/usr/bin/env python3
"""IODA internet outage collector. Monitors Baltic ASNs for connectivity drops."""
import json, os, sys, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

BALTIC_ASNS = {"3249": "Estonia (Telia)", "6712": "Estonia (Elisa)", "8728": "Latvia (Tet)",
               "12578": "Latvia (LMT)", "8764": "Lithuania (Telia)", "13194": "Lithuania (Bite)"}

def main():
    # Using flat API
    now = datetime.now(timezone.utc)
    from_ts = int((now - timedelta(hours=2)).timestamp())
    until_ts = int(now.timestamp())
    signals = []

    for asn, label in BALTIC_ASNS.items():
        url = f"https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/asn/{asn}?from={from_ts}&until={until_ts}"
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read())
        except (urllib.error.URLError, json.JSONDecodeError, OSError): continue

        # data is [[{datasource1}, {datasource2}, ...]] — flatten nested list
        raw = data.get("data", [])
        entries = raw[0] if raw and isinstance(raw[0], list) else raw
        for series in entries:
            if not isinstance(series, dict): continue
            # merit-nt has normalized 0-1 connectivity scores
            if series.get("datasource") != "merit-nt": continue
            values = [v for v in series.get("values", []) if v is not None]
            if not values: continue
            level = values[-1]
            if not isinstance(level, (int, float)): continue
            if level < 0.5:
                signals.append({
                    "source_type": "ioda", "source_id": f"ioda:{asn}:{now.strftime('%Y-%m-%dT%H')}",
                    "title": f"IODA: {label} connectivity drop ({level:.0%})",
                    "content": f"Internet connectivity for AS{asn} ({label}) at {level:.0%}",
                    "published_at": now.isoformat(), "severity": "HIGH" if level < 0.3 else "MODERATE",
                    "metadata": {"asn": asn, "label": label, "level": level},
                })

    if signals:
        result = ingest_signals(signals)
        print(f"IODA: {result['inserted']} alerts from {len(BALTIC_ASNS)} ASNs")
    else:
        print("IODA: all Baltic ASNs healthy")

if __name__ == "__main__": main()
