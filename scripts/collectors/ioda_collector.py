#!/usr/bin/env python3
"""IODA internet outage collector. Monitors Baltic ASNs for connectivity drops."""
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

BALTIC_ASNS = {"3249": "Estonia (Telia)", "6712": "Estonia (Elisa)", "8728": "Latvia (Tet)",
               "12578": "Latvia (LMT)", "8764": "Lithuania (Telia)", "13194": "Lithuania (Bite)"}

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)
    from_ts = int((now - timedelta(hours=2)).timestamp())
    until_ts = int(now.timestamp())
    signals = []

    for asn, label in BALTIC_ASNS.items():
        url = f"https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/asn/{asn}?from={from_ts}&until={until_ts}"
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read())
        except: continue

        for series in data.get("data", []):
            values = series.get("values", [])
            if not values: continue
            latest = values[-1] if values else [0, 0]
            level = latest[1] if len(latest) > 1 else 0
            if level is not None and level < 0.5:
                signals.append({
                    "source_type": "ioda", "source_id": f"ioda:{asn}:{now.strftime('%Y-%m-%dT%H')}",
                    "title": f"IODA: {label} connectivity drop ({level:.0%})",
                    "content": f"Internet connectivity for AS{asn} ({label}) at {level:.0%}",
                    "published_at": now.isoformat(), "severity": "HIGH" if level < 0.3 else "MODERATE",
                    "metadata": {"asn": asn, "label": label, "level": level},
                })

    if signals:
        result = client.ingest_signals(signals)
        print(f"IODA: {result['inserted']} alerts from {len(BALTIC_ASNS)} ASNs")
    else:
        print("IODA: all Baltic ASNs healthy")

if __name__ == "__main__": main()
