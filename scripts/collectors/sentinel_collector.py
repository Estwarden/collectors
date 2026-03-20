#!/usr/bin/env python3
"""Sentinel SAR collector — Baltic Sea radar acquisitions via Copernicus Data Space."""
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

BALTIC_BBOX = [18.0, 53.5, 30.5, 61.5]

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=3)).strftime("%Y-%m-%dT00:00:00Z")
    end = now.strftime("%Y-%m-%dT23:59:59Z")

    url = "https://catalogue.dataspace.copernicus.eu/stac/search"
    body = json.dumps({
        "collections": ["sentinel-1-grd"],
        "bbox": BALTIC_BBOX,
        "datetime": f"{start}/{end}",
        "limit": 50,
    }).encode()
    try:
        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json", "User-Agent": "EstWarden/1.0"
        })
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"Sentinel STAC: {e}", file=sys.stderr); return

    features = data.get("features", [])
    if not features:
        print("Sentinel: no recent acquisitions"); return

    signals = []
    for feat in features:
        props = feat.get("properties", {})
        fid = feat.get("id", "")
        dt = props.get("datetime", "")
        platform = props.get("platform", "sentinel-1")
        mode = fid.split("_")[1] if "_" in fid else ""

        signals.append({
            "source_type": "sentinel",
            "source_id": f"s1:{fid}",
            "title": f"SAR {platform} {mode}: Baltic overpass {dt[:10]}",
            "content": f"Sentinel-1 GRD acquisition over Baltic Sea. Platform: {platform}, Mode: {mode}, Time: {dt}",
            "published_at": dt or now.isoformat(),
            "metadata": {"product_id": fid, "platform": platform, "mode": mode},
        })

    result = client.ingest_signals(signals)
    print(f"Sentinel: {result['inserted']} SAR acquisitions ({len(features)} total, {result.get('duplicates',0)} dupes)")

if __name__ == "__main__": main()
