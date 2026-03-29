#!/usr/bin/env python3
"""Satellite imagery analysis — uses Gemini to analyze publicly available
satellite imagery metadata and generate intelligence assessments.
For now: Sentinel-2 cloud-free imagery counts as activity proxy."""
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals
from google_client import GoogleClient

# Military sites to monitor (public knowledge, from Wikipedia/OSM)
SITES = [
    {"name": "Tapa military base", "lat": 59.26, "lon": 25.97, "country": "EE"},
    {"name": "Amari Air Base", "lat": 59.26, "lon": 24.21, "country": "EE"},
    {"name": "Adazi military base", "lat": 57.08, "lon": 24.33, "country": "LV"},
    {"name": "Lielvarde Air Base", "lat": 56.77, "lon": 24.85, "country": "LV"},
    {"name": "Rukla military base", "lat": 55.38, "lon": 24.20, "country": "LT"},
    {"name": "Siauliai Air Base", "lat": 55.89, "lon": 23.39, "country": "LT"},
    {"name": "Pskov military base", "lat": 57.78, "lon": 28.39, "country": "RU"},
    {"name": "Kaliningrad naval base", "lat": 54.72, "lon": 20.50, "country": "RU"},
    {"name": "Ostrov Air Base", "lat": 57.35, "lon": 28.15, "country": "RU"},
]

def count_acquisitions(lat, lon, days=7):
    """Count Sentinel-2 acquisitions over a point (cloud-free proxy)."""
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    end = now.strftime("%Y-%m-%dT23:59:59Z")
    # 0.1 degree bbox (~10km)
    bbox = [lon-0.1, lat-0.1, lon+0.1, lat+0.1]
    url = "https://catalogue.dataspace.copernicus.eu/stac/search"
    body = json.dumps({
        "collections": ["sentinel-2-l2a"],
        "bbox": bbox,
        "datetime": f"{start}/{end}",
        "limit": 10,
        "query": {"eo:cloud_cover": {"lt": 30}},
    }).encode()
    try:
        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json", "User-Agent": "EstWarden/1.0"
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        return len(data.get("features", []))
    except Exception as e:
        print(f"Satellite analysis fetch error: {e}", file=sys.stderr)
        return -1

def main():
    signals = []

    for site in SITES:
        acq_count = count_acquisitions(site["lat"], site["lon"])
        if acq_count < 0:
            continue
        signals.append({
            "source_type": "satellite_analysis",
            "source_id": f"satmon:{site['name'].replace(' ','_').lower()}:{datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
            "title": f"Satellite coverage: {site['name']} — {acq_count} cloud-free passes (7d)",
            "content": f"{acq_count} Sentinel-2 cloud-free acquisitions over {site['name']} ({site['country']}) in last 7 days",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "latitude": site["lat"],
            "longitude": site["lon"],
            "metadata": {"site": site["name"], "country": site["country"], "acquisitions_7d": acq_count},
        })

    if signals:
        result = ingest_signals(signals)
        print(f"SatMon: {result['inserted']} sites monitored")

if __name__ == "__main__": main()
