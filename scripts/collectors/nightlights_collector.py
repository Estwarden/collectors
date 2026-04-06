#!/usr/bin/env python3
"""VIIRS nighttime lights collector. Detects anomalous activity at military sites via radiance changes."""
import json
import os
import sys
from datetime import datetime, timezone, timedelta
import math

# Add lib path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

try:
    import ee
except ImportError:
    print("ERROR: earthengine-api not installed", file=sys.stderr)
    sys.exit(1)


# Priority military sites for nighttime activity monitoring
SITES = [
    {"name": "Pskov-76th-VDV", "lat": 57.82, "lon": 28.35, "radius_m": 5000},
    {"name": "Kaliningrad-Chkalovsk", "lat": 54.77, "lon": 20.34, "radius_m": 5000},
    {"name": "Baltiysk-naval", "lat": 54.65, "lon": 19.89, "radius_m": 5000},
    {"name": "Kronstadt-naval", "lat": 59.99, "lon": 29.77, "radius_m": 5000},
    {"name": "Ostrov-airbase", "lat": 57.35, "lon": 28.52, "radius_m": 5000},
]

Z_SCORE_THRESHOLD = 2.0  # Report if z-score > 2 (95th percentile)


def init_gee():
    """Initialize Earth Engine with service account credentials."""
    cred_file = os.environ.get("GOOGLE_EE_KEY")
    if not cred_file:
        print("ERROR: GOOGLE_EE_KEY not set", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(cred_file):
        print(f"ERROR: Credentials file not found: {cred_file}", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(cred_file, 'r') as f:
            cred_data = json.load(f)
            email = cred_data.get('client_email')
        
        project = os.environ.get("GCP_PROJECT", "")
        credentials = ee.ServiceAccountCredentials(email, cred_file)
        ee.Initialize(credentials, project=project)
        print("GEE initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize GEE: {e}", file=sys.stderr)
        sys.exit(1)


def compute_nightlights_anomaly(site, end_date):
    """Compute nighttime lights anomaly for a site."""
    point = ee.Geometry.Point([site["lon"], site["lat"]])
    aoi = point.buffer(site["radius_m"])
    
    # VIIRS DNB (Day/Night Band) monthly composites
    collection = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
    
    # Get latest available image
    latest = collection.sort('system:time_start', False).first()
    latest_date = datetime.fromtimestamp(
        latest.get('system:time_start').getInfo() / 1000, 
        tz=timezone.utc
    )
    
    # Get 30-day baseline (previous composites)
    baseline_start = (latest_date - timedelta(days=90)).strftime("%Y-%m-%d")
    baseline_end = (latest_date - timedelta(days=30)).strftime("%Y-%m-%d")
    
    baseline_collection = collection.filterDate(baseline_start, baseline_end)
    baseline_count = baseline_collection.size().getInfo()
    
    if baseline_count < 2:
        return None  # Not enough baseline data
    
    # Average radiance band (avg_rad)
    band = 'avg_rad'
    
    # Get current radiance
    current_stats = latest.select(band).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=500,
        maxPixels=1e8
    ).getInfo()
    
    current_radiance = current_stats.get(band)
    if current_radiance is None:
        return None
    
    # Get baseline statistics
    baseline_mean_img = baseline_collection.select(band).mean()
    baseline_stddev_img = baseline_collection.select(band).reduce(ee.Reducer.stdDev())
    
    baseline_stats = baseline_mean_img.addBands(baseline_stddev_img).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=500,
        maxPixels=1e8
    ).getInfo()
    
    baseline_mean = baseline_stats.get(band)
    baseline_stddev = baseline_stats.get(f'{band}_stdDev', 0.1)  # Default small stddev
    
    if baseline_mean is None or baseline_stddev == 0:
        return None
    
    # Compute z-score
    z_score = (current_radiance - baseline_mean) / baseline_stddev
    
    return {
        'radiance_mean': current_radiance,
        'radiance_baseline': baseline_mean,
        'radiance_stddev': baseline_stddev,
        'z_score': z_score,
        'date': latest_date.isoformat(),
        'baseline_count': baseline_count,
    }


def main():
    init_gee()
    # Using flat API
    
    now = datetime.now(timezone.utc)
    signals = []
    
    for site in SITES:
        print(f"Processing {site['name']}...")
        try:
            result = compute_nightlights_anomaly(site, now)
            if result is None:
                print(f"  No data available")
                continue
            
            z_score = result['z_score']
            radiance = result['radiance_mean']
            baseline = result['radiance_baseline']
            
            # Report if z-score exceeds threshold
            if z_score > Z_SCORE_THRESHOLD:
                severity = "HIGH" if z_score > 3.0 else "MODERATE"
                signals.append({
                    "source_type": "nightlights",
                    "source_id": f"nightlights:{site['name']}:{now.strftime('%Y-%m-%d')}",
                    "title": f"Nighttime activity anomaly at {site['name']}: z={z_score:.2f}",
                    "content": (
                        f"VIIRS nighttime lights at {site['name']} show anomalous radiance: "
                        f"{radiance:.2f} nW/cm²/sr (baseline: {baseline:.2f}, z-score: {z_score:.2f}). "
                        f"Based on {result['baseline_count']} baseline composites. "
                        f"Possible increased nighttime activity, operations, or construction."
                    ),
                    "published_at": result['date'],
                    "latitude": site['lat'],
                    "longitude": site['lon'],
                    "severity": severity,
                    "metadata": {
                        "site_name": site['name'],
                        "radiance_mean": radiance,
                        "radiance_baseline": baseline,
                        "radiance_stddev": result['radiance_stddev'],
                        "z_score": z_score,
                        "baseline_count": result['baseline_count'],
                    },
                })
                print(f"  ⚠ Nighttime anomaly: radiance={radiance:.2f}, z={z_score:.2f}")
            else:
                print(f"  ✓ Nighttime normal: radiance={radiance:.2f}, z={z_score:.2f}")
        
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            continue
    
    if signals:
        result = ingest_signals(signals)
        print(f"\nNightlights: {result['inserted']} new signals from {len(signals)} anomalies")
    else:
        print("\nNightlights: no anomalies detected")


if __name__ == "__main__":
    main()
