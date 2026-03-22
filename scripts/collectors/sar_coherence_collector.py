#!/usr/bin/env python3
"""Sentinel-1 SAR coherence collector. Detects surface changes at military sites via interferometric coherence."""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# Add lib path for EstWardenClient
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

try:
    import ee
except ImportError:
    print("ERROR: earthengine-api not installed", file=sys.stderr)
    sys.exit(1)


# Priority military sites for SAR monitoring
SITES = [
    {"name": "Pskov-76th-VDV", "lat": 57.82, "lon": 28.35, "radius_m": 2000},
    {"name": "Kaliningrad-Chkalovsk", "lat": 54.77, "lon": 20.34, "radius_m": 2000},
    {"name": "Baltiysk-naval", "lat": 54.65, "lon": 19.89, "radius_m": 3000},
    {"name": "Kronstadt-naval", "lat": 59.99, "lon": 29.77, "radius_m": 2500},
    {"name": "Ostrov-airbase", "lat": 57.35, "lon": 28.52, "radius_m": 1500},
]

COHERENCE_DROP_THRESHOLD = 0.15  # Report if coherence drops more than this


def init_gee():
    """Initialize Earth Engine with service account credentials."""
    cred_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_file:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS not set", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(cred_file):
        print(f"ERROR: Credentials file not found: {cred_file}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Read service account email from credentials file
        with open(cred_file, 'r') as f:
            cred_data = json.load(f)
            email = cred_data.get('client_email')
        
        credentials = ee.ServiceAccountCredentials(email, cred_file)
        ee.Initialize(credentials)
        print("GEE initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize GEE: {e}", file=sys.stderr)
        sys.exit(1)


def compute_coherence(site, end_date):
    """Compute SAR coherence for a site between two latest acquisitions."""
    point = ee.Geometry.Point([site["lon"], site["lat"]])
    aoi = point.buffer(site["radius_m"])
    
    # Get Sentinel-1 GRD images (VV polarization, IW mode)
    start_date = (end_date - timedelta(days=30)).strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    collection = (ee.ImageCollection('COPERNICUS/S1_GRD')
                  .filterBounds(aoi)
                  .filterDate(start_date, end_str)
                  .filter(ee.Filter.eq('instrumentMode', 'IW'))
                  .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
                  .select(['VV']))
    
    # Get two most recent images
    sorted_collection = collection.sort('system:time_start', False)
    count = sorted_collection.size().getInfo()
    
    if count < 2:
        return None  # Not enough data
    
    img1 = ee.Image(sorted_collection.first())
    img2 = ee.Image(sorted_collection.toList(2).get(1))
    
    # Compute coherence as correlation between two images
    # Simple coherence proxy: normalized difference of log-transformed intensities
    log1 = img1.log10()
    log2 = img2.log10()
    
    # Coherence approximation: 1 - abs(diff) / max_range
    diff = log1.subtract(log2).abs()
    coherence = ee.Image(1).subtract(diff.divide(3))  # 3 = approximate log range
    
    # Get mean coherence over AOI
    stats = coherence.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=10,
        maxPixels=1e8
    ).getInfo()
    
    mean_coherence = stats.get('VV')
    if mean_coherence is None:
        return None
    
    # Get acquisition dates
    date1 = datetime.fromtimestamp(img1.get('system:time_start').getInfo() / 1000, tz=timezone.utc)
    date2 = datetime.fromtimestamp(img2.get('system:time_start').getInfo() / 1000, tz=timezone.utc)
    
    return {
        'coherence': mean_coherence,
        'date1': date1.isoformat(),
        'date2': date2.isoformat(),
    }


def main():
    init_gee()
    client = EstWardenClient()
    
    now = datetime.now(timezone.utc)
    signals = []
    
    for site in SITES:
        print(f"Processing {site['name']}...")
        try:
            result = compute_coherence(site, now)
            if result is None:
                print(f"  No data available")
                continue
            
            coherence = result['coherence']
            
            # Baseline coherence assumption: 0.7 for stable areas
            baseline = 0.7
            delta = baseline - coherence
            
            # Report if coherence dropped significantly
            if delta > COHERENCE_DROP_THRESHOLD:
                severity = "HIGH" if delta > 0.3 else "MODERATE"
                signals.append({
                    "source_type": "sar_coherence",
                    "source_id": f"sar:{site['name']}:{result['date1']}:{result['date2']}",
                    "title": f"SAR coherence drop at {site['name']}: {coherence:.2f}",
                    "content": (
                        f"Sentinel-1 SAR coherence at {site['name']} dropped to {coherence:.2f} "
                        f"(delta: {delta:.2f} from baseline {baseline:.2f}). "
                        f"Acquisitions: {result['date2']} vs {result['date1']}. "
                        f"Possible surface changes: movement, construction, or disturbance."
                    ),
                    "published_at": result['date2'],
                    "latitude": site['lat'],
                    "longitude": site['lon'],
                    "severity": severity,
                    "metadata": {
                        "site_name": site['name'],
                        "coherence_value": coherence,
                        "coherence_delta": delta,
                        "acquisition_dates": [result['date1'], result['date2']],
                        "baseline_coherence": baseline,
                    },
                })
                print(f"  ⚠ Coherence drop detected: {coherence:.2f} (Δ{delta:.2f})")
            else:
                print(f"  ✓ Coherence stable: {coherence:.2f} (Δ{delta:.2f})")
        
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            continue
    
    if signals:
        result = client.ingest_signals(signals)
        print(f"\nSAR Coherence: {result['inserted']} new signals from {len(signals)} detections")
    else:
        print("\nSAR Coherence: no anomalies detected")


if __name__ == "__main__":
    main()
