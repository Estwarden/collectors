#!/usr/bin/env python3
"""Dynamic World land cover collector. Detects construction/changes at military sites via land cover shifts."""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# Add lib path for EstWardenClient
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

try:
    import ee
except ImportError:
    print("ERROR: earthengine-api not installed", file=sys.stderr)
    sys.exit(1)


# Priority military sites for land cover monitoring
SITES = [
    {"name": "Pskov-76th-VDV", "lat": 57.82, "lon": 28.35, "radius_m": 2000},
    {"name": "Kaliningrad-Chkalovsk", "lat": 54.77, "lon": 20.34, "radius_m": 2000},
    {"name": "Baltiysk-naval", "lat": 54.65, "lon": 19.89, "radius_m": 3000},
    {"name": "Kronstadt-naval", "lat": 59.99, "lon": 29.77, "radius_m": 2500},
    {"name": "Ostrov-airbase", "lat": 57.35, "lon": 28.52, "radius_m": 1500},
]

# Dynamic World land cover classes
CLASSES = {
    0: 'water',
    1: 'trees',
    2: 'grass',
    3: 'flooded_vegetation',
    4: 'crops',
    5: 'shrub_and_scrub',
    6: 'built',
    7: 'bare',
    8: 'snow_and_ice'
}

# Threshold for significant change (percentage points)
CHANGE_THRESHOLD = 5.0


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
        
        credentials = ee.ServiceAccountCredentials(email, cred_file)
        ee.Initialize(credentials)
        print("GEE initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize GEE: {e}", file=sys.stderr)
        sys.exit(1)


def compute_class_distribution(image, aoi):
    """Compute land cover class distribution as percentages."""
    # Get the label band (most likely class)
    label = image.select('label')
    
    # Compute class frequencies
    freq_dict = label.reduceRegion(
        reducer=ee.Reducer.frequencyHistogram(),
        geometry=aoi,
        scale=10,
        maxPixels=1e8
    ).getInfo()
    
    freq = freq_dict.get('label', {})
    
    # Convert to percentages
    total = sum(freq.values()) if freq else 0
    if total == 0:
        return {}
    
    distribution = {CLASSES[int(k)]: (v / total) * 100 for k, v in freq.items()}
    return distribution


def compute_landcover_changes(site, end_date):
    """Compute land cover changes for a site."""
    point = ee.Geometry.Point([site["lon"], site["lat"]])
    aoi = point.buffer(site["radius_m"])
    
    # Dynamic World V1 - real-time 10m land cover
    collection = ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
    
    # Get recent composite (last 7 days)
    recent_start = (end_date - timedelta(days=7)).strftime("%Y-%m-%d")
    recent_end = end_date.strftime("%Y-%m-%d")
    
    recent_images = collection.filterBounds(aoi).filterDate(recent_start, recent_end)
    recent_count = recent_images.size().getInfo()
    
    if recent_count == 0:
        return None
    
    recent_composite = recent_images.mode()  # Most common class
    
    # Get baseline composite (90 days ago, 7-day window)
    baseline_end = end_date - timedelta(days=90)
    baseline_start = baseline_end - timedelta(days=7)
    
    baseline_images = collection.filterBounds(aoi).filterDate(
        baseline_start.strftime("%Y-%m-%d"),
        baseline_end.strftime("%Y-%m-%d")
    )
    baseline_count = baseline_images.size().getInfo()
    
    if baseline_count == 0:
        return None
    
    baseline_composite = baseline_images.mode()
    
    # Compute distributions
    recent_dist = compute_class_distribution(recent_composite, aoi)
    baseline_dist = compute_class_distribution(baseline_composite, aoi)
    
    if not recent_dist or not baseline_dist:
        return None
    
    # Compute changes
    changes = {}
    for cls in CLASSES.values():
        recent_pct = recent_dist.get(cls, 0)
        baseline_pct = baseline_dist.get(cls, 0)
        delta = recent_pct - baseline_pct
        if abs(delta) > 0.5:  # Only store significant changes
            changes[cls] = {
                'recent': round(recent_pct, 2),
                'baseline': round(baseline_pct, 2),
                'delta': round(delta, 2)
            }
    
    return {
        'changes': changes,
        'recent_distribution': {k: round(v, 2) for k, v in recent_dist.items()},
        'baseline_distribution': {k: round(v, 2) for k, v in baseline_dist.items()},
        'date': end_date.isoformat(),
        'recent_count': recent_count,
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
            result = compute_landcover_changes(site, now)
            if result is None:
                print(f"  No data available")
                continue
            
            changes = result['changes']
            
            # Check for significant changes in built or bare classes (construction indicators)
            built_change = changes.get('built', {}).get('delta', 0)
            bare_change = changes.get('bare', {}).get('delta', 0)
            
            # Report if significant construction-related changes
            if abs(built_change) > CHANGE_THRESHOLD or abs(bare_change) > CHANGE_THRESHOLD:
                # Determine severity and description
                if built_change > CHANGE_THRESHOLD:
                    severity = "HIGH"
                    change_desc = f"Built area increased by {built_change:.1f}%"
                elif bare_change > CHANGE_THRESHOLD:
                    severity = "MODERATE"
                    change_desc = f"Bare ground increased by {bare_change:.1f}%"
                elif built_change < -CHANGE_THRESHOLD:
                    severity = "MODERATE"
                    change_desc = f"Built area decreased by {abs(built_change):.1f}%"
                else:
                    severity = "LOW"
                    change_desc = f"Bare ground decreased by {abs(bare_change):.1f}%"
                
                # Format changes summary
                change_summary = ", ".join([
                    f"{cls}: {data['delta']:+.1f}% ({data['baseline']:.1f}→{data['recent']:.1f})"
                    for cls, data in sorted(changes.items())
                ])
                
                signals.append({
                    "source_type": "landcover",
                    "source_id": f"landcover:{site['name']}:{result['date']}",
                    "title": f"Land cover change at {site['name']}: {change_desc}",
                    "content": (
                        f"Dynamic World land cover analysis at {site['name']} shows significant changes: "
                        f"{change_summary}. "
                        f"Based on {result['recent_count']} recent images vs {result['baseline_count']} baseline images. "
                        f"Possible construction, earthworks, or facility expansion."
                    ),
                    "published_at": result['date'],
                    "latitude": site['lat'],
                    "longitude": site['lon'],
                    "severity": severity,
                    "metadata": {
                        "site_name": site['name'],
                        "changes": changes,
                        "recent_distribution": result['recent_distribution'],
                        "baseline_distribution": result['baseline_distribution'],
                        "image_counts": {
                            "recent": result['recent_count'],
                            "baseline": result['baseline_count']
                        }
                    },
                })
                print(f"  ⚠ Land cover change: {change_desc}")
            else:
                print(f"  ✓ Land cover stable: built Δ{built_change:.1f}%, bare Δ{bare_change:.1f}%")
        
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            continue
    
    if signals:
        result = ingest_signals(signals)
        print(f"\nLand Cover: {result['inserted']} new signals from {len(signals)} changes")
    else:
        print("\nLand Cover: no significant changes detected")


if __name__ == "__main__":
    main()
