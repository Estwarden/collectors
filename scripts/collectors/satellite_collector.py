#!/usr/bin/env python3
"""Earth Engine satellite collector for military site monitoring.

Reads site coordinates from config YAML, fetches Sentinel-2 optical thumbnails
and Sentinel-1 SAR change detection from Google Earth Engine, then POSTs raw
imagery + metadata to the ingest API for storage and analysis.

All infrastructure config via environment variables — no hardcoded secrets.

Env:
    GCP_PROJECT          — GCP project registered with Earth Engine
    GCP_SERVICE_ACCOUNT  — service account email with EE access
    GOOGLE_EE_KEY        — path to service account JSON key file
    SITES_CONFIG         — path to military_sites.yaml (default: /dags/config/military_sites.yaml)
    ESTWARDEN_API_URL    — ingest API base URL
    ESTWARDEN_API_KEY    — pipeline API key
    SAT_LOOKBACK_DAYS    — days to search for cloud-free imagery (default: 10)
    SAT_CLOUD_MAX        — max cloud cover percentage (default: 25)
    SAT_BATCH_SIZE       — sites per batch (default: 5)
    SAT_THUMB_DIM        — thumbnail dimension in px (default: 1024)
    SAT_HTTP_RETRIES     — HTTP retry attempts for EE/download/post (default: 3)
    SAT_HTTP_TIMEOUT     — HTTP timeout seconds (default: 60)
"""

import base64
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import ee
import yaml

# ── Earth Engine init (all from env) ─────────────────────────────────────
_project = os.environ.get("GCP_PROJECT", "")
_sa = os.environ.get("GCP_SERVICE_ACCOUNT", "")
_key = os.environ.get("GOOGLE_EE_KEY", "")

if not (_project and _sa and _key and os.path.isfile(_key)):
    print("FATAL: set GCP_PROJECT, GCP_SERVICE_ACCOUNT, GOOGLE_EE_KEY", file=sys.stderr)
    sys.exit(1)

_ee_creds = ee.ServiceAccountCredentials(_sa, _key)
ee.Initialize(_ee_creds, project=_project)

# OAuth token for authenticated thumbnail downloads
from google.oauth2 import service_account as _sa_mod
import google.auth.transport.requests as _gatr
from urllib.request import Request, urlopen

_oauth = _sa_mod.Credentials.from_service_account_file(
    _key, scopes=["https://www.googleapis.com/auth/earthengine"]
)
_oauth.refresh(_gatr.Request())

# ── Config ───────────────────────────────────────────────────────────────
LOOKBACK = int(os.environ.get("SAT_LOOKBACK_DAYS", "10"))
CLOUD_MAX = int(os.environ.get("SAT_CLOUD_MAX", "25"))
BATCH_SIZE = int(os.environ.get("SAT_BATCH_SIZE", "5"))
THUMB_DIM = int(os.environ.get("SAT_THUMB_DIM", "1024"))
HTTP_RETRIES = int(os.environ.get("SAT_HTTP_RETRIES", "3"))
HTTP_TIMEOUT = int(os.environ.get("SAT_HTTP_TIMEOUT", "60"))

sites_path = os.environ.get("SITES_CONFIG", "/dags/config/military_sites.yaml")
with open(sites_path) as f:
    SITES = yaml.safe_load(f)["sites"]


def fetch_bytes(url: str, headers: dict | None = None, timeout: int = HTTP_TIMEOUT, label: str = "request") -> bytes:
    """Fetch bytes with simple retry/backoff for flaky EE/network responses."""
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            return urlopen(Request(url, headers=headers or {}), timeout=timeout).read()
        except Exception as e:
            last_err = e
            if attempt >= HTTP_RETRIES:
                break
            wait = min(5 * attempt, 15)
            print(f"    {label} retry {attempt}/{HTTP_RETRIES} after error: {e}", file=sys.stderr)
            time.sleep(wait)
    raise last_err


def get_thumbnail(lat: float, lon: float) -> tuple:
    """Fetch latest cloud-free Sentinel-2 thumbnail from Earth Engine.

    Returns (jpeg_bytes, scene_date_str) or (None, None).
    """
    point = ee.Geometry.Point(lon, lat)
    region = point.buffer(2000).bounds()  # 4km area
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    col = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_MAX))
        .sort("CLOUDY_PIXEL_PERCENTAGE")  # prefer clearest
    )

    if col.size().getInfo() == 0:
        return None, None

    img = col.first()
    props = img.getInfo()["properties"]
    scene_date = datetime.fromtimestamp(
        props["system:time_start"] / 1000
    ).strftime("%Y-%m-%d")

    # True color with contrast enhancement.
    # Keep thumbnails modest for reliability; Sentinel-2 is still a 10m source.
    vis = img.select(["B4", "B3", "B2"]).visualize(min=300, max=4000, gamma=1.3)
    url = vis.getThumbURL({"region": region, "dimensions": THUMB_DIM, "format": "jpg"})

    # Refresh token if needed
    if not _oauth.valid:
        _oauth.refresh(_gatr.Request())

    headers = {"Authorization": "Bearer %s" % _oauth.token}
    data = fetch_bytes(url, headers=headers, label=f"thumbnail {scene_date}")
    return data, scene_date


def get_sar_change(lat: float, lon: float) -> dict:
    """Compute Sentinel-1 SAR backscatter change vs 30-day baseline.

    Returns {"mean_change_db": float, "std_change_db": float}.
    """
    try:
        point = ee.Geometry.Point(lon, lat)
        region = point.buffer(1200)
        now = datetime.now(timezone.utc)

        def s1_col(start, end):
            return (
                ee.ImageCollection("COPERNICUS/S1_GRD")
                .filterBounds(point)
                .filterDate(start, end)
                .filter(ee.Filter.eq("instrumentMode", "IW"))
                .select("VV")
            )

        recent = s1_col(
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        baseline = s1_col(
            (now - timedelta(days=37)).strftime("%Y-%m-%d"),
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
        )

        if recent.size().getInfo() == 0 or baseline.size().getInfo() == 0:
            return {"mean_change_db": 0.0, "std_change_db": 0.0}

        diff = recent.mean().subtract(baseline.mean())
        stats = diff.reduceRegion(
            reducer=ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True),
            geometry=region,
            scale=10,
        ).getInfo()

        return {
            "mean_change_db": round(stats.get("VV_mean", 0) or 0, 2),
            "std_change_db": round(stats.get("VV_stdDev", 0) or 0, 2),
        }
    except Exception as e:
        print("  SAR error: %s" % e, file=sys.stderr)
        return {"mean_change_db": 0.0, "std_change_db": 0.0}


def compute_indices_ee(lat: float, lon: float) -> dict:
    """Compute spectral indices server-side in Earth Engine.

    Returns dict with current values, snow fraction, and seasonally-adjusted deltas.
    Uses 3-year same-month median as baseline to reduce interannual weather noise.
    Snow pixels (SCL=11) are masked before index computation.
    """
    try:
        point = ee.Geometry.Point(lon, lat)
        region = point.buffer(2000)
        now = datetime.now(timezone.utc)
        MP = 5000000

        # Current image
        img = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(point)
            .filterDate((now - timedelta(days=14)).strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_MAX))
            .sort("CLOUDY_PIXEL_PERCENTAGE")
            .first()
        )

        # Snow detection from SCL band (Scene Classification Layer)
        # SCL=11 is snow/ice, SCL=6 is water
        scl = img.select("SCL")
        snow_mask = scl.eq(11)
        cloud_mask = scl.gte(8).And(scl.lte(10))  # 8=cloud_medium, 9=cloud_high, 10=cirrus
        snow_pct = snow_mask.reduceRegion(ee.Reducer.mean(), region, 20, maxPixels=MP).getInfo()
        snow_frac = round((snow_pct.get("SCL", 0) or 0) * 100, 1)

        # Mask snow + clouds for index computation
        valid_mask = snow_mask.Not().And(cloud_mask.Not())
        img_masked = img.updateMask(valid_mask)

        # 3-year same-month median baseline (seasonally adjusted)
        month = now.month
        baselines = []
        for yr_offset in [1, 2, 3]:
            yr = now.year - yr_offset
            start = "%d-%02d-01" % (yr, month)
            end_m = month + 1 if month < 12 else 1
            end_y = yr if month < 12 else yr + 1
            end = "%d-%02d-01" % (end_y, end_m)
            baselines.append(
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(point).filterDate(start, end)
                .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
            )
        baseline = ee.ImageCollection(baselines[0].merge(baselines[1]).merge(baselines[2])).median()

        def idx(im):
            B2, B3, B4, B8 = im.select("B2"), im.select("B3"), im.select("B4"), im.select("B8")
            B11, B12 = im.select("B11"), im.select("B12")
            ndvi = B8.subtract(B4).divide(B8.add(B4).add(0.001)).rename("ndvi")
            ndbi = B11.subtract(B8).divide(B11.add(B8).add(0.001)).rename("ndbi")
            bsi = (B11.add(B4).subtract(B8.add(B2))
                   .divide(B11.add(B4).add(B8).add(B2).add(0.001))).rename("bsi")
            fuel = ndvi.lt(0.1).And(B12.gt(1500)).rename("fuel")
            metal = B8.gt(3000).And(ndvi.lt(0.2)).rename("metal")
            active = ndbi.gt(0).And(bsi.gt(0)).rename("active")
            return ndvi.addBands([ndbi, bsi, fuel, metal, active])

        cur = idx(img_masked)
        bas = idx(baseline)
        delta = (cur.select(["ndvi", "ndbi", "bsi"])
                 .subtract(bas.select(["ndvi", "ndbi", "bsi"]))
                 .rename(["d_ndvi", "d_ndbi", "d_bsi"]))

        stats = cur.addBands(delta).reduceRegion(
            ee.Reducer.mean(), region, 10, maxPixels=MP
        ).getInfo()

        return {
            "ndvi": round(stats.get("ndvi", 0) or 0, 4),
            "ndbi": round(stats.get("ndbi", 0) or 0, 4),
            "bsi": round(stats.get("bsi", 0) or 0, 4),
            "fuel_pct": round((stats.get("fuel", 0) or 0) * 100, 2),
            "metal_pct": round((stats.get("metal", 0) or 0) * 100, 2),
            "active_pct": round((stats.get("active", 0) or 0) * 100, 2),
            "snow_pct": snow_frac,
            "delta_ndvi": round(stats.get("d_ndvi", 0) or 0, 4),
            "delta_ndbi": round(stats.get("d_ndbi", 0) or 0, 4),
            "delta_bsi": round(stats.get("d_bsi", 0) or 0, 4),
        }
    except Exception as e:
        print("  Indices error: %s" % e, file=sys.stderr)
        return {}


def post_to_ingest(site_id, lat, lon, thumb_b64, scene_date, sar, country, site_type, indices=None):
    """POST satellite data to ingest API."""
    api_url = os.environ.get("ESTWARDEN_API_URL", "http://ingest:9090")
    api_key = os.environ.get("ESTWARDEN_API_KEY", "")

    payload = {
        "site_id": site_id,
        "lat": lat,
        "lon": lon,
        "thumbnail_b64": thumb_b64,
        "scene_date": scene_date,
        "sar_change": sar,
        "country": country,
        "site_type": site_type,
        "spectral_indices": indices or {},
    }

    import urllib.request
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        "%s/api/v1/collect/satellite" % api_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Pipeline-Key": api_key,
        },
    )

    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
            return json.loads(resp.read())
        except Exception as e:
            last_err = e
            if attempt >= HTTP_RETRIES:
                break
            wait = min(3 * attempt, 10)
            print(f"    ingest retry {attempt}/{HTTP_RETRIES} after error: {e}", file=sys.stderr)
            time.sleep(wait)
    raise last_err


def main():
    total = len(SITES)
    ok = 0
    skip = 0
    fail = 0

    print("Satellite collector: %d sites, lookback=%dd, cloud<%d%%" % (total, LOOKBACK, CLOUD_MAX))

    for i in range(0, total, BATCH_SIZE):
        batch = SITES[i : i + BATCH_SIZE]
        print("Batch %d/%d (%d sites)" % (i // BATCH_SIZE + 1, (total + BATCH_SIZE - 1) // BATCH_SIZE, len(batch)))

        for site in batch:
            sid = site["id"]
            try:
                thumb, scene_date = get_thumbnail(site["lat"], site["lon"])
                if not thumb:
                    print("  %s: no cloud-free scene" % sid)
                    skip += 1
                    continue

                sar = get_sar_change(site["lat"], site["lon"])
                indices = compute_indices_ee(site["lat"], site["lon"])

                thumb_b64 = base64.b64encode(thumb).decode()
                result = post_to_ingest(
                    site_id=sid,
                    lat=site["lat"],
                    lon=site["lon"],
                    thumb_b64=thumb_b64,
                    scene_date=scene_date,
                    sar=sar,
                    country=site.get("country", ""),
                    site_type=site.get("branch", site.get("unit", "military")),
                    indices=indices,
                )
                level = result.get("activity_level", "?")
                print("  %s: %s → %s (date=%s)" % (sid, scene_date, level, scene_date))
                ok += 1

            except Exception as e:
                print("  %s: error — %s" % (sid, e), file=sys.stderr)
                fail += 1

        # Rate-limit between batches
        if i + BATCH_SIZE < total:
            time.sleep(2)

    print("Done: %d ok, %d skipped, %d failed" % (ok, skip, fail))


if __name__ == "__main__":
    main()
