#!/usr/bin/env python3
"""DeepState frontline collector.

Fetches the latest DeepState GeoJSON snapshot and emits:
- 1 summary signal for overall frontline status
- up to 10 point signals for attack-direction markers

Fail-closed: exits non-zero if the API shape is missing the expected map/features.
"""
import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

URL = "https://deepstatemap.live/api/history/last"
UA = "EstWarden/1.0"


def fetch_snapshot():
    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    if not isinstance(data, dict):
        raise ValueError(f"unexpected payload type: {type(data).__name__}")
    geo = data.get("map") or {}
    features = geo.get("features") or []
    if not isinstance(features, list) or not features:
        raise ValueError("missing map.features in DeepState payload")
    return data, geo, features


def normalize_label(name: str) -> str:
    if not name:
        return "Frontline marker"
    parts = [p.strip() for p in name.replace("\xa0", " ").split("///") if p.strip()]
    # Prefer the middle English label when present.
    for part in parts:
        if all(ord(ch) < 128 for ch in part) and "geoJSON" not in part:
            return part
    # Fallback: first non-geoJSON segment
    for part in parts:
        if "geoJSON" not in part:
            return part
    return name.strip()


def classify_polygon(props: dict) -> str | None:
    name = (props.get("name") or "").lower()
    fill = (props.get("fill") or "").lower()

    if "geojson.territories.ordlo" in name or " cadr and calr" in name or "ордло" in name:
        return "ordlo"
    if "geojson.territories.crimea" in name or "occupied crimea" in name or "occupied tuzla" in name or "tuzla island" in name:
        return "crimea"
    if "geojson.status.occupied" in name or fill == "#a52714":
        return "occupied"
    if "geojson.status.unknown" in name or fill in {"#bcaaa4", "#bdbdbd"}:
        return "contested"
    return None


def is_attack_point(props: dict) -> bool:
    name = (props.get("name") or "").lower()
    return "attack_direction" in name or "direction of attack" in name or "напрямок удару" in name


def main():
    client = EstWardenClient()
    data, geo, features = fetch_snapshot()

    ds_id = str(data.get("id") or "latest")
    ds_datetime = str(data.get("datetime") or "")
    snapshot_hash = hashlib.sha256(json.dumps(geo, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]
    published_at = datetime.now(timezone.utc).isoformat()

    categories = {"occupied": 0, "crimea": 0, "ordlo": 0, "contested": 0}
    point_signals = []
    attack_points = []

    for feature in features:
        geom = feature.get("geometry") or {}
        props = feature.get("properties") or {}
        gtype = geom.get("type")

        if gtype == "Polygon":
            cat = classify_polygon(props)
            if cat:
                categories[cat] += 1
            continue

        if gtype != "Point":
            continue

        coords = geom.get("coordinates") or []
        if not isinstance(coords, list) or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        try:
            lon = float(lon)
            lat = float(lat)
        except (ValueError, TypeError):
            continue

        label = normalize_label(props.get("name") or "")
        if is_attack_point(props):
            attack_points.append({
                "label": label,
                "lat": lat,
                "lon": lon,
            })

    total_polygons = sum(categories.values())

    signals = [{
        "source_type": "deepstate",
        "source_id": f"deepstate:frontline:{ds_id}",
        "title": f"Frontline update: {ds_datetime or 'latest'}",
        "content": (
            f"DeepState frontline map updated. Polygons: {categories['occupied']} occupied, "
            f"{categories['crimea']} Crimea, {categories['ordlo']} ORDLO, "
            f"{categories['contested']} contested. Total: {total_polygons} zones."
        ),
        "url": "https://deepstatemap.live",
        "published_at": published_at,
        "metadata": {
            "hash": snapshot_hash,
            "categories": categories,
            "polygon_count": total_polygons,
            "ds_datetime": ds_datetime,
            "source_feed": "deepstate",
            "source_name": "DeepState Map",
            "source_category": "frontline_osint",
            "geo_tagged": True,
        },
    }]

    for pt in attack_points[:10]:
        signals.append({
            "source_type": "deepstate",
            "source_id": f"deepstate:pt:{pt['lat']:.3f}:{pt['lon']:.3f}:{ds_id}",
            "title": f"Attack: {pt['label']}",
            "content": pt["label"],
            "url": "https://deepstatemap.live",
            "published_at": published_at,
            "latitude": pt["lat"],
            "longitude": pt["lon"],
            "metadata": {
                "latitude": pt["lat"],
                "longitude": pt["lon"],
                "geo_tagged": True,
                "source_feed": "deepstate",
                "source_name": "DeepState Map",
                "point_category": "attack",
                "source_category": "frontline_osint",
            },
        })

    result = client.ingest_signals(signals)
    print(
        f"DeepState: {len(features)} features → {result.get('inserted', 0)} new, "
        f"{result.get('duplicates', 0)} dups; polygons={total_polygons}, attack_points={len(attack_points)}"
    )


if __name__ == "__main__":
    main()
