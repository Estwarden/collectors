#!/usr/bin/env python3
"""Ukrainian USF Grouping killboard (sbs-group.army) — unit-level combat stats.

Collects daily cumulative stats from the Pidrakhuyka online killboard.
Source: https://sbs-group.army/en

API: /api/public/subdivisions?limit=20  → list of units
     /api/public/subdivisions/by-division-id/{id} → unit details + period IDs

Each unit produces one signal per day with cumulative kills + equipment hits.
"""
import os, sys, json, hashlib
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

API_BASE = "https://sbs-group.army/api/public"
SOURCE_TYPE = "milwatch"


def api_get(path):
    """GET JSON from SBS API."""
    url = f"{API_BASE}{path}"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "EstWarden/1.0"})
    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (URLError, json.JSONDecodeError) as e:
        print(f"  API error {url}: {e}")
        return None


def get_subdivisions():
    """Get all USF units."""
    data = api_get("/subdivisions?limit=50")
    if not data or not data.get("success"):
        return []
    return data.get("data", {}).get("subdivisions", [])


def get_unit_detail(division_id):
    """Get unit details including periods."""
    data = api_get(f"/subdivisions/by-division-id/{division_id}")
    if not data or not data.get("success"):
        return None
    return data.get("data", {}).get("subdivision")


def make_signal(unit, period_name="cumulative"):
    """Create a signal from unit data."""
    uid = unit.get("_id", "")
    title_en = unit.get("title_en") or unit.get("title") or "Unknown"
    desc_en = unit.get("description_en") or unit.get("description") or ""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    source_id = f"sbs:cumulative:{uid}:{today}"
    content_hash = hashlib.sha256(source_id.encode()).hexdigest()[:16]

    # Build title from available data
    title = f"🎯 {title_en}"
    if desc_en:
        title += f" — {desc_en[:100]}"

    return {
        "source_type": SOURCE_TYPE,
        "source_id": source_id,
        "title": title,
        "content": f"USF unit {title_en}: active unit in the Ukrainian Unmanned Systems Forces grouping. Data from sbs-group.army killboard.",
        "url": "https://sbs-group.army/en",
        "content_hash": content_hash,
    }


def main():
    client = EstWardenClient()
    print("Milwatch: fetching USF subdivisions...")

    subs = get_subdivisions()
    if not subs:
        print("Milwatch: no subdivisions found")
        return

    print(f"Milwatch: {len(subs)} units found")

    signals = []
    for sub in subs:
        div_id = sub.get("division_id")
        if div_id is None:
            continue

        # Get full unit detail
        detail = get_unit_detail(div_id)
        if not detail:
            continue

        # Skip the umbrella grouping (division_id=0), collect individual units
        if str(div_id) == "0":
            continue

        signal = make_signal(detail)
        signals.append(signal)

    if signals:
        result = client.ingest_signals(signals)
        print(f"Milwatch: {len(signals)} signals ingested — {result}")
    else:
        print("Milwatch: no signals to ingest")


if __name__ == "__main__":
    main()
