#!/usr/bin/env python3
"""ACLED conflict events collector via myACLED OAuth."""
import json, os, sys, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals
from lib.ua import random_ua

API_BASE = "https://acleddata.com"
FIELDS = "|".join([
    "event_id_cnty", "event_date", "event_type", "sub_event_type",
    "location", "country", "notes", "latitude", "longitude",
    "fatalities", "source_url",
])


def fetch_json(url, *, data=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def oauth_token():
    username = os.environ.get("ACLED_USERNAME", "").strip()
    password = os.environ.get("ACLED_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("ACLED_USERNAME/ACLED_PASSWORD not set")

    payload = urllib.parse.urlencode({
        "username": username,
        "password": password,
        "grant_type": "password",
        "client_id": "acled",
    }).encode()
    try:
        data = fetch_json(
            f"{API_BASE}/oauth/token",
            data=payload,
            headers={"User-Agent": random_ua()},
        )
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:200]
        raise RuntimeError(f"ACLED OAuth failed: HTTP {e.code} {detail}") from e

    token = data.get("access_token", "")
    if not token:
        raise RuntimeError("ACLED OAuth response missing access_token")
    return token


def iso_date(value):
    value = (value or "").strip()
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return value if "T" in value else f"{value}T00:00:00Z"


def main():
    now = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    params = urllib.parse.urlencode({
        "_format": "json",
        "event_date": f"{from_date}|{now.strftime('%Y-%m-%d')}",
        "event_date_where": "BETWEEN",
        "region": "Europe",
        "limit": "500",
        "fields": FIELDS,
    })
    headers = {
        "Authorization": f"Bearer {oauth_token()}",
        "User-Agent": random_ua(),
    }
    try:
        data = fetch_json(f"{API_BASE}/api/acled/read?{params}", headers=headers)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:200]
        raise RuntimeError(f"ACLED fetch failed: HTTP {e.code} {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"ACLED fetch failed: {e.reason}") from e

    signals = []
    for ev in data.get("data", []):
        try:
            lat = float(ev.get("latitude", 0) or 0)
            lon = float(ev.get("longitude", 0) or 0)
        except (TypeError, ValueError):
            lat, lon = 0, 0
        event_id = ev.get("event_id_cnty") or ev.get("data_id") or "unknown"
        signals.append({
            "source_type": "acled",
            "source_id": f"acled:{event_id}",
            "title": f"ACLED: {ev.get('event_type', '')} in {ev.get('country', '')}",
            "content": f"{ev.get('event_type', '')} / {ev.get('sub_event_type', '')} at {ev.get('location', '')}, {ev.get('country', '')}. {ev.get('notes', '')[:300]}",
            "url": ev.get("source_url", ""),
            "published_at": iso_date(ev.get("event_date", "")),
            "latitude": lat if lat else None,
            "longitude": lon if lon else None,
            "metadata": {
                "event_type": ev.get("event_type"),
                "sub_type": ev.get("sub_event_type"),
                "country": ev.get("country"),
                "fatalities": ev.get("fatalities", 0),
            },
        })
    if signals:
        result = ingest_signals(signals[:500])
        print(f"ACLED: {result['inserted']} events")
    else:
        print("ACLED: no events returned")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
