#!/usr/bin/env python3
"""NOTAM / CZIB collector. Fetches EASA Conflict Zone Information Bulletins."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

def main():
    client = EstWardenClient()
    url = "https://www.easa.europa.eu/en/domains/air-operations/czibs/export-json?page&_format=json"
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())

    signals = []
    for zone in data.get("ConflictZones", data if isinstance(data, list) else []):
        nid = zone.get("Nid", zone.get("nid", ""))
        title = zone.get("Title", zone.get("title", "CZIB"))
        issued = zone.get("issued_date", zone.get("IssuedDate", ""))
        valid = zone.get("valid_until_date", zone.get("ValidUntil", ""))
        body = zone.get("Body", zone.get("body", ""))

        signals.append({
            "source_type": "notam", "source_id": f"easa:czib:{nid}",
            "title": f"CZIB: {title}", "content": f"{title}. Valid until: {valid}. {body[:500]}",
            "url": f"https://www.easa.europa.eu/en/domains/air-operations/czibs",
            "published_at": issued or datetime.now(timezone.utc).isoformat(),
            "metadata": {"nid": nid, "valid_until": valid, "source": "easa_czib"},
        })
    if signals:
        result = client.ingest_signals(signals)
        print(f"NOTAMs: {result['inserted']} CZIBs")

if __name__ == "__main__": main()
