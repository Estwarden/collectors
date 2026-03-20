#!/usr/bin/env python3
"""OpenSanctions sync. Downloads latest consolidated dataset."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

def main():
    client = EstWardenClient()
    url = "https://data.opensanctions.org/datasets/latest/default/targets.simple.json"
    req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        lines = r.read().decode("utf-8", errors="replace").strip().split("\n")

    signals, count = [], 0
    for line in lines:
        if count >= 500: break
        try:
            entity = json.loads(line)
        except: continue
        schema = entity.get("schema", "")
        if schema not in ("Person", "Organization", "Company"): continue

        name = entity.get("caption", entity.get("name", ""))
        datasets = entity.get("datasets", [])
        props = entity.get("properties", {})
        countries = props.get("country", [])

        # Filter to Russia/Belarus/relevant
        if not any(c in countries for c in ["ru", "by", "ua", "ee", "lv", "lt"]): continue

        eid = entity.get("id", hashlib.sha256(name.encode()).hexdigest()[:16])
        signals.append({
            "source_type": "sanctions", "source_id": f"opensanctions:{eid}",
            "title": f"Sanctions: {name} ({schema})",
            "content": f"{name} ({schema}). Countries: {', '.join(countries)}. Datasets: {', '.join(datasets[:5])}",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {"schema": schema, "countries": countries, "datasets": datasets, "entity_id": eid},
        })
        count += 1

    if signals:
        result = client.ingest_signals(signals)
        print(f"Sanctions: {result['inserted']} entities synced")

if __name__ == "__main__": main()
