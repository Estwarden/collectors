#!/usr/bin/env python3
"""OpenSanctions collector — streams line-by-line, handles nested entity format."""
import json, os, sys, urllib.request, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

# Keywords to match Russian/Belarusian entities
RU_KEYWORDS = {"russia", "russian", "belarus", "moscow", "kremlin", "fsb", "gru", "svr",
               "kaliningrad", "военн", "минобороны", "российск", "белорус"}

def main():
    client = EstWardenClient()
    # Get version
    try:
        with urllib.request.urlopen("https://data.opensanctions.org/datasets/latest/sanctions/index.json", timeout=15) as r:
            version = json.loads(r.read()).get("version", "?")
    except:
        version = "unknown"

    url = "https://data.opensanctions.org/datasets/latest/sanctions/targets.nested.json"
    signals = []
    total = 0
    matched = 0
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
        with urllib.request.urlopen(req, timeout=180) as r:
            for line in r:
                total += 1
                if matched >= 500:
                    break
                try:
                    entity = json.loads(line)
                except:
                    continue

                # Check multiple fields for Russia/Belarus connection
                caption = str(entity.get("caption", "")).lower()
                countries = entity.get("countries", []) or []
                props = entity.get("properties", {})
                country_vals = props.get("country", []) + props.get("jurisdiction", []) + props.get("nationality", [])
                all_text = caption + " ".join(str(v) for v in country_vals).lower()

                if not (set(countries) & {"ru", "by"} or any(kw in all_text for kw in RU_KEYWORDS)):
                    continue

                matched += 1
                eid = entity.get("id", hashlib.sha256(line).hexdigest()[:16])
                name = entity.get("caption", "Unknown")
                schema = entity.get("schema", "Thing")
                datasets = entity.get("datasets", [])

                signals.append({
                    "source_type": "sanctions",
                    "source_id": f"osanc:{eid}",
                    "title": f"Sanctioned: {name} ({schema})",
                    "content": f"{name}. Datasets: {', '.join(datasets[:5])}",
                    "published_at": entity.get("first_seen", datetime.now(timezone.utc).isoformat()),
                    "metadata": {"entity_id": eid, "schema": schema, "datasets": datasets[:5], "version": version},
                })
    except Exception as e:
        print(f"  Stream error at line {total}: {e}", file=sys.stderr)

    if signals:
        for i in range(0, len(signals), 100):
            result = client.ingest_signals(signals[i:i+100])
            print(f"  Batch {i//100+1}: {result['inserted']}")
        print(f"Sanctions: {matched} entities from {total} scanned (version {version})")
    else:
        print(f"Sanctions: no matches in {total} entities")

if __name__ == "__main__": main()
