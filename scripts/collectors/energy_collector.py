#!/usr/bin/env python3
"""Multi-region energy collector. Fetches electricity prices from national grid APIs.

Supported grids:
- Estonia: Elering (dashboard.elering.ee)
- Finland: Fingrid / ENTSO-E transparency (via Nord Pool)
- Latvia: AST (via Nord Pool)
- Lithuania: Litgrid (via Nord Pool)
- Poland: PSE (via Nord Pool)

All use the ENTSO-E Transparency Platform as a common fallback.
Nord Pool day-ahead prices are published for all countries.
"""
import json, os, sys, urllib.request
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

# Nord Pool areas mapped to country codes and regions
AREAS = {
    "EE": {"name": "Estonia", "region": "estonia,baltic", "elering": True},
    "FI": {"name": "Finland", "region": "finland"},
    "LV": {"name": "Latvia", "region": "latvia,baltic"},
    "LT": {"name": "Lithuania", "region": "lithuania,baltic"},
    "PL": {"name": "Poland", "region": "poland"},
}

def fetch_elering(start, end):
    """Fetch from Elering API (Estonia + neighbors)."""
    url = f"https://dashboard.elering.ee/api/nps/price?start={start}&end={end}"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read())
        return data.get("data", {})
    except Exception as e:
        print(f"  Elering error: {e}", file=sys.stderr)
        return {}

def fetch_nordpool_entso(country, start_date):
    """Fetch day-ahead prices from ENTSO-E transparency (public, no key needed)."""
    # ENTSO-E area codes
    area_codes = {
        "EE": "10Y1001A1001A39I", "FI": "10YFI-1--------U",
        "LV": "10YLV-1001A00074", "LT": "10YLT-1001A0008Q",
        "PL": "10YPL-AREA-----S",
    }
    code = area_codes.get(country)
    if not code:
        return []
    
    # Use the public ENTSO-E restful API (no auth for day-ahead prices)
    url = (f"https://web-api.tp.entsoe.eu/api?"
           f"securityToken=ANONYMOUS&documentType=A44"
           f"&in_Domain={code}&out_Domain={code}"
           f"&periodStart={start_date}0000&periodEnd={start_date}2300")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            # ENTSO-E returns XML — parse price points
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.read())
            ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
            prices = []
            for ts in root.findall(".//ns:TimeSeries", ns):
                for point in ts.findall(".//ns:Point", ns):
                    pos = point.find("ns:position", ns)
                    price = point.find("ns:price.amount", ns)
                    if pos is not None and price is not None:
                        prices.append(float(price.text))
            return prices
    except Exception as e:
        print(f"  ENTSO-E {country}: {e}", file=sys.stderr)
        return []

def main():
    client = EstWardenClient()
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = now.strftime("%Y%m%d")
    
    signals = []
    
    # Try Elering first (covers EE, FI, LV, LT via Nord Pool)
    elering_data = fetch_elering(start, end)
    
    for country, cfg in AREAS.items():
        cc_lower = country.lower()
        entries = elering_data.get(cc_lower, [])
        
        if entries:
            # Elering data available for this country
            for entry in entries:
                ts = datetime.fromtimestamp(entry["timestamp"], tz=timezone.utc)
                price = entry["price"]
                signals.append({
                    "source_type": "energy",
                    "source_id": f"nordpool:{country}:{ts.strftime('%Y-%m-%dT%H')}",
                    "title": f"{country} electricity: {price:.1f} EUR/MWh",
                    "content": f"{cfg['name']} electricity price at {ts.strftime('%H:%M')} UTC: {price:.2f} EUR/MWh",
                    "published_at": ts.isoformat(),
                    "region": cfg["region"],
                    "metric_value": price,
                    "metadata": {"country": country, "unit": "EUR/MWh", 
                                 "source": "elering_nordpool", "grid": cfg["name"]},
                })
        else:
            # Fallback: try ENTSO-E
            prices = fetch_nordpool_entso(country, start_date)
            if prices:
                avg_price = sum(prices) / len(prices)
                signals.append({
                    "source_type": "energy",
                    "source_id": f"entsoe:{country}:{start_date}",
                    "title": f"{country} avg price: {avg_price:.1f} EUR/MWh",
                    "content": f"{cfg['name']} average day-ahead price: {avg_price:.2f} EUR/MWh ({len(prices)} hours)",
                    "published_at": now.isoformat(),
                    "region": cfg["region"],
                    "metric_value": avg_price,
                    "metadata": {"country": country, "unit": "EUR/MWh",
                                 "source": "entsoe", "hours": len(prices)},
                })
    
    if signals:
        result = client.ingest_signals(signals)
        countries = set(s["metadata"]["country"] for s in signals)
        print(f"Energy: {result['inserted']} new from {', '.join(sorted(countries))}")
    else:
        print("Energy: no data")

if __name__ == "__main__":
    main()
