#!/usr/bin/env python3
"""Airspace restriction collector — EASA Conflict Zone Information Bulletins (HTML scraping)."""
import os, sys, urllib.request, hashlib, re
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals
from bs4 import BeautifulSoup

def main():
    # Using flat API
    signals = []

    # Scrape EASA CZIBs page
    url = "https://www.easa.europa.eu/en/domains/air-operations/czibs"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")

        # Find CZIB entries — they're usually in tables or list items
        for row in soup.find_all(["tr", "article", "li", "div"]):
            text = row.get_text(" ", strip=True)
            # Match CZIB identifiers
            match = re.search(r"CZIB[-\s](\d{4}[-/]\d+[A-Z]?)", text)
            if not match:
                continue
            czib_id = match.group(0).replace(" ", "-")
            
            # Try to find links
            link = row.find("a", href=True)
            czib_url = link["href"] if link else url
            if czib_url.startswith("/"):
                czib_url = f"https://www.easa.europa.eu{czib_url}"

            sid = hashlib.sha256(czib_id.encode()).hexdigest()[:16]
            signals.append({
                "source_type": "notam",
                "source_id": f"easa:{sid}",
                "title": f"CZIB: {czib_id}",
                "content": text[:1000],
                "url": czib_url,
                "published_at": datetime.now(timezone.utc).isoformat(),
                "metadata": {"czib_id": czib_id, "source": "easa"},
            })
    except Exception as e:
        print(f"  EASA scrape: {e}", file=sys.stderr)

    if signals:
        result = ingest_signals(signals)
        print(f"Airspace: {result['inserted']} CZIBs from EASA")
    else:
        print("Airspace: no CZIBs found")

if __name__ == "__main__": main()
