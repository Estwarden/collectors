#!/usr/bin/env python3
"""NATO and defense think tank RSS collector.

Fetches RSS/Atom feeds from defense/security think tanks.
Ingests as source_type 'defense_rss'.
"""
import hashlib
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

FEEDS = [
    ("CEPA", "https://cepa.org/feed/"),
    ("ICDS", "https://icds.ee/en/feed/"),
    ("Carnegie Endowment", "https://carnegieendowment.org/feeds/rss"),
    ("War on the Rocks", "https://warontherocks.com/feed/"),
    ("UK Defence Journal", "https://ukdefencejournal.org.uk/feed/"),
    ("European Leadership Network", "https://www.europeanleadershipnetwork.org/feed/"),
    ("FIIA", "https://www.fiia.fi/en/feed/rss"),
    ("Atlantic Council", "https://www.atlanticcouncil.org/feed/"),
]

NS = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}


def parse_date(text):
    if not text:
        return None
    try:
        return parsedate_to_datetime(text).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def fetch_feed(name, url):
    items = []
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; EstWarden/1.0)"})
        with urlopen(req, timeout=20) as r:
            raw = r.read()
        root = ET.fromstring(raw)
    except Exception as e:
        print(f"  {name}: {e}")
        return items

    # RSS 2.0
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        pub = item.findtext("pubDate") or item.findtext("dc:date", namespaces=NS)
        if title and link:
            items.append((title, link, desc[:500], parse_date(pub)))

    # Atom
    for entry in root.findall("atom:entry", NS):
        title = (entry.findtext("atom:title", namespaces=NS) or "").strip()
        link_el = entry.find("atom:link[@rel='alternate']", NS) or entry.find("atom:link", NS)
        link = link_el.get("href", "") if link_el is not None else ""
        desc = (entry.findtext("atom:summary", namespaces=NS) or "").strip()
        pub = entry.findtext("atom:published", namespaces=NS) or entry.findtext("atom:updated", namespaces=NS)
        if title and link:
            items.append((title, link, desc[:500], parse_date(pub)))

    return items


def main():
    client = EstWardenClient()
    signals = []

    for name, url in FEEDS:
        items = fetch_feed(name, url)
        for title, link, desc, pub_at in items:
            url_hash = hashlib.sha256(link.encode()).hexdigest()[:16]
            signals.append({
                "source_type": "defense_rss",
                "source_id": f"defense_rss:{url_hash}",
                "title": title,
                "content": desc if desc else title,
                "url": link,
                "published_at": pub_at or datetime.now(timezone.utc).isoformat(),
                "metadata": {"feed_name": name},
            })
        if items:
            print(f"  {name}: {len(items)} items")

    if signals:
        result = client.ingest_signals(signals)
        print(f"Defense RSS: {result.get('inserted', 0)} new / {len(signals)} total")
    else:
        print("Defense RSS: 0 items from all feeds")


if __name__ == "__main__":
    main()
