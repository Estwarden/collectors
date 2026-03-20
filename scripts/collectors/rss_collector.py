#!/usr/bin/env python3
"""RSS/Atom feed collector for EstWarden.

Reads feeds from config/feeds.yaml, parses each, submits signals via Data API.
Handles: RSS 2.0, Atom, malformed dates, encoding issues, timeouts.

Usage:
    python3 rss_collector.py [--feeds /path/to/feeds.yaml] [--category counter_disinfo]

Environment:
    ESTWARDEN_API_URL — Data API base URL
    ESTWARDEN_API_KEY — Pipeline API key
"""

import argparse
import hashlib
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

import yaml  # PyYAML — installed in Dagu container


def parse_rss_date(date_str):
    """Try multiple date formats, return ISO string or None."""
    if not date_str:
        return None
    # RFC 2822 (most RSS feeds)
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        pass
    # ISO 8601 variants
    for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d", "%d %b %Y %H:%M:%S %z"]:
        try:
            return datetime.strptime(date_str, fmt).isoformat()
        except ValueError:
            continue
    return None


def strip_html(text):
    """Remove HTML tags, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_feed(url, timeout=15):
    """Fetch a feed URL, return raw XML bytes."""
    req = Request(url, headers={"User-Agent": "EstWarden/1.0 (https://estwarden.eu)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_feed(xml_bytes, handle, source_type, category, tier):
    """Parse RSS/Atom XML into signal dicts."""
    signals = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}", file=sys.stderr)
        return signals

    # Detect Atom vs RSS
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item")  # RSS 2.0
    if not items:
        items = root.findall(".//atom:entry", ns)  # Atom

    for item in items:
        # Extract fields (try RSS then Atom)
        title = (
            _text(item, "title") or
            _text(item, "atom:title", ns) or
            ""
        )
        link = (
            _text(item, "link") or
            _attr(item, "atom:link", "href", ns) or
            ""
        )
        content = (
            _text(item, "description") or
            _text(item, "content:encoded") or
            _text(item, "atom:content", ns) or
            _text(item, "atom:summary", ns) or
            ""
        )
        pub_date = (
            _text(item, "pubDate") or
            _text(item, "dc:date") or
            _text(item, "atom:published", ns) or
            _text(item, "atom:updated", ns) or
            ""
        )
        guid = (
            _text(item, "guid") or
            _text(item, "atom:id", ns) or
            link or
            ""
        )

        if not guid and not link:
            continue

        source_id = f"{handle}:{hashlib.sha256(guid.encode()).hexdigest()[:16]}"

        signals.append({
            "source_type": source_type,
            "source_id": source_id,
            "title": strip_html(title)[:500],
            "content": strip_html(content)[:5000],
            "url": link,
            "published_at": parse_rss_date(pub_date),
            "metadata": {
                "feed_handle": handle,
                "category": category,
                "tier": tier,
                "guid": guid[:500],
            },
        })

    return signals


def _text(elem, tag, ns=None):
    """Get text content of a child element."""
    child = elem.find(tag, ns) if ns else elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _attr(elem, tag, attr, ns=None):
    """Get attribute of a child element."""
    child = elem.find(tag, ns) if ns else elem.find(tag)
    if child is not None:
        return child.get(attr, "")
    return None


def main():
    parser = argparse.ArgumentParser(description="RSS collector for EstWarden")
    parser.add_argument("--feeds", default="/dags/config/feeds.yaml", help="Feeds config file")
    parser.add_argument("--category", help="Only collect feeds in this category")
    args = parser.parse_args()

    client = EstWardenClient()

    with open(args.feeds) as f:
        config = yaml.safe_load(f)

    feeds = config.get("feeds", [])
    if args.category:
        feeds = [f for f in feeds if f.get("category") == args.category]

    total_inserted = 0
    total_dupes = 0
    total_errors = 0

    for feed in feeds:
        handle = feed["handle"]
        url = feed["url"]
        source_type = feed.get("source_type", "rss")
        category = feed.get("category", "")
        tier = feed.get("tier", "T2")

        try:
            xml_bytes = fetch_feed(url)
            signals = parse_feed(xml_bytes, handle, source_type, category, tier)

            if signals:
                # Submit in batches of 100
                for i in range(0, len(signals), 100):
                    batch = signals[i:i + 100]
                    result = client.ingest_signals(batch)
                    total_inserted += result.get("inserted", 0)
                    total_dupes += result.get("duplicates", 0)
                    total_errors += len(result.get("errors", []))

                print(f"  {handle}: {len(signals)} parsed")
            else:
                print(f"  {handle}: empty feed")

        except (URLError, HTTPError) as e:
            print(f"  {handle}: fetch error — {e}", file=sys.stderr)
            total_errors += 1
        except Exception as e:
            print(f"  {handle}: error — {e}", file=sys.stderr)
            total_errors += 1

        time.sleep(0.5)  # Be polite

    print(f"\nTotal: {total_inserted} inserted, {total_dupes} duplicates, {total_errors} errors")


if __name__ == "__main__":
    main()
