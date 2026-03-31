#!/usr/bin/env python3
"""RSS/Atom feed collector for EstWarden.

Reads feeds from config/feeds.yaml, parses each with feedparser, submits signals.

Usage:
    python3 rss_collector.py [--feeds /path/to/feeds.yaml] [--category counter_disinfo]
"""
import argparse
import hashlib
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

import feedparser
import yaml


def strip_html(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def entry_date(entry):
    """Extract published date from feedparser entry as ISO string."""
    for field in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, field, None)
        if tp:
            try:
                from datetime import datetime, timezone
                dt = datetime(*tp[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except (ValueError, TypeError):
                pass
    return None


def collect_feed(feed_cfg):
    """Parse a single feed, return list of signal dicts."""
    handle = feed_cfg["handle"]
    url = feed_cfg["url"]
    source_type = feed_cfg.get("source_type", "rss")
    category = feed_cfg.get("category", "")
    tier = feed_cfg.get("tier", "T2")
    region = feed_cfg.get("region", [])
    region_str = ",".join(region) if isinstance(region, list) else (region or "")

    from lib.ua import random_ua, jitter, jitter_sleep
    parsed = feedparser.parse(url, agent=random_ua())
    if parsed.bozo and not parsed.entries:
        print(f"  {handle}: feed error — {parsed.bozo_exception}", file=sys.stderr)
        return []

    signals = []
    for entry in parsed.entries:
        guid = getattr(entry, "id", "") or getattr(entry, "link", "") or ""
        link = getattr(entry, "link", "") or ""
        if not guid and not link:
            continue

        title = strip_html(getattr(entry, "title", ""))[:500]
        content = strip_html(
            getattr(entry, "summary", "") or
            getattr(entry, "description", "") or ""
        )[:5000]

        source_id = f"{handle}:{hashlib.sha256(guid.encode()).hexdigest()[:16]}"
        sig = {
            "source_type": source_type,
            "source_id": source_id,
            "title": title,
            "content": content,
            "url": link,
            "published_at": entry_date(entry),
            "metadata": {
                "feed_handle": handle,
                "category": category,
                "tier": tier,
                "guid": guid[:500],
            },
        }
        if region_str:
            sig["region"] = region_str
        signals.append(sig)

    return signals


def main():
    from lib.ua import jitter, jitter_sleep
    jitter(90)
    parser = argparse.ArgumentParser(description="RSS collector for EstWarden")
    parser.add_argument("--feeds", default="/dags/config/feeds.yaml", help="Feeds config file")
    parser.add_argument("--category", help="Only collect feeds in this category")
    parser.add_argument("--handles", help="Comma-separated list of feed handles to collect")
    args = parser.parse_args()

    with open(args.feeds) as f:
        config = yaml.safe_load(f)

    feeds = config.get("feeds", [])
    if args.category:
        feeds = [f for f in feeds if f.get("category") == args.category]
    if args.handles:
        handle_set = set(args.handles.split(","))
        feeds = [f for f in feeds if f.get("handle") in handle_set]

    total_inserted = 0
    total_dupes = 0
    total_errors = 0

    for feed in feeds:
        try:
            signals = collect_feed(feed)
            if signals:
                for i in range(0, len(signals), 100):
                    batch = signals[i:i + 100]
                    result = ingest_signals(batch)
                    total_inserted += result.get("inserted", 0)
                    total_dupes += result.get("duplicates", 0)
                    errs = result.get("errors") or []
                    total_errors += len(errs)
                print(f"  {feed['handle']}: {len(signals)} parsed")
            else:
                print(f"  {feed['handle']}: empty feed")
        except Exception as e:
            print(f"  {feed['handle']}: error — {e}", file=sys.stderr)
            total_errors += 1
        jitter_sleep(0.5)

    print(f"\nTotal: {total_inserted} inserted, {total_dupes} duplicates, {total_errors} errors")


if __name__ == "__main__":
    main()
