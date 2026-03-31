#!/usr/bin/env python3
"""NATO and defense think tank RSS collector.

Fetches RSS/Atom feeds from defense/security think tanks via feedparser.
Ingests as source_type 'defense_rss'.
"""
import hashlib
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals
from lib.ua import random_ua

import feedparser

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


def entry_date(entry):
    for field in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, field, None)
        if tp:
            try:
                return datetime(*tp[:6], tzinfo=timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass
    return None


def main():
    signals = []
    for name, url in FEEDS:
        parsed = feedparser.parse(url, agent=random_ua())
        count = 0
        for entry in parsed.entries:
            title = getattr(entry, "title", "").strip()
            link = getattr(entry, "link", "").strip()
            if not title or not link:
                continue
            desc = (getattr(entry, "summary", "") or "").strip()[:500]
            url_hash = hashlib.sha256(link.encode()).hexdigest()[:16]
            signals.append({
                "source_type": "defense_rss",
                "source_id": f"defense_rss:{url_hash}",
                "title": title,
                "content": desc or title,
                "url": link,
                "published_at": entry_date(entry) or datetime.now(timezone.utc).isoformat(),
                "metadata": {"feed_name": name},
            })
            count += 1
        if count:
            print(f"  {name}: {count} items")

    if signals:
        result = ingest_signals(signals)
        print(f"Defense RSS: {result.get('inserted', 0)} new / {len(signals)} total")
    else:
        print("Defense RSS: 0 items from all feeds")


if __name__ == "__main__":
    main()
