#!/usr/bin/env python3
"""Telegram public channel collector for EstWarden.

Scrapes public channel previews (t.me/s/{channel}) and submits posts as signals.
Only accesses publicly available data — no API keys, no MTProto, no user accounts.

Usage:
    python3 telegram_collector.py --config /path/to/watchlist.yaml [--category untrusted]

Environment:
    ESTWARDEN_API_URL — Data API base URL
    ESTWARDEN_API_KEY — Pipeline API key
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

import yaml

# ── Scraping ──

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EstWarden/1.0; +https://estwarden.eu)",
    "Accept-Language": "en-US,en;q=0.9",
}

# Regexes for t.me/s/ HTML parsing (avoids BeautifulSoup for simple case)
RE_MESSAGE = re.compile(
    r'<div class="tgme_widget_message_wrap[^"]*"[^>]*>'
    r'.*?data-post="([^"]+)"',
    re.DOTALL,
)
RE_TEXT = re.compile(
    r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
    re.DOTALL,
)
RE_DATE = re.compile(
    r'<time[^>]*datetime="([^"]+)"',
)
RE_VIEWS = re.compile(
    r'<span class="tgme_widget_message_views">([^<]+)</span>',
)


def strip_html(text):
    """Remove HTML tags, decode entities, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_views(s):
    """Parse '1.2K', '3.4M' style view counts."""
    if not s:
        return 0
    s = s.strip().upper()
    try:
        if s.endswith("K"):
            return int(float(s[:-1]) * 1000)
        if s.endswith("M"):
            return int(float(s[:-1]) * 1_000_000)
        return int(s)
    except (ValueError, TypeError):
        return 0


def scrape_channel(channel_handle, base_url):
    """Scrape t.me/s/{channel} and return list of post dicts."""
    url = base_url.rstrip("/")
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError, OSError) as e:
        print(f"  ✗ {channel_handle}: {e}", file=sys.stderr)
        return []

    posts = []
    # Split by message blocks
    blocks = html.split('class="tgme_widget_message_wrap')

    for block in blocks[1:]:  # skip first (before first message)
        # Post ID
        post_match = re.search(r'data-post="([^"]+)"', block)
        if not post_match:
            continue
        post_id = post_match.group(1)  # e.g., "channel/12345"

        # Text
        text_match = RE_TEXT.search(block)
        text = strip_html(text_match.group(1)) if text_match else ""
        if not text or len(text) < 10:
            continue

        # Date
        date_match = RE_DATE.search(block)
        published = date_match.group(1) if date_match else None

        # Views
        views_match = RE_VIEWS.search(block)
        views = parse_views(views_match.group(1)) if views_match else 0

        posts.append({
            "post_id": post_id,
            "text": text[:5000],  # cap at 5K chars
            "published_at": published,
            "views": views,
        })

    return posts


# ── Main ──

def main():
    parser = argparse.ArgumentParser(description="Telegram channel collector")
    parser.add_argument("--config", required=True, help="Watchlist YAML path")
    parser.add_argument("--category", help="Only collect channels in this category")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    channels = config.get("channels", [])
    if args.category:
        channels = [c for c in channels if c.get("category") == args.category]

    if not channels:
        print("No channels to collect")
        return

    client = EstWardenClient()
    total_signals = 0
    total_errors = 0

    for ch in channels:
        handle = ch["handle"]
        url = ch.get("url", "")
        if not url or "t.me/s/" not in url:
            continue

        time.sleep(1.5)  # rate limit: ~1 req per 1.5s

        posts = scrape_channel(handle, url)
        if not posts:
            continue

        signals = []
        for post in posts:
            source_id = hashlib.sha256(
                f"tg:{post['post_id']}".encode()
            ).hexdigest()[:16]

            # Build public URL from post_id (channel/msgnum)
            post_url = f"https://t.me/{post['post_id']}"

            title = post["text"][:120]
            if len(post["text"]) > 120:
                title = title.rsplit(" ", 1)[0] + "…"

            metadata = {
                "channel": handle,
                "channel_name": ch.get("name", handle),
                "views": post["views"],
                "category": ch.get("category", ""),
                "lang": ch.get("lang", ""),
            }

            signals.append({
                "source_type": "telegram_channel",
                "source_id": source_id,
                "title": title,
                "content": post["text"],
                "url": post_url,
                "published_at": post.get("published_at") or datetime.now(timezone.utc).isoformat(),
                "metadata": json.dumps(metadata),
            })

        if signals:
            try:
                result = client.ingest_signals(signals)
                inserted = result.get("inserted", 0)
                total_signals += inserted
                print(f"  ✓ {handle}: {inserted}/{len(signals)} new")
            except Exception as e:
                total_errors += 1
                print(f"  ✗ {handle}: ingest error: {e}", file=sys.stderr)

    print(f"\nDone: {total_signals} signals from {len(channels)} channels, {total_errors} errors")


if __name__ == "__main__":
    main()
