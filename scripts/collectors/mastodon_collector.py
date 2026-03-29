#!/usr/bin/env python3
"""Mastodon/Fediverse collector — trending + hashtag posts from security instances."""
import json, os, sys, urllib.request, hashlib, re
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

INSTANCES = ["mastodon.social", "infosec.exchange", "ioc.exchange"]
KEYWORDS = {"nato", "baltic", "osint", "ukraine", "russia", "estonia", "latvia", "lithuania",
            "disinformation", "hybrid", "cyber", "military", "defense", "defence", "jamming"}

def main():
    # Using flat API
    signals = []
    seen = set()

    for inst in INSTANCES:
        # Trending statuses (no auth required)
        try:
            url = f"https://{inst}/api/v1/trends/statuses?limit=40"
            req = urllib.request.Request(url, headers={"User-Agent": "EstWarden/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                posts = json.loads(r.read())
        except Exception as e:
            print(f"Mastodon fetch error: {e}", file=sys.stderr)
            posts = []

        for post in posts:
            uri = post.get("uri", post.get("url", ""))
            key = hashlib.sha256(uri.encode()).hexdigest()[:16]
            if key in seen:
                continue

            content = post.get("content", "")
            text = re.sub(r"<[^>]+>", " ", content)
            text = re.sub(r"\s+", " ", text).strip()
            text_lower = text.lower()

            # Filter for security-relevant content
            if not any(kw in text_lower for kw in KEYWORDS):
                continue

            seen.add(key)
            account = post.get("account", {})
            signals.append({
                "source_type": "mastodon",
                "source_id": f"masto:{key}",
                "title": f"@{account.get('acct', '?')}: {text[:100]}",
                "content": text[:3000],
                "url": post.get("url", uri),
                "published_at": post.get("created_at", datetime.now(timezone.utc).isoformat()),
                "metadata": {
                    "instance": inst,
                    "author": account.get("acct", ""),
                    "reblogs": post.get("reblogs_count", 0),
                    "favourites": post.get("favourites_count", 0),
                    "language": post.get("language", ""),
                },
            })

    if signals:
        result = ingest_signals(signals)
        print(f"Mastodon: {result['inserted']} security-relevant trending posts from {len(INSTANCES)} instances")
    else:
        print("Mastodon: no security-relevant trending posts")

if __name__ == "__main__": main()
