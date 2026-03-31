#!/usr/bin/env python3
"""YouTube transcript collector for EstWarden.

Finds recent videos per channel via YouTube Data API v3, then fetches
auto-generated captions via youtube-transcript-api (no API key needed).
Only accesses publicly available data.

Usage:
    python3 youtube_collector.py --config /path/to/watchlist.yaml [--hours 12]

Environment:
    ESTWARDEN_API_URL   — Data API base URL
    ESTWARDEN_API_KEY   — Pipeline API key
    YOUTUBE_API_KEY     — YouTube Data API v3 key
    TRANSCRIPT_PROXY    — Optional SOCKS5/HTTP proxy for transcript fetching
                          (YouTube blocks cloud provider IPs)
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import ingest_signals

import yaml

HAS_TRANSCRIPT = False
YT_TRANSCRIPT_API = None
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    YT_TRANSCRIPT_API = YouTubeTranscriptApi()
    HAS_TRANSCRIPT = True
except ImportError:
    pass


# ── YouTube Data API ──

def search_recent_videos(channel_id, api_key, hours=12, max_results=5):
    """Find recent videos for a channel using YouTube Data API v3."""
    after = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://www.googleapis.com/youtube/v3/search"
        f"?part=snippet&channelId={channel_id}&order=date"
        f"&publishedAfter={after}&maxResults={max_results}"
        f"&type=video&key={api_key}"
    )
    try:
        req = Request(url)
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        videos = []
        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            snippet = item.get("snippet", {})
            if vid:
                videos.append({
                    "video_id": vid,
                    "title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "description": snippet.get("description", "")[:500],
                    "channel_title": snippet.get("channelTitle", ""),
                })
        return videos
    except (URLError, HTTPError, OSError) as e:
        print(f"  YouTube API error: {e}", file=sys.stderr)
        return []


def get_transcript(video_id):
    """Fetch auto-generated transcript. Returns text or empty string.

    YouTube blocks transcript requests from cloud provider IPs.
    Set TRANSCRIPT_PROXY env var to work around this.
    Without proxy, collector still works — stores title + description.
    """
    if not HAS_TRANSCRIPT:
        return ""

    proxy_url = os.environ.get("TRANSCRIPT_PROXY", "")
    api = YT_TRANSCRIPT_API

    if proxy_url:
        try:
            from youtube_transcript_api.proxies import GenericProxyConfig
            api = YouTubeTranscriptApi(proxy_config=GenericProxyConfig(proxy_url))
        except ImportError:
            pass

    try:
        snippets = api.fetch(video_id, languages=["uk", "ru", "en"])
        text = " ".join(s.text for s in snippets)
        return text[:15000]
    except Exception as e:
        name = type(e).__name__
        # Don't log IP blocks — expected from cloud
        if "Block" not in name:
            print(f"    transcript({video_id}): {name}", file=sys.stderr)
        return ""


# ── Main ──

def main():
    from lib.ua import jitter, jitter_sleep
    jitter(90)
    parser = argparse.ArgumentParser(description="YouTube transcript collector")
    parser.add_argument("--config", required=True, help="Watchlist YAML path")
    parser.add_argument("--hours", type=int, default=12, help="Look back N hours")
    parser.add_argument("--category", help="Only collect channels in this category")
    args = parser.parse_args()

    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        print("ERROR: YOUTUBE_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    channels = config.get("channels", [])
    if args.category:
        channels = [c for c in channels if c.get("category") == args.category]
    channels = [c for c in channels if c.get("channel_id")]

    if not channels:
        print("No YouTube channels to collect")
        return

    # Using flat API
    total_signals = 0
    total_videos = 0
    total_transcripts = 0

    for ch in channels:
        handle = ch["handle"]
        channel_id = ch["channel_id"]

        videos = search_recent_videos(channel_id, api_key, hours=args.hours)
        if not videos:
            continue

        jitter_sleep(0.5)

        signals = []
        for vid in videos:
            total_videos += 1
            video_id = vid["video_id"]

            transcript = get_transcript(video_id)
            has_transcript = bool(transcript)
            if has_transcript:
                total_transcripts += 1

            parts = [vid["title"]]
            if vid["description"]:
                parts.append(vid["description"])
            if transcript:
                parts.append("--- TRANSCRIPT ---")
                parts.append(transcript)

            content = "\n\n".join(parts)
            if len(content) < 20:
                continue

            source_id = hashlib.sha256(f"yt:{video_id}".encode()).hexdigest()[:16]

            metadata = {
                "channel": handle,
                "channel_name": ch.get("name", handle),
                "channel_id": channel_id,
                "video_id": video_id,
                "has_transcript": has_transcript,
                "transcript_length": len(transcript),
                "category": ch.get("category", ""),
                "lang": ch.get("lang", ""),
            }

            signals.append({
                "source_type": "youtube_transcript",
                "source_id": source_id,
                "title": vid["title"][:200],
                "content": content,
                "url": f"https://youtube.com/watch?v={video_id}",
                "published_at": vid.get("published_at") or datetime.now(timezone.utc).isoformat(),
                "metadata": json.dumps(metadata),
            })

            jitter_sleep(0.3)

        if signals:
            try:
                result = ingest_signals(signals)
                inserted = result.get("inserted", 0)
                tc = sum(1 for s in signals if json.loads(s["metadata"])["has_transcript"])
                total_signals += inserted
                print(f"  ✓ {handle}: {inserted}/{len(signals)} new ({tc} transcripts)")
            except Exception as e:
                print(f"  ✗ {handle}: {e}", file=sys.stderr)

    print(f"\nDone: {total_signals} signals, {total_videos} videos, {total_transcripts} transcripts")


if __name__ == "__main__":
    main()
