#!/usr/bin/env python3
"""YouTube collector — monitor Baltic security-related channels."""
import json, os, sys, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

CHANNELS = [
    ("UCDGknihPmGRo2nOGCEWPJgg", "Propastop"),  
    ("UCBi2mrWuNuyYy4gbM6fU18Q", "CEPA"),
]

def main():
    client = EstWardenClient()
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        print("YOUTUBE_API_KEY not set"); return

    signals = []
    for channel_id, name in CHANNELS:
        url = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet&order=date&maxResults=10&type=video"
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read())
            for item in data.get("items", []):
                snip = item.get("snippet", {})
                vid = item.get("id", {}).get("videoId", "")
                signals.append({
                    "source_type": "youtube",
                    "source_id": f"youtube:{vid}",
                    "title": snip.get("title", ""),
                    "content": snip.get("description", "")[:2000],
                    "url": f"https://youtube.com/watch?v={vid}",
                    "published_at": snip.get("publishedAt", ""),
                    "metadata": {"channel": name, "channel_id": channel_id},
                })
        except Exception as e:
            print(f"  {name}: {e}", file=sys.stderr)
    if signals:
        result = client.ingest_signals(signals)
        print(f"YouTube: {result['inserted']} videos")

if __name__ == "__main__": main()
