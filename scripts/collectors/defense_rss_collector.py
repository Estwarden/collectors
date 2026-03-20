#!/usr/bin/env python3
"""Defense & NATO RSS collector — think tanks, defense blogs, military news.
Uses source_type 'defense_rss' to avoid dedup collision with main rss collector."""
import os, sys, re, hashlib
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient
import feedparser

FEEDS = [
    ("https://cepa.org/feed/", "CEPA"),
    ("https://warontherocks.com/feed/", "War on the Rocks"),
    ("https://www.fpri.org/feed/", "FPRI"),
    ("https://www.crisisgroup.org/rss.xml", "Crisis Group"),
    ("https://www.atlanticcouncil.org/feed/", "Atlantic Council"),
    ("https://jamestown.org/feed/", "Jamestown Foundation"),
    ("https://breakingdefense.com/feed/", "Breaking Defense"),
    ("https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml", "Defense News"),
    ("https://www.rand.org/pubs/commentary.xml", "RAND"),
    ("https://rusi.org/rss.xml", "RUSI"),
    ("https://www.iiss.org/rss", "IISS"),
    ("https://www.chathamhouse.org/rss", "Chatham House"),
    ("https://www.csis.org/rss.xml", "CSIS"),
    ("https://www.understandingwar.org/rss.xml", "Understanding War"),
    ("https://foreignpolicy.com/feed/", "Foreign Policy"),
]

def main():
    client = EstWardenClient()
    signals = []
    working = 0
    for url, name in FEEDS:
        try:
            feed = feedparser.parse(url)
            if not feed.entries: continue
            working += 1
            for entry in feed.entries[:10]:
                link = entry.get("link", "")
                eid = entry.get("id", link)
                sid = hashlib.sha256(eid.encode()).hexdigest()[:16]
                pub = entry.get("published", entry.get("updated", ""))
                content = entry.get("summary", entry.get("description", ""))
                content = re.sub(r"<[^>]+>", " ", content)
                content = re.sub(r"\s+", " ", content).strip()
                signals.append({
                    "source_type": "defense_rss",
                    "source_id": f"defrss:{name}:{sid}",
                    "title": entry.get("title", ""),
                    "content": content[:3000],
                    "url": link,
                    "published_at": pub or datetime.now(timezone.utc).isoformat(),
                    "metadata": {"feed": name},
                })
        except Exception as e:
            print(f"  {name}: {e}", file=sys.stderr)
    if signals:
        result = client.ingest_signals(signals)
        print(f"Defense RSS: {result['inserted']} articles from {working}/{len(FEEDS)} feeds ({result.get('duplicates',0)} dupes)")
    else:
        print("Defense RSS: no feeds responding")

if __name__ == "__main__": main()
