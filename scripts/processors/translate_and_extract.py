#!/usr/bin/env python3
"""Translate non-English signals and extract entities using Google Cloud APIs.

Fetches recent signals, detects language, translates Russian/Estonian to English,
extracts named entities (persons, organizations, locations), stores results
back as signal metadata updates via the Data API.

Environment:
    ESTWARDEN_API_URL, ESTWARDEN_API_KEY — Data API
    GOOGLE_APPLICATION_CREDENTIALS — service account key
"""

import os
import sys
import time

sys.path.insert(0, os.path.join("/dags/scripts/lib"))
from estwarden_client import query_signals
from google_client import translate, detect_language, extract_entities


def main():
    # Using flat API

    # Fetch recent text signals that might need translation
    signals = query_signals(since="6h", limit=100)
    if not signals:
        print("No recent signals")
        return

    translated = 0
    entities_found = 0

    for sig in signals:
        content = sig.get("content", "")
        if not content or len(content) < 50:
            continue

        title = sig.get("title", "")
        text = f"{title}. {content}" if title else content

        # Detect language
        lang = detect_language(text[:500])

        # Translate if Russian or Estonian
        en_text = text
        if lang in ("ru", "et", "lv", "lt"):
            try:
                en_text = translate(text[:2000], target="en", source=lang)
                translated += 1
            except Exception as e:
                print(f"  translate error ({sig.get('source_type','?')}:{sig.get('id','')}): {e}", file=sys.stderr)
                continue

        # Extract entities from English text
        try:
            entities = extract_entities(en_text[:2000], language="en")
            if entities:
                # Filter to significant entities
                significant = [e for e in entities if e["salience"] > 0.01]
                if significant:
                    entities_found += 1
                    # Log notable ones
                    for e in significant[:5]:
                        print(f"  [{sig.get('source_type','?')}] {e['type']}: {e['name']} ({e['salience']:.2f})")
        except Exception as e:
            print(f"  entity error: {e}", file=sys.stderr)

        time.sleep(0.1)  # Rate limit courtesy

    print(f"\nProcessed {len(signals)} signals: {translated} translated, {entities_found} with entities")


if __name__ == "__main__":
    main()
