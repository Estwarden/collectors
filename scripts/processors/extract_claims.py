#!/usr/bin/env python3
"""LLM-based claim extraction for EstWarden media scoring.

Extracts testable predictions and factual assertions from media posts
using Gemini (via Google Cloud) or OpenRouter. Stores claims in the
narrative_claims table for later calibration evaluation.

Usage:
    python3 extract_claims.py [--hours 6] [--limit 50]

Environment:
    ESTWARDEN_API_URL           — Data API base URL
    ESTWARDEN_API_KEY           — Pipeline API key
    DATABASE_URL                — Postgres connection
    GOOGLE_APPLICATION_CREDENTIALS — Google Cloud service account key
    OPENROUTER_API_KEY          — Alternative: OpenRouter API key
    LLM_MODEL                   — OpenRouter model (default: google/gemini-2.0-flash-001)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

EXTRACTION_PROMPT = """Analyze this media post and extract any testable claims.

A testable claim is a specific factual assertion or prediction that can be verified:
- Predictions: "Russia will attack X by Y date", "City Z will fall within N weeks"
- Factual assertions: "N troops are deployed at location X", "Country Y has done Z"
- Attribution: "Source X says Y" (testable by checking if source X actually said Y)

Do NOT extract:
- Opinions without factual basis ("this is bad")
- Vague statements ("the situation is getting worse")
- Questions or speculation explicitly marked as such

For each claim, output JSON:
```json
[
  {
    "claim": "short testable statement",
    "type": "prediction|assertion|attribution",
    "timeframe": "by spring 2026" or null if no timeframe,
    "confidence_expressed": "high|medium|low|none",
    "verifiable": true
  }
]
```

If there are no testable claims, output: []

POST:
{text}
"""


def call_llm(text, max_tokens=1000):
    """Call LLM for claim extraction. Tries Google Cloud first, then OpenRouter."""
    # Try OpenRouter (works without special setup)
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
    if openrouter_key:
        return call_openrouter(text, openrouter_key, max_tokens)

    # Try Google Cloud Gemini
    google_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if google_creds and os.path.exists(google_creds):
        return call_gemini(text, max_tokens)

    return None


def call_openrouter(text, api_key, max_tokens):
    """Call OpenRouter API."""
    import urllib.request
    model = os.environ.get("LLM_MODEL", "google/gemini-2.0-flash-001")
    prompt = EXTRACTION_PROMPT.replace("{text}", text[:3000])

    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }).encode()

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        content = data["choices"][0]["message"]["content"]
        return parse_claims_json(content)
    except Exception as e:
        print(f"  OpenRouter error: {e}", file=sys.stderr)
        return None


def call_gemini(text, max_tokens):
    """Call Gemini via Google Cloud AI Platform."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
        from google_client import GoogleClient
        gc = GoogleClient()
        prompt = EXTRACTION_PROMPT.replace("{text}", text[:3000])
        response = gc.generate_text(prompt, max_tokens=max_tokens, temperature=0.1)
        return parse_claims_json(response)
    except Exception as e:
        print(f"  Gemini error: {e}", file=sys.stderr)
        return None


def parse_claims_json(text):
    """Extract JSON array from LLM response (handles markdown code blocks)."""
    if not text:
        return []
    # Strip markdown code blocks
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        claims = json.loads(text)
        if isinstance(claims, list):
            return [c for c in claims if isinstance(c, dict) and c.get("claim")]
        return []
    except json.JSONDecodeError:
        # Try to find JSON array in the text
        import re
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return []


def main():
    parser = argparse.ArgumentParser(description="LLM claim extraction")
    parser.add_argument("--hours", type=int, default=6, help="Process signals from last N hours")
    parser.add_argument("--limit", type=int, default=50, help="Max signals to process")
    args = parser.parse_args()

    client = EstWardenClient()

    # Get recent signals from monitored sources
    all_signals = []
    for st in ["telegram_channel", "youtube_transcript"]:
        try:
            sigs = client.query_signals(source_type=st, since=f"{args.hours}h", limit=args.limit)
            if sigs:
                all_signals.extend(sigs)
        except Exception:
            pass

    if not all_signals:
        print("No signals to process")
        return

    # Filter to signals with enough content
    signals = [s for s in all_signals if len(s.get("content", "") or "") > 100]
    print(f"Processing {len(signals)} signals (from {len(all_signals)} total)")

    db_url = os.environ.get("DATABASE_URL", "")
    conn = None
    cur = None

    if db_url:
        try:
            import psycopg2
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS narrative_claims (
                    id            serial PRIMARY KEY,
                    signal_id     text,
                    source_handle text NOT NULL,
                    claim_text    text NOT NULL,
                    claim_type    text,
                    timeframe     text,
                    confidence    text,
                    resolved      boolean DEFAULT false,
                    outcome       text,
                    extracted_at  timestamptz DEFAULT now(),
                    evaluated_at  timestamptz
                );
                CREATE INDEX IF NOT EXISTS idx_claims_handle ON narrative_claims (source_handle);
                CREATE INDEX IF NOT EXISTS idx_claims_resolved ON narrative_claims (resolved) WHERE resolved = false;
            """)
            conn.commit()
        except Exception as e:
            print(f"DB setup error: {e}", file=sys.stderr)
            conn = None
            cur = None

    total_claims = 0
    for sig in signals:
        content = sig.get("content", "") or ""
        meta = sig.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        handle = meta.get("channel", "")
        if not handle:
            continue

        claims = call_llm(content)
        if claims is None:
            print(f"  ✗ {handle}: LLM call failed")
            continue

        if not claims:
            continue

        total_claims += len(claims)
        print(f"  ✓ {handle}: {len(claims)} claims extracted")

        if cur:
            for c in claims:
                cur.execute("""
                    INSERT INTO narrative_claims
                        (signal_id, source_handle, claim_text, claim_type, timeframe, confidence)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    sig.get("source_id", ""),
                    handle,
                    c.get("claim", "")[:500],
                    c.get("type", "assertion"),
                    c.get("timeframe"),
                    c.get("confidence_expressed", "none"),
                ))

    if conn:
        conn.commit()
        if cur:
            cur.close()
        conn.close()

    print(f"\nDone: {total_claims} claims extracted from {len(signals)} signals")


if __name__ == "__main__":
    main()
