#!/usr/bin/env python3
"""Fabrication detection via Gemini.

For each multi-category event cluster:
1. Get the earliest signal (root) and cross-category downstream signals
2. Ask Gemini: "Does signal B add claims not in signal A?"
3. Store fabrication alerts for high-scoring mutations

Uses the existing Gemini API (same key as embeddings).
Cost: ~$0.01/day (1 Gemini call per multi-cat cluster with enough signals).
"""

import argparse
import base64
import json
import os
import sys
import time as _time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.join("/dags/scripts/lib"))

DB_URL = os.environ.get("DATABASE_URL", "")
# Auth handled by _google_auth() via GOOGLE_APPLICATION_CREDENTIALS
GEMINI_MODEL = "models/gemini-2.5-flash"

# ── Google Auth via google-auth library ──
import google.auth
import google.auth.transport.requests

_credentials = None
_auth_request = google.auth.transport.requests.Request()

def _google_auth():
    """Returns (url_suffix, extra_headers) for Gemini API."""
    global _credentials
    if _credentials is None or not _credentials.valid:
        _credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/generative-language"])
    if not _credentials.valid:
        _credentials.refresh(_auth_request)
    return "", {"Authorization": f"Bearer {_credentials.token}"}

# Load EUvsDisinfo source credibility (431 known disinfo domains, 369 trusted)
_CREDIBILITY = None
def get_credibility():
    global _CREDIBILITY
    if _CREDIBILITY is None:
        try:
            _CREDIBILITY = json.load(open("/dags/config/source-credibility.json"))
        except Exception as e:
            print(f"credibility config load error: {e}", file=sys.stderr)
            _CREDIBILITY = {"disinfo_domains": [], "trustworthy_domains": []}
    return _CREDIBILITY

def is_known_disinfo_source(category):
    """Check if a source category maps to EUvsDisinfo-confirmed disinfo."""
    return category in ("ru_state", "ru_proxy", "russian_state")


def gemini_call(prompt, max_tokens=500):
    """Single Gemini API call (service account or API key)."""
    auth_suffix, auth_headers = _google_auth()
    url = f"https://generativelanguage.googleapis.com/v1beta/{GEMINI_MODEL}:generateContent{auth_suffix}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.1,
                             "responseMimeType": "application/json",
                             "thinkingConfig": {"thinkingBudget": 0}},
    }
    headers = {"Content-Type": "application/json"}
    headers.update(auth_headers)
    req = urllib.request.Request(url, json.dumps(body).encode(), headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read())
    text = resp["candidates"][0]["content"]["parts"][0]["text"]
    return text


def parse_gemini_json(text):
    """Extract JSON from Gemini response, handling markdown/thinking blocks."""
    import re
    text = text.strip()
    # Remove thinking blocks
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    # Try extracting from code block
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Try finding raw JSON object
    m = re.search(r'\{[^{}]*"fabrication_score"[^{}]*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # Try the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


FABRICATION_PROMPT = """You are an information integrity analyst. Compare two media signals about the same event.

SIGNAL A (root/earlier):
Source: {root_source} [{root_category}]
Title: {root_title}

SIGNAL B (downstream/later):
Source: {down_source} [{down_category}]  
Title: {down_title}

Analyze whether Signal B adds claims, certainty, or emotional framing not present in Signal A.

Reply ONLY with this JSON (no markdown, no explanation):
{{
  "same_event": true/false,
  "added_claims": ["list of specific claims in B not in A"],
  "certainty_escalation": true/false,
  "emotional_amplification": true/false,
  "fabrication_score": 0-10,
  "summary": "one sentence describing the difference"
}}

Score guide:
0 = identical or faithful translation
1-3 = minor editorial differences (normal journalism)
4-6 = significant framing shift or added context not in source
7-10 = ONLY if Signal B discusses a completely different topic than Signal A (cluster error)

IMPORTANT: If the signals are about the same event, the maximum score is 6.
Score 7+ means they are NOT about the same event at all."""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-signals", type=int, default=3, help="Min signals per cluster")
    parser.add_argument("--max-clusters", type=int, default=30, help="Max clusters to check per run")
    parser.add_argument("--days", type=int, default=3, help="Look back N days")
    parser.add_argument("--min-score", type=float, default=4.0, help="Min fabrication score to store")
    args = parser.parse_args()

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("GOOGLE_APPLICATION_CREDENTIALS not set")
        sys.exit(1)
    if not DB_URL:
        print("DATABASE_URL not set")
        sys.exit(1)

    import psycopg2
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Ensure fabrication_alerts table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fabrication_alerts (
            id              serial PRIMARY KEY,
            cluster_id      int NOT NULL,
            root_signal_id  int NOT NULL,
            down_signal_id  int NOT NULL,
            root_source     text,
            root_category   text,
            down_source     text,
            down_category   text,
            root_title      text,
            down_title      text,
            fabrication_score float,
            added_claims    jsonb,
            certainty_escalation bool,
            emotional_amplification bool,
            summary         text,
            down_views      int DEFAULT 0,
            detected_at     timestamptz DEFAULT now(),
            UNIQUE(cluster_id, root_signal_id, down_signal_id)
        );
        CREATE INDEX IF NOT EXISTS idx_fab_score ON fabrication_alerts (fabrication_score DESC);
        CREATE INDEX IF NOT EXISTS idx_fab_detected ON fabrication_alerts (detected_at DESC);
    """)
    conn.commit()

    # Also ensure narrative_origins table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS narrative_origins (
            id              serial PRIMARY KEY,
            cluster_id      int NOT NULL UNIQUE,
            first_signal_id int NOT NULL,
            first_source    text,
            first_category  text,
            first_title     text,
            first_published timestamptz,
            signal_count    int,
            category_count  int,
            categories      text[],
            is_state_origin bool DEFAULT false,
            created_at      timestamptz DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_narr_origin_state ON narrative_origins (is_state_origin) WHERE is_state_origin;
    """)
    conn.commit()

    # Find multi-category clusters from recent days
    cur.execute("""
        SELECT ec.id, ec.signal_count, ec.categories, ec.has_state, ec.has_trusted
        FROM event_clusters ec
        WHERE ec.created_at >= now() - interval '%s days'
          AND ec.signal_count >= %s
          AND array_length(ec.categories, 1) >= 2
          AND ec.id NOT IN (SELECT DISTINCT cluster_id FROM fabrication_alerts)
        ORDER BY ec.signal_count DESC
        LIMIT %s
    """, (args.days, args.min_signals, args.max_clusters))
    clusters = cur.fetchall()
    print(f"Multi-category clusters to analyze: {len(clusters)}")

    # Also record narrative origins for ALL recent clusters
    cur.execute("""
        SELECT ec.id, ec.signal_count, ec.categories
        FROM event_clusters ec
        WHERE ec.created_at >= now() - interval '%s days'
          AND ec.signal_count >= 2
          AND ec.id NOT IN (SELECT cluster_id FROM narrative_origins)
    """, (args.days,))
    origin_clusters = cur.fetchall()
    
    origins_recorded = 0
    for cid, sig_count, categories in origin_clusters:
        cur.execute("""
            SELECT s.id, s.title, s.published_at,
                   COALESCE(s.metadata->>'channel', 'rss:' || s.source_type) as source,
                   COALESCE(s.metadata->>'category', '') as category
            FROM cluster_signals cs
            JOIN signals s ON s.id = cs.signal_id
            WHERE cs.cluster_id = %s
            ORDER BY s.published_at ASC
            LIMIT 1
        """, (cid,))
        row = cur.fetchone()
        if not row:
            continue
        sig_id, title, pub_at, source, category = row
        is_state = category in ("ru_state", "ru_proxy", "russian_state")
        cat_count = len(categories) if categories else 0
        
        cur.execute("""
            INSERT INTO narrative_origins 
                (cluster_id, first_signal_id, first_source, first_category, first_title,
                 first_published, signal_count, category_count, categories, is_state_origin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cluster_id) DO NOTHING
        """, (cid, sig_id, source, category, title[:500], pub_at, sig_count,
              cat_count, categories, is_state))
        origins_recorded += 1

    conn.commit()
    print(f"Narrative origins recorded: {origins_recorded}")
    
    # Count state-origin narratives that reached trusted media
    cur.execute("""
        SELECT COUNT(*) FROM narrative_origins
        WHERE is_state_origin AND category_count >= 2
          AND created_at >= now() - interval '%s days'
    """, (args.days,))
    state_laundered = cur.fetchone()[0]
    print(f"State-origin narratives in multi-category clusters: {state_laundered}")

    # Show state-origin narratives that penetrated non-state categories
    cur.execute("""
        SELECT no.cluster_id, no.first_source, no.first_category, 
               LEFT(no.first_title, 100), no.signal_count, no.categories
        FROM narrative_origins no
        WHERE no.is_state_origin AND no.category_count >= 2
          AND no.created_at >= now() - interval '%s days'
        ORDER BY no.signal_count DESC
        LIMIT 10
    """, (args.days,))
    for row in cur.fetchall():
        cid, src, cat, title, cnt, cats = row
        non_state = [c for c in (cats or []) if c not in ("ru_state","ru_proxy","russian_state")]
        if non_state:
            print(f"  🔴 State origin → {','.join(non_state)}: [{cnt} signals] {title}")

    # Now check multi-category clusters for fabrication via Gemini
    checked = 0
    alerts = 0
    
    for cid, sig_count, categories, has_state, has_trusted in clusters:
        # Get root (earliest) and downstream signals
        cur.execute("""
            SELECT s.id, s.title, s.published_at,
                   COALESCE(s.metadata->>'channel', 'rss:' || s.source_type) as source,
                   COALESCE(s.metadata->>'category', '') as category,
                   COALESCE((s.metadata->>'views')::int, 0) as views
            FROM cluster_signals cs
            JOIN signals s ON s.id = cs.signal_id
            WHERE cs.cluster_id = %s
            ORDER BY s.published_at ASC
        """, (cid,))
        signals = cur.fetchall()
        
        if len(signals) < 2:
            continue
        
        root_id, root_title, root_pub, root_source, root_cat, _ = signals[0]
        
        # Find downstream signals from DIFFERENT categories
        for sig_id, down_title, down_pub, down_source, down_cat, down_views in signals[1:]:
            if down_cat == root_cat or not down_cat or not root_cat:
                continue
            if down_source == root_source:
                continue
            
            # Call Gemini
            prompt = FABRICATION_PROMPT.format(
                root_source=root_source, root_category=root_cat, root_title=root_title[:300],
                down_source=down_source, down_category=down_cat, down_title=down_title[:300],
            )
            
            try:
                response = gemini_call(prompt)
                result = parse_gemini_json(response)
                if result is None:
                    print(f"  Cluster {cid}: parse failed, response[:200]: {response[:200]}")
                    continue
            except Exception as e:
                print(f"  Cluster {cid}: Gemini error — {e}")
                continue
            
            checked += 1
            score = result.get("fabrication_score", 0)
            
            if score >= args.min_score:
                alerts += 1
                added = result.get("added_claims", [])
                summary = result.get("summary", "")
                certainty = result.get("certainty_escalation", False)
                emotional = result.get("emotional_amplification", False)
                
                print(f"  ⚠️  Fabrication [{score}/10] cluster {cid}: "
                      f"{root_cat}→{down_cat} | {summary[:80]}")
                
                cur.execute("""
                    INSERT INTO fabrication_alerts
                        (cluster_id, root_signal_id, down_signal_id,
                         root_source, root_category, down_source, down_category,
                         root_title, down_title, fabrication_score,
                         added_claims, certainty_escalation, emotional_amplification,
                         summary, down_views)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (cluster_id, root_signal_id, down_signal_id) DO NOTHING
                """, (cid, root_id, sig_id,
                      root_source, root_cat, down_source, down_cat,
                      root_title[:500], down_title[:500], score,
                      json.dumps(added), certainty, emotional,
                      summary[:500], down_views))
                conn.commit()
            
            # Only check first cross-category pair per cluster to limit API cost
            break
    
    print(f"\nChecked: {checked} clusters")
    print(f"Fabrication alerts (score≥{args.min_score}): {alerts}")
    
    # Summary of all alerts
    cur.execute("""
        SELECT fabrication_score, root_category, down_category, summary, down_views
        FROM fabrication_alerts
        WHERE detected_at >= now() - interval '7 days'
        ORDER BY fabrication_score DESC, down_views DESC
        LIMIT 10
    """)
    recent = cur.fetchall()
    if recent:
        print(f"\nTop fabrication alerts (7d):")
        for score, rcat, dcat, summary, views in recent:
            print(f"  [{score}/10] {rcat}→{dcat} | {views:,} views | {summary[:80]}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
