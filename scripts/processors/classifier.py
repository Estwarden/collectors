#!/usr/bin/env python3
"""Narrative classifier for EstWarden.

Fetches untagged signals via Data API, sends batches to LLM for classification,
submits results back via Data API.

Environment:
    ESTWARDEN_API_URL, ESTWARDEN_API_KEY — Data API
    OPENROUTER_API_KEY — LLM API key
    LLM_MODEL — model name (default: qwen/qwen3-235b-a22b-2507)
"""

import json
import os
import re
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

SYSTEM_PROMPT = """You classify information operations targeting European NATO member states.

## GEOGRAPHIC SCOPE — MOST IMPORTANT RULE
Only classify signals ABOUT or TARGETING Estonia, Latvia, Lithuania, Finland, Poland,
or the broader NATO eastern flank. If about Middle East, domestic politics of
non-European countries, or unrelated global issues → return empty narratives [].

## Narrative codes
  N1 — Russophobia / Persecution (claims of anti-Russian discrimination)
  N2 — War Escalation Panic (fear-mongering about imminent conflict)
  N3 — Aid = Theft (Ukraine support wastes money)
  N4 — Delegitimization (EU/NATO leaders corrupt/incompetent)
  N5 — Isolation / Victimhood (minority communities ignored/oppressed)
  FI1 — Finland NATO Aggressor (Finland joined NATO for offensive purposes)
  FI2 — Finland Border Brutality (border closure is inhumane/Russophobic)
  FI3 — Finland Historical Guilt (WWII Nazi ally, owes territory to Russia)
  PL1 — Anti-Ukrainian Refugees (Ukrainians exploit/abuse Poland)
  PL2 — Poland US Puppet (Poland is American colony/Trojan horse)
  PL3 — Poland Russophobia Hysteria (security concerns are paranoid)
  PL4 — Poland-Belarus Border (border response is inhumane)

## Output JSON schema
{"classifications": [{"signal_id": 123, "narratives": [{"code": "N1", "confidence": 0.85, "target_countries": ["EE","LV"]}]}]}

## Rules
- Return ONLY valid JSON, no markdown fences
- MOST signals should have empty narratives [] — be very selective
- Only tag if confidence >= 0.7
- Analyze FRAMING, not topic
- Include target_countries: which specific countries are targeted (ISO 2-letter codes: EE, LV, LT, FI, PL)
- Use FI1-FI3 codes ONLY for Finland-specific narratives
- Use PL1-PL4 codes ONLY for Poland-specific narratives
- If narrative targets all NATO/EU → use ["EU"]
- When in doubt → empty narratives []
"""

VALID_CODES = {"N1", "N2", "N3", "N4", "N5", "FI1", "FI2", "FI3", "PL1", "PL2", "PL3", "PL4"}
MIN_CONFIDENCE = 0.70


def classify_batch(signals, api_key, model):
    """Send a batch of signals to LLM, return parsed classifications."""
    # Format signals for prompt
    items = []
    for s in signals:
        items.append(f"[ID:{s['id']}] {s.get('title', '')} — {s.get('content', '')[:300]}")

    user_prompt = "Classify these signals:\n\n" + "\n\n".join(items)

    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 2000,
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

    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())

    text = resp["choices"][0]["message"]["content"]

    # Strip <think> blocks
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Extract JSON
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        print(f"  No JSON in LLM response", file=sys.stderr)
        return []

    data = json.loads(match.group())
    return data.get("classifications", [])


def main():
    client = EstWardenClient()
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    model = os.environ.get("LLM_MODEL", "qwen/qwen3-235b-a22b-2507")

    if not api_key:
        print("OPENROUTER_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    # Fetch untagged signals
    signals = client.query_untagged(limit=30)
    if not signals:
        print("No untagged signals")
        return

    print(f"Classifying {len(signals)} signals...")

    # Process in batches of 10 (LLM context limits)
    total_tags = 0
    for i in range(0, len(signals), 10):
        batch = signals[i:i + 10]
        try:
            classifications = classify_batch(batch, api_key, model)
        except Exception as e:
            print(f"  Batch {i // 10}: LLM error — {e}", file=sys.stderr)
            continue

        # Build tags list
        tags = []
        for c in classifications:
            for n in c.get("narratives", []):
                code = n.get("code", "")
                conf = n.get("confidence", 0)
                if code in VALID_CODES and conf >= MIN_CONFIDENCE:
                    tags.append({
                        "signal_id": c["signal_id"],
                        "code": code,
                        "confidence": conf,
                        "tagged_by": "llm:pipeline",
                    })

        if tags:
            result = client.ingest_tags(tags)
            total_tags += result.get("inserted", 0)
            print(f"  Batch {i // 10}: {result.get('inserted', 0)} tags inserted")
        else:
            print(f"  Batch {i // 10}: no narratives detected (all clean)")

    print(f"\nTotal: {total_tags} tags created from {len(signals)} signals")


if __name__ == "__main__":
    main()
