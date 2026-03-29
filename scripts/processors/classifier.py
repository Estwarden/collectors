#!/usr/bin/env python3
"""Narrative classifier for EstWarden.

Fetches untagged signals via Data API, sends batches to LLM for classification,
submits results back via Data API.

KEY DESIGN DECISIONS:
  - Uses full taxonomy (20 specific narrative IDs), NOT broad N1-N5 buckets
  - Passes source credibility context (category, tier) to the LLM
  - Trusted media reporting on social issues is NOT disinformation
  - LLM can also propose NEW narrative slugs for emerging patterns
  - N1-N5 codes are deprecated — we store specific IDs like 'russian_speakers_oppressed'

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

import yaml

sys.path.insert(0, os.path.join("/dags/scripts/lib"))
from estwarden_client import ingest_signals

TAXONOMY_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "narrative_taxonomy.yaml")

# Map old N-codes to narrative IDs for backward compatibility in queries
LEGACY_CODE_MAP = {
    "N1": ["russian_speakers_oppressed", "nazi_baltic"],
    "N2": ["baltic_attack_imminent", "nuclear_threat", "suwalki_gap", "nato_provocation"],
    "N3": ["western_fatigue", "sanctions_backfire"],
    "N4": ["article5_failure", "hybrid_attacks_staged", "gps_jamming_false_flag"],
    "N5": ["baltic_failed_states", "migration_weapon"],
}


def load_taxonomy():
    """Load narrative taxonomy and build the classifier prompt section."""
    try:
        with open(TAXONOMY_PATH) as f:
            data = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: could not load taxonomy: {e}", file=sys.stderr)
        return [], ""

    narratives = data.get("narratives", [])
    lines = []
    for n in narratives:
        regions = ", ".join(n.get("target_regions", ["global"]))
        lines.append(f"  {n['id']} — {n['theme']} (targets: {regions})")
        lines.append(f"    {n.get('description', '')[:150]}")

    return narratives, "\n".join(lines)


def build_system_prompt(taxonomy_text):
    return f"""You are a disinformation analyst classifying information operations targeting European NATO member states (Estonia, Latvia, Lithuania, Finland, Poland, NATO eastern flank).

## YOUR JOB
Identify when a signal is PART OF an information operation — meaning it uses manipulative FRAMING to advance a hostile narrative. Reporting facts is NOT disinformation.

## CRITICAL DISTINCTION — SOURCE CONTEXT
You will receive the SOURCE CATEGORY for each signal. This is essential:

- **trusted / estonian_media / baltic_media / finnish_media / polish_media / ukraine_media / counter_disinfo / government**: These are legitimate media. They report on real problems (housing costs, poverty, demographic decline). Reporting facts is NOT disinformation. Only tag if the framing itself is manipulative (extremely rare for trusted sources).

- **russian_state / russian_language_ee**: State-controlled or state-adjacent media. When they report on Baltic social problems, the FRAMING often serves hostile narratives (e.g., "Baltic states are failing" vs neutral reporting on housing costs). Evaluate framing, not topic.

- **russian_independent**: Independent Russian media (e.g., Meduza). Treat like trusted — they report critically on Russia too. Only tag if framing is clearly hostile.

- **other / defense_osint / military / civilian**: Context-dependent. Evaluate framing carefully.

## NARRATIVE TAXONOMY
Use these specific narrative IDs (NOT generic N1-N5 codes):

{taxonomy_text}

## NEW/EMERGING NARRATIVES
If you detect a clear disinformation framing that doesn't match any existing narrative, you may propose a new slug:
- Format: lowercase_with_underscores, descriptive (e.g., "narva_republic_separatism", "kalinka_cancel_culture")
- Only for CLEAR manipulation patterns, not vague discomfort
- Include a brief "theme" description

## OUTPUT FORMAT
Return ONLY valid JSON, no markdown fences, no explanation:
{{"classifications": [
  {{"signal_id": 123, "narratives": [
    {{"code": "russian_speakers_oppressed", "confidence": 0.85, "target_countries": ["EE","LV"]}}
  ]}}
]}}

## RULES
1. MOST signals should have empty narratives [] — be VERY selective
2. Minimum confidence: 0.75
3. Trusted media reporting facts = empty narratives (not disinformation)
4. Russian state media using hostile FRAMING on same facts = may be disinformation
5. Analyze FRAMING and INTENT, not just topic keywords
6. Each signal can match 0-2 narratives (rarely more)
7. Include target_countries: ISO 2-letter codes (EE, LV, LT, FI, PL) or ["EU"] for broad
8. When in doubt → empty narratives []
9. Do NOT use legacy codes N1, N2, N3, N4, N5 — use specific narrative IDs from taxonomy
"""


# All narrative IDs from taxonomy + legacy N-codes (for the ingest API validator)
def get_valid_codes(narratives):
    codes = {"N1", "N2", "N3", "N4", "N5",
             "FI1", "FI2", "FI3", "PL1", "PL2", "PL3", "PL4"}
    for n in narratives:
        codes.add(n["id"])
    return codes


MIN_CONFIDENCE = 0.75


def classify_batch(signals, api_key, model, system_prompt):
    """Send a batch of signals to LLM, return parsed classifications."""
    items = []
    for s in signals:
        meta = s.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}

        category = meta.get("category", "other")
        feed = meta.get("feed_handle", meta.get("channel", "unknown"))
        source_info = f"[source: {feed}, category: {category}]"

        content = s.get("content", "") or ""
        title = s.get("title", "") or ""
        text = f"{title}\n{content[:400]}" if content else title

        items.append(f"[ID:{s['id']}] {source_info}\n{text}")

    user_prompt = "Classify these signals:\n\n" + "\n\n---\n\n".join(items)

    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 3000,
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

    with urllib.request.urlopen(req, timeout=90) as r:
        resp = json.loads(r.read())

    text = resp["choices"][0]["message"]["content"]

    # Strip <think> blocks (reasoning models)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Extract JSON
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        print(f"  No JSON in LLM response: {text[:200]}", file=sys.stderr)
        return []

    data = json.loads(match.group())
    return data.get("classifications", [])


def main():
    # Using flat API
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    model = os.environ.get("LLM_MODEL", "qwen/qwen3-235b-a22b-2507")

    if not api_key:
        print("OPENROUTER_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    # Load taxonomy
    narratives, taxonomy_text = load_taxonomy()
    valid_codes = get_valid_codes(narratives)
    system_prompt = build_system_prompt(taxonomy_text)

    print(f"Loaded {len(narratives)} narrative templates")

    # Fetch untagged signals — include telegram_channel and youtube_transcript
    source_types = [
        "rss", "telegram", "telegram_channel", "youtube", "youtube_transcript",
        "milwatch", "ru_legislation", "deepstate", "gdelt",
    ]
    signals = query_untagged(source_types=source_types, limit=120)
    if not signals:
        print("No untagged signals")
        return

    print(f"Classifying {len(signals)} signals...")

    # Process in batches of 10
    total_tags = 0
    batch_size = 10
    for i in range(0, len(signals), batch_size):
        batch = signals[i:i + batch_size]
        try:
            classifications = classify_batch(batch, api_key, model, system_prompt)
        except Exception as e:
            print(f"  Batch {i // batch_size}: LLM error — {e}", file=sys.stderr)
            continue

        # Build tags list
        tags = []
        for c in classifications:
            for n in c.get("narratives", []):
                code = n.get("code", "")
                conf = n.get("confidence", 0)

                # Accept taxonomy IDs and new proposed slugs
                # Reject if confidence too low
                if conf < MIN_CONFIDENCE:
                    continue

                # Allow known codes + any valid-looking slug (lowercase_with_underscores)
                if code not in valid_codes and not re.match(r'^[a-z][a-z0-9_]{2,50}$', code):
                    print(f"    Rejected invalid code: {code}", file=sys.stderr)
                    continue

                tags.append({
                    "signal_id": c["signal_id"],
                    "code": code,
                    "confidence": conf,
                    "tagged_by": "llm:pipeline:v2",
                })

        if tags:
            result = ingest_tags(tags)
            total_tags += result.get("inserted", 0)
            codes_summary = ", ".join(set(t["code"] for t in tags))
            print(f"  Batch {i // batch_size}: {result.get('inserted', 0)} tags ({codes_summary})")
        else:
            print(f"  Batch {i // batch_size}: no disinformation detected (all clean)")

    print(f"\nTotal: {total_tags} tags created from {len(signals)} signals")


if __name__ == "__main__":
    main()
