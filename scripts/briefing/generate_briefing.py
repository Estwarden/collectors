#!/usr/bin/env python3
"""Generate daily intelligence briefing and send to Telegram.

Uses the estwarden public API (no direct DB access needed).

Env vars:
  ESTWARDEN_URL — base URL (default: http://web:8080)
  OPENAI_API_KEY — for LLM generation
  TELEGRAM_BOT_TOKEN — Telegram bot
  TELEGRAM_CHAT_ID — channel ID
  TELEGRAM_ADMIN_ID — admin chat for preview
"""
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

ESTWARDEN_URL = os.environ.get("ESTWARDEN_URL", "http://web:8080")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ADMIN_ID = os.environ.get("TELEGRAM_ADMIN_ID", "")
LLM_MODEL = os.environ.get("BRIEFING_MODEL", "gpt-4o-mini")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def api_get(path):
    """Fetch from estwarden API."""
    url = f"{ESTWARDEN_URL}{path}"
    try:
        req = urllib.request.Request(url, headers={"Host": "estwarden.eu"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  API error ({path}): {e}", file=sys.stderr)
        return {}


def fetch_briefing_data():
    """Gather all data needed for the briefing from the API."""
    data = {}

    # Threat index (current)
    data["cti"] = api_get("/api/threat-index")

    # CTI history (30 days)
    data["cti_history"] = api_get("/api/threat-index/history")

    return data


def generate_briefing_text(data):
    """Generate briefing text via LLM."""
    cti = data.get("cti", {})

    level = cti.get("level", "UNKNOWN")
    score = cti.get("score", 0)
    trend = cti.get("trend", "STABLE")

    # Build context from available data
    cti_history = data.get("cti_history", [])

    context = f"""EstWarden Baltic Security Monitor — Daily Intelligence Report
Date: {TODAY}
Threat Level: {level} ({score:.1f}/100) — Trend: {trend}

"""

    # CTI history (last 7 days)
    if cti_history:
        recent = cti_history[-7:]
        context += "CTI History (last 7 days):\n"
        for h in recent:
            context += f"  {h.get('date', '?')}: {h.get('level', '?')} ({h.get('score', 0):.1f})\n"
        context += "\n"

        # Calculate week-over-week change
        if len(cti_history) >= 2:
            latest = cti_history[-1].get("score", 0)
            prev = cti_history[-2].get("score", 0)
            if prev > 0:
                pct = ((latest - prev) / prev) * 100
                context += f"Day-over-day change: {pct:+.1f}%\n\n"

    # Generate via LLM
    prompt = f"""Generate a concise daily intelligence briefing for the EstWarden Baltic Security Monitor Telegram channel.

FORMAT RULES:
- Use Telegram HTML formatting (<b>, <i>, <code>)
- Start with a status emoji based on threat level: 🟢 GREEN, 🟡 YELLOW, 🟠 ORANGE, 🔴 RED
- Keep under 1500 characters total
- Structure: threat level → key changes → notable signals → outlook
- Professional but accessible tone
- End with: 🔗 https://estwarden.eu
- Do NOT use markdown, only HTML tags

DATA:
{context}"""

    body = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You are the EstWarden intelligence briefing writer. You produce concise, data-driven daily security briefings for the Baltic region. Use ONLY Telegram HTML formatting (<b>, <i>, <code>). Never use markdown."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 800
    }).encode()

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_KEY}"
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            return result["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"LLM error: {e}", file=sys.stderr)
        return generate_fallback_briefing(data)


def generate_fallback_briefing(data):
    """Simple briefing without LLM."""
    cti = data.get("cti", {})
    level = cti.get("level", "UNKNOWN")
    score = cti.get("score", 0)
    trend = cti.get("trend", "STABLE")
    emoji = {"GREEN": "🟢", "YELLOW": "🟡", "ORANGE": "🟠", "RED": "🔴"}.get(level, "⚪")
    trend_arrow = {"RISING": "↑", "FALLING": "↓", "STABLE": "→"}.get(trend, "→")

    lines = [
        f"{emoji} <b>EstWarden Daily Brief — {TODAY}</b>",
        f"",
        f"<b>Threat Level:</b> {level} ({score:.1f}/100) {trend_arrow} {trend}",
    ]

    # CTI history context
    cti_history = data.get("cti_history", [])
    if len(cti_history) >= 2:
        prev = cti_history[-2]
        lines.append(f"<b>Previous:</b> {prev.get('level', '?')} ({prev.get('score', 0):.1f}/100)")

    lines.append(f"\n🔗 https://estwarden.eu")
    return "\n".join(lines)


def send_telegram(text, chat_id):
    """Send message to Telegram."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true"
    }).encode()

    req = urllib.request.Request(url, data=body)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                print(f"✓ Sent to {chat_id}")
                return True
            else:
                print(f"✗ Telegram error: {result}", file=sys.stderr)
                return False
    except Exception as e:
        print(f"✗ Telegram send failed: {e}", file=sys.stderr)
        return False


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "preview"

    if not BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN not set", file=sys.stderr)
        sys.exit(1)

    print(f"▸ Fetching briefing data for {TODAY}...")
    data = fetch_briefing_data()

    cti = data.get("cti", {})
    print(f"  CTI: {cti.get('level', '?')} ({cti.get('score', 0):.1f}/100)")

    if OPENAI_KEY:
        print(f"▸ Generating briefing via {LLM_MODEL}...")
        text = generate_briefing_text(data)
    else:
        print("▸ No LLM key — using fallback template...")
        text = generate_fallback_briefing(data)

    print(f"\n{'─'*60}")
    print(text)
    print(f"{'─'*60}\n")

    if mode == "publish":
        print("▸ Publishing to channel...")
        send_telegram(text, CHANNEL_ID)
    elif mode == "preview":
        target = ADMIN_ID or CHANNEL_ID
        print(f"▸ Sending preview to admin ({target})...")
        send_telegram(text, target)
    else:
        print("▸ Dry run — not sending.")


if __name__ == "__main__":
    main()
