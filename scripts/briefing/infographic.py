#!/usr/bin/env python3
"""Generate the daily briefing infographic as PNG.

Matches the existing estwarden dark theme briefing visual style:
- Dark navy background (#0f172a)
- Blue accent headers (#60a5fa)
- Status-colored indicators (GREEN/YELLOW/ORANGE/RED)
- Indicator table with category, label, finding

Fetches data from /api/briefing/{date}.html and /api/threat-index.

Env vars:
  ESTWARDEN_URL — base URL (default: http://web:8080)
"""
import io
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

from PIL import Image, ImageDraw, ImageFont

ESTWARDEN_URL = os.environ.get("ESTWARDEN_URL", "http://web:8080")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Colors matching the briefing HTML template
BG = (15, 23, 42)         # #0f172a
BG_CARD = (30, 41, 59)    # #1e293b
TEXT = (226, 232, 240)     # #e2e8f0
TEXT_DIM = (148, 163, 184) # #94a3b8
ACCENT = (96, 165, 250)   # #60a5fa
STATUS_COLORS = {
    "GREEN": (74, 222, 128),   # #4ade80
    "YELLOW": (250, 204, 21),  # #facc15
    "ORANGE": (251, 146, 60),  # #fb923c
    "RED": (248, 113, 113),    # #f87171
}
WHITE = (255, 255, 255)


def api_get(path):
    try:
        req = urllib.request.Request(f"{ESTWARDEN_URL}{path}",
                                     headers={"Host": "estwarden.eu"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode()
    except Exception as e:
        print(f"  API error ({path}): {e}", file=sys.stderr)
        return ""


class BriefingParser(HTMLParser):
    """Parse the briefing HTML to extract structured data."""
    def __init__(self):
        super().__init__()
        self.date = ""
        self.cti_text = ""
        self.summary = ""
        self.indicators = []  # [(status, category, label, finding)]
        self._in = None
        self._row = []
        self._td_color = ""

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        cls = d.get("class", "")
        if cls == "date": self._in = "date"
        elif cls == "cti": self._in = "cti"
        elif cls == "summary": self._in = "summary"
        elif tag == "td":
            self._in = "td"
            style = d.get("style", "")
            m = re.search(r'color:(#[0-9a-f]+)', style)
            self._td_color = m.group(1) if m else ""
        elif tag == "tr" and self._in != "header":
            self._row = []

    def handle_endtag(self, tag):
        if tag in ("p", "div") and self._in in ("date", "cti", "summary"):
            self._in = None
        elif tag == "td":
            self._in = None
        elif tag == "tr" and len(self._row) == 4:
            self.indicators.append(tuple(self._row))
            self._row = []

    def handle_data(self, data):
        data = data.strip()
        if not data: return
        if self._in == "date": self.date = data
        elif self._in == "cti": self.cti_text += data
        elif self._in == "summary": self.summary += data + " "
        elif self._in == "td":
            self._row.append(data)


def try_font(size, bold=False):
    """Try to load a font, fall back to default."""
    names = [
        "/usr/share/fonts/truetype/freefont/FreeSans" + ("Bold" if bold else "") + ".ttf",
        "/usr/share/fonts/ttf-freefont/FreeSans" + ("Bold" if bold else "") + ".ttf",
        "/usr/share/fonts/freefont/FreeSans" + ("Bold" if bold else "") + ".ttf",
    ]
    for name in names:
        try:
            return ImageFont.truetype(name, size)
        except (IOError, OSError):
            continue
    return ImageFont.load_default()


def wrap_text(draw, text, font, max_width):
    """Word-wrap text to fit within max_width."""
    words = text.split()
    lines = []
    current = ""
    for w in words:
        test = (current + " " + w).strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = w
    if current:
        lines.append(current)
    return lines


def generate_infographic(date=None):
    """Generate the infographic PNG and return bytes."""
    date = date or TODAY

    # Fetch briefing HTML
    html = api_get(f"/api/briefing/{date}.html")
    if not html:
        print("ERROR: Could not fetch briefing HTML", file=sys.stderr)
        return None

    # Parse HTML
    parser = BriefingParser()
    parser.feed(html)

    # Also get current CTI for the score
    cti_json = api_get("/api/threat-index")
    cti = json.loads(cti_json) if cti_json else {}

    level = cti.get("level", "GREEN")
    score = cti.get("score", 0)
    trend = cti.get("trend", "STABLE")

    # Fonts
    font_title = try_font(28, bold=True)
    font_heading = try_font(22, bold=True)
    font_cti = try_font(36, bold=True)
    font_body = try_font(15)
    font_body_bold = try_font(15, bold=True)
    font_small = try_font(12)
    font_footer = try_font(11)

    # Layout constants
    W = 800
    MARGIN = 32
    CONTENT_W = W - 2 * MARGIN

    # Pre-calculate height
    summary_lines = wrap_text(ImageDraw.Draw(Image.new("RGB", (1, 1))),
                               parser.summary.strip(), font_body, CONTENT_W - 32)

    indicator_count = min(len(parser.indicators), 12)
    H = (
        MARGIN +        # top
        40 +            # title
        20 +            # date
        60 +            # CTI score
        20 +            # gap
        len(summary_lines) * 22 + 32 +  # summary box
        20 +            # gap
        30 +            # "Indicators" heading
        indicator_count * 52 +  # indicators
        40 +            # footer
        MARGIN          # bottom
    )

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    y = MARGIN

    # Title
    draw.text((MARGIN, y), "🛡 EstWarden Daily Briefing", fill=ACCENT, font=font_title)
    y += 40

    # Date
    draw.text((MARGIN, y), date, fill=TEXT_DIM, font=font_body)
    y += 28

    # CTI Score — big number with level color
    level_color = STATUS_COLORS.get(level, TEXT)
    cti_text = f"Threat Index: {score:.0f}/100 — {level}"
    draw.text((MARGIN, y), cti_text, fill=level_color, font=font_cti)
    y += 50

    # Trend arrow
    trend_sym = {"RISING": "↑ Rising", "FALLING": "↓ Falling", "STABLE": "→ Stable"}.get(trend, trend)
    draw.text((MARGIN, y), trend_sym, fill=TEXT_DIM, font=font_body)
    y += 28

    # Summary box
    summary = parser.summary.strip()
    if summary:
        box_h = len(summary_lines) * 22 + 24
        draw.rounded_rectangle(
            [MARGIN, y, W - MARGIN, y + box_h],
            radius=8, fill=BG_CARD
        )
        ty = y + 12
        for line in summary_lines:
            draw.text((MARGIN + 16, ty), line, fill=TEXT, font=font_body)
            ty += 22
        y += box_h + 16

    # Indicators heading
    draw.text((MARGIN, y), "Indicators", fill=ACCENT, font=font_heading)
    y += 32

    # Indicator rows
    for status, category, label, finding in parser.indicators[:12]:
        status_color = STATUS_COLORS.get(status, TEXT_DIM)

        # Status pill
        pill_w = 70
        draw.rounded_rectangle(
            [MARGIN, y + 2, MARGIN + pill_w, y + 22],
            radius=4, fill=(*status_color, 40)
        )
        draw.text((MARGIN + 6, y + 4), status, fill=status_color, font=font_small)

        # Category + Label
        draw.text((MARGIN + pill_w + 10, y + 2), f"{category}", fill=TEXT_DIM, font=font_small)
        draw.text((MARGIN + pill_w + 10, y + 18), label, fill=WHITE, font=font_body_bold)

        # Finding (truncated)
        finding_lines = wrap_text(draw, finding, font_small, CONTENT_W - pill_w - 20)
        for i, fl in enumerate(finding_lines[:2]):
            draw.text((MARGIN + pill_w + 10, y + 34 + i * 16), fl, fill=TEXT_DIM, font=font_small)

        y += 52

    # Footer
    y += 8
    draw.line([(MARGIN, y), (W - MARGIN, y)], fill=(51, 65, 85), width=1)
    y += 8
    draw.text((MARGIN, y), f"Generated by estwarden.eu — Baltic Security Monitor", fill=(71, 85, 105), font=font_footer)

    # Save to bytes
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else TODAY
    outfile = sys.argv[2] if len(sys.argv) > 2 else f"/tmp/briefing-{date}.png"

    print(f"▸ Generating infographic for {date}...")
    png_bytes = generate_infographic(date)
    if png_bytes:
        with open(outfile, "wb") as f:
            f.write(png_bytes)
        print(f"✓ Saved {len(png_bytes):,} bytes → {outfile}")
    else:
        print("✗ Failed to generate infographic", file=sys.stderr)
        sys.exit(1)
