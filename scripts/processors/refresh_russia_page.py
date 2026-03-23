#!/usr/bin/env python3
"""Refresh /russia page — update numbers in the golden HTML template."""
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import psycopg2

db_url = os.environ.get("DATABASE_URL", "")
GOLDEN = "/dags/config/russia_golden.html"


def fetch_losses():
    url = "https://russianwarship.rip/api/v2/statistics/latest"
    req = Request(url, headers={"User-Agent": "EstWarden/1.0"})
    with urlopen(req, timeout=15) as r:
        d = json.loads(r.read())
    data = d.get("data", {})
    return data.get("stats", {}), data.get("increase", {}), data.get("date", "")


def fmt(n):
    return f"{n:,}"


def replace_between(html, before_marker, after_marker, new_value):
    """Replace text between two literal markers."""
    idx1 = html.find(before_marker)
    if idx1 < 0:
        return html
    start = idx1 + len(before_marker)
    idx2 = html.find(after_marker, start)
    if idx2 < 0:
        return html
    return html[:start] + new_value + html[idx2:]


def replace_ri_row(html, label_text, new_value):
    """Replace value in a ri-r row: finds 'label_text' then updates the ri-r-v span."""
    idx = html.find(label_text)
    if idx < 0:
        return html
    # Find next ri-r-v after this label
    marker = 'class="ri-r-v">'
    vidx = html.find(marker, idx)
    if vidx < 0:
        return html
    vstart = vidx + len(marker)
    vend = html.find("</span>", vstart)
    if vend < 0:
        return html
    return html[:vstart] + new_value + html[vend:]


def replace_hero_num(html, label_after, new_num, new_inc=None):
    """Replace a hero number. Finds the label, then goes back to find the num div."""
    idx = html.find(label_after)
    if idx < 0:
        return html
    # Find the ri-hero-num before this label
    chunk = html[:idx]
    marker = 'class="ri-hero-num"'
    nidx = chunk.rfind(marker)
    if nidx < 0:
        return html
    # Find the > after the style
    gt = html.find(">", nidx)
    if gt < 0:
        return html
    nstart = gt + 1
    nend = html.find("</div>", nstart)
    if nend < 0:
        return html
    html = html[:nstart] + new_num + html[nend:]

    # Update increment if provided
    if new_inc is not None:
        inc_marker = 'class="ri-hero-inc">'
        iidx = html.find(inc_marker, nstart)
        if iidx >= 0:
            istart = iidx + len(inc_marker)
            iend = html.find("</div>", istart)
            if iend >= 0:
                html = html[:istart] + new_inc + html[iend:]
    return html


def main():
    if not os.path.exists(GOLDEN):
        print(f"ERROR: {GOLDEN} not found", file=sys.stderr)
        sys.exit(1)

    with open(GOLDEN) as f:
        html = f.read()

    # Fetch live data
    try:
        stats, inc, loss_date = fetch_losses()
    except Exception as e:
        print(f"ERROR fetching losses: {e}", file=sys.stderr)
        print(html, end="")
        sys.exit(0)

    now = datetime.now(timezone.utc)
    day = (now - datetime(2022, 2, 24, tzinfo=timezone.utc)).days
    ts = now.strftime("%Y-%m-%dT%H:%M UTC")

    # Day counter + timestamp
    html = re.sub(r'Day \d+', f'Day {day}', html)
    html = re.sub(r'Last updated [^<]+', f'Last updated {ts}', html)

    # Hero cards
    p = stats.get("personnel_units", 0)
    pi = inc.get("personnel_units", 0)
    html = replace_hero_num(html, "Personnel eliminated", fmt(p), f"+{fmt(pi)} today")

    t = stats.get("tanks", 0)
    ti = inc.get("tanks", 0)
    html = replace_hero_num(html, "Tanks destroyed", fmt(t), f"+{fmt(ti)} today")

    u = stats.get("uav_systems", 0)
    ui = inc.get("uav_systems", 0)
    html = replace_hero_num(html, "UAVs destroyed", fmt(u), f"+{fmt(ui)} today")

    cost_b = round(p * 0.0005 + t * 3 + u * 0.05 + stats.get("planes", 0) * 50 +
                   stats.get("artillery_systems", 0) * 1 + stats.get("warships_cutters", 0) * 200, 1)
    html = replace_hero_num(html, "Estimated war cost", f"${cost_b:.1f}B")

    # Equipment rows
    equip = {
        "Tanks":             ("tanks", "tanks"),
        "IFVs / APCs":       ("armoured_fighting_vehicles", "armoured_fighting_vehicles"),
        "Vehicles / fuel":   ("vehicles_fuel_tanks", "vehicles_fuel_tanks"),
        "Artillery systems": ("artillery_systems", "artillery_systems"),
        "MLRS":              ("mlrs", "mlrs"),
        "Air defence":       ("aa_warfare_systems", "aa_warfare_systems"),
        "Fixed-wing aircraft": ("planes", "planes"),
        "Helicopters":       ("helicopters", "helicopters"),
        "Warships / cutters": ("warships_cutters", "warships_cutters"),
        "UAV systems":       ("uav_systems", "uav_systems"),
        "Cruise missiles":   ("cruise_missiles", "cruise_missiles"),
        "Special equipment": ("special_military_equip", "special_military_equip"),
    }
    for label, (sk, ik) in equip.items():
        val = stats.get(sk, 0)
        delta = inc.get(ik, 0)
        if delta > 0:
            txt = f'{fmt(val)} <small style="color:var(--red);">+{delta}</small>'
        else:
            txt = fmt(val)
        html = replace_ri_row(html, label, txt)

    # Frontline from DB
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FILTER (WHERE title ~* 'attack|атак|наступ|assault'),
                       COUNT(*) FILTER (WHERE title ~* 'direction|напрям'),
                       COUNT(*) FILTER (WHERE title ~* 'defen|оборон|позиці')
                FROM signals WHERE source_type = 'deepstate'
                  AND published_at >= now() - interval '48 hours'
            """)
            row = cur.fetchone()
            if row:
                html = replace_ri_row(html, "Active combat zones", str(row[0] or 0))
                html = replace_ri_row(html, "Attack directions", str(row[1] or 0))
                html = replace_ri_row(html, "Defensive positions", str(row[2] or 0))
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Frontline skip: {e}", file=sys.stderr)

    print(html, end="")
    print(f"Updated: day={day} personnel={p:,} tanks={t:,} uav={u:,} date={loss_date}", file=sys.stderr)


if __name__ == "__main__":
    main()
