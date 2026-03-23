#!/usr/bin/env python3
"""Regenerate /russia page HTML from live database data.

Outputs HTML to stdout. The DAG pipes it into redis-cli SET.
"""
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2

db_url = os.environ.get("DATABASE_URL", "")

def esc(s):
    if not s:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def main():
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    war_day = (now - datetime(2022, 2, 24, tzinfo=timezone.utc)).days
    updated = now.strftime("%Y-%m-%d %H:%M UTC")

    # CTI
    cur.execute("""
        SELECT score, level, components::text FROM threat_index_cache
        WHERE region = 'baltic' ORDER BY computed_at DESC LIMIT 1
    """)
    r = cur.fetchone()
    cti_score, cti_level = (float(r[0]), r[1]) if r else (0, "UNKNOWN")
    comps = json.loads(r[2]) if r and r[2] else {}
    lc = {"GREEN": "var(--green)", "YELLOW": "var(--yellow)",
          "ORANGE": "var(--orange)", "RED": "var(--red)"}.get(cti_level, "var(--text-2)")

    # Campaigns
    cur.execute("""
        SELECT name, severity, detection_method, event_fact,
               (SELECT COUNT(*) FROM campaign_signals cs WHERE cs.campaign_id = c.id)
        FROM campaigns c WHERE status = 'ACTIVE'
          AND detection_method IS NOT NULL AND detection_method != ''
          AND detected_at >= now()-interval '14 days'
        ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
        LIMIT 5
    """)
    campaigns = cur.fetchall()

    # DeepState
    cur.execute("""
        SELECT title, published_at FROM signals
        WHERE source_type = 'deepstate' AND published_at >= now()-interval '48 hours'
        ORDER BY published_at DESC LIMIT 8
    """)
    frontline = cur.fetchall()

    # GPS
    cur.execute("""
        SELECT title, published_at FROM signals
        WHERE source_type = 'gpsjam' AND published_at >= now()-interval '7 days'
        ORDER BY published_at DESC LIMIT 3
    """)
    gps = cur.fetchall()

    # FIRMS Russian bases
    cur.execute("""
        SELECT title, published_at FROM signals
        WHERE source_type = 'firms' AND published_at >= now()-interval '7 days'
          AND title ~* 'kaliningrad|pskov|kronstadt|murmansk|pechenga|severomorsk|alakurtti'
        ORDER BY published_at DESC LIMIT 5
    """)
    firms = cur.fetchall()

    # Loss reports
    cur.execute("""
        SELECT title, url, published_at FROM signals
        WHERE source_type = 'rss' AND published_at >= now()-interval '48 hours'
          AND (title ~* 'general staff.*(report|update|enemy|losses)'
               OR title ~* 'ліквідовано|знищено.*(ворог|окупант|техніки|росі)'
               OR title ~* 'потери.*(росси|оккупант|противник)'
               OR title ~* 'enemy losses|russian (losses|casualties)')
        ORDER BY published_at DESC LIMIT 5
    """)
    losses = cur.fetchall()

    # Econ
    cur.execute("""
        SELECT title, url, published_at FROM signals
        WHERE source_type IN ('rss','business','energy') AND published_at >= now()-interval '7 days'
          AND (title ~* 'рубл|ruble|курс.*(доллар|евро|юан)'
               OR title ~* 'ЦБ.*(ставк|процент)|CBR.*(rate|key)'
               OR title ~* 'росси.*(инфляц|экономик|бюджет|дефицит)'
               OR title ~* 'russian.*(economy|inflation|budget|deficit)'
               OR title ~* 'нефт.*(росси|urals|цена)|oil.*(russia|urals|price)'
               OR title ~* 'газ.*(росси|газпром)|gazprom'
               OR title ~* 'санкци.*(росси|против)|sanction.*(russia|against)')
        ORDER BY published_at DESC LIMIT 7
    """)
    econ = cur.fetchall()

    # Freshness
    cur.execute("""
        SELECT source_type,
               COUNT(*) FILTER (WHERE published_at > now()-interval '24h'),
               MAX(published_at)
        FROM signals
        WHERE source_type IN ('rss','telegram_channel','deepstate','gpsjam','firms','adsb','ais')
        GROUP BY source_type
    """)
    fresh = {}
    for st, cnt, latest in cur.fetchall():
        hrs = (now - latest.replace(tzinfo=timezone.utc if latest.tzinfo is None else latest.tzinfo)).total_seconds() / 3600 if latest else 999
        fresh[st] = (cnt, f"{hrs:.0f}h", "var(--green)" if hrs < 24 else ("var(--yellow)" if hrs < 72 else "var(--red)"))

    sev_c = {"CRITICAL": "#e74c3c", "HIGH": "#e67e22", "MEDIUM": "#f1c40f", "LOW": "#2ecc71"}

    def ts(dt):
        return dt.strftime("%b %d %H:%M") if dt else ""

    def tsd(dt):
        return dt.strftime("%b %d") if dt else ""

    s = []  # sections

    # Hero
    s.append(f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:16px;margin:0 0 32px;">')
    for val, label in [(f"{cti_score:.1f}", f"Baltic CTI ({cti_level})"),
                       (str(war_day), "Day of invasion"),
                       (str(len(campaigns)), "Active campaigns"),
                       (str(len(frontline)), "Frontline updates (48h)")]:
        color = lc if "CTI" in label else "var(--text-0)"
        s.append(f'<div style="text-align:center;"><div style="font-size:28px;font-weight:800;color:{color};">{val}</div><div style="font-size:12px;color:var(--text-2);">{label}</div></div>')
    s.append('</div>')

    # CTI bars
    s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">Threat Breakdown</div>')
    for c in ['security', 'fimi', 'hybrid', 'economic']:
        v = float(comps.get(c, 0))
        pct = min(v / 25 * 100, 100)
        s.append(f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0;"><span style="width:80px;font-size:13px;color:var(--text-2);">{c}</span><div style="flex:1;height:8px;background:var(--bg-2);border-radius:4px;"><div style="width:{pct:.0f}%;height:100%;background:{lc};border-radius:4px;"></div></div><span style="width:40px;font-size:13px;text-align:right;">{v:.1f}</span></div>')
    s.append('</div>')

    # Campaigns
    if campaigns:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">🎯 Active Influence Campaigns</div>')
        for name, sev, method, fact, sc in campaigns:
            s.append(f'<div style="border-left:3px solid {sev_c.get(sev,"#888")};padding:8px 12px;margin:8px 0;background:var(--bg-1);border-radius:0 6px 6px 0;"><div style="font-size:14px;font-weight:600;">{esc(name[:80])}</div><div style="font-size:12px;color:var(--text-2);margin-top:4px;">{sev} · {method or "detected"} · {sc} signals</div>{"<div style=font-size:12px;margin-top:4px;>" + esc((fact or "")[:150]) + "</div>" if fact else ""}</div>')
        s.append('</div>')

    # Frontline
    if frontline:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">⚔️ Frontline (48h)</div>')
        for title, pub in frontline:
            s.append(f'<div style="margin:6px 0;font-size:13px;"><span style="color:var(--text-3);font-size:11px;">{ts(pub)}</span> {esc((title or "")[:120])}</div>')
        s.append('</div>')

    # GPS
    if gps:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">📡 GPS Interference (7d)</div>')
        for title, pub in gps:
            s.append(f'<div style="margin:6px 0;font-size:13px;"><span style="color:var(--text-3);font-size:11px;">{tsd(pub)}</span> {esc((title or "")[:100])}</div>')
        s.append('</div>')

    # FIRMS
    if firms:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">🛰️ Russian Base Thermal Activity (7d)</div>')
        for title, pub in firms:
            s.append(f'<div style="margin:6px 0;font-size:13px;"><span style="color:var(--text-3);font-size:11px;">{tsd(pub)}</span> {esc((title or "")[:100])}</div>')
        s.append('</div>')

    # Losses
    if losses:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">📊 Equipment Loss Reports (48h)</div>')
        for title, url, pub in losses:
            s.append(f'<div style="margin:6px 0;font-size:13px;"><span style="color:var(--text-3);font-size:11px;">{tsd(pub)}</span> <a href="{esc(url or "")}" target="_blank" rel="noopener" style="color:var(--accent);">{esc((title or "")[:100])}</a></div>')
        s.append('</div>')

    # Econ
    if econ:
        s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">💰 Russian Economy (7d)</div>')
        for title, url, pub in econ:
            s.append(f'<div style="margin:6px 0;font-size:13px;"><span style="color:var(--text-3);font-size:11px;">{tsd(pub)}</span> <a href="{esc(url or "")}" target="_blank" rel="noopener" style="color:var(--accent);">{esc((title or "")[:100])}</a></div>')
        s.append('</div>')

    # Freshness
    s.append('<div style="margin:0 0 24px;"><div style="font-size:15px;font-weight:700;margin:0 0 12px;">📶 Source Freshness</div>')
    for st in ['rss', 'telegram_channel', 'deepstate', 'gpsjam', 'firms', 'adsb', 'ais']:
        cnt, age, col = fresh.get(st, (0, "no data", "var(--red)"))
        s.append(f'<div style="display:flex;justify-content:space-between;margin:3px 0;font-size:13px;"><span>{st}</span><span style="color:{col};">{cnt}/24h · {age}</span></div>')
    s.append('</div>')

    total_24h = sum(v[0] for v in fresh.values())
    html = f"""<main>
<div style="max-width:720px;margin:0 auto;padding:24px 16px;">
  <h1 style="font-size:22px;font-weight:800;margin:0 0 4px;">Russia Intelligence — Day {war_day}</h1>
  <div style="font-size:12px;color:var(--text-3);margin:0 0 24px;">Updated {updated}</div>
  {''.join(s)}
  <div style="font-size:11px;color:var(--text-3);margin-top:32px;border-top:1px solid var(--bg-2);padding-top:12px;">
    Auto-generated from {total_24h:,} signals collected in the last 24 hours.
  </div>
</div>
</main>"""

    # Output for piping to redis-cli
    print(html, end='')

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
