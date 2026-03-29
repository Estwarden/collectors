#!/usr/bin/env python3
"""Generate a minimal, fail-closed daily report for the public dashboard.

Design goals:
- Never invent "all clear" claims from missing data.
- Only publish when core collectors are fresh.
- Surface stale coverage explicitly in indicators/summary.
- Write the smallest valid daily_reports + indicators payload the Go web app needs.

Environment:
    ESTWARDEN_API_URL — Ingest API base URL (http://ingest:8090)
    ESTWARDEN_API_KEY — Pipeline API key

Usage:
    python3 report_generator.py [--date YYYY-MM-DD]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join("/dags/scripts/lib"))
from estwarden_client import query_report_data,write_report

STATUS_RANK = {"GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}

DEFAULT_URLS = {
    "gpsjam": "https://gpsjam.org",

    "satellite_analysis": "https://estwarden.eu/intelligence",
}


def fmt_dt(s):
    if not s:
        return "never"
    return s[:19].replace("T", " ") + " UTC"


def clamp_status(s):
    return s if s in STATUS_RANK else "YELLOW"


def max_status(statuses):
    best = "GREEN"
    for s in statuses:
        if STATUS_RANK.get(clamp_status(s), 0) > STATUS_RANK[best]:
            best = clamp_status(s)
    return best


def source_from_api(freshness, key, fresh_hours):
    """Build source stats dict from API freshness data."""
    sf = freshness.get(key, {})
    latest = sf.get("latest", "")
    url = sf.get("url", "") or DEFAULT_URLS.get(key, "")
    count_24h = sf.get("count_24h", 0)
    avg_7d = sf.get("avg_7d", 0)
    stddev_7d = sf.get("stddev_7d", 0)

    fresh = False
    if latest and latest != "never":
        try:
            dt = datetime.fromisoformat(latest.replace(" ", "T").rstrip("Z+00"))
            dt = dt.replace(tzinfo=timezone.utc)
            fresh = dt > datetime.now(timezone.utc) - timedelta(hours=fresh_hours)
        except (ValueError, TypeError):
            pass

    z = 0.0
    if avg_7d > 0:
        z = max((count_24h - avg_7d) / max(stddev_7d, 1.0), 0.0)

    return {
        "latest": latest, "url": url, "count_24h": count_24h,
        "avg_7d": avg_7d, "stddev_7d": stddev_7d, "zscore": z, "fresh": fresh,
    }


def append_indicator(items, *, status, category, label, finding, source_url="",
                     source_count=0, confidence="MEDIUM", target_region="global"):
    items.append({
        "status": clamp_status(status), "category": category,
        "label": label, "finding": finding,
        "source_url": source_url or None, "source_count": int(source_count or 0),
        "confidence": confidence, "target_region": target_region,
    })


def main():
    parser = argparse.ArgumentParser(description="Generate daily report")
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = parser.parse_args()

    # Using flat API
    resp = query_report_data(region="baltic")
    data = resp.get("data", {})
    freshness = data.get("source_freshness", {})

    # ── CTI ──
    cti = data.get("cti")
    if not cti:
        print("No CTI data available", file=sys.stderr)
        sys.exit(1)

    cti_score = cti["score"]
    cti_level = cti["level"]
    cti_trend = cti.get("trend", "STABLE")
    components = json.loads(cti.get("components", "{}")) if isinstance(cti.get("components"), str) else (cti.get("components") or {})
    cti_status = clamp_status(cti_level)

    # ── Source freshness ──
    rss = source_from_api(freshness, "rss", 8)
    adsb = source_from_api(freshness, "adsb", 8)
    ais = source_from_api(freshness, "ais", 8)
    energy = source_from_api(freshness, "energy", 8)
    telegram = source_from_api(freshness, "telegram_channel", 12)
    youtube = source_from_api(freshness, "youtube_transcript", 12)
    gpsjam = source_from_api(freshness, "gpsjam", 72)

    satellite = source_from_api(freshness, "satellite_analysis", 24)

    core = {"rss": rss, "adsb": adsb, "ais": ais, "energy": energy}
    stale_core = [n for n, s in core.items() if not s["fresh"]]
    if stale_core:
        details = ", ".join(f"{n} (latest {fmt_dt(core[n]['latest'])})" for n in stale_core)
        raise RuntimeError(f"core collectors stale: {details}")

    # ── Campaign / narrative data ──
    active_campaigns = data.get("active_campaigns", 0)
    campaign_names = data.get("campaign_names", [])
    tags_24h = data.get("narrative_tag_count", 0)
    top_campaign = campaign_names[0] if campaign_names else ""

    # ── Energy ──
    energy_avg = data.get("energy_price_7d_avg", 0)
    energy_sd = data.get("energy_price_7d_std", 0)
    energy_current = data.get("energy_price_24h_avg", 0)
    energy_z = max((energy_current - energy_avg) / max(energy_sd, 1.0), 0.0) if energy_avg > 0 else 0.0

    # ── Breaking incidents ──
    highlights = data.get("highlight_signals", [])

    # ── Build indicators ──
    indicators = []

    # CTI composite
    blind_sources = []  # Could be extracted from CTI details if available
    append_indicator(indicators, status=cti_status, category="HYBRID", label="Composite Threat Index",
        finding=f"Baltic CTI is {cti_score:.1f}/100 ({cti_level}); "
                f"security {float(components.get('security', 0)):.1f}, "
                f"fimi {float(components.get('fimi', 0)):.1f}, "
                f"hybrid {float(components.get('hybrid', 0)):.1f}, "
                f"economic {float(components.get('economic', 0)):.1f}.",
        source_count=len(core), confidence="HIGH", target_region="global")

    # ADS-B
    adsb_status = "YELLOW" if adsb["zscore"] >= 4.0 else ("GREEN" if adsb["fresh"] else "YELLOW")
    adsb_finding = f"ADS-B fresh ({adsb['count_24h']} signals/24h, latest {fmt_dt(adsb['latest'])})." if adsb["fresh"] else f"ADS-B stale since {fmt_dt(adsb['latest'])}."
    if adsb["fresh"] and adsb["zscore"] >= 4.0:
        adsb_finding = f"ADS-B fresh ({adsb['count_24h']} signals/24h); volume elevated vs 7d baseline (z={adsb['zscore']:.1f})."
    append_indicator(indicators, status=adsb_status, category="MILITARY", label="Air Activity Monitoring",
        finding=adsb_finding, source_url=adsb["url"], source_count=adsb["count_24h"],
        confidence="HIGH" if adsb["fresh"] else "LOW")

    # AIS
    ais_status = "YELLOW" if ais["zscore"] >= 4.0 else ("GREEN" if ais["fresh"] else "YELLOW")
    ais_finding = f"AIS fresh ({ais['count_24h']} signals/24h, latest {fmt_dt(ais['latest'])})." if ais["fresh"] else f"AIS stale since {fmt_dt(ais['latest'])}."
    if ais["fresh"] and ais["zscore"] >= 4.0:
        ais_finding = f"AIS fresh ({ais['count_24h']} signals/24h); volume elevated vs 7d baseline (z={ais['zscore']:.1f})."
    append_indicator(indicators, status=ais_status, category="MARITIME", label="Maritime Activity Monitoring",
        finding=ais_finding, source_url=ais["url"], source_count=ais["count_24h"],
        confidence="HIGH" if ais["fresh"] else "LOW")

    # RSS
    append_indicator(indicators, status="GREEN" if rss["fresh"] else "YELLOW", category="DIPLOMATIC",
        label="Open-source Media Coverage",
        finding=f"RSS fresh ({rss['count_24h']} signals/24h)." if rss["fresh"] else f"RSS stale since {fmt_dt(rss['latest'])}.",
        source_url=rss["url"], source_count=rss["count_24h"],
        confidence="HIGH" if rss["fresh"] else "LOW")

    # Social
    social_fresh = telegram["fresh"] and youtube["fresh"]
    append_indicator(indicators, status="GREEN" if social_fresh else "YELLOW", category="HYBRID",
        label="Telegram Monitoring",
        finding=f"Telegram/YouTube fresh ({telegram['count_24h']} tg, {youtube['count_24h']} yt in 24h)." if social_fresh
            else f"Social partially stale: tg latest {fmt_dt(telegram['latest'])}, yt latest {fmt_dt(youtube['latest'])}.",
        source_count=telegram["count_24h"] + youtube["count_24h"],
        confidence="HIGH" if social_fresh else "LOW")

    # Influence
    influence_status = "GREEN"
    if active_campaigns >= 1 or tags_24h >= 10:
        influence_status = "YELLOW"
    if active_campaigns >= 5:
        influence_status = "ORANGE"
    influence_finding = f"{active_campaigns} active campaign(s), {tags_24h} narrative tags/24h."
    if top_campaign:
        influence_finding += f" Top: {top_campaign}."
    append_indicator(indicators, status=influence_status, category="HYBRID", label="Influence Activity",
        finding=influence_finding, source_count=active_campaigns + tags_24h, confidence="MEDIUM")

    # GPS jamming
    gps_fresh_24h = gpsjam["fresh"] and gpsjam["count_24h"] > 0
    if not gpsjam["fresh"]:
        gps_status, gps_finding, gps_conf = "YELLOW", f"GPSJam stale since {fmt_dt(gpsjam['latest'])}.", "LOW"
    elif not gps_fresh_24h:
        gps_status, gps_conf = "YELLOW", "MEDIUM"
        gps_finding = f"GPSJam snapshot from {fmt_dt(gpsjam['latest'])}; source delayed."
    else:
        gps_status, gps_conf = "GREEN", "HIGH"
        gps_finding = f"GPSJam fresh (latest {fmt_dt(gpsjam['latest'])})."
    append_indicator(indicators, status=gps_status, category="HYBRID", label="GPS Interference Monitoring",
        finding=gps_finding, source_url=gpsjam["url"], confidence=gps_conf,
        target_region="baltic,finland,poland")

    # Satellite
    append_indicator(indicators, status="GREEN" if satellite["fresh"] else "YELLOW", category="MILITARY",
        label="Satellite Coverage",
        finding=f"Satellite analysis fresh (latest {fmt_dt(satellite['latest'])})." if satellite["fresh"]
            else f"Satellite stale since {fmt_dt(satellite['latest'])}.",
        source_url=satellite["url"], source_count=satellite["count_24h"],
        confidence="MEDIUM" if satellite["fresh"] else "LOW", target_region="baltic,finland,poland")

    # Energy
    energy_status = "GREEN"
    energy_finding = f"Energy fresh ({energy['count_24h']} readings/24h)." if energy["fresh"] else f"Energy stale since {fmt_dt(energy['latest'])}."
    if energy["fresh"] and energy_current and energy_z >= 2.5:
        energy_status = "YELLOW"
        energy_finding = f"Energy fresh; price elevated vs 7d baseline ({energy_current:.1f} vs {energy_avg:.1f}, z={energy_z:.1f})."
    append_indicator(indicators, status=energy_status, category="HYBRID", label="Energy Monitoring",
        finding=energy_finding, source_url=energy["url"], source_count=energy["count_24h"],
        confidence="HIGH" if energy["fresh"] else "LOW")

    # Breaking incidents
    if highlights:
        max_sev = "YELLOW"
        lines = []
        for h in highlights:
            lines.append(f"[{h.get('severity','?')}] {h.get('title','?')}")
            if h.get("severity") == "CRITICAL":
                max_sev = "RED"
            elif h.get("severity") == "HIGH" and max_sev != "RED":
                max_sev = "ORANGE"
        append_indicator(indicators, status=max_sev, category="SECURITY",
            label="Breaking Security Incidents",
            finding=f"{len(highlights)} incident(s): " + "; ".join(lines),
            source_url=highlights[0].get("url", ""), source_count=len(highlights),
            confidence="HIGH", target_region="global")

    threat_level = max_status([i["status"] for i in indicators])

    # ── Summary ──
    gaps = [n for n, s in [("gpsjam", gpsjam), ("satellite", satellite)] if not s["fresh"]]
    weekday = datetime.strptime(args.date, "%Y-%m-%d").strftime("%A")
    summary = (
        f"{weekday} automated briefing. Baltic CTI is {cti_level} at {cti_score:.1f}/100. "
        f"{active_campaigns} active influence campaign(s). "
        f"Core collectors fresh. "
        f"{'Coverage gaps: ' + ', '.join(gaps) + '.' if gaps else 'No coverage gaps.'}"
    )

    raw_items = [
        {"type": "summary", "content": summary},
        {"type": "cti", "content": indicators[0]["finding"]},
    ]
    for item in indicators:
        if item["status"] != "GREEN":
            raw_items.append({"type": "elevated", "label": item["label"], "status": item["status"], "content": item["finding"]})
    raw_items.append({"type": "generated_at", "content": f"Generated {fmt_dt(datetime.now(timezone.utc).isoformat())} from current collector state."})
    raw_intel = json.dumps(raw_items, ensure_ascii=False)

    # ── Write report via API ──
    result = write_report(
        date=args.date, threat_level=threat_level,
        raw_intel=raw_intel, summary=summary,
        cti_score=float(cti_score), cti_level=cti_level, cti_trend=cti_trend,
        indicators=indicators,
    )

    print(f"Report {args.date}: threat={threat_level} cti={cti_score:.1f}/{cti_level} indicators={len(indicators)}")
    for item in indicators:
        print(f"  {item['status']:6s} {item['label']}: {item['finding']}")


if __name__ == "__main__":
    main()
