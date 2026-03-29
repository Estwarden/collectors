#!/usr/bin/env python3
"""Compute per-region Composite Threat Index.

Four-pillar formula:
1. Signal z-scores (anomaly detection from baselines) — per-region
2. Active campaigns × severity (persistent disinfo threat) — per-region
3. Narrative volume (information warfare intensity) — global (shared)
4. GPS jamming severity (persistent hybrid threat) — per-region

All data fetched from ingest API — no direct DB access.
"""
import sys
import os
import math
from datetime import datetime, timezone

sys.path.insert(0, os.path.join("/dags/scripts/lib"))
from estwarden_client import query_cti_input, ingest_threat_index

# Using flat API
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# === WEIGHTS ===
SIGNAL_WEIGHTS = {
    "gpsjam": 15, "adsb": 2, "acled": 0, "firms": 5,
    "ais": 3, "telegram_channel": 4, "rss": 4, "gdelt": 2,
    "energy": 4, "business": 2, "ioda": 0,
    "deepstate": 2, "rss_security": 1, "satellite_analysis": 1,
}
MIN_DAY_COUNT = {
    "ais": 10000, "adsb": 50, "firms": 5, "energy": 20,
    "rss": 100, "deepstate": 3, "rss_security": 5,
    "satellite_analysis": 1, "telegram_channel": 50,
}
SIGNAL_TOTAL = sum(SIGNAL_WEIGHTS.values())
CAMPAIGN_WEIGHT = 10
FABRICATION_WEIGHT = 8
LAUNDERING_WEIGHT = 6
NARRATIVE_WEIGHT = 4
GPSJAM_SEV_WEIGHT = 10
INCIDENT_WEIGHT = 15
TOTAL_WEIGHT = SIGNAL_TOTAL + CAMPAIGN_WEIGHT + FABRICATION_WEIGHT + LAUNDERING_WEIGHT + NARRATIVE_WEIGHT + GPSJAM_SEV_WEIGHT + INCIDENT_WEIGHT

CAT_MAP = {
    "gpsjam": "hybrid", "adsb": "security", "acled": "security",
    "firms": "hybrid", "ais": "hybrid", "telegram_channel": "fimi",
    "rss": "fimi", "gdelt": "fimi", "ioda": "economic",
    "energy": "economic", "business": "economic",
    "deepstate": "security", "rss_security": "security",
    "satellite_analysis": "security",
}

THRESHOLDS = {"yellow": 15.2, "orange": 59.7, "red": 92.8}

REGIONS = ["estonia", "latvia", "lithuania", "finland", "poland"]

# === FETCH ALL DATA FROM API ===
print("Fetching CTI input data from API...")
resp = query_cti_input()
api_data = resp["data"]
resolved = resp.get("resolved_campaigns", 0)
if resolved:
    print(f"  auto-resolved {resolved} stale campaigns")

# === DEGRADED FLAG ===
_active_weight = 0
_total_signal_weight = sum(w for w in SIGNAL_WEIGHTS.values() if w > 0)
coverage = api_data.get("signal_coverage", {})
for st, w in SIGNAL_WEIGHTS.items():
    if w == 0:
        continue
    cnt = coverage.get(st, 0)
    min_count = max(MIN_DAY_COUNT.get(st, 0), 1)
    if cnt >= min_count:
        _active_weight += w

is_degraded = _active_weight < _total_signal_weight * 0.7
if is_degraded:
    print(f"  ⚠ DEGRADED: {_active_weight}/{_total_signal_weight} signal weight active ({_active_weight/_total_signal_weight*100:.0f}%)")
else:
    print(f"  signal coverage: {_active_weight}/{_total_signal_weight} ({_active_weight/_total_signal_weight*100:.0f}%)")

# Sensor blind check
freshness = api_data.get("freshness", {})
blind_sources = [st for st, ok in freshness.items() if not ok]
if blind_sources:
    print(f"  sensors blind: {blind_sources}")

baselines_map = api_data.get("baselines", {})
campaigns_map = api_data.get("campaigns", {})
fabrications_map = api_data.get("fabrications", {})
laundering_map = api_data.get("laundering_count", {})
narrative_map = api_data.get("narrative_count", {})
gpsjam_map = api_data.get("gpsjam_avg", {})
energy_map = api_data.get("energy_prices", {})
incidents_map = api_data.get("incidents", {})

for region in REGIONS:
    signal_score = 0.0
    components = {"security": 0.0, "fimi": 0.0, "hybrid": 0.0, "economic": 0.0}

    # --- Signal z-scores ---
    for st, w in SIGNAL_WEIGHTS.items():
        if w == 0:
            continue
        key = f"{region}:{st}"
        b = baselines_map.get(key)
        if not b or b.get("n_days", 0) < 3:
            continue
        median = b["median"]
        mad = max(b.get("mad", 1), 1)
        current = b["current_24h"]
        min_count = MIN_DAY_COUNT.get(st, 0)
        if current < min_count and min_count > 0:
            continue
        robust_std = 1.4826 * mad
        z = max((current - median) / robust_std, 0)
        contrib = min(z * 10, 100) * (w / TOTAL_WEIGHT)
        signal_score += contrib
        components[CAT_MAP.get(st, "security")] += contrib

    # --- Campaign score ---
    campaign_score = 0.0
    sev_scores = {"CRITICAL": 25, "HIGH": 15, "MEDIUM": 8, "LOW": 3}
    for c in campaigns_map.get(region, []):
        base = sev_scores.get(c["severity"], 5)
        age = c["age_days"]
        decay = 1.0 if age <= 1 else (0.7 if age <= 3 else 0.4)
        campaign_score += base * decay
    campaign_score = min(campaign_score, 100) * (CAMPAIGN_WEIGHT / TOTAL_WEIGHT)
    components["fimi"] += campaign_score

    # --- Fabrication score ---
    fabrication_score = 0.0
    fab_total = 0.0
    for f in fabrications_map.get(region, []):
        impact = f["score"] * math.log10(max(f["views"], 1) + 1)
        if f.get("certainty_escalation"):
            impact *= 1.5
        if f.get("emotional_amplification"):
            impact *= 1.2
        fab_total += impact
    fabrication_score = min(fab_total / 5, 100) * (FABRICATION_WEIGHT / TOTAL_WEIGHT)
    components["fimi"] += fabrication_score

    # --- Laundering score ---
    laundering_count = laundering_map.get(region, 0)
    laundering_score = min(laundering_count / 20 * 100, 100) * (LAUNDERING_WEIGHT / TOTAL_WEIGHT)
    components["fimi"] += laundering_score

    # --- Narrative tag score ---
    narr_count = narrative_map.get(region, 0)
    narrative_score = min(narr_count / 10, 100) * (NARRATIVE_WEIGHT / TOTAL_WEIGHT)
    components["fimi"] += narrative_score

    # --- GPS jamming severity ---
    avg_rate = gpsjam_map.get(region, 0)
    gpsjam_score = min(avg_rate * 200, 100) * (GPSJAM_SEV_WEIGHT / TOTAL_WEIGHT)
    components["hybrid"] += gpsjam_score

    # --- Energy price anomaly ---
    energy_score = 0.0
    ep = energy_map.get(region, {})
    current_price = ep.get("current_24h_avg", 0)
    avg_price = ep.get("baseline_7d_avg", 0)
    std_price = max(ep.get("baseline_7d_std", 1), 1)
    if current_price and avg_price:
        price_z = max((current_price - avg_price) / std_price, 0)
        energy_score = min(price_z * 10, 50) * (6 / TOTAL_WEIGHT)
        components["economic"] += energy_score

    # --- Security incidents ---
    incident_score = 0.0
    inc_sev_scores = {"CRITICAL": 40, "HIGH": 20}
    incident_total = sum(inc_sev_scores.get(i["severity"], 10) for i in incidents_map.get(region, []))
    incident_score = min(incident_total, 100) * (INCIDENT_WEIGHT / TOTAL_WEIGHT)
    components["security"] += incident_score

    # --- TOTAL ---
    score = min(signal_score + campaign_score + fabrication_score + laundering_score +
                narrative_score + gpsjam_score + energy_score + incident_score, 100)

    if is_degraded:
        level = "DEGRADED"
    elif score >= THRESHOLDS["red"]:
        level = "RED"
    elif score >= THRESHOLDS["orange"]:
        level = "ORANGE"
    elif score >= THRESHOLDS["yellow"]:
        level = "YELLOW"
    else:
        level = "GREEN"

    details = {}
    if blind_sources:
        details["blind_sources"] = blind_sources
    details["breakdown"] = {
        "signal_zscore": round(signal_score, 1),
        "campaigns": round(campaign_score, 1),
        "fabrication": round(fabrication_score, 1),
        "laundering": round(laundering_score, 1),
        "narratives": round(narrative_score, 1),
        "gpsjam_severity": round(gpsjam_score, 1),
        "energy_price": round(energy_score, 1),
    }
    if is_degraded:
        details["degraded"] = True
        details["signal_coverage_pct"] = round(_active_weight / _total_signal_weight * 100, 1)

    ingest_threat_index(today, score, level, region=region,
        components=components, details=details)

    flag = " ⚠DEGRADED" if is_degraded else ""
    print(f"  {region}: {score:.1f}/100 ({level}){flag} "
          f"sec={components['security']:.1f} fimi={components['fimi']:.1f} "
          f"hybrid={components['hybrid']:.1f} econ={components['economic']:.1f}")

# === BALTIC AGGREGATE ===
baltic = resp.get("baltic_aggregate")
if baltic and baltic.get("score") is not None:
    baltic_score = min(baltic["score"], 100)
    if is_degraded:
        baltic_level = "DEGRADED"
    elif baltic_score >= THRESHOLDS["red"]:
        baltic_level = "RED"
    elif baltic_score >= THRESHOLDS["orange"]:
        baltic_level = "ORANGE"
    elif baltic_score >= THRESHOLDS["yellow"]:
        baltic_level = "YELLOW"
    else:
        baltic_level = "GREEN"
    ingest_threat_index(today, baltic_score, baltic_level, region="baltic",
        components=baltic.get("components", {}), details=baltic.get("details", {}))
    print(f"  baltic: {baltic_score:.1f}/100 ({baltic_level}) [aggregate of estonia, latvia, lithuania]")
