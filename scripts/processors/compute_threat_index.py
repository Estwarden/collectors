#!/usr/bin/env python3
"""Compute per-region Composite Threat Index.

Four-pillar formula:
1. Signal z-scores (anomaly detection from baselines) — per-region
2. Active campaigns × severity (persistent disinfo threat) — per-region
3. Narrative volume (information warfare intensity) — global (shared)
4. GPS jamming severity (persistent hybrid threat) — per-region
"""
import sys, os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient
import psycopg2

client = EstWardenClient()
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
db_url = os.environ.get("DATABASE_URL", "")

# === WEIGHTS ===
SIGNAL_WEIGHTS = {
    "gpsjam": 12, "adsb": 10, "acled": 8, "firms": 8,
    "ais": 6, "telegram": 6, "rss": 4, "gdelt": 4,
    "energy": 6, "business": 4, "ioda": 4,
}
SIGNAL_TOTAL = sum(SIGNAL_WEIGHTS.values())
CAMPAIGN_WEIGHT = 14
NARRATIVE_WEIGHT = 8
GPSJAM_SEV_WEIGHT = 10
TOTAL_WEIGHT = SIGNAL_TOTAL + CAMPAIGN_WEIGHT + NARRATIVE_WEIGHT + GPSJAM_SEV_WEIGHT

CAT_MAP = {
    "gpsjam": "hybrid", "adsb": "security", "acled": "security",
    "firms": "hybrid", "ais": "hybrid", "telegram": "fimi",
    "rss": "fimi", "gdelt": "fimi", "ioda": "economic",
    "energy": "economic", "business": "economic",
}

THRESHOLDS = {"yellow": 15.2, "orange": 59.7, "red": 92.8}

REGIONS = {
    "baltic": {
        "filter": "global,baltic,estonia,latvia,lithuania,kaliningrad,pskov,stpetersburg,belarus_north",
        "campaign_regions": ["global", "baltic", "estonia", "latvia", "lithuania"],
        "gpsjam_zones": ["Estonia", "Latvia", "Lithuania", "Kaliningrad", "Baltic-Sea", "Gulf-Finland"],
    },
    "finland": {
        "filter": "global,finland,baltic,stpetersburg,murmansk",
        "campaign_regions": ["global", "baltic", "finland"],
        "gpsjam_zones": ["Finland_S", "Gulf-Finland", "Estonia"],
    },
    "poland": {
        "filter": "global,poland,baltic,kaliningrad,belarus_north",
        "campaign_regions": ["global", "baltic", "poland"],
        "gpsjam_zones": ["Poland_NE", "Kaliningrad", "Baltic-Sea"],
    },
}

# === SENSOR BLIND CHECK ===
EXPECTED = ["rss", "adsb", "ais", "firms", "gpsjam", "radiation", "energy"]
blind_sources = []
conn = None

if db_url:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT source_type FROM signals
        WHERE published_at >= now() - interval '6 hours'
        AND source_type = ANY(%s)""", (EXPECTED,))
    active = {r[0] for r in cur.fetchall()}
    blind_sources = [s for s in EXPECTED if s not in active]

is_degraded = len(blind_sources) > len(EXPECTED) // 2
if blind_sources:
    print(f"Sensors: {len(blind_sources)}/{len(EXPECTED)} blind: {blind_sources}")

for region, cfg in REGIONS.items():
    # --- Signal z-scores (per-region baselines) ---
    baselines = client.query_baselines(region=cfg["filter"])
    signal_score = 0.0
    components = {"security": 0.0, "fimi": 0.0, "hybrid": 0.0, "economic": 0.0}

    for b in baselines:
        st = b.get("source_type", "")
        w = SIGNAL_WEIGHTS.get(st, 0)
        if w == 0:
            continue
        mean = b.get("mean_7d", 0)
        stddev = max(b.get("stddev_7d", 1), 1)
        current = b.get("current_24h", 0)
        z = max((current - mean) / stddev, 0)
        contrib = min(z * 10, 100) * (w / TOTAL_WEIGHT)
        signal_score += contrib
        components[CAT_MAP.get(st, "security")] += contrib

    # --- Campaign score (per-region) ---
    campaign_score = 0.0
    if conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT severity, COUNT(*) FROM campaigns
            WHERE COALESCE(status, 'ACTIVE') = 'ACTIVE'
            AND (target_regions && %s)
            GROUP BY severity
        """, (cfg["campaign_regions"],))
        sev_scores = {"CRITICAL": 25, "HIGH": 15, "MEDIUM": 8, "LOW": 3}
        for sev, cnt in cur.fetchall():
            campaign_score += sev_scores.get(sev, 5) * min(cnt, 5)
        campaign_score = min(campaign_score, 100) * (CAMPAIGN_WEIGHT / TOTAL_WEIGHT)
        components["fimi"] += campaign_score

    # --- Narrative score (per-region signals where available) ---
    narrative_score = 0.0
    if conn:
        cur = conn.cursor()
        # Count narratives from signals in this region
        cur.execute("""
            SELECT COUNT(*) FROM narrative_tags nt
            JOIN signals s ON s.id = nt.signal_id
            WHERE s.published_at >= now() - interval '7 days'
            AND (s.region IS NULL OR s.region = '' OR s.region LIKE ANY(%s))
        """, ([f"%{r}%" for r in cfg["campaign_regions"]],))
        narr_count = cur.fetchone()[0]
        narrative_score = min(narr_count / 10, 100) * (NARRATIVE_WEIGHT / TOTAL_WEIGHT)
        components["fimi"] += narrative_score

    # --- GPS jamming severity (per-region zones) ---
    gpsjam_score = 0.0
    if conn and cfg["gpsjam_zones"]:
        cur = conn.cursor()
        cur.execute("""
            SELECT AVG((metadata->>'avg_rate')::float)
            FROM signals WHERE source_type = 'gpsjam'
            AND published_at >= now() - interval '3 days'
            AND metadata->>'avg_rate' IS NOT NULL
            AND metadata->>'zone' = ANY(%s)
        """, (cfg["gpsjam_zones"],))
        row = cur.fetchone()
        avg_rate = row[0] if row and row[0] else 0
        gpsjam_score = min(avg_rate * 200, 100) * (GPSJAM_SEV_WEIGHT / TOTAL_WEIGHT)
        components["hybrid"] += gpsjam_score

    # --- Energy price anomaly (per-region) ---
    energy_score = 0.0
    if conn:
        cur = conn.cursor()
        region_patterns = [f"%{r}%" for r in cfg["campaign_regions"]]
        # Current 24h average price
        cur.execute("""
            SELECT AVG((metadata->>'price')::float)
            FROM signals WHERE source_type = 'energy'
            AND published_at >= now() - interval '24 hours'
            AND metadata->>'price' IS NOT NULL
            AND region LIKE ANY(%s)
        """, (region_patterns,))
        current_price = (cur.fetchone() or [None])[0]
        # 7-day baseline
        cur.execute("""
            SELECT AVG((metadata->>'price')::float),
                   COALESCE(STDDEV((metadata->>'price')::float), 1)
            FROM signals WHERE source_type = 'energy'
            AND published_at >= now() - interval '7 days'
            AND published_at < now() - interval '24 hours'
            AND metadata->>'price' IS NOT NULL
            AND region LIKE ANY(%s)
        """, (region_patterns,))
        row = cur.fetchone()
        if current_price and row and row[0]:
            avg_price, std_price = row[0], max(row[1], 1)
            price_z = max((current_price - avg_price) / std_price, 0)
            energy_score = min(price_z * 10, 50) * (6 / TOTAL_WEIGHT)
            components["economic"] += energy_score

    # --- TOTAL ---
    score = min(signal_score + campaign_score + narrative_score + gpsjam_score + energy_score, 100)

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
        "narratives": round(narrative_score, 1),
        "gpsjam_severity": round(gpsjam_score, 1),
        "energy_price": round(energy_score, 1),
    }

    client.ingest_threat_index(today, score, level, region=region,
        components=components, details=details)

    print(f"{region}: {score:.1f}/100 ({level}) "
          f"sec={components['security']:.1f} fimi={components['fimi']:.1f} "
          f"hybrid={components['hybrid']:.1f} econ={components['economic']:.1f}")

if conn:
    conn.close()
