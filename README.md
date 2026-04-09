# EstWarden Collectors

Open-source data collection pipelines for [EstWarden](https://estwarden.eu) — a Baltic Security Monitor tracking Russian military posture, disinformation campaigns, and influence operations.

## Architecture

These collectors run on [Dagu](https://github.com/dagu-org/dagu) — a Go-based DAG workflow engine. They fetch data from public sources and submit it to the EstWarden Data API.

```
Source (RSS, API, sensor) → Collector Script → Data API → EstWarden Dashboard
```

Collectors never touch the database directly. All writes go through the authenticated Data API.

## Collectors

| Source | Script | Schedule | What it collects |
|--------|--------|----------|-----------------|
| **RSS feeds** (54) | `rss_collector.py` | every 2h | Baltic/Russian media, think tanks, government |
| **Telegram channels** (73) | `telegram_collector.py` | every 4h | Public channel posts for narrative monitoring |
| **ADS-B aircraft** | `adsb_collector.py` | every 15min | Military flights in Baltic airspace |
| **AIS vessels** | `ais_collector.py` | every 5min | Ship positions, shadow fleet detection |
| **NASA FIRMS** | `firms_collector.py` | every 6h | Thermal anomalies at military bases |
| **GPS jamming** | `gpsjam_collector.py` | every 12h | Electronic warfare activity (H3 hex) |
| **ACLED** | `acled_collector.py` | every 12h | European conflict events |
| **GDELT** | `gdelt_collector.py` | every 4h | Military-relevant news near bases |
| **Deepstate** | DAG only | 4x/day | Ukraine frontline data |
| **Space weather** | DAG only | every 6h | NOAA Kp index (geomagnetic storms) |
| **Balloons** | DAG only | every 4h | Weather balloon positions (SondeHub) |
| **IODA** | `ioda_collector.py` | every 4h | Baltic internet outage monitoring |
| **NOTAMs** | `notam_collector.py` | every 6h | EASA conflict zone bulletins |
| **OpenSanctions** | `sanctions_collector.py` | weekly | Sanctions entity sync |
| **Elering** | DAG only | hourly | Estonian electricity prices |

## Enrichment & Scoring

| Pipeline | Script | What it does |
|----------|--------|-------------|
| **Classify** | `classifier.py` | LLM narrative tagging (N1-N5) via OpenRouter |
| **Translate + Extract** | `translate_and_extract.py` | Google Cloud Translation + NER |
| **Media Score (fast)** | `media_score_fast.py` | Keyword-based evidence & uncertainty scoring |
| **Campaign detection** | DAG (baselines) | Narrative volume spike detection |
| **Threat index** | DAG (computed) | Composite Threat Index from all sources |
| **Anomaly detection** | DAG (baselines) | 7-day rolling z-score anomalies |

## Media Monitor

EstWarden tracks Telegram media sources for narrative amplification patterns. Each source is rated on five [epistemic rationality metrics](https://www.lesswrong.com/rationality) — see the [Media Monitor page](https://estwarden.eu/media) for the full list.

Channel watchlists are maintained in `config/watchlists/`. The Telegram list is active; the YouTube list is legacy metadata from the retired YouTube collector.

---

## Contributing

We welcome contributions. No database access, no credentials, no infrastructure knowledge required — you edit YAML and Python, we handle the rest.

### Quick start

```bash
git clone https://github.com/Estwarden/collectors.git
cd collectors
```

### 1. Add a media channel to monitor

**Easiest contribution.** Add a Telegram channel to the watchlist.

Edit `config/watchlists/telegram_channels.yaml`:

```yaml
- handle: channel_handle        # unique identifier, no spaces
  name: Channel Display Name
  url: https://t.me/s/channel_handle   # must be t.me/s/ (public preview)
  lang: uk                      # uk, ru, en, et
  category: untrusted           # always start as untrusted
  tier: T2                      # T1 = high priority, T2 = secondary
  notes: "Brief description of the channel and why it matters"
  rationality:                  # always start as unknown
    calibration: unknown
    updating: unknown
    evidence: unknown
    uncertainty: unknown
    independence: unknown
```

**Rules:**
- New channels always start as `category: untrusted` and `rationality: unknown`
- The auto-scoring pipeline will fill in rationality scores from observed behavior
- To propose a channel as `trusted`, include evidence (editorial standards, institutional backing, fact-checking track record)
- To propose a channel as `ru_proxy`, cite a published source (Detector Media, EUvsDisinfo, DFRLab, etc.)
- Don't set rationality scores manually unless you have specific published evidence

### 2. Add an RSS feed

Edit `config/feeds.yaml`:

```yaml
- handle: source_handle
  name: Source Name
  source_type: rss              # or rss_security
  tier: T2
  category: defense_osint       # see existing categories in the file
  url: https://example.com/feed/
```

### 3. Write a new collector

Create `scripts/collectors/your_collector.py`:

```python
#!/usr/bin/env python3
"""Your source collector for EstWarden."""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from estwarden_client import EstWardenClient

def main():
    client = EstWardenClient()  # reads ESTWARDEN_API_URL + ESTWARDEN_API_KEY from env

    # Fetch data from your public source
    signals = [
        {
            "source_type": "your_type",    # must be added to API whitelist
            "source_id": "unique-id",       # dedup key
            "title": "Signal title",
            "content": "Full text content",
            "url": "https://source-url",
            "published_at": "2026-03-20T08:00:00Z",
        }
    ]

    result = client.ingest_signals(signals)
    print(f"Inserted: {result.get('inserted', 0)}")

if __name__ == "__main__":
    main()
```

Create `dags/collect/your-source.yaml`:

```yaml
name: collect-your-source
description: What this collector does
schedule: "0 */6 * * *"

env:
  - ESTWARDEN_API_URL: "${ESTWARDEN_API_URL}"
  - ESTWARDEN_API_KEY: "${ESTWARDEN_API_KEY}"

steps:
  - name: fetch
    shell: /bin/bash
    script: |
      cd /dags
      python3 scripts/collectors/your_collector.py
    retry:
      limit: 2
      interval: 300
```

**Collector contract:**
- Reads config from environment variables only (never hardcode credentials)
- Fetches data from **public** sources only
- Submits signals via `EstWardenClient` (never writes to DB directly)
- Handles errors gracefully (timeouts, rate limits, malformed data)
- Is idempotent (duplicate signals are rejected by the API via `source_id`)
- New `source_type` values must be added to the API whitelist — mention this in your PR

### 4. Improve the scoring pipeline

The media scoring system rates sources on five epistemic metrics from [Rationality: A-Z](https://www.lesswrong.com/rationality):

| Metric | What it measures | Current method |
|--------|-----------------|---------------|
| **Calibration** | Prediction accuracy | Planned: claim extraction + delayed evaluation |
| **Updating** | Correction behavior | Planned: self-contradiction detection |
| **Evidence** | Source citation quality | `media_score_fast.py` — keyword patterns |
| **Uncertainty** | Confidence expression | `media_score_fast.py` — hedging vs. certainty ratio |
| **Independence** | Motivated reasoning | Planned: sentiment consistency analysis |

Ways to help:
- Add keyword patterns for more languages in `media_score_fast.py`
- Improve the hedging/certainty word lists
- Write the claim extraction pipeline
- Write the calibration evaluation pipeline

### What NOT to include in PRs

- **No credentials, API keys, tokens, or passwords** — ever
- **No internal infrastructure details** (IPs, paths, hostnames)
- **No personal data** — we monitor public channel content, not individuals
- **No copy-pasted content from paywalled sources**

### Signal schema

```json
{
  "source_type": "rss",
  "source_id": "unique-per-source-id",
  "title": "Signal title (max 200 chars)",
  "content": "Full text body",
  "url": "https://source-url",
  "published_at": "2026-03-20T08:00:00Z",
  "latitude": 59.43,
  "longitude": 24.75,
  "severity": "HIGH",
  "metadata": {"key": "value"}
}
```

### Categories reference

**Channel categories:**
| Category | Meaning |
|----------|---------|
| `trusted` | Institutional media with editorial standards |
| `unverified_commentator` | Named individuals, legitimate but claims need verification |
| `unverified_media` | Outlets without verified editorial standards |
| `unverified_anonymous` | Anonymous channels ("помийки"), no accountability |
| `unverified_independent` | Independent content creators |
| `ru_state` | Russian state media (reference corpus) |
| `ru_proxy` | Pro-Kremlin amplifier channels (reference corpus) |

**Rationality scores:**
| Score | Meaning |
|-------|---------|
| `high` | Consistently meets epistemic standard |
| `medium` | Inconsistent |
| `low` | Consistently fails |
| `unknown` | Insufficient data (default for new channels) |

---

## Structure

```
dags/
├── collect/          — Data collection DAGs (cron-scheduled)
├── process/          — Enrichment & scoring DAGs
├── maintain/         — Maintenance DAGs (dedup, retention, quality)
└── pipelines/        — Orchestration DAGs (morning pipeline)
scripts/
├── lib/              — Shared libraries (API client, Google Cloud client)
├── collectors/       — Collector scripts (one per source)
└── processors/       — Enrichment & scoring scripts
config/
├── feeds.yaml        — RSS feed registry (54 feeds)
└── watchlists/       — Media channel watchlists
    ├── telegram_channels.yaml
    └── youtube_channels.yaml   — legacy metadata, collector retired
```

## License

MIT — contributions welcome.
