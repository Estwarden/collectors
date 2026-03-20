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

## Enrichment

| Pipeline | Script | What it does |
|----------|--------|-------------|
| **Classify** | `classifier.py` | LLM narrative tagging (N1-N5) via OpenRouter |
| **Translate + Extract** | `translate_and_extract.py` | Google Cloud Translation + NER |
| **Campaign detection** | DAG (baselines) | Narrative volume spike detection |
| **Threat index** | DAG (computed) | Composite Threat Index from all sources |
| **Anomaly detection** | DAG (baselines) | 7-day rolling z-score anomalies |

## How to contribute

1. Fork this repo
2. Write a collector:
   - Create `scripts/collectors/your_collector.py`
   - Create `dags/collect/your-source.yaml`
   - Use the `EstWardenClient` from `scripts/lib/estwarden_client.py`
3. Test locally: `python3 scripts/collectors/your_collector.py` (with env vars set)
4. Submit a PR — we'll review and schedule it

### Collector contract

Every collector:
- Takes config from environment variables (API URL, API key, source-specific keys)
- Fetches data from a **public** source
- Submits signals as JSON to the Data API
- Handles errors gracefully (timeouts, rate limits, malformed data)
- Is idempotent (ON CONFLICT dedup via source_type + source_id)

### Signal schema

```json
{
  "source_type": "rss",
  "source_id": "unique-per-source-id",
  "title": "Signal title",
  "content": "Signal body text",
  "url": "https://source-url",
  "published_at": "2026-03-20T08:00:00Z",
  "latitude": 59.43,
  "longitude": 24.75,
  "severity": "HIGH",
  "metadata": {"feed_handle": "propastop", "category": "counter_disinfo"}
}
```

## Structure

```
dags/
├── collect/          — Data collection DAGs (cron-scheduled)
├── process/          — Enrichment DAGs (classify, translate, campaigns)
├── maintain/         — Maintenance DAGs (dedup, retention, quality)
└── pipelines/        — Orchestration DAGs (morning pipeline, etc.)
scripts/
├── lib/              — Shared libraries (API client, Google Cloud client)
├── collectors/       — Collector scripts (one per source)
└── processors/       — Enrichment scripts (classifier, NER)
config/
└── feeds.yaml        — RSS feed registry (54 feeds)
```

## License

MIT — contributions welcome.
