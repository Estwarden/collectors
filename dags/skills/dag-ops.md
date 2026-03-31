---
name: DAG Operations
description: List, start, stop, and debug Dagu DAGs
tags: [ops, dags, debug]
---

# DAG Operations

## List all DAGs
```bash
find /var/lib/dagu/dags -name "*.yaml" -not -path "*/souls/*" -not -path "*/skills/*" -not -path "*/memory/*" | sort
```

## Start a DAG
```bash
dagu start <path>
# Examples:
# dagu start collect/rss
# dagu start process/threat-index
# dagu start maintain/watchdog
```

## Check recent failures
```bash
dagu history <dag-name> | tail -5
```

## DAG categories
- collect/ — data collectors (RSS, ADS-B, AIS, Telegram, etc)
- process/ — enrichment (classify, embed, threat-index, report)
- maintain/ — maintenance (dedup, retention, watchdog, backfill)
- pipelines/ — multi-step workflows (morning)

## Queue assignments
- collectors: max 5 concurrent
- llm: max 2 concurrent (OpenRouter rate limits)
- maintenance: max 1
