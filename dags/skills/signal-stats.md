---
name: Signal Statistics
description: Query signal counts, narrative tags, and threat metrics
tags: [ops, signals, metrics]
---

# Signal Statistics

## Total signals by source (last 24h)
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT source_type, COUNT(*) FROM signals
WHERE collected_at >= now()-interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC
"
```

## Narrative tag breakdown (last 7d)
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT code, COUNT(*) FROM narrative_tags
WHERE created_at >= now()-interval '7 days'
AND code NOT IN ('N1','N2','N3','N4','N5')
GROUP BY 1 ORDER BY 2 DESC LIMIT 10
"
```

## CTI threat level
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT date, region, level, ROUND(score::numeric,1)
FROM threat_index_cache
WHERE date >= CURRENT_DATE - 1
ORDER BY date DESC, region
"
```

## Telegram group message counts (last 24h)
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT metadata->>'channel_name', COUNT(*)
FROM signals WHERE source_type='telegram_group'
AND collected_at >= now()-interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC
"
```
