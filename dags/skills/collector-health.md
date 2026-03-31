---
name: Collector Health Check
description: Check all data collectors for freshness and restart stale ones
tags: [ops, health, collectors]
---

# Collector Health Check

Run this SQL to check all collectors:

```bash
psql "$DATABASE_URL" -t -A -c "
SELECT source_type,
       COUNT(*) FILTER (WHERE collected_at >= now()-interval '4 hours') as last_4h,
       ROUND(EXTRACT(EPOCH FROM now()-MAX(collected_at))/3600, 1) as hours_ago,
       CASE
         WHEN MAX(collected_at) >= now()-interval '4 hours' THEN 'OK'
         WHEN MAX(collected_at) >= now()-interval '24 hours' THEN 'STALE'
         ELSE 'DEAD'
       END as status
FROM signals WHERE collected_at >= now()-interval '30 days'
GROUP BY source_type ORDER BY hours_ago DESC
"
```

Expected thresholds:
- adsb, ais: < 1h
- rss, energy: < 4h
- telegram_channel, telegram_group: < 4h
- firms, gpsjam, deepstate: < 24h

To restart a stale collector: `dagu start collect/<name>`
Collector DAGs are in: /var/lib/dagu/dags/collect/
