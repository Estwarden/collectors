---
name: Narrative Monitor
description: Query Z-channel narratives, bot detection, and Ida-Viru sentiment
tags: [narratives, telegram, monitor]
---

# Narrative Monitor

## Active narratives (last 7d)
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT key as narrative, SUM(value::int) as volume
FROM narrative_metrics, jsonb_each_text(narrative_breakdown)
WHERE scope = 'zchannel' AND metric_date >= CURRENT_DATE - 7
AND key NOT IN ('N1','N2','N3','N4','N5')
GROUP BY key HAVING SUM(value::int) > 0
ORDER BY volume DESC LIMIT 10
"
```

## Ida-Viru vs Z-Channel comparison
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT scope, SUM(total_posts) as posts, SUM(hostile_posts) as hostile,
       ROUND(SUM(hostile_posts)::numeric/NULLIF(SUM(total_posts),0)*100, 1) as pct
FROM narrative_metrics WHERE metric_date >= CURRENT_DATE - 7
GROUP BY scope ORDER BY scope
"
```

## Bot detection results
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT channel_name, post_count_30d, is_bot_suspect,
       ROUND(content_duplication_rate::numeric*100) as dup_pct
FROM channel_bot_scores
WHERE computed_date = (SELECT MAX(computed_date) FROM channel_bot_scores)
ORDER BY post_count_30d DESC
"
```

## Telegram group activity (last 24h)
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT metadata->>'channel_name',
       COUNT(*) as msgs,
       COUNT(DISTINCT metadata->>'sender_id') as users
FROM signals WHERE source_type='telegram_group'
AND collected_at >= now()-interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC
"
```

## Scopes
- zchannel: ru_state + ru_proxy channels and chats
- estonia: russian_language_ee + region=estonia
- global: all telegram + rss
