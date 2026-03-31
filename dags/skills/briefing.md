---
name: Briefing Operations
description: Generate, check, and manage the daily security briefing
tags: [ops, briefing, report]
---

# Briefing Operations

## Check today's report
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT date, cti_level, ROUND(cti_score::numeric,1),
       (SELECT COUNT(*) FROM indicators WHERE report_id = daily_reports.id) as indicators
FROM daily_reports WHERE date >= CURRENT_DATE ORDER BY date DESC
"
```

## Check indicators
```bash
psql "$DATABASE_URL" -t -A -c "
SELECT status, label, LEFT(finding, 100)
FROM indicators i JOIN daily_reports r ON r.id = i.report_id
WHERE r.date = CURRENT_DATE
ORDER BY CASE status WHEN 'RED' THEN 0 WHEN 'ORANGE' THEN 1 WHEN 'YELLOW' THEN 2 ELSE 3 END
"
```

## Regenerate report
```bash
dagu start process/refresh-report
```

## Regenerate CTI
```bash
dagu start process/threat-index
```

## Pipeline order
1. collect/rss (fresh data)
2. process/threat-index (compute CTI)
3. process/refresh-report (generate report from CTI + indicators)

The morning Telegram briefing (infographic + podcasts) runs via cron on Gaming PC, not via Dagu.
