#!/usr/bin/env python3
"""Recompute event_clusters.regions from cluster member signals.

One-off maintenance script for existing clusters created before region metadata
was refreshed on every cluster update.

Env:
    DATABASE_URL (optional)
"""

import os
import sys

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://estwarden:estwarden@postgres:5432/estwarden",
)


def main():
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 is required", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM event_clusters")
    total = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM event_clusters
        WHERE COALESCE(array_length(regions, 1), 0) = 0
        """
    )
    empty_before = cur.fetchone()[0]

    cur.execute(
        """
        WITH recomputed AS (
            SELECT
                ec.id AS cluster_id,
                COALESCE(
                    array_agg(DISTINCT r ORDER BY r)
                        FILTER (WHERE r IS NOT NULL AND r != ''),
                    ARRAY[]::text[]
                ) AS regions
            FROM event_clusters ec
            LEFT JOIN cluster_signals cs ON cs.cluster_id = ec.id
            LEFT JOIN signals s ON s.id = cs.signal_id
            LEFT JOIN signal_embeddings se ON se.signal_id = cs.signal_id
            LEFT JOIN LATERAL unnest(
                COALESCE(se.regions, string_to_array(COALESCE(s.region, ''), ','))
            ) AS r ON true
            GROUP BY ec.id
        )
        UPDATE event_clusters ec
        SET regions = recomputed.regions
        FROM recomputed
        WHERE ec.id = recomputed.cluster_id
          AND COALESCE(ec.regions, ARRAY[]::text[]) IS DISTINCT FROM recomputed.regions
        """
    )
    updated = cur.rowcount
    conn.commit()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM event_clusters
        WHERE COALESCE(array_length(regions, 1), 0) = 0
        """
    )
    empty_after = cur.fetchone()[0]

    print(f"Clusters total: {total}")
    print(f"Clusters updated: {updated}")
    print(f"Empty regions before: {empty_before}")
    print(f"Empty regions after: {empty_after}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
