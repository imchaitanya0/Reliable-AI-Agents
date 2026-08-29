"""
Lane C — Atomic Task Claim Query (Contract C5)
==============================================

Uses FOR UPDATE SKIP LOCKED to claim work without locks, leader election, or consensus.
"""

from __future__ import annotations

from typing import Any

import psycopg
from db.pool import get_conn


def claim_task(worker_id: str, pool_tier: str, ttl_seconds: int = 30) -> dict[str, Any] | None:
    """
    Claim one runnable task matching the worker pool's tier.
    Enforces sequential DAG ordering (t.seq = a.cursor) and exponential backoff gating (t.next_run_at <= now()).
    """
    query = """
    UPDATE task_instances SET
        status = 'running',
        lease_owner = %(worker_id)s,
        lease_expires = now() + make_interval(secs => %(ttl)s),
        attempt = attempt + 1,
        updated_at = now()
    WHERE id = (
        SELECT t.id FROM task_instances t
        JOIN agents a ON a.id = t.agent_id
        WHERE t.status = 'pending'
          AND t.next_run_at <= now()
          AND t.tier = %(pool_tier)s
          AND a.status = 'running'
          AND t.seq = a.cursor
        ORDER BY t.next_run_at
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    RETURNING id, agent_id, seq, task_def_id, tier, attempt, max_attempts_per_tier;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"worker_id": worker_id, "ttl": ttl_seconds, "pool_tier": pool_tier})
            row = cur.fetchone()
            if row:
                # Log attempt start in attempts table
                cur.execute(
                    """
                    INSERT INTO attempts (task_instance_id, agent_id, seq, attempt_no, tier, worker_id, outcome, started_at)
                    VALUES (%s, %s, %s, %s, %s, %s, 'running', now())
                    RETURNING id;
                    """,
                    (row["id"], row["agent_id"], row["seq"], row["attempt"], row["tier"], worker_id),
                )
                attempt_row = cur.fetchone()
                row["attempt_id"] = attempt_row["id"] if attempt_row else None
            return row
