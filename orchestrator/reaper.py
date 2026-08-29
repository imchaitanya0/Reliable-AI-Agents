"""
5.2 Task Leasing -- the reaper (Lane F).
Reclaims expired leases and requeues tasks at the SAME tier.
"""

from __future__ import annotations

import logging
from typing import Any

from common.config import REAPER_BATCH, REAPER_JITTER_SECONDS
from db.pool import pool

log = logging.getLogger("orchestrator.reaper")

REAP_SQL = """
WITH expired AS (
    SELECT id, agent_id, seq, tier, attempt, lease_owner,
           extract(epoch FROM (now() - lease_expires)) AS overdue_seconds
    FROM task_instances
    WHERE status = 'running'
      AND lease_expires < now()
    ORDER BY lease_expires
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch)s
),
reclaimed AS (
    UPDATE task_instances t
    SET status        = 'pending',
        lease_owner   = NULL,
        lease_expires = NULL,
        failure_class = 'INFRA',
        last_error    = 'lease expired -- worker presumed dead',
        next_run_at   = now() + make_interval(secs => random() * %(jitter)s),
        updated_at    = now()
    FROM expired e
    WHERE t.id = e.id
    RETURNING t.id
),
evidence AS (
    INSERT INTO attempts (task_instance_id, agent_id, seq, attempt_no, tier,
                          worker_id, outcome, failure_class, ended_at)
    SELECT e.id, e.agent_id, e.seq, e.attempt, e.tier,
           e.lease_owner, 'reclaimed', 'INFRA', now()
    FROM expired e
    RETURNING task_instance_id
)
SELECT id, agent_id, seq, tier, attempt, lease_owner, overdue_seconds
FROM expired
"""

ORPHAN_COUNT_SQL = """
SELECT count(*) AS n FROM task_instances
WHERE status = 'running' AND lease_expires < now()
"""


def reap(batch: int | None = None, jitter: float = REAPER_JITTER_SECONDS) -> list[dict[str, Any]]:
    """
    Reclaim one batch of expired leases. Safe to run concurrently in N instances.
    """
    limit = REAPER_BATCH if batch is None else batch
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(REAP_SQL, {"batch": limit, "jitter": jitter})
        rows = cur.fetchall()

    for r in rows:
        log.warning(
            "reclaimed agent=%s seq=%s tier=%s attempt=%s from worker=%s (lease %.1fs overdue)",
            r["agent_id"], r["seq"], r["tier"], r["attempt"],
            r["lease_owner"], float(r["overdue_seconds"] or 0.0),
        )
    return rows


def sweep_expired_leases(jitter: float = 0.0) -> list[dict[str, Any]]:
    """Compatibility alias for reap()."""
    return reap(batch=REAPER_BATCH, jitter=jitter)


def orphaned_leases() -> int:
    """Expired leases not yet reclaimed."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(ORPHAN_COUNT_SQL)
        return cur.fetchone()["n"]
