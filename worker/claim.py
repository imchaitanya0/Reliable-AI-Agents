"""
THE CLAIM QUERY -- this is the entire scheduler (Lane C).

There is no scheduler process. Dependency ordering, tier routing and mutual
exclusion all fall out of one statement.
"""

from __future__ import annotations

from typing import Any

from db.pool import pool

CLAIM_SQL = """
UPDATE task_instances SET
    status        = 'running',
    lease_owner   = %(worker_id)s,
    lease_expires = now() + make_interval(secs => %(ttl)s),
    attempt       = attempt + 1,
    updated_at    = now()
WHERE id = (
    SELECT t.id
    FROM task_instances t
    JOIN agents a ON a.id = t.agent_id
    WHERE t.status      = 'pending'
      AND t.next_run_at <= now()          -- backoff gate
      AND t.tier        = %(pool_tier)s   -- junior pool ignores escalated work
      AND a.status      = 'running'
      AND t.seq         = a.cursor        -- <- the sequential dependency
    ORDER BY t.next_run_at
    FOR UPDATE OF t SKIP LOCKED           -- <- mutual exclusion, never blocks
    LIMIT 1
)
RETURNING id, agent_id, seq, task_def_id, tier, attempt,
          max_attempts_per_tier, lease_expires;
"""


def claim_one(pool_tier: str, worker_id: str, ttl_seconds: int = 30) -> dict[str, Any] | None:
    """
    Atomically take the next runnable task for this tier, or return None.
    """
    with pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                CLAIM_SQL,
                {"worker_id": worker_id, "ttl": ttl_seconds, "pool_tier": pool_tier},
            )
            return cur.fetchone()


def claim_task(worker_id: str, pool_tier: str, ttl_seconds: int = 30) -> dict[str, Any] | None:
    """Alias for claim_one."""
    return claim_one(pool_tier=pool_tier, worker_id=worker_id, ttl_seconds=ttl_seconds)
