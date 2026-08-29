"""
Lane F — Lease Expiry Reaper Sweep (Recovery Path)
=================================================

Sweeps expired leases and requeues tasks at the SAME tier.
"""

from __future__ import annotations

import logging
from typing import Any

from common.failures import backoff_seconds
from db.pool import get_conn, get_transaction

logger = logging.getLogger("Orchestrator.Reaper")


def sweep_expired_leases() -> list[dict[str, Any]]:
    """
    Find all running tasks whose lease expired and reclaim them.
    Preserves tier='junior' (INFRA failure never escalates).
    """
    query = """
    UPDATE task_instances SET
        status = 'pending',
        lease_owner = NULL,
        failure_class = 'INFRA',
        next_run_at = now(),
        updated_at = now()
    WHERE status = 'running' AND lease_expires < now()
    RETURNING id, agent_id, seq, tier, attempt, lease_owner;
    """

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            reclaimed = cur.fetchall()

            for t in reclaimed:
                cur.execute(
                    """
                    INSERT INTO attempts (task_instance_id, agent_id, seq, attempt_no, tier, worker_id, outcome, failure_class, started_at, ended_at)
                    VALUES (%s, %s, %s, %s, %s, %s, 'reclaimed', 'INFRA', now() - interval '30 seconds', now());
                    """,
                    (t["id"], t["agent_id"], t["seq"], t["attempt"], t["tier"], t["lease_owner"]),
                )
                logger.info(
                    f"RECLAIMED expired task {str(t['id'])[:8]} (agent={str(t['agent_id'])[:8]}, seq={t['seq']}, tier={t['tier']}) from worker '{t['lease_owner']}'"
                )

    return reclaimed
