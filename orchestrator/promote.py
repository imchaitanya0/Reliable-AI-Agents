"""
Lane F — Tier Promotion (Escalation Path)
=========================================

Escalates capability-failed tasks to Senior tier.
INVARIANT: Promotion is strictly task-scoped, never agent-scoped.
"""

from __future__ import annotations

import logging
from typing import Any

from db.pool import get_transaction

logger = logging.getLogger("Orchestrator.Promote")


def promote_task(task_id: str) -> bool:
    """Promote a junior task instance to senior tier after exhausting attempts."""
    query = """
    UPDATE task_instances SET
        tier = 'senior',
        attempt = 0,
        status = 'pending',
        next_run_at = now(),
        updated_at = now()
    WHERE id = %s AND tier = 'junior' AND status = 'pending'
    RETURNING id, agent_id, seq;
    """

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (task_id,))
            row = cur.fetchone()
            if row:
                logger.info(
                    f"PROMOTED task {task_id[:8]} (agent={str(row['agent_id'])[:8]}, seq={row['seq']}) to SENIOR tier."
                )
                return True
    return False
