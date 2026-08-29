"""
Lane F — Failure Classifier & Routing
=====================================

Routes failed tasks down exactly one path: Retry (INFRA), Promote (CAPABILITY), or DLQ (POISON).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from db.pool import get_transaction
from orchestrator.promote import promote_task

logger = logging.getLogger("Orchestrator.Classifier")


def process_failures() -> int:
    """
    Scan pending tasks with failure_class and route appropriately:
    - INFRA: Retry same tier with exponential backoff.
    - CAPABILITY: If attempts >= max_attempts, promote to senior.
    - POISON: Move directly to DLQ; mark task dead and agent failed.
    """
    processed = 0
    query = """
    SELECT id, agent_id, seq, task_def_id, tier, attempt, max_attempts_per_tier, failure_class, last_error
    FROM task_instances
    WHERE status = 'pending' AND failure_class IS NOT NULL
    FOR UPDATE SKIP LOCKED;
    """

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            failures = cur.fetchall()

            for f in failures:
                task_id = str(f["id"])
                agent_id = str(f["agent_id"])
                f_class = f["failure_class"]
                attempt = int(f["attempt"])
                max_att = int(f["max_attempts_per_tier"])
                tier = f["tier"]

                if f_class == "POISON":
                    # Unrecoverable poison pill -> DLQ immediately
                    logger.error(f"POISON failure on task {task_id[:8]} (agent={agent_id[:8]}): {f['last_error']}")
                    _route_to_dlq(cur, f)
                    processed += 1

                elif f_class == "CAPABILITY":
                    if tier == "junior" and attempt >= max_att:
                        # Escalation: Junior attempts exhausted -> Promote to Senior
                        logger.info(f"CAPABILITY failure exhausted junior attempts ({attempt}/{max_att}) on task {task_id[:8]}. Promoting...")
                        cur.execute(
                            """
                            UPDATE task_instances SET
                                tier = 'senior',
                                attempt = 0,
                                failure_class = NULL,
                                next_run_at = now(),
                                updated_at = now()
                            WHERE id = %s;
                            """,
                            (task_id,),
                        )
                        processed += 1
                    elif tier == "senior" and attempt >= max_att:
                        # Senior tier failed after max attempts -> Terminal failure
                        logger.error(f"CAPABILITY failure exhausted senior attempts on task {task_id[:8]}. Sending to DLQ.")
                        _route_to_dlq(cur, f)
                        processed += 1

                elif f_class == "INFRA":
                    # Infra failures are already scheduled with backoff by worker/reaper
                    pass

    return processed


def _route_to_dlq(cur: Any, task_row: dict[str, Any]) -> None:
    task_id = str(task_row["id"])
    agent_id = str(task_row["agent_id"])

    # 1. Fetch attempt trail
    cur.execute(
        """
        SELECT attempt_no, tier, worker_id, outcome, failure_class, started_at, ended_at
        FROM attempts WHERE task_instance_id = %s ORDER BY attempt_no;
        """,
        (task_id,),
    )
    trail = cur.fetchall() or []

    # 2. Insert into DLQ
    cur.execute(
        """
        INSERT INTO dlq (agent_id, seq, task_def_id, failure_class, last_error, attempt_trail)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
        """,
        (
            agent_id,
            task_row["seq"],
            task_row["task_def_id"],
            task_row["failure_class"],
            task_row["last_error"],
            json.dumps(trail, default=str),
        ),
    )

    # 3. Mark task dead and agent failed
    cur.execute("UPDATE task_instances SET status = 'dead', updated_at = now() WHERE id = %s;", (task_id,))
    cur.execute("UPDATE agents SET status = 'failed', updated_at = now() WHERE id = %s;", (agent_id,))
