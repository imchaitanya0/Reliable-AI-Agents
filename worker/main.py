"""
Lane C — Worker Main Process Loop
=================================

Stateless worker process coordinating task claims, heartbeats, and atomic checkpoints.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from typing import Any

from common.config import (
    LEASE_TTL_SECONDS,
    POLL_INTERVAL_SECONDS,
    POOL_TIER,
    WORKER_ID,
)
from common.failures import TaskFailure, backoff_seconds
from db.pool import get_conn, get_transaction
from worker.claim import claim_task
from worker.executor import execute_claimed_task
from worker.heartbeat import task_heartbeat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(f"Worker({WORKER_ID})")


def run_worker_loop(pool_tier: str = POOL_TIER, worker_id: str = WORKER_ID, once: bool = False) -> None:
    """Continuous worker execution loop."""
    logger.info(f"Starting worker process. Pool Tier: {pool_tier}, ID: {worker_id}")
    running = True

    def _handle_sigterm(signum, frame):
        nonlocal running
        logger.info("Received termination signal. Shutting down gracefully...")
        running = False

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    while running:
        task = claim_task(worker_id=worker_id, pool_tier=pool_tier, ttl_seconds=LEASE_TTL_SECONDS)
        if not task:
            if once:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        task_id = str(task["id"])
        agent_id = str(task["agent_id"])
        seq = int(task["seq"])
        attempt_id = task.get("attempt_id")
        attempt_no = int(task["attempt"])

        logger.info(f"Claimed task instance {task_id[:8]} (agent={agent_id[:8]}, seq={seq}, tier={pool_tier}, attempt={attempt_no})")

        # Execute with background lease renewal
        try:
            with task_heartbeat(task_id, ttl_seconds=LEASE_TTL_SECONDS):
                result, cost_units = execute_claimed_task(task)

            # Atomic Checkpoint Commit on Success
            with get_transaction() as conn:
                with conn.cursor() as cur:
                    # 1. Fetch agent plan to check if this is the final step
                    cur.execute("SELECT plan FROM agents WHERE id = %s;", (agent_id,))
                    agent = cur.fetchone()
                    plan = agent["plan"] if agent else []
                    total_steps = len(plan)

                    # 2. Mark task succeeded
                    cur.execute(
                        """
                        UPDATE task_instances
                        SET status = 'succeeded', result = %s, failure_class = NULL, updated_at = now()
                        WHERE id = %s;
                        """,
                        (json.dumps(result), task_id),
                    )

                    # 3. Advance cursor, merge context, update cost units
                    cur.execute(
                        """
                        UPDATE agents
                        SET cursor = cursor + 1,
                            context = context || jsonb_build_object(%s::text, %s::jsonb),
                            cost_units = cost_units + %s,
                            updated_at = now()
                        WHERE id = %s;
                        """,
                        (str(seq), json.dumps(result), cost_units, agent_id),
                    )

                    # 4. Spawn next task instance at tier='junior' OR mark agent completed
                    if seq + 1 < total_steps:
                        next_task_def_id = plan[seq + 1]
                        cur.execute(
                            """
                            INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier)
                            VALUES (%s, %s, %s, 'pending', 'junior')
                            ON CONFLICT (agent_id, seq) DO NOTHING;
                            """,
                            (agent_id, seq + 1, next_task_def_id),
                        )
                    else:
                        cur.execute("UPDATE agents SET status = 'completed', updated_at = now() WHERE id = %s;", (agent_id,))

                    # 5. Record attempt completion
                    if attempt_id:
                        cur.execute(
                            """
                            UPDATE attempts
                            SET outcome = 'succeeded', cost_units = %s, ended_at = now()
                            WHERE id = %s;
                            """,
                            (cost_units, attempt_id),
                        )

            logger.info(f"Task {task_id[:8]} SUCCEEDED. Checkpoint committed for seq={seq}.")

        except TaskFailure as fail:
            logger.warning(f"Task {task_id[:8]} FAILED with {fail.failure_class}: {fail.detail}")
            backoff_sec = backoff_seconds(attempt_no)

            with get_transaction() as conn:
                with conn.cursor() as cur:
                    # Update task failure info
                    cur.execute(
                        """
                        UPDATE task_instances
                        SET status = 'pending',
                            lease_owner = NULL,
                            failure_class = %s,
                            last_error = %s,
                            next_run_at = now() + make_interval(secs => %s),
                            updated_at = now()
                        WHERE id = %s;
                        """,
                        (fail.failure_class, str(fail.detail), backoff_sec, task_id),
                    )
                    # Update attempt log
                    if attempt_id:
                        cur.execute(
                            """
                            UPDATE attempts
                            SET outcome = 'failed', failure_class = %s, ended_at = now()
                            WHERE id = %s;
                            """,
                            (fail.failure_class, attempt_id),
                        )

        except Exception as exc:
            logger.error(f"Unexpected error executing task {task_id[:8]}: {exc}", exc_info=True)
            with get_transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE task_instances
                        SET status = 'pending', lease_owner = NULL, failure_class = 'INFRA',
                            last_error = %s, next_run_at = now() + interval '2 seconds', updated_at = now()
                        WHERE id = %s;
                        """,
                        (str(exc), task_id),
                    )

        if once:
            break


if __name__ == "__main__":
    run_worker_loop()
