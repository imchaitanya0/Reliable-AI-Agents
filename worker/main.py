"""
Worker loop: claim -> heartbeat -> execute -> checkpoint -> repeat.
Stateless worker process coordinating task claims, heartbeats, and atomic checkpoints.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from psycopg.types.json import Jsonb

from common.config import (
    HEARTBEAT_INTERVAL,
    LEASE_TTL_SECONDS,
    POLL_INTERVAL_SECONDS,
    POOL_TIER,
    WORKER_ID,
    WORKER_POLL_SECONDS,
)
from common.failures import TaskFailure
from db.pool import pool
from worker.claim import claim_one, claim_task
from worker.executor import load_registry, run_task
from worker.heartbeat import Heartbeat

log = logging.getLogger("worker")

_shutdown = False


def _handle_sigterm(signum: int, frame: object) -> None:
    """Graceful stop: finish the task in hand, then exit."""
    global _shutdown
    _shutdown = True
    log.info("shutdown requested -- finishing current task")


# --- SQL ---------------------------------------------------------------------

LOAD_AGENT_SQL = "SELECT id, plan, cursor, status, context FROM agents WHERE id = %s"

MARK_SUCCEEDED_SQL = """
UPDATE task_instances
SET status = 'succeeded', result = %(result)s,
    failure_class = NULL, last_error = NULL, updated_at = now()
WHERE id = %(task_id)s
  AND (lease_owner = %(worker_id)s OR %(worker_id)s IS NULL)
  AND status = 'running'
"""

ADVANCE_AGENT_SQL = """
UPDATE agents
SET context     = context || %(entry)s,
    cursor      = cursor + 1,
    cost_units  = cost_units + %(cost)s,
    tokens_used = tokens_used + %(tokens)s,
    status      = CASE
                    WHEN cursor + 1 >= coalesce(array_length(plan, 1), 0)
                    THEN 'completed' ELSE status
                  END,
    updated_at  = now()
WHERE id = %(agent_id)s AND cursor = %(seq)s
RETURNING cursor, status
"""

REPORT_FAILURE_SQL = """
UPDATE task_instances
SET status = 'failed', failure_class = %(fc)s,
    last_error = %(err)s, updated_at = now()
WHERE id = %(task_id)s
  AND (lease_owner = %(worker_id)s OR %(worker_id)s IS NULL)
  AND status = 'running'
"""

RECORD_ATTEMPT_SQL = """
INSERT INTO attempts (task_instance_id, agent_id, seq, attempt_no, tier,
                      worker_id, outcome, failure_class, cost_units, tokens,
                      started_at, ended_at)
VALUES (%(task_id)s, %(agent_id)s, %(seq)s, %(attempt_no)s, %(tier)s,
        %(worker_id)s, %(outcome)s, %(fc)s, %(cost)s, %(tokens)s,
        %(started)s, now())
"""


def load_agent(agent_id: str) -> dict[str, Any] | None:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(LOAD_AGENT_SQL, (agent_id,))
        return cur.fetchone()


def checkpoint_success(
    task: dict[str, Any],
    result: dict[str, Any],
    cost: int,
    tokens: int,
    started: datetime,
    worker_id: str,
) -> bool:
    """
    Commit a completed task.
    """
    with pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                MARK_SUCCEEDED_SQL,
                {
                    "result": Jsonb(result),
                    "task_id": task["id"],
                    "worker_id": worker_id,
                },
            )
            if cur.rowcount == 0:
                conn.rollback()
                log.warning("lease lost before checkpoint task=%s -- discarding result", task["id"])
                return False

            cur.execute(
                ADVANCE_AGENT_SQL,
                {
                    "entry": Jsonb({str(task["seq"]): result}),
                    "cost": cost,
                    "tokens": tokens,
                    "agent_id": task["agent_id"],
                    "seq": task["seq"],
                },
            )
            if cur.fetchone() is None:
                conn.rollback()
                log.warning("cursor moved under us agent=%s seq=%s -- discarding", task["agent_id"], task["seq"])
                return False

            cur.execute(
                RECORD_ATTEMPT_SQL,
                {
                    "task_id": task["id"],
                    "agent_id": task["agent_id"],
                    "seq": task["seq"],
                    "attempt_no": task["attempt"],
                    "tier": task["tier"],
                    "worker_id": worker_id,
                    "outcome": "succeeded",
                    "fc": None,
                    "cost": cost,
                    "tokens": tokens,
                    "started": started,
                },
            )
    return True


def report_failure(
    task: dict[str, Any],
    failure: TaskFailure,
    started: datetime,
    worker_id: str,
) -> None:
    """Record what happened. The orchestrator decides what it means."""
    with pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                REPORT_FAILURE_SQL,
                {
                    "fc": failure.failure_class,
                    "err": failure.detail[:2000],
                    "task_id": task["id"],
                    "worker_id": worker_id,
                },
            )
            if cur.rowcount == 0:
                conn.rollback()
                log.warning("lease lost before failure report task=%s", task["id"])
                return

            cur.execute(
                RECORD_ATTEMPT_SQL,
                {
                    "task_id": task["id"],
                    "agent_id": task["agent_id"],
                    "seq": task["seq"],
                    "attempt_no": task["attempt"],
                    "tier": task["tier"],
                    "worker_id": worker_id,
                    "outcome": "failed",
                    "fc": failure.failure_class,
                    "cost": 0,
                    "tokens": 0,
                    "started": started,
                },
            )


def process_one(
    pool_tier: str, worker_id: str, registry: dict | None = None
) -> bool:
    """Claim and run a single task. Returns False when the queue is empty."""
    task = claim_one(pool_tier, worker_id, LEASE_TTL_SECONDS)
    if task is None:
        return False

    started = datetime.now(timezone.utc)
    hb = Heartbeat(task["id"], worker_id, LEASE_TTL_SECONDS, HEARTBEAT_INTERVAL).start()
    try:
        agent = load_agent(task["agent_id"])
        if agent is None:
            from common.failures import PoisonFailure
            report_failure(task, PoisonFailure("agent row vanished"), started, worker_id)
            return True

        result, cost, tokens, duplicate = run_task(task, agent, registry)

        if hb.lost:
            log.warning("lease lost during execution task=%s -- discarding", task["id"])
            return True

        ok = checkpoint_success(task, result, cost, tokens, started, worker_id)
        if ok:
            log.info(
                "ok agent=%s seq=%s tier=%s%s",
                task["agent_id"],
                task["seq"],
                task["tier"],
                " (duplicate suppressed)" if duplicate else "",
            )
    except TaskFailure as exc:
        log.info(
            "%s agent=%s seq=%s tier=%s: %s",
            exc.failure_class,
            task["agent_id"],
            task["seq"],
            task["tier"],
            exc.detail,
        )
        report_failure(task, exc, started, worker_id)
    finally:
        hb.stop()
    return True


def run_worker_loop(pool_tier: str = POOL_TIER, worker_id: str = WORKER_ID, once: bool = False) -> None:
    registry = load_registry()
    if once:
        process_one(pool_tier, worker_id, registry)
        return

    while not _shutdown:
        try:
            did_work = process_one(pool_tier, worker_id, registry)
        except Exception:
            time.sleep(1.0)
            continue
        if not did_work:
            time.sleep(WORKER_POLL_SECONDS)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [" + WORKER_ID + "] %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    registry = load_registry()
    log.info("worker up tier=%s lease_ttl=%ss tasks_known=%d", POOL_TIER, LEASE_TTL_SECONDS, len(registry))

    idle = 0
    while not _shutdown:
        try:
            did_work = process_one(POOL_TIER, WORKER_ID, registry)
        except Exception:
            log.exception("worker loop error")
            time.sleep(1.0)
            continue

        if did_work:
            idle = 0
        else:
            idle += 1
            time.sleep(min(WORKER_POLL_SECONDS * min(idle, 6), 3.0))

    log.info("worker stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
