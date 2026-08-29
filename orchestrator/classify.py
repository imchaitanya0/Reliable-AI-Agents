"""
The failure classifier -- Lane F.
Routes failed tasks down exactly one path: Retry (INFRA), Promote (CAPABILITY), or DLQ (POISON).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from common.failures import backoff_seconds
from common.tiers import next_tier
from db.pool import pool

log = logging.getLogger("orchestrator.classify")

MAX_INFRA_ATTEMPTS = 5

TAKE_FAILED_SQL = """
SELECT id, agent_id, seq, task_def_id, tier, attempt,
       max_attempts_per_tier, failure_class, last_error, status
FROM task_instances
WHERE (status = 'failed')
   OR (status = 'pending' AND failure_class IS NOT NULL)
ORDER BY updated_at
FOR UPDATE SKIP LOCKED
LIMIT %(limit)s
"""

RETRY_SQL = """
UPDATE task_instances
SET status = 'pending', lease_owner = NULL, failure_class = NULL,
    next_run_at = now() + make_interval(secs => %(backoff)s), updated_at = now()
WHERE id = %(id)s
"""

PROMOTE_SQL = """
UPDATE task_instances
SET status = 'pending', tier = %(tier)s, attempt = 0, lease_owner = NULL,
    failure_class = NULL, next_run_at = now(), updated_at = now()
WHERE id = %(id)s
"""

KILL_SQL = """
UPDATE task_instances
SET status = 'dead', lease_owner = NULL, updated_at = now()
WHERE id = %(id)s
"""

DLQ_SQL = """
INSERT INTO dlq (agent_id, seq, task_def_id, failure_class, last_error, attempt_trail)
VALUES (%(agent_id)s, %(seq)s, %(task_def_id)s, %(fc)s, %(err)s, %(trail)s)
"""

FAIL_AGENT_SQL = "UPDATE agents SET status='failed', updated_at=now() WHERE id=%(id)s"

TRAIL_SQL = """
SELECT attempt_no, tier, outcome, failure_class, worker_id, started_at, ended_at
FROM attempts WHERE task_instance_id = %(id)s ORDER BY id
"""

CAPABILITY_ATTEMPTS_SQL = """
SELECT count(*) AS n FROM attempts
WHERE task_instance_id = %(id)s
  AND tier            = %(tier)s
  AND outcome         = 'failed'
  AND failure_class   = 'CAPABILITY'
"""


def capability_attempts(cur, task: dict) -> int:
    cur.execute(CAPABILITY_ATTEMPTS_SQL, {"id": task["id"], "tier": task["tier"]})
    row = cur.fetchone()
    count = row["n"] if row else 0
    if count == 0 and task.get("attempt", 0) > 0:
        return task["attempt"]
    return count


def _kill(cur, task: dict, reason: str) -> None:
    cur.execute(TRAIL_SQL, {"id": task["id"]})
    trail = [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in (cur.fetchall() or [])
    ]
    cur.execute(KILL_SQL, {"id": task["id"]})
    cur.execute(
        DLQ_SQL,
        {
            "agent_id": task["agent_id"],
            "seq": task["seq"],
            "task_def_id": task["task_def_id"],
            "fc": task["failure_class"] or "INFRA",
            "err": f"{reason}: {task.get('last_error') or ''}"[:2000],
            "trail": json.dumps(trail),
        },
    )
    cur.execute(FAIL_AGENT_SQL, {"id": task["agent_id"]})
    log.error(
        "DEAD agent=%s seq=%s class=%s -- %s",
        task["agent_id"], task["seq"], task["failure_class"], reason,
    )


def route_one(cur, task: dict) -> str:
    fc = task["failure_class"] or "INFRA"

    if fc == "POISON":
        _kill(cur, task, "poison -- no tier can fix this")
        return "dlq"

    if fc == "INFRA":
        if task["attempt"] >= MAX_INFRA_ATTEMPTS:
            _kill(cur, task, f"infra failed {task['attempt']}x")
            return "dlq"
        cur.execute(
            RETRY_SQL,
            {"id": task["id"], "backoff": backoff_seconds(task["attempt"])},
        )
        return "retry"

    attempts = capability_attempts(cur, task)
    if attempts < task["max_attempts_per_tier"]:
        cur.execute(
            RETRY_SQL,
            {"id": task["id"], "backoff": backoff_seconds(task["attempt"])},
        )
        return "retry"

    promoted = next_tier(task["tier"])
    if promoted is None or promoted == task["tier"]:
        _kill(cur, task, f"capability exhausted at top tier {task['tier']}")
        return "dlq"

    cur.execute(PROMOTE_SQL, {"id": task["id"], "tier": promoted})
    log.info("PROMOTE agent=%s seq=%s %s -> %s", task["agent_id"], task["seq"], task["tier"], promoted)
    return "promote"


def classify(limit: int = 50) -> dict[str, int]:
    """Route every failure waiting for a decision."""
    counts = {"retry": 0, "promote": 0, "dlq": 0}
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(TAKE_FAILED_SQL, {"limit": limit})
        for task in cur.fetchall():
            res = route_one(cur, task)
            counts[res] = counts.get(res, 0) + 1
    return counts


def process_failures(limit: int = 50) -> int:
    """Compatibility wrapper returning total processed."""
    counts = classify(limit=limit)
    return sum(counts.values())
