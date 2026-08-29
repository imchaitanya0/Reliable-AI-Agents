"""
The classifier -- the project's intellectual contribution.

Every other runtime (Celery, Temporal, SQS, Airflow) retries with the identical
configuration. That is correct for deterministic code and wrong for a
capability-bounded workload: if a model failed because it was not strong enough,
running the same model again buys the same failure at full price.

So failures are classified before they are retried:

  INFRA       the machine broke      -> retry SAME tier, backoff.  Cost: 0
  CAPABILITY  the attempt failed     -> retry, then PROMOTE.       Cost: real
  POISON      nothing fixes this     -> dead-letter queue.         Cost: 0

Escalating on INFRA would mean a kill -9 promotes work to the expensive model
for zero benefit. Escalating on POISON burns senior compute on something no
model can fix. This routing is what makes the cost claim defensible.

WHICH COUNTER PROMOTION READS -- AND WHY IT IS NOT `attempt`
------------------------------------------------------------
`task_instances.attempt` is incremented by the CLAIM query, so it counts every
time the task was handed to a worker -- INCLUDING the handouts the reaper caused
after a worker died. Driving promotion from it means a task evicted twice by
infrastructure is promoted on its FIRST genuine capability failure, having never
had its second fair attempt.

That is not a corner case here. It is the common path in the very demo this
system is built around: kill a worker, and every task it held moves a step
closer to the expensive tier. Escalation rate and cost both drift upward, the
cost thesis quietly weakens, and every test still passes.

So promotion counts CAPABILITY failures, and only those, from the `attempts`
evidence table -- where a reclaim is recorded as 'reclaimed', not 'failed'. A
kill -9 must never cost senior tokens.

INFRA retries still use `attempt`, deliberately: that budget exists to stop a
permanently dead tool consuming worker slots forever, and for that purpose the
total number of handouts is exactly the right measure.
"""

from __future__ import annotations

import json
import logging

from common.failures import backoff_seconds
from common.tiers import next_tier
from db.pool import pool

log = logging.getLogger("orchestrator.classify")

# INFRA does not count against a tier's capability budget, but it cannot retry
# forever either -- a permanently dead tool must eventually stop consuming slots.
MAX_INFRA_ATTEMPTS = 5

# Claim failed rows the same way workers claim runnable ones, so N orchestrators
# never double-route the same failure.
TAKE_FAILED_SQL = """
SELECT id, agent_id, seq, task_def_id, tier, attempt,
       max_attempts_per_tier, failure_class, last_error
FROM task_instances
WHERE status = 'failed'
ORDER BY updated_at
FOR UPDATE SKIP LOCKED
LIMIT %(limit)s
"""

RETRY_SQL = """
UPDATE task_instances
SET status = 'pending', lease_owner = NULL,
    next_run_at = now() + make_interval(secs => %(backoff)s), updated_at = now()
WHERE id = %(id)s
"""

PROMOTE_SQL = """
UPDATE task_instances
SET status = 'pending', tier = %(tier)s, attempt = 0, lease_owner = NULL,
    next_run_at = now(), updated_at = now()
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

# Capability failures AT THE CURRENT TIER. Reclaims are excluded by
# construction: the reaper records them as outcome='reclaimed', so a worker
# eviction can never consume this budget.
CAPABILITY_ATTEMPTS_SQL = """
SELECT count(*) AS n FROM attempts
WHERE task_instance_id = %(id)s
  AND tier            = %(tier)s
  AND outcome         = 'failed'
  AND failure_class   = 'CAPABILITY'
"""


def capability_attempts(cur, task: dict) -> int:
    """
    How many times this task has failed ON ITS OWN MERITS at its current tier.

    THIS is the number promotion is allowed to read -- never
    `task_instances.attempt`. See the module docstring.
    """
    cur.execute(CAPABILITY_ATTEMPTS_SQL, {"id": task["id"], "tier": task["tier"]})
    return cur.fetchone()["n"]


def _kill(cur, task: dict, reason: str) -> None:
    """Terminal. Record the whole trail -- the DLQ is for humans to read."""
    cur.execute(TRAIL_SQL, {"id": task["id"]})
    trail = [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in cur.fetchall()
    ]
    cur.execute(KILL_SQL, {"id": task["id"]})
    cur.execute(
        DLQ_SQL,
        {
            "agent_id": task["agent_id"],
            "seq": task["seq"],
            "task_def_id": task["task_def_id"],
            "fc": task["failure_class"],
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
    """Decide what one failure means. Returns the action taken."""
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

    # CAPABILITY -- the only path that ever spends more money.
    # Counted from evidence, so infra reclaims cannot fund an escalation.
    if capability_attempts(cur, task) < task["max_attempts_per_tier"]:
        cur.execute(
            RETRY_SQL,
            {"id": task["id"], "backoff": backoff_seconds(task["attempt"])},
        )
        return "retry"

    promoted = next_tier(task["tier"])
    if promoted is None:
        _kill(cur, task, f"capability exhausted at top tier {task['tier']}")
        return "dlq"

    cur.execute(PROMOTE_SQL, {"id": task["id"], "tier": promoted})
    log.info(
        "PROMOTE agent=%s seq=%s %s -> %s",
        task["agent_id"], task["seq"], task["tier"], promoted,
    )
    return "promote"


def classify(limit: int = 50) -> dict[str, int]:
    """Route every failure waiting for a decision."""
    counts = {"retry": 0, "promote": 0, "dlq": 0}
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(TAKE_FAILED_SQL, {"limit": limit})
        for task in cur.fetchall():
            counts[route_one(cur, task)] += 1
    return counts
