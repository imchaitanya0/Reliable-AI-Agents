"""
5.1 Durable Task Queue -- keeping the queue's promise.

    Task #123 -> Persistent Queue -> Worker A crashes
              -> Task remains available -> Worker B picks it up

Durability itself is free. The queue is a table, so a task exists independently
of any worker's memory and losing a worker cannot destroy it. What is NOT free
is the property that makes "the task remains available" true in every case
rather than merely the usual one:

    EVERY RUNNING AGENT HAS A TASK IN THE QUEUE AT ITS CURSOR.

An agent that violates this is neither failed nor complete -- it is STALLED, and
nothing will ever notice on its own. Every actor in this system is triggered by
finding a row: workers claim rows, the reaper reclaims rows, the classifier
routes rows. A stalled agent is exactly the case where there is no row to find,
so it is the one failure mode that never resolves and never reports itself. The
task did not remain available; it ceased to exist.

The orchestrator is the only component that can repair it, because the thing
that should have enqueued the task is the thing that died.

WHEN DOES THIS ACTUALLY HAPPEN?
-------------------------------
The API inserts every task row up front, so in the happy path it never fires.
It fires when a row is lost some other way -- a partial insert, a manual
deletion during a demo, an agent submitted by a client that only created the
first task, or a future change to lazy row creation. The sweep costs one indexed
query per tick and converts a silent permanent stall into a self-healing blip.
That trade is worth making for a guarantee the whole pitch rests on.

Both sweeps are idempotent and bounded, so any number of orchestrators may run
them concurrently.
"""

from __future__ import annotations

import logging

from common.config import ORCHESTRATOR_BATCH
from common.tiers import base_tier
from db.pool import pool

log = logging.getLogger("orchestrator.queue")

# `plan[cursor + 1]` because Postgres arrays are 1-indexed while `cursor` is a
# 0-indexed offset into the plan.
#
# FOR UPDATE OF a SKIP LOCKED: lock the agent row, not the task row -- the task
# row is the thing that does not exist yet. Two orchestrators therefore never
# repair the same agent, and neither blocks the other.
REPAIR_SQL = """
WITH stalled AS (
    SELECT a.id AS agent_id,
           a.cursor AS seq,
           a.plan[a.cursor + 1] AS task_def_id
    FROM agents a
    WHERE a.status = 'running'
      AND a.cursor < coalesce(array_length(a.plan, 1), 0)
      AND NOT EXISTS (
          SELECT 1 FROM task_instances t
          WHERE t.agent_id = a.id AND t.seq = a.cursor
      )
    ORDER BY a.created_at
    FOR UPDATE OF a SKIP LOCKED
    LIMIT %(batch)s
)
INSERT INTO task_instances (agent_id, seq, task_def_id, tier)
SELECT agent_id, seq, task_def_id, %(tier)s FROM stalled
ON CONFLICT (agent_id, seq) DO NOTHING
RETURNING agent_id, seq, task_def_id, tier
"""

# The `status <> 'succeeded'` check is deliberate. A cursor past the end of the
# plan is necessary but NOT sufficient: without this, the sweep would happily
# mark an agent complete while one of its tasks sat dead or pending, converting
# lost work into a reported success.
FINALISE_SQL = """
WITH exhausted AS (
    SELECT a.id
    FROM agents a
    WHERE a.status = 'running'
      AND a.cursor >= coalesce(array_length(a.plan, 1), 0)
      AND NOT EXISTS (
          SELECT 1 FROM task_instances t
          WHERE t.agent_id = a.id AND t.status <> 'succeeded'
      )
    ORDER BY a.updated_at
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch)s
)
UPDATE agents a SET status = 'completed', updated_at = now()
FROM exhausted e WHERE a.id = e.id
RETURNING a.id
"""

STALLED_COUNT_SQL = """
SELECT count(*) AS n
FROM agents a
WHERE a.status = 'running'
  AND a.cursor < coalesce(array_length(a.plan, 1), 0)
  AND NOT EXISTS (
      SELECT 1 FROM task_instances t
      WHERE t.agent_id = a.id AND t.seq = a.cursor
  )
"""

# Joined to agents and restricted to running ones, because the claim query is:
# a task belonging to a stopped agent is in the table but is not work. Counting
# it would report a backlog that nothing can ever drain.
DEPTH_SQL = """
SELECT
    count(*) FILTER (
        WHERE t.status = 'pending' AND t.seq = a.cursor AND t.next_run_at <= now()
    ) AS claimable,
    count(*) FILTER (
        WHERE t.status = 'pending' AND t.seq = a.cursor AND t.next_run_at > now()
    ) AS scheduled,
    count(*) FILTER (
        WHERE t.status = 'pending' AND t.seq <> a.cursor
    ) AS waiting,
    count(*) FILTER (WHERE t.status = 'running') AS running,
    coalesce(extract(epoch FROM (now() - min(t.next_run_at) FILTER (
        WHERE t.status = 'pending' AND t.seq = a.cursor
          AND t.next_run_at <= now()))), 0) AS oldest_seconds
FROM task_instances t
JOIN agents a ON a.id = t.agent_id
WHERE t.status IN ('pending', 'running')
  AND a.status = 'running'
"""


def repair_stalled(batch: int | None = None) -> list[dict]:
    """
    Enqueue the cursor's task for any running agent that has none.

    Created at the BASE tier, always. That is not a default, it is the cost
    invariant: promotion is scoped to a task and must never leak onto the agent.
    A repaired row appearing at a higher tier would make crash recovery a silent
    upgrade path, and the cost claim would decay a little every time a worker
    died.
    """
    limit = ORCHESTRATOR_BATCH if batch is None else batch
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(REPAIR_SQL, {"batch": limit, "tier": base_tier()})
        rows = cur.fetchall()

    for r in rows:
        log.warning(
            "stalled agent repaired: agent=%s seq=%s -- no task existed at the "
            "cursor, so nothing could ever have claimed it; enqueued task_def=%s "
            "at tier=%s",
            r["agent_id"], r["seq"], r["task_def_id"], r["tier"],
        )
    return rows


def finalise_agents(batch: int | None = None) -> list[str]:
    """
    Close out agents whose plan is exhausted and whose every task succeeded.

    The worker normally does this in the same transaction as its last
    checkpoint. This is the safety net for the one that committed its final
    result and died before the status write: without it a finished agent sits in
    'running' forever and the completion count under-reports, which on stage
    reads as "the system lost work" when the work is done.
    """
    limit = ORCHESTRATOR_BATCH if batch is None else batch
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(FINALISE_SQL, {"batch": limit})
        ids = [str(r["id"]) for r in cur.fetchall()]

    for agent_id in ids:
        log.info("agent %s finalised: plan exhausted, all tasks succeeded", agent_id)
    return ids


def stalled_agents() -> int:
    """
    Agents violating the invariant right now.

    Should read zero on every tick once the repair sweep has run. A count that
    stays non-zero across ticks means the repair is failing, which matters far
    more than raw queue depth.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(STALLED_COUNT_SQL)
        return cur.fetchone()["n"]


def depth() -> dict:
    """
    Queue depth, split by what is actually claimable.

    `scheduled` rows are in the queue but gated by `next_run_at` -- backoff or
    reaper jitter. Counting them as claimable would make the queue look ready
    when nothing can be claimed.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(DEPTH_SQL)
        row = cur.fetchone() or {}
    return {
        "claimable": int(row.get("claimable") or 0),
        "scheduled": int(row.get("scheduled") or 0),
        "waiting": int(row.get("waiting") or 0),
        "running": int(row.get("running") or 0),
        "oldest_claimable_seconds": round(float(row.get("oldest_seconds") or 0.0), 2),
    }
