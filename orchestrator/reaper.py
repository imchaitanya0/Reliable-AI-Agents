"""
5.2 Task Leasing -- the reaper.

    Task #123 -> Worker A gets lease -> Worker A crashes
              -> Lease expires -> Worker B executes Task #123

A worker holds a task under a temporary lease and must keep renewing it. When it
stops -- crashed, hung, partitioned, evicted -- the task is stranded in 'running'
with nobody executing it. The reaper is the only thing that notices, which makes
it the whole mechanism behind "a task is not silently lost when a worker dies".

You cannot distinguish a crashed worker from a slow one. That is a real
impossibility result, not a gap, so the runtime does not try: it reclaims on
expiry and defends against the consequence with lease fencing in the worker's
checkpoint and the idempotency ledger on side-effecting actions.

Requeues at the SAME tier. A dead machine says nothing about model capability,
so promoting here would spend senior tokens for no reason.

WHY THE OBVIOUS VERSION IS NOT ENOUGH
-------------------------------------
    UPDATE task_instances SET status='pending'
    WHERE status='running' AND lease_expires < now();

Correct on one machine with one orchestrator and a handful of tasks. Every one
of its problems appears only under the conditions the reaper exists for -- many
workers failing at once:

  FOR UPDATE SKIP LOCKED   Without it a second orchestrator BLOCKS on the first
                           rather than stepping past it. "Run three, kill one,
                           nothing changes" becomes "run three, one works and
                           two wait", and killing the busy one stalls recovery
                           for a lock timeout.

  LIMIT batch              A mass eviction must not be reclaimed in one
                           statement that holds locks for as long as the outage
                           lasted. Bounded sweeps simply run again next tick:
                           recovery is marginally slower and the database stays
                           responsive when it can least afford not to be.

  random() jitter          Returning a thousand tasks at exactly now() does not
                           remove the stampede, it relocates it from the reaper
                           to the workers' claim query.

The reclaim and its `attempts` evidence are written in ONE statement. A reaper
that dies mid-sweep can then never leave a reclaimed-but-unrecorded task -- and
that evidence row is what makes "% tasks recovered" a count rather than a guess.
A killed worker never writes its own attempt row, because it died.
"""

from __future__ import annotations

import logging

from common.config import REAPER_BATCH, REAPER_JITTER_SECONDS
from db.pool import pool

log = logging.getLogger("orchestrator.reaper")

# One statement: select the batch under SKIP LOCKED, requeue it, and record the
# evidence. The CTEs share a single snapshot, so the rows reclaimed and the rows
# recorded are guaranteed to be the same rows.
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


def reap(batch: int | None = None) -> list[dict]:
    """
    Reclaim one batch of expired leases. Safe to run concurrently in N instances.

    `attempt` is deliberately NOT reset. It is the honest count of how many times
    the task has been handed to a worker, and resetting it would hide a task
    being evicted over and over. Because of that, promotion must never be driven
    by it -- see orchestrator/classify.py, which counts CAPABILITY evidence
    instead so that a kill -9 can never cost senior tokens.
    """
    limit = REAPER_BATCH if batch is None else batch
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(REAP_SQL, {"batch": limit, "jitter": REAPER_JITTER_SECONDS})
        rows = cur.fetchall()

    for r in rows:
        log.warning(
            "reclaimed agent=%s seq=%s tier=%s attempt=%s from worker=%s "
            "(lease %.1fs overdue)",
            r["agent_id"], r["seq"], r["tier"], r["attempt"],
            r["lease_owner"], float(r["overdue_seconds"] or 0.0),
        )

    if len(rows) == limit:
        log.warning(
            "reaper batch saturated at %d -- more expired leases remain, "
            "continuing next tick", limit,
        )
    return rows


def orphaned_leases() -> int:
    """
    Expired leases not yet reclaimed -- the reaper's backlog.

    Briefly non-zero is normal: it is the window between a lease expiring and
    the next tick. Persistently non-zero means the batch size is too small for
    the current failure rate.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(ORPHAN_COUNT_SQL)
        return cur.fetchone()["n"]
