"""
The reaper -- crash recovery, in full.

A worker that stops renewing its lease has, as far as anyone can tell, died.
You cannot distinguish that from merely slow, so the runtime does not try: it
reclaims on expiry and defends against double-execution with lease fencing in
the worker's checkpoint and idempotency keys on side-effecting actions.

Requeues at the SAME tier. A dead machine says nothing about model capability,
so promoting here would spend senior tokens for no reason.
"""

from __future__ import annotations

import logging

from db.pool import pool

log = logging.getLogger("orchestrator.reaper")

REAP_SQL = """
UPDATE task_instances
SET status        = 'pending',
    lease_owner   = NULL,
    failure_class = 'INFRA',
    last_error    = 'lease expired -- worker presumed dead',
    next_run_at   = now(),
    updated_at    = now()
WHERE status = 'running'
  AND lease_expires < now()
RETURNING id, agent_id, seq, tier, attempt, lease_owner
"""

# A killed worker never gets to write its own attempts row -- it died. The
# reaper writes it instead, so recovery stays visible in the evidence table
# even after the retry succeeds and clears failure_class on the task row.
RECORD_RECLAIM_SQL = """
INSERT INTO attempts (task_instance_id, agent_id, seq, attempt_no, tier,
                      worker_id, outcome, failure_class, ended_at)
VALUES (%(id)s, %(agent_id)s, %(seq)s, %(attempt)s, %(tier)s,
        %(worker)s, 'reclaimed', 'INFRA', now())
"""


def reap() -> list[dict]:
    """Reclaim every expired lease. Safe to run concurrently in N instances."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(REAP_SQL)
        rows = cur.fetchall()
        for r in rows:
            cur.execute(
                RECORD_RECLAIM_SQL,
                {
                    "id": r["id"], "agent_id": r["agent_id"], "seq": r["seq"],
                    "attempt": r["attempt"], "tier": r["tier"],
                    "worker": r["lease_owner"],
                },
            )
    for r in rows:
        log.warning(
            "reclaimed agent=%s seq=%s tier=%s attempt=%s",
            r["agent_id"], r["seq"], r["tier"], r["attempt"],
        )
    return rows
