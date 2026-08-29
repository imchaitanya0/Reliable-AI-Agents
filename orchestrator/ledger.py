"""
5.3 Idempotency -- recognising an action that already happened.

    Worker A -> Create Jira ticket -> Success
             -> Worker crashes before acknowledgement
             -> Retry -> Check Action ID
             -> Already executed -> do not create duplicate

Lease recovery guarantees that a slow-but-alive worker and its replacement will
sometimes run the same task at the same time. That is accepted, not prevented --
you cannot tell a dead worker from a slow one. This ledger is what makes it
harmless: every externally visible action carries an action ID derived from the
task's identity, so the original attempt and every recovery attempt compute the
same value.

WHY TWO PHASES -- READ THE FAILURE ABOVE AGAIN
----------------------------------------------
The crash is AFTER the action succeeds. That single detail rules out the obvious
implementation, which is to perform the action and then record the id:

    create Jira ticket  ->  [WORKER DIES HERE]  ->  record action id

The retry checks the id, finds nothing, and creates a second ticket -- which is
precisely the sequence this section exists to prevent. It also fails invisibly:
`SELECT count(*) FROM idempotency WHERE action_type = ...` still answers 1 while
two tickets exist in Jira, so the natural assertion passes.

No single-phase ledger can close that window, because the result does not exist
until the action has already had its effect. So the id is RESERVED before the
action and SETTLED after it:

    begin() -> PROCEED    nobody holds this id. It is yours: act, then settle.
            -> DONE       already executed. `result` is authoritative -- return
                          it, do not act.
            -> IN_FLIGHT  reserved and never settled. Either a twin is
                          mid-action, or a worker died in the window above.
                          EITHER WAY THE EFFECT MAY ALREADY EXIST, so do not act.

IN_FLIGHT is genuine ambiguity, and this module reports it instead of guessing.
Calling it "not done" recreates the duplicate; calling it "done" invents a
result that may never have existed. The caller treats it as an INFRA failure and
retries shortly, by which time a live twin has usually settled the id -- and the
real result is returned.

WHEN THE TWIN NEVER COMES BACK: RECONCILIATION
-----------------------------------------------
Refusing to act is correct, but refusing forever is not. A worker that died in
the window leaves a reservation nobody will ever settle, and a caller that keeps
retrying it makes no progress -- turning one crashed worker into a permanently
stuck task, which is strictly worse than either answer. Strictness without a
resolution path is not safety, it is a different way to lose the work.

So after RECONCILE_AFTER lease periods the reservation is provably orphaned, and
`reconcile()` closes it as UNRESOLVED: the action is marked done so the workflow
proceeds, with a result that says plainly that the effect is unknown.

That is a deliberate choice of AT-MOST-ONCE over at-least-once for the ambiguous
case, and it is the same call a payment system makes -- you do not blind-retry a
charge you might already have made, you flag it for reconciliation. The
alternative policy, re-running the action, would trade a stuck task for exactly
the duplicate this section exists to prevent.

Every reconciled action is counted and logged loudly. It is not swept away: an
unresolved effect is a real operational fact and a human may need to check Jira.

The reservation MUST be committed before the action runs. A reservation that
rolls back together with the action is not a reservation -- it is the
single-phase design again, with the same hole.
"""

from __future__ import annotations

import hashlib
import json
import logging

from common.config import LEASE_TTL_SECONDS
from db.pool import pool

log = logging.getLogger("orchestrator.ledger")

PROCEED = "proceed"
DONE = "done"
IN_FLIGHT = "in_flight"

# How many lease periods an unsettled reservation must survive before its owner
# is considered provably gone. Longer than one lease so a merely slow worker --
# which still holds a renewable lease -- is never reconciled out from under.
RECONCILE_AFTER = 2

#: Marker stored when an action's effect could not be determined. Deliberately
#: not shaped like a real tool result, so nothing downstream mistakes it for one.
UNRESOLVED = {"status": "unresolved",
              "reason": "worker died between the action and its acknowledgement",
              "effect": "unknown -- may or may not have been applied"}

# One statement, so two concurrent workers cannot both read "absent" and both
# act on it.
#
# DO UPDATE, not DO NOTHING -- and the difference is the whole race.
# With DO NOTHING the loser's INSERT is skipped and its follow-up SELECT runs on
# the SAME statement snapshot, which predates the winner's commit. The row is
# therefore invisible to it: the loser sees neither its own insert nor the
# winner's, concludes nothing at all, and has no basis to refuse to act. DO
# UPDATE instead takes a lock on the conflicting row, waits for the winner to
# commit, and hands back what is actually there.
#
# The SET is a deliberate no-op that rewrites a column to its own value. It
# exists purely to make the conflict path lock and return; state, result and
# settled_at must survive untouched, or a second caller would erase the very
# result it is supposed to replay.
#
# `xmax = 0` distinguishes the two paths: it is zero only for a row this
# statement inserted, non-zero for one it locked.
BEGIN_SQL = """
INSERT INTO idempotency (key, agent_id, seq, action_type, state)
VALUES (%(key)s, %(agent_id)s, %(seq)s, %(action_type)s, 'in_flight')
ON CONFLICT (key) DO UPDATE SET action_type = idempotency.action_type
RETURNING key, state, result, (xmax = 0) AS reserved_now
"""

SETTLE_SQL = """
UPDATE idempotency
SET state = 'done', result = %(result)s, settled_at = now()
WHERE key = %(key)s AND state = 'in_flight'
"""

RELEASE_SQL = "DELETE FROM idempotency WHERE key = %(key)s AND state = 'in_flight'"

LOOKUP_SQL = "SELECT key, state, result FROM idempotency WHERE key = %(key)s"

STRANDED_SQL = """
SELECT key, agent_id, seq, action_type,
       extract(epoch FROM (now() - created_at)) AS age_seconds
FROM idempotency
WHERE state = 'in_flight'
  AND created_at < now() - make_interval(secs => %(age)s)
ORDER BY created_at
"""

COUNTS_SQL = """
SELECT count(*) FILTER (WHERE state = 'done')      AS settled,
       count(*) FILTER (WHERE state = 'in_flight') AS in_flight
FROM idempotency
"""


def action_id(agent_id: str, seq: int, action_type: str) -> str:
    """
    The deterministic action ID.

    Derived from the task's identity and nothing else, so the original attempt
    and every recovery attempt compute the same digest. That property IS the
    mechanism. Mirrors common.protocol.TaskContext.key_for.
    """
    return hashlib.sha256(f"{agent_id}:{seq}:{action_type}".encode()).hexdigest()


def begin(agent_id: str, seq: int, action_type: str) -> tuple[str, dict | None, str]:
    """
    Reserve an action ID. Call BEFORE the side effect.

    Returns (status, stored_result, key) where status is PROCEED, DONE or
    IN_FLIGHT. Only PROCEED clears the caller to act.
    """
    key = action_id(agent_id, seq, action_type)
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            BEGIN_SQL,
            {"key": key, "agent_id": agent_id, "seq": seq, "action_type": action_type},
        )
        row = cur.fetchone()

    if row is None:  # pragma: no cover - only if the row vanished mid-statement
        raise RuntimeError(f"action id {key} could neither be reserved nor read")

    if row["reserved_now"]:
        return PROCEED, None, key

    if row["state"] == "done":
        log.info(
            "already executed agent=%s seq=%s action=%s -- returning stored "
            "result, no duplicate created",
            agent_id, seq, action_type,
        )
        return DONE, row["result"], key

    log.warning(
        "action id in flight agent=%s seq=%s action=%s -- a twin holds it and "
        "its outcome is unknown; NOT acting again",
        agent_id, seq, action_type,
    )
    return IN_FLIGHT, None, key


def settle(key: str, result: dict) -> bool:
    """
    Record the action's result. Call AFTER the side effect succeeded.

    Idempotent: settling an already-settled id is a no-op returning False rather
    than an overwrite. The first result is the one the action actually produced.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(SETTLE_SQL, {"key": key, "result": json.dumps(result)})
        return cur.rowcount == 1


def release(key: str) -> bool:
    """
    Drop an unsettled reservation.

    ONLY legitimate when the caller knows the action did not happen -- it raised
    before making the call, or the tool refused it outright. If there is any
    doubt, leave the reservation and let the audit sweep surface it: releasing
    an id whose action DID fire is exactly how a duplicate gets created.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(RELEASE_SQL, {"key": key})
        return cur.rowcount == 1


def lookup(key: str) -> dict | None:
    """Read an action ID without reserving it. Diagnostics and tests."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(LOOKUP_SQL, {"key": key})
        return cur.fetchone()


def stranded(older_than_seconds: float | None = None) -> list[dict]:
    """
    Reservations never settled and older than a lease.

    Each one is an action whose outcome is genuinely unknown: a worker died
    between reserving the id and settling it. This is the honest measurement
    behind "duplicate actions prevented" -- it counts the cases where a
    duplicate WOULD have happened without the ledger.
    """
    age = LEASE_TTL_SECONDS if older_than_seconds is None else older_than_seconds
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(STRANDED_SQL, {"age": age})
        return cur.fetchall()


def counts() -> dict[str, int]:
    """Settled and in-flight totals, for the metrics snapshot."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(COUNTS_SQL)
        row = cur.fetchone() or {}
    return {
        "settled": int(row.get("settled") or 0),
        "in_flight": int(row.get("in_flight") or 0),
    }


RECONCILE_SQL = """
UPDATE idempotency
SET state = 'done', result = %(marker)s, settled_at = now()
WHERE state = 'in_flight'
  AND created_at < now() - make_interval(secs => %(age)s)
RETURNING key, agent_id, seq, action_type
"""


def audit() -> int:
    """
    Report reservations that have outlived a lease. Returns how many.

    Reporting only -- it does not resolve them. A reservation younger than
    RECONCILE_AFTER leases may still belong to a live worker, and settling it
    would erase a result that is about to arrive.
    """
    rows = stranded()
    for r in rows:
        log.warning(
            "stranded action id agent=%s seq=%s action=%s unsettled for %.0fs "
            "-- outcome unknown, duplicate prevented",
            r["agent_id"], r["seq"], r["action_type"], float(r["age_seconds"]),
        )
    return len(rows)


def reconcile(after_seconds: float | None = None) -> list[dict]:
    """
    Close out reservations whose owner is provably gone.

    Without this, one worker dying in the reserve/settle window leaves a task
    that can never make progress: every retry sees IN_FLIGHT, refuses to act,
    and fails again until it dead-letters. That is a worse outcome than either
    honest answer.

    The action is marked done with the UNRESOLVED marker rather than re-run.
    Re-running would produce exactly the duplicate side effect this ledger
    exists to prevent; marking it lets the workflow continue while stating
    plainly that the effect is unknown. Each one is logged at ERROR because it
    is a real operational fact -- somebody may have to go and look in Jira.
    """
    from json import dumps

    age = (
        LEASE_TTL_SECONDS * RECONCILE_AFTER
        if after_seconds is None
        else after_seconds
    )
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(RECONCILE_SQL, {"marker": dumps(UNRESOLVED), "age": age})
        rows = cur.fetchall()

    for r in rows:
        log.error(
            "UNRESOLVED action agent=%s seq=%s action=%s -- reserved but never "
            "acknowledged; marked unresolved so the workflow proceeds. The "
            "effect may or may not exist and was NOT retried.",
            r["agent_id"], r["seq"], r["action_type"],
        )
    return rows
