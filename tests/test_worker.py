"""
Lane C invariants, as automated fault injection.

Screenshots are not evidence. Every guarantee the worker claims is exercised
here against a real Postgres.
"""

from __future__ import annotations

import threading

from tests.conftest import get_agent, get_tasks, seed_agent

from db.pool import pool
from worker.claim import claim_one
from worker.main import process_one

TTL = 3


# --- ordering ----------------------------------------------------------------

def test_task_not_claimable_before_predecessor_commits(registry):
    """INVARIANT 4: seq=n never starts before seq=n-1 commits."""
    agent_id = seed_agent([1, 2, 1])

    first = claim_one("junior", "w1", TTL)
    assert first["seq"] == 0

    # seq=1 exists and is pending, but the cursor is still 0.
    assert claim_one("junior", "w2", TTL) is None

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM task_instances WHERE agent_id=%s AND seq=1",
                    (agent_id,))
        assert cur.fetchone()["status"] == "pending"


def test_full_plan_runs_in_order(registry):
    agent_id = seed_agent([1, 2, 1])
    for _ in range(3):
        assert process_one("junior", "w1", registry) is True

    agent = get_agent(agent_id)
    assert agent["cursor"] == 3
    assert agent["status"] == "completed"
    assert sorted(int(k) for k in agent["context"]) == [0, 1, 2]
    assert [t["status"] for t in get_tasks(agent_id)] == ["succeeded"] * 3


def test_context_accumulates_forward(registry):
    """Each task sees every prior result."""
    agent_id = seed_agent([1, 2, 1])
    for _ in range(3):
        process_one("junior", "w1", registry)

    ctx = get_agent(agent_id)["context"]
    assert ctx["0"]["saw"] == []
    assert ctx["1"]["saw"] == [0]
    assert ctx["2"]["saw"] == [0, 1]


# --- mutual exclusion --------------------------------------------------------

def test_concurrent_workers_never_claim_the_same_task(registry):
    """SKIP LOCKED is the whole mutual-exclusion story."""
    for _ in range(12):
        seed_agent([1])

    claimed: list[str] = []
    lock = threading.Lock()

    def grab(worker: str) -> None:
        for _ in range(12):
            row = claim_one("junior", worker, TTL)
            if row is None:
                return
            with lock:
                claimed.append(str(row["id"]))

    threads = [threading.Thread(target=grab, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == 12
    assert len(set(claimed)) == 12, "a task was claimed twice"


def test_junior_pool_does_not_claim_senior_work(registry):
    """Tier scoping: pools drain disjoint queues."""
    seed_agent([4], tiers=["senior"])
    assert claim_one("junior", "w1", TTL) is None
    assert claim_one("senior", "s1", TTL) is not None


# --- leasing and recovery ----------------------------------------------------

def test_expired_lease_becomes_claimable_again(registry):
    """INVARIANT 1: an unrenewed lease is eventually reclaimable."""
    seed_agent([1])
    first = claim_one("junior", "w1", TTL)
    assert first is not None
    assert claim_one("junior", "w2", TTL) is None, "claimable while leased"

    # Simulate w1 dying: expire the lease, then run the reaper's UPDATE.
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET lease_expires = now() - interval '1s' WHERE id=%s",
            (first["id"],),
        )
        cur.execute(
            """UPDATE task_instances
               SET status='pending', lease_owner=NULL, failure_class='INFRA'
               WHERE status='running' AND lease_expires < now()"""
        )

    second = claim_one("junior", "w2", TTL)
    assert second is not None and second["id"] == first["id"]
    assert second["attempt"] == 2


def test_stale_worker_checkpoint_is_discarded(registry):
    """
    Lease fencing. A slow-but-alive worker whose task was reclaimed must not be
    able to commit -- that would double-advance the agent's cursor.
    """
    agent_id = seed_agent([1, 2])
    task = claim_one("junior", "slow-worker", TTL)

    # Reaper reclaims it and a second worker takes over and finishes it.
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE task_instances
               SET status='pending', lease_owner=NULL WHERE id=%s""",
            (task["id"],),
        )
    process_one("junior", "fast-worker", registry)
    assert get_agent(agent_id)["cursor"] == 1

    # Now the original worker tries to commit its stale result.
    from datetime import datetime, timezone

    from worker.main import checkpoint_success

    ok = checkpoint_success(
        task, {"stale": True}, 1, 100, datetime.now(timezone.utc), "slow-worker"
    )
    assert ok is False, "stale worker was allowed to commit"
    assert get_agent(agent_id)["cursor"] == 1, "cursor double-advanced"


# --- failure classification --------------------------------------------------

def test_failures_are_reported_not_routed(registry):
    """
    The worker records a class and stops. Routing belongs to the orchestrator,
    which is what keeps that component stateless.
    """
    for task_id, expected in ((5, "INFRA"), (6, "POISON"), (4, "CAPABILITY")):
        agent_id = seed_agent([task_id])
        process_one("junior", "w1", registry)
        task = get_tasks(agent_id)[0]
        assert task["status"] == "failed"
        assert task["failure_class"] == expected
        assert get_agent(agent_id)["cursor"] == 0, "cursor moved on failure"


def test_unknown_task_id_is_poison(registry):
    """Nothing fixes an unknown task id -- not a retry, not a bigger model."""
    agent_id = seed_agent([999])
    process_one("junior", "w1", registry)
    assert get_tasks(agent_id)[0]["failure_class"] == "POISON"


def test_hard_task_succeeds_on_senior(registry):
    """
    The escalation payoff. Same task, same context, different tier.
    (The orchestrator performs the promotion; here we assert the tier actually
    changes the outcome, which is what makes promotion worth paying for.)
    """
    agent_id = seed_agent([4])
    process_one("junior", "w1", registry)
    assert get_tasks(agent_id)[0]["failure_class"] == "CAPABILITY"

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE task_instances SET status='pending', tier='senior',
               attempt=0, lease_owner=NULL WHERE agent_id=%s""",
            (agent_id,),
        )

    process_one("senior", "s1", registry)
    task = get_tasks(agent_id)[0]
    assert task["status"] == "succeeded"
    assert task["result"]["solved_by"] == "senior"
    assert get_agent(agent_id)["cost_units"] == 12


def test_promotion_does_not_leak_to_successor(registry):
    """
    INVARIANT 6 -- the one the entire cost argument rests on. After a promoted
    task succeeds, the NEXT task must still be junior.
    """
    agent_id = seed_agent([4, 1])
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET tier='senior' WHERE agent_id=%s AND seq=0",
            (agent_id,),
        )
    process_one("senior", "s1", registry)

    tasks = get_tasks(agent_id)
    assert tasks[0]["tier"] == "senior" and tasks[0]["status"] == "succeeded"
    assert tasks[1]["tier"] == "junior", "promotion leaked onto the successor"
    assert claim_one("senior", "s1", TTL) is None
    assert claim_one("junior", "w1", TTL) is not None


def test_escalated_task_receives_full_prior_context(registry):
    """INVARIANT 7: senior must solve the identical problem, not a different one."""
    agent_id = seed_agent([1, 2, 1])
    process_one("junior", "w1", registry)
    process_one("junior", "w1", registry)

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET tier='senior' WHERE agent_id=%s AND seq=2",
            (agent_id,),
        )
    process_one("senior", "s1", registry)

    assert get_agent(agent_id)["context"]["2"]["saw"] == [0, 1]


# --- idempotency -------------------------------------------------------------

def test_side_effecting_task_runs_once_under_double_execution(registry):
    """
    INVARIANT 3. Reclaim-on-timeout guarantees a task will sometimes run twice;
    the idempotency key is what makes the EFFECT happen once.
    """
    agent_id = seed_agent([7])
    process_one("junior", "w1", registry)
    assert get_tasks(agent_id)[0]["result"]["invocations"] == 1

    # Force a replay of the same task instance.
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE task_instances SET status='pending', lease_owner=NULL WHERE agent_id=%s""",
            (agent_id,),
        )
        cur.execute("UPDATE agents SET cursor=0, status='running' WHERE id=%s", (agent_id,))

    process_one("junior", "w2", registry)

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM idempotency WHERE agent_id=%s", (agent_id,))
        assert cur.fetchone()["n"] == 1
    assert get_tasks(agent_id)[0]["result"]["invocations"] == 1, "action ran twice"


# --- evidence ----------------------------------------------------------------

def test_every_attempt_is_recorded(registry):
    """attempts is not a log -- it is the evidence for every dashboard number."""
    agent_id = seed_agent([1, 5])
    process_one("junior", "w1", registry)
    process_one("junior", "w1", registry)

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT seq, outcome, tier, cost_units FROM attempts WHERE agent_id=%s ORDER BY seq",
            (agent_id,),
        )
        rows = cur.fetchall()

    assert [r["outcome"] for r in rows] == ["succeeded", "failed"]
    assert rows[0]["cost_units"] == 1 and rows[1]["cost_units"] == 0
