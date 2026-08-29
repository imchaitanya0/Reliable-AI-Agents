"""Lane F invariants: routing must match the failure class, every time."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.conftest import get_agent, get_tasks, seed_agent

from db.pool import pool
from orchestrator.classify import MAX_INFRA_ATTEMPTS, classify
from orchestrator.reaper import reap
from worker.claim import claim_one
from worker.main import process_one


def _fail(agent_id: str, fc: str, attempt: int = 1, tier: str = "junior") -> None:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE task_instances SET status='failed', failure_class=%s,
               attempt=%s, tier=%s WHERE agent_id=%s AND seq=0""",
            (fc, attempt, tier, agent_id),
        )


def _skip_backoff(agent_id: str) -> None:
    """Simulate the backoff window elapsing, so tests do not sleep for it."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET next_run_at = now() WHERE agent_id=%s",
            (agent_id,),
        )


def test_reaper_reclaims_expired_lease():
    agent_id = seed_agent([1])
    claim_one("junior", "dead-worker", 3)
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET lease_expires = now() - interval '1s' WHERE agent_id=%s",
            (agent_id,),
        )
    assert len(reap()) == 1
    t = get_tasks(agent_id)[0]
    assert t["status"] == "pending" and t["failure_class"] == "INFRA"
    assert t["lease_owner"] is None


def test_infra_retries_at_the_same_tier():
    """A dead machine says nothing about model capability."""
    agent_id = seed_agent([1])
    _fail(agent_id, "INFRA", attempt=1)
    assert classify()["retry"] == 1
    t = get_tasks(agent_id)[0]
    assert t["status"] == "pending" and t["tier"] == "junior", "INFRA must not promote"


def test_infra_eventually_dead_letters():
    """A permanently dead tool must stop consuming worker slots."""
    agent_id = seed_agent([1])
    _fail(agent_id, "INFRA", attempt=MAX_INFRA_ATTEMPTS)
    assert classify()["dlq"] == 1
    assert get_tasks(agent_id)[0]["status"] == "dead"
    assert get_agent(agent_id)["status"] == "failed"


def test_poison_never_reaches_senior():
    agent_id = seed_agent([1])
    _fail(agent_id, "POISON")
    assert classify()["dlq"] == 1
    t = get_tasks(agent_id)[0]
    assert t["status"] == "dead" and t["tier"] == "junior"

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM dlq WHERE agent_id=%s", (agent_id,))
        assert cur.fetchone()["n"] == 1


def test_capability_retries_before_promoting():
    """Promotion is a last resort, not a first response."""
    agent_id = seed_agent([4])
    _fail(agent_id, "CAPABILITY", attempt=1)   # max_attempts_per_tier = 2
    assert classify()["retry"] == 1
    assert get_tasks(agent_id)[0]["tier"] == "junior"


def test_capability_promotes_once_the_tier_is_exhausted():
    agent_id = seed_agent([4])
    _fail(agent_id, "CAPABILITY", attempt=2)
    assert classify()["promote"] == 1
    t = get_tasks(agent_id)[0]
    assert t["tier"] == "senior"
    assert t["status"] == "pending"
    assert t["attempt"] == 0, "attempt counter must reset for the new tier"


def test_capability_at_top_tier_dead_letters():
    """Nowhere left to escalate to."""
    agent_id = seed_agent([4], tiers=["senior"])
    _fail(agent_id, "CAPABILITY", attempt=2, tier="senior")
    assert classify()["dlq"] == 1
    assert get_tasks(agent_id)[0]["status"] == "dead"


def test_full_escalation_cycle_end_to_end(registry):
    """
    The money shot as a unit test: hard task fails twice on junior, the
    orchestrator promotes it, senior finishes it, and the NEXT task drops back
    to junior.
    """
    agent_id = seed_agent([4, 1])

    # Attempt 1 at junior: deterministic CAPABILITY failure -> retry, not promote.
    assert process_one("junior", "w1", registry) is True
    assert classify() == {"retry": 1, "promote": 0, "dlq": 0}

    t = get_tasks(agent_id)[0]
    assert t["tier"] == "junior", "one failure must not promote"
    assert t["next_run_at"] > datetime.now(timezone.utc), "backoff not applied"
    _skip_backoff(agent_id)

    # Attempt 2 exhausts the tier, so now it escalates.
    assert process_one("junior", "w1", registry) is True
    assert classify() == {"retry": 0, "promote": 1, "dlq": 0}

    assert get_tasks(agent_id)[0]["tier"] == "senior", "should have promoted"

    assert process_one("senior", "s1", registry) is True
    tasks = get_tasks(agent_id)
    assert tasks[0]["status"] == "succeeded"
    assert tasks[0]["result"]["solved_by"] == "senior"

    # The invariant the whole cost argument rests on.
    assert tasks[1]["tier"] == "junior", "promotion leaked onto the next task"
    assert process_one("junior", "w1", registry) is True
    assert get_agent(agent_id)["status"] == "completed"

    # 2 junior attempts (free, failed) + 1 senior + 1 junior = 13 cost units
    assert get_agent(agent_id)["cost_units"] == 13
