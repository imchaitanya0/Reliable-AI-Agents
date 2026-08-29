from __future__ import annotations

import time

from db.pool import RuntimeDB, utc_now
from orchestrator.main import run_once as orchestrate_once
from worker.executor import execute_task
from worker.main import run_once as worker_once


def test_infra_failures_do_not_escalate() -> None:
    db = RuntimeDB()
    agent_id = db.create_agent([6])
    for i in range(3):
        task = db.claim_task("junior", f"worker-{i}", lease_ttl=30)
        assert task is not None
        assert db.fail_task(task, f"worker-{i}", "INFRA", "worker died")
        orchestrate_once(db)
        current = db.conn.execute("SELECT * FROM task_instances WHERE agent_id=?", (agent_id,)).fetchone()
        assert current["tier"] == "junior"
        db.conn.execute("UPDATE task_instances SET next_run_at=? WHERE id=?", (utc_now() - 1, current["id"]))
    task = db.conn.execute("SELECT * FROM task_instances WHERE agent_id=?", (agent_id,)).fetchone()
    capability_attempts = db.conn.execute(
        "SELECT COUNT(*) AS n FROM attempts WHERE task_instance_id=? AND failure_class='CAPABILITY'",
        (task["id"],),
    ).fetchone()["n"]
    assert capability_attempts == 0


def test_zombie_fencing_blocks_stale_write() -> None:
    db = RuntimeDB()
    db.create_agent([1])
    stale = db.claim_task("junior", "worker-a", lease_ttl=1)
    assert stale is not None
    db.conn.execute("UPDATE task_instances SET lease_expires=? WHERE id=?", (utc_now() - 1, stale["id"]))
    reclaimed = db.reap_expired(batch_size=100, jitter_seconds=0)
    assert len(reclaimed) == 1
    fresh = db.claim_task("junior", "worker-b", lease_ttl=30)
    assert fresh is not None
    assert db.complete_task(fresh, "worker-b", {"ok": True})
    assert not db.complete_task(stale, "worker-a", {"stale": True})
    agent = db.get_agent(fresh["agent_id"])
    assert agent["status"] == "completed"
    assert agent["context"]["0"] == {"ok": True}
    assert db.metrics()["zombie_writes_blocked"] == 1


def test_two_phase_idempotency_blocks_duplicate_fire() -> None:
    db = RuntimeDB()
    agent_id = db.create_agent([8])
    key = "same-action"
    state, _ = db.reserve_idempotency(key, agent_id, 0, "jira")
    assert state == "reserved"
    fired = {"count": 0}

    def fire() -> dict:
        fired["count"] += 1
        return {"ticket": "JIRA-1"}

    state, result = db.run_idempotent(key, agent_id, 0, "jira", fire)
    assert state == "in_flight"
    assert result is None
    assert fired["count"] == 0
    assert db.metrics()["duplicate_actions_blocked"] == 1


def test_batched_reaper_reclaims_500_with_jitter() -> None:
    db = RuntimeDB()
    for i in range(500):
        agent_id = db.create_agent([1])
        task = db.claim_task("junior", f"worker-{i}", lease_ttl=1)
        assert task is not None
        db.conn.execute("UPDATE task_instances SET lease_expires=? WHERE id=?", (utc_now() - 1, task["id"]))
    reclaimed_total = 0
    started = time.time()
    for _ in range(5):
        reclaimed_total += len(db.reap_expired(batch_size=100, jitter_seconds=5))
    elapsed = time.time() - started
    assert reclaimed_total == 500
    assert elapsed < 2
    spread = db.conn.execute(
        "SELECT MAX(next_run_at)-MIN(next_run_at) AS spread FROM task_instances"
    ).fetchone()["spread"]
    assert spread > 0


def test_semantic_dedup_returns_existing_agent() -> None:
    db = RuntimeDB()
    first = db.create_agent([1, 2], query_text="Fix payment")
    second = db.create_agent([1, 2], query_text="Fix payment")
    assert first == second
    assert db.metrics()["tasks_deduplicated"] == 1
    assert db.conn.execute("SELECT COUNT(*) AS n FROM agents").fetchone()["n"] == 1


def test_capability_promotes_then_successor_returns_to_junior() -> None:
    db = RuntimeDB()
    agent_id = db.create_agent([1, 6, 8, 9])
    assert worker_once(db, "junior", "junior-1", lease_ttl=30)
    for _ in range(2):
        task = db.claim_task("junior", "junior-1", lease_ttl=30)
        assert task is not None
        assert execute_task(db, task, "junior-1")
        orchestrate_once(db)
        db.conn.execute("UPDATE task_instances SET next_run_at=? WHERE id=?", (utc_now() - 1, task["id"]))
    promoted = db.conn.execute(
        "SELECT * FROM task_instances WHERE agent_id=? AND seq=1", (agent_id,)
    ).fetchone()
    assert promoted["tier"] == "senior"
    senior_task = db.claim_task("senior", "senior-1", lease_ttl=30)
    assert senior_task is not None
    assert execute_task(db, senior_task, "senior-1")
    next_task = db.claim_task("junior", "junior-2", lease_ttl=30)
    assert next_task is not None
    assert next_task["seq"] == 2
    assert next_task["tier"] == "junior"


def test_full_happy_path_completes_with_tiered_cost() -> None:
    db = RuntimeDB()
    agent_id = db.create_agent([1, 2, 6, 8, 9])
    idle = 0
    while idle < 5:
        did = worker_once(db, "junior", "junior", lease_ttl=30)
        did = worker_once(db, "senior", "senior", lease_ttl=30) or did
        orchestrate_once(db)
        db.conn.execute("UPDATE task_instances SET next_run_at=? WHERE status='pending'", (utc_now() - 1,))
        idle = 0 if did else idle + 1
    agent = db.get_agent(agent_id)
    assert agent["status"] == "completed"
    metrics = db.metrics()
    assert metrics["duplicate_actions_blocked"] == 0
    assert metrics["cost_comparison"]["tiered"] < metrics["cost_comparison"]["all_senior"]
