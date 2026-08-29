from __future__ import annotations

from db.pool import RuntimeDB
from orchestrator.main import run_once as orchestrate_once
from worker.main import run_once as worker_once


def run_demo(db: RuntimeDB, count: int = 20) -> dict:
    ids = [db.create_agent([1, 2, 6, 8, 9], query_text=f"demo agent {i}") for i in range(count)]
    idle_rounds = 0
    while idle_rounds < 5:
        did_work = worker_once(db, "junior", "demo-junior", lease_ttl=2)
        did_work = worker_once(db, "senior", "demo-senior", lease_ttl=2) or did_work
        orchestrate_once(db)
        idle_rounds = 0 if did_work else idle_rounds + 1
    return {"agent_ids": ids, "metrics": db.metrics()}
