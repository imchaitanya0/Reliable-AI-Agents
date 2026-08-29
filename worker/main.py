from __future__ import annotations

import os
import time
import uuid

from db.pool import open_runtime_db
from worker.claim import claim_next
from worker.executor import execute_task
from worker.heartbeat import Heartbeat


def run_once(db, pool_tier: str, worker_id: str, lease_ttl: int = 30) -> bool:
    task = claim_next(db, pool_tier, worker_id, lease_ttl)
    if task is None:
        return False
    print(
        f"[worker:{worker_id}] claimed task_def={task['task_def_id']} "
        f"seq={task['seq']} tier={task['tier']} attempt={task['attempt']}",
        flush=True,
    )
    with Heartbeat(db, task["id"], worker_id, task["attempt"], lease_ttl):
        ok = execute_task(db, task, worker_id)
    print(
        f"[worker:{worker_id}] {'committed' if ok else 'abandoned'} "
        f"seq={task['seq']} tier={task['tier']} attempt={task['attempt']}",
        flush=True,
    )
    return True


def main() -> None:
    db = open_runtime_db()
    pool_tier = os.getenv("POOL_TIER", "junior")
    worker_id = os.getenv("WORKER_ID", f"{pool_tier}-{uuid.uuid4()}")
    lease_ttl = int(os.getenv("LEASE_TTL_SECONDS", "30"))
    idle_ticks = 0
    print(f"[worker:{worker_id}] running pool={pool_tier} db={db.path}", flush=True)
    while True:
        did_work = run_once(db, pool_tier, worker_id, lease_ttl)
        if not did_work:
            idle_ticks += 1
            if idle_ticks % 20 == 0:
                print(f"[worker:{worker_id}] idle: no {pool_tier} task claimable", flush=True)
            time.sleep(0.5)
        else:
            idle_ticks = 0


if __name__ == "__main__":
    main()
