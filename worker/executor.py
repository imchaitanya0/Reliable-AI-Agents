from __future__ import annotations

import hashlib
from typing import Any

from common.failures import InfraFailure, TaskFailure
from common.protocol import TaskContext
from db.pool import RuntimeDB, decode_json
from tasks.registry import get_task
from tasks.tiers import run_for_tier


def _idem_key(agent_id: str, seq: int, action_type: str) -> str:
    return hashlib.sha256(f"{agent_id}:{seq}:{action_type}".encode()).hexdigest()


def execute_task(db: RuntimeDB, task_row, worker_id: str) -> bool:
    task_def = get_task(task_row["task_def_id"])
    agent = db.get_agent(task_row["agent_id"])
    prior = {int(k): v for k, v in agent["context"].items()} if agent else {}

    def key_for(action_type: str) -> str:
        return _idem_key(task_row["agent_id"], task_row["seq"], action_type)

    ctx = TaskContext(
        agent_id=task_row["agent_id"],
        seq=task_row["seq"],
        tier=task_row["tier"],
        prior=prior,
        tool_overrides=db.get_config("tool_overrides") or {},
        idem_key=key_for,
    )

    try:
        if task_def.side_effecting:
            action_type = task_def.tool or task_def.name

            def fire() -> dict[str, Any]:
                return run_for_tier(task_def, ctx)

            state, result = db.run_idempotent(
                key_for(action_type),
                task_row["agent_id"],
                task_row["seq"],
                action_type,
                fire,
            )
            if state == "in_flight":
                raise InfraFailure("idempotency action already in flight")
            if result is None:
                raise InfraFailure("idempotency action did not return a settled result")
        else:
            result = run_for_tier(task_def, ctx)
    except TaskFailure as exc:
        return db.fail_task(task_row, worker_id, exc.failure_class, exc.detail)
    except Exception as exc:
        return db.fail_task(task_row, worker_id, "INFRA", str(exc))
    return db.complete_task(task_row, worker_id, result)
