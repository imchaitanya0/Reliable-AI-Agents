"""
Lane C — Task Execution & Idempotency Enforcement.
Runs one TaskDef against an agent's accumulated context.
"""

from __future__ import annotations

import logging
from typing import Any

from common.failures import InfraFailure, PoisonFailure, TaskFailure
from common.protocol import TaskContext, TaskDef
from common.tiers import cost_of
from db.pool import pool
from orchestrator import ledger

log = logging.getLogger(__name__)


def load_registry() -> dict[int, TaskDef]:
    """Resolve the task registry."""
    from common.registry import discover
    return discover("tasks").as_dict()


def idem_key(agent_id: str, seq: int, action_type: str) -> str:
    """Deterministic key for an externally visible action."""
    return ledger.action_id(agent_id, seq, action_type)


def run_task(
    task_row: dict[str, Any],
    agent_row: dict[str, Any],
    registry: dict[int, TaskDef] | None = None,
) -> tuple[dict[str, Any], int, int, bool]:
    """
    Execute one task.
    Returns (result, cost_units, tokens, was_duplicate).
    """
    registry = load_registry() if registry is None else registry

    task_def_id = task_row["task_def_id"]
    task_def = registry.get(task_def_id)
    if task_def is None:
        raise PoisonFailure(f"task_def_id {task_def_id} not in registry")

    agent_id = str(task_row["agent_id"])
    seq = task_row["seq"]
    tier = task_row["tier"]

    prior_raw = agent_row.get("context") or {}
    prior = {int(k): v for k, v in prior_raw.items()}

    ctx = TaskContext(
        agent_id=agent_id,
        seq=seq,
        tier=tier,
        prior=prior,
        idem_key=lambda action, a=agent_id, s=seq: idem_key(a, s, action),
    )

    action_key: str | None = None
    if task_def.side_effecting:
        status, stored, action_key = ledger.begin(agent_id, seq, f"task:{task_def_id}")

        if status == ledger.DONE:
            log.info("duplicate suppressed agent=%s seq=%s", agent_id, seq)
            return stored or {}, 0, 0, True

        if status == ledger.IN_FLIGHT:
            # If in flight, check idempotency table fallback
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT result FROM idempotency WHERE key = %s;", (action_key,))
                cached = cur.fetchone()
                if cached and cached.get("result"):
                    return cached["result"], 0, 0, True

    try:
        result = task_def.run(ctx)
    except TaskFailure:
        raise
    except Exception as exc:
        raise InfraFailure(f"{type(exc).__name__}: {exc}") from exc

    if not isinstance(result, dict):
        raise PoisonFailure(f"task {task_def_id} returned {type(result).__name__}, expected dict")

    if action_key is not None:
        ledger.settle(action_key, result)

    cost, tokens = cost_of(tier)
    return result, cost, tokens, False


def execute_claimed_task(task_row: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Compatibility wrapper returning (result, cost_units)."""
    agent_id = str(task_row["agent_id"])
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM agents WHERE id = %s;", (agent_id,))
        agent_row = cur.fetchone()
        if not agent_row:
            raise PoisonFailure(f"Agent {agent_id} not found in database.")

    result, cost, tokens, _ = run_task(task_row, agent_row)
    return result, cost
