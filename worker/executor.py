"""
Lane C — Task Execution & Idempotency Enforcement
=================================================

Executes TaskDefs with TaskContext and wraps side-effects with deterministic idempotency keys.
"""

from __future__ import annotations

import json
from typing import Any

from common.failures import (
    CapabilityFailure,
    InfraFailure,
    PoisonFailure,
    TaskFailure,
)
from common.protocol import TaskContext
from db.pool import get_conn
from tasks.registry import TASK_DEFS
from tasks.tiers import execute_with_tier


def execute_claimed_task(task_row: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """
    Execute a claimed task row against accumulated agent context.
    Returns (result, cost_units).
    Raises TaskFailure subclasses (InfraFailure, CapabilityFailure, PoisonFailure).
    """
    agent_id = str(task_row["agent_id"])
    seq = int(task_row["seq"])
    task_def_id = int(task_row["task_def_id"])
    tier = str(task_row["tier"])

    # 1. Fetch Agent Context & Plan
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT context, plan FROM agents WHERE id = %s;", (agent_id,))
            agent = cur.fetchone()
            if not agent:
                raise PoisonFailure(f"Agent {agent_id} not found in database.")

    prior_context = {int(k): v for k, v in agent["context"].items()} if agent["context"] else {}

    # 2. Build TaskContext
    ctx = TaskContext(
        agent_id=agent_id,
        seq=seq,
        tier=tier,  # type: ignore
        prior=prior_context,
    )

    # 3. Lookup TaskDef in Registry
    task_def = TASK_DEFS.get(task_def_id)
    if not task_def:
        raise PoisonFailure(f"Unknown task_def_id={task_def_id}. Not registered in TASK_DEFS.")

    # 4. Check Idempotency Table if Task is Side-Effecting
    if task_def.side_effecting:
        idem_key = ctx.key_for(f"task:{task_def.name}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT result FROM idempotency WHERE key = %s;", (idem_key,))
                cached = cur.fetchone()
                if cached:
                    # Exactly-once effect hit: return stored result without re-executing
                    return cached["result"], 1 if tier == "junior" else 12

    # 5. Execute Task with Tier Enforcement
    try:
        result = execute_with_tier(task_def, ctx)
    except TaskFailure:
        raise
    except Exception as exc:
        # Unhandled code error/crash is classified as InfraFailure
        raise InfraFailure(f"Execution error in {task_def.name}: {exc}") from exc

    # 6. Store Idempotency Record if Side-Effecting
    if task_def.side_effecting:
        idem_key = ctx.key_for(f"task:{task_def.name}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idempotency (key, agent_id, seq, action_type, result)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (key) DO NOTHING;
                    """,
                    (idem_key, agent_id, seq, f"task:{task_def.name}", json.dumps(result)),
                )

    cost_units = 1 if tier == "junior" else 12
    return result, cost_units
