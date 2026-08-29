"""
Runs one TaskDef against an agent's accumulated context.

The executor owns two things the task implementations must not have to think
about: resolving the registry, and guarding externally visible actions with an
idempotency key.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from common.config import TIERS
from common.failures import PoisonFailure, TaskFailure
from common.protocol import TaskContext, TaskDef
from db.pool import pool

log = logging.getLogger(__name__)


def load_registry() -> dict[int, TaskDef]:
    """
    Resolve Lane A's registry lazily so the worker still imports cleanly before
    that lane has landed.
    """
    try:
        from tasks.registry import TASK_DEFS  # type: ignore[import-not-found]

        return TASK_DEFS
    except Exception:  # pragma: no cover - Lane A not merged yet
        log.warning("tasks.registry unavailable -- worker has nothing to run")
        return {}


def idem_key(agent_id: str, seq: int, action_type: str) -> str:
    """
    Deterministic key for an externally visible action.

    Both the original attempt and any recovery attempt compute the SAME digest,
    which is what turns at-least-once delivery into exactly-once effect.
    """
    return hashlib.sha256(f"{agent_id}:{seq}:{action_type}".encode()).hexdigest()


RESERVE_SQL = """
INSERT INTO idempotency (key, agent_id, seq, action_type, result)
VALUES (%(key)s, %(agent_id)s, %(seq)s, %(action_type)s, %(result)s)
ON CONFLICT (key) DO NOTHING
"""


def run_task(
    task_row: dict[str, Any],
    agent_row: dict[str, Any],
    registry: dict[int, TaskDef] | None = None,
) -> tuple[dict[str, Any], int, int, bool]:
    """
    Execute one task.

    Returns (result, cost_units, tokens, was_duplicate).

    Raises TaskFailure subclasses; the worker records the class and the
    orchestrator decides what it means.
    """
    registry = load_registry() if registry is None else registry

    task_def_id = task_row["task_def_id"]
    task_def = registry.get(task_def_id)
    if task_def is None:
        # Nothing fixes an unknown task id -- not a retry, not a bigger model.
        raise PoisonFailure(f"task_def_id {task_def_id} not in registry")

    agent_id = str(task_row["agent_id"])
    seq = task_row["seq"]
    tier = task_row["tier"]

    # agents.context is {"<seq>": result}; tasks expect int keys.
    prior_raw = agent_row.get("context") or {}
    prior = {int(k): v for k, v in prior_raw.items()}

    ctx = TaskContext(
        agent_id=agent_id,
        seq=seq,
        tier=tier,
        prior=prior,
        idem_key=lambda action, a=agent_id, s=seq: idem_key(a, s, action),
    )

    # ---- idempotency guard for externally visible actions --------------------
    # Reclaim-on-timeout guarantees a slow-but-alive worker and its replacement
    # will sometimes run the same task concurrently. Whoever wins the INSERT
    # owns the action; the loser returns the stored result instead of doing it
    # twice.
    #
    # Known window: a crash after reserving but before the action completes
    # loses that action. Closing it properly needs a transactional outbox, which
    # is out of scope. The metric we claim -- duplicate actions prevented -- is
    # correct under this design.
    if task_def.side_effecting:
        key = idem_key(agent_id, seq, f"task:{task_def_id}")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                RESERVE_SQL,
                {
                    "key": key,
                    "agent_id": agent_id,
                    "seq": seq,
                    "action_type": f"task:{task_def_id}",
                    "result": json.dumps({"status": "reserved"}),
                },
            )
            reserved = cur.rowcount == 1

        if not reserved:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT result FROM idempotency WHERE key = %s", (key,)
                )
                row = cur.fetchone()
            stored = (row or {}).get("result") or {}
            log.info("duplicate suppressed agent=%s seq=%s", agent_id, seq)
            return stored, 0, 0, True

    # ---- run it -------------------------------------------------------------
    try:
        result = task_def.run(ctx)
    except TaskFailure:
        raise
    except Exception as exc:  # anything unexpected is infrastructure's problem
        from common.failures import InfraFailure

        raise InfraFailure(f"{type(exc).__name__}: {exc}") from exc

    if not isinstance(result, dict):
        raise PoisonFailure(
            f"task {task_def_id} returned {type(result).__name__}, expected dict"
        )

    if task_def.side_effecting:
        key = idem_key(agent_id, seq, f"task:{task_def_id}")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET result = %s WHERE key = %s",
                (json.dumps(result), key),
            )

    tier_cfg = TIERS.get(tier, TIERS["junior"])
    return result, int(tier_cfg["cost_units"]), int(tier_cfg["tokens"]), False
