"""
Runs one TaskDef against an agent's accumulated context.

The executor owns two things the task implementations must not have to think
about: resolving the registry, and guarding externally visible actions with an
idempotency key.
"""

from __future__ import annotations

import logging
from typing import Any

from common.failures import InfraFailure, PoisonFailure, TaskFailure
from orchestrator import ledger
from common.tiers import cost_of
from common.protocol import TaskContext, TaskDef

log = logging.getLogger(__name__)


def load_registry() -> dict[int, TaskDef]:
    """
    Resolve the task registry.

    Supports BOTH registration styles so no lane is blocked on the other:
      * @task(...) decorators anywhere under tasks/  (preferred, pluggable)
      * a legacy TASK_DEFS dict in tasks/registry.py

    Adding a capability is one decorated function in a new file. Nothing in the
    worker changes.
    """
    from common.registry import discover

    return discover("tasks").as_dict()


def idem_key(agent_id: str, seq: int, action_type: str) -> str:
    """
    Deterministic key for an externally visible action.

    Both the original attempt and any recovery attempt compute the SAME digest,
    which is what turns at-least-once delivery into exactly-once effect. The
    digest itself lives in orchestrator.ledger so there is exactly one
    definition of it.
    """
    return ledger.action_id(agent_id, seq, action_type)


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
    # Lease recovery guarantees a slow-but-alive worker and its replacement will
    # sometimes run the same task concurrently. The ledger is what makes that
    # harmless. It is two-phase because the failure it defends against is a
    # crash AFTER the action succeeded -- see orchestrator/ledger.py.
    action_key: str | None = None
    if task_def.side_effecting:
        status, stored, action_key = ledger.begin(
            agent_id, seq, f"task:{task_def_id}"
        )

        if status == ledger.DONE:
            # Already executed. Return what it produced; do NOT run it again.
            log.info("duplicate suppressed agent=%s seq=%s", agent_id, seq)
            return stored or {}, 0, 0, True

        if status == ledger.IN_FLIGHT:
            # A twin reserved this action and never settled it. The effect may
            # already exist, so acting now risks the exact duplicate the ledger
            # exists to prevent -- and inventing a result would be worse. Report
            # it as INFRA so the task is retried once the ambiguity resolves:
            # by then the twin has either settled the id (-> DONE, real result
            # returned) or is provably gone.
            raise InfraFailure(
                f"action task:{task_def_id} is in flight for agent={agent_id} "
                f"seq={seq}; outcome unknown, refusing to duplicate"
            )

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

    if action_key is not None:
        # Phase two: the action succeeded, so record what it produced. Any later
        # retry now sees DONE and replays this instead of acting again.
        ledger.settle(action_key, result)

    cost, tokens = cost_of(tier)
    return result, cost, tokens, False
