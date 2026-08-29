"""
Lane A — Tier Execution Simulation (Junior vs Senior)
=====================================================

Determines execution success vs capability failure based on tier and difficulty.
"""

from __future__ import annotations

from typing import Any

from common.failures import CapabilityFailure
from common.protocol import TaskContext, TaskDef


def execute_with_tier(task_def: TaskDef, ctx: TaskContext) -> dict[str, Any]:
    """
    Execute task_def.run(ctx) enforcing tier-based capability constraints.
    - Tasks with difficulty='hard' deterministically fail on 'junior' tier.
    - 'senior' tier succeeds on both 'easy' and 'hard' tasks.
    """
    if ctx.tier == "junior" and task_def.difficulty == "hard":
        raise CapabilityFailure(
            f"Model capability bound: Junior model was unable to solve task '{task_def.name}' (id={task_def.id}). "
            "Output failed schema verification or logical constraints.",
            retryable_hint=True,
        )

    # Execute underlying task implementation
    return task_def.run(ctx)
