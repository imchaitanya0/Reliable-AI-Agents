from __future__ import annotations

from common.failures import CapabilityFailure
from common.protocol import TaskContext, TaskDef


def run_for_tier(task: TaskDef, ctx: TaskContext) -> dict:
    if task.difficulty == "hard" and ctx.tier == "junior":
        raise CapabilityFailure(f"{task.name} requires the senior tier")
    return task.run(ctx)
