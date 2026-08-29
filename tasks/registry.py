from __future__ import annotations

from typing import Any

from common.failures import CapabilityFailure, InfraFailure, PoisonFailure
from common.protocol import TaskContext, TaskDef
from tasks.tools import call_tool


def _tool_task(tool: str, action: str) -> Any:
    def run(ctx: TaskContext) -> dict[str, Any]:
        return call_tool(tool, {"action": action, "seq": ctx.seq, "tier": ctx.tier, "prior": ctx.prior}, ctx.tool_overrides)

    return run


def _hard_reasoning(ctx: TaskContext) -> dict[str, Any]:
    if ctx.tier == "junior":
        raise CapabilityFailure("junior model could not produce a valid root-cause analysis")
    return {"analysis": "senior root-cause analysis accepted", "prior_keys": sorted(ctx.prior.keys())}


def _side_effect(ctx: TaskContext) -> dict[str, Any]:
    if ctx.idem_key is None:
        raise PoisonFailure("side-effecting task requires an idempotency adapter")
    return call_tool(
        "jira",
        {"ticket": f"agent-{ctx.agent_id}-seq-{ctx.seq}", "prior": ctx.prior},
        ctx.tool_overrides,
    )


def _summarize(ctx: TaskContext) -> dict[str, Any]:
    return {"summary": "agent workflow completed", "completed_steps": sorted(ctx.prior.keys())}


TASK_DEFS: dict[int, TaskDef] = {
    1: TaskDef(1, "fetch_repository_context", _tool_task("github", "fetch"), tool="github"),
    2: TaskDef(2, "scan_runtime_logs", _tool_task("logs", "scan"), tool="logs"),
    3: TaskDef(3, "classify_incident", _tool_task("logs", "classify"), tool="logs"),
    4: TaskDef(4, "draft_patch_plan", _tool_task("github", "draft"), tool="github"),
    5: TaskDef(5, "validate_patch_plan", _tool_task("logs", "validate"), tool="logs"),
    6: TaskDef(6, "hard_root_cause", _hard_reasoning, difficulty="hard"),
    7: TaskDef(7, "notify_owner", _tool_task("github", "notify"), tool="github"),
    8: TaskDef(8, "create_jira_ticket", _side_effect, side_effecting=True, tool="jira"),
    9: TaskDef(9, "summarize_run", _summarize),
}


def get_task(task_def_id: int) -> TaskDef:
    try:
        return TASK_DEFS[task_def_id]
    except KeyError as exc:
        raise PoisonFailure(f"unknown task definition {task_def_id}") from exc
