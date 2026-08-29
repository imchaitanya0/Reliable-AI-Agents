"""
Lane B — Pydantic Request & Response Schemas (Contracts C3 & C4)
"""

from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class CreateAgentRequest(BaseModel):
    plan: list[int] = Field(default_factory=lambda: [1, 2, 6, 8, 9], description="Ordered list of task_def IDs")
    count: int = Field(default=1, ge=1, le=1000, description="Number of duplicate agents to spawn for batch testing")


class CreateAgentResponse(BaseModel):
    agent_ids: list[str]
    count: int
    plan: list[int]


class TaskInstanceDetail(BaseModel):
    id: str
    seq: int
    task_def_id: int
    status: str
    tier: str
    attempt: int
    lease_owner: str | None
    lease_expires: Any | None
    result: Any | None
    last_error: str | None
    failure_class: str | None


class AgentDetailResponse(BaseModel):
    id: str
    plan: list[int]
    cursor: int
    status: str
    context: dict[str, Any]
    cost_units: int
    tokens_used: int
    tasks: list[TaskInstanceDetail]


class MetricsResponse(BaseModel):
    active_tasks: int
    completed_agents: int
    failed_agents: int
    total_agents: int
    reclaimed_tasks: int
    tasks_reexecuted: int
    tasks_avoided: int
    promotion_count: int
    promotion_rate_pct: float
    duplicate_actions_prevented: int
    cost_units_junior: int
    cost_units_senior: int
    cost_units_tiered: int
    cost_savings_pct: float
    p50_latency_ms: float
    p95_latency_ms: float
    throughput_tasks_per_sec: float


class DLQItem(BaseModel):
    id: int
    agent_id: str
    seq: int
    task_def_id: int
    failure_class: str
    last_error: str | None
    attempt_trail: list[dict[str, Any]]
    created_at: Any


class ChaosToolRequest(BaseModel):
    name: str = Field(..., description="Tool name: github, logs, jira")
    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    latency_ms: int = Field(default=300, ge=0)


class ChaosConfigRequest(BaseModel):
    retries_enabled: bool = True
    escalation_enabled: bool = True
    force_tier: Literal["junior", "senior"] | None = None
    lease_ttl_seconds: int = 30
