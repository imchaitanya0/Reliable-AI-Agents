"""
Lane B — Task API & Chaos Control Surface (Contract C3).
Stateless FastAPI service for submitting workflows, querying metrics, and injecting chaos.
"""

from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from common.metrics import snapshot
from common.tiers import all_tiers, base_tier
from db.init_db import init_database
from db.pool import close_pool, fetchall, fetchone, pool
from tasks.registry import DEMO_PLAN, TASK_DEFS

app = FastAPI(
    title="Reliable AI Agent Runtime",
    description="Submission and observation surface for the agent runtime.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# request models
# ---------------------------------------------------------------------------


class SubmitAgents(BaseModel):
    """`POST /agents` body."""
    plan: list[int] = Field(default_factory=lambda: list(DEMO_PLAN))
    count: int = Field(default=1, ge=1, le=1000)


class ToolChaos(BaseModel):
    """`POST /chaos/tool` body."""
    name: str
    failure_rate: float = Field(ge=0.0, le=1.0)
    latency_ms: int | None = None


class ConfigChaos(BaseModel):
    """`POST /chaos/config` body."""
    retries_enabled: bool | None = None
    escalation_enabled: bool | None = None
    force_tier: str | None = None
    lease_ttl_seconds: int | None = None


# ---------------------------------------------------------------------------
# lifecycle & health
# ---------------------------------------------------------------------------


@app.on_event("startup")
def _startup() -> None:
    init_database()
    pool()


@app.on_event("shutdown")
def _shutdown() -> None:
    close_pool()


@app.get("/health")
def health() -> dict[str, Any]:
    """Liveness plus a real database round trip."""
    row = fetchone("SELECT 1 AS ok")
    return {"status": "ok" if row else "degraded"}


# ---------------------------------------------------------------------------
# agents
# ---------------------------------------------------------------------------


@app.post("/agents", status_code=201)
def submit_agents(body: SubmitAgents) -> dict[str, Any]:
    """
    Create agents and ALL of their task rows up front at the base tier.
    """
    unknown = sorted({tid for tid in body.plan if tid not in TASK_DEFS})
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown task ids {unknown}; known ids are {sorted(TASK_DEFS)}",
        )
    if not body.plan:
        raise HTTPException(status_code=422, detail="plan must not be empty")

    tier = base_tier()
    agent_ids: list[str] = []

    with pool().connection() as conn, conn.cursor() as cur:
        for _ in range(body.count):
            cur.execute(
                "INSERT INTO agents (plan, status) VALUES (%s, 'running') RETURNING id",
                (body.plan,),
            )
            agent_id = cur.fetchone()["id"]
            for seq, task_def_id in enumerate(body.plan):
                cur.execute(
                    """INSERT INTO task_instances (agent_id, seq, task_def_id, tier)
                       VALUES (%s, %s, %s, %s)""",
                    (agent_id, seq, task_def_id, tier),
                )
            agent_ids.append(str(agent_id))

    return {"agent_ids": agent_ids, "count": len(agent_ids), "plan": body.plan}


@app.get("/agents/{agent_id}")
def get_agent(agent_id: str) -> dict[str, Any]:
    """One agent with its task instances, ordered by position in the plan."""
    agent = fetchone(
        """SELECT id, plan, cursor, status, context, cost_units, tokens_used,
                  created_at, updated_at
           FROM agents WHERE id = %s""",
        (str(agent_id),),
    )
    if agent is None:
        raise HTTPException(status_code=404, detail=f"no agent {agent_id}")

    agent["tasks"] = fetchall(
        """SELECT id, seq, task_def_id, status, tier, attempt, lease_owner,
                  lease_expires, next_run_at, result, last_error, failure_class,
                  created_at, updated_at
           FROM task_instances
           WHERE agent_id = %s
           ORDER BY seq""",
        (str(agent_id),),
    )
    return agent


# ---------------------------------------------------------------------------
# metrics & dlq
# ---------------------------------------------------------------------------


@app.get("/metrics")
def get_metrics() -> dict[str, Any]:
    """Contract C4 snapshot."""
    return snapshot()


@app.get("/dlq")
def get_dlq() -> list[dict[str, Any]]:
    """Dead-letter queue entries."""
    rows = fetchall(
        """SELECT id, agent_id, seq, task_def_id, failure_class, last_error,
                  attempt_trail, created_at
           FROM dlq
           ORDER BY id DESC
           LIMIT 100"""
    )
    return rows


# ---------------------------------------------------------------------------
# chaos endpoints
# ---------------------------------------------------------------------------


@app.post("/chaos/tool")
def chaos_tool(body: ToolChaos) -> dict[str, Any]:
    from chaos.harness import set_tool
    set_tool(body.name, body.failure_rate)
    return {"ok": True, "tool": body.name, "failure_rate": body.failure_rate}


@app.post("/chaos/config")
def chaos_config(body: ConfigChaos) -> dict[str, Any]:
    from chaos.harness import set_runtime_flag
    if body.retries_enabled is not None:
        set_runtime_flag("retries_enabled", body.retries_enabled)
    if body.escalation_enabled is not None:
        set_runtime_flag("escalation_enabled", body.escalation_enabled)
    if body.force_tier is not None:
        if body.force_tier not in all_tiers():
            raise HTTPException(status_code=422, detail=f"unknown tier {body.force_tier}")
        set_runtime_flag("force_tier", body.force_tier)
    if body.lease_ttl_seconds is not None:
        set_runtime_flag("lease_ttl_seconds", body.lease_ttl_seconds)
    return {"ok": True, "config": body.dict()}
