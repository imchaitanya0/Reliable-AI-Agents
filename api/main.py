"""
Lane B -- the Task API.

The submission surface and the read surface. It is deliberately thin: it inserts
work and reads state, and it executes nothing. Workers discover work by polling,
so a submitted agent starts running whether or not this process is alive -- which
is why the API can be stateless, replicated and restarted freely.

Everything it reports is computed elsewhere. `GET /metrics` returns
`common.metrics.snapshot()` verbatim rather than assembling its own numbers, so
the API, the demo CLI and the dashboard can never disagree about what happened.

    uvicorn api.main:app --port 8000
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from chaos.harness import set_tool
from common.metrics import snapshot
from common.runtime import force_tier
from common.tiers import all_tiers, base_tier
from db.pool import close_pool, fetchall, fetchone, pool
from tasks.registry import DEMO_PLAN, TASK_DEFS

app = FastAPI(
    title="Reliable AI Agent Runtime",
    description="Submission and observation surface for the agent runtime.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# request models
# ---------------------------------------------------------------------------


class SubmitAgents(BaseModel):
    """`POST /agents` body."""

    #: Task ids executed in sequence. Data, not code -- which is what makes an
    #: agent resumable at exact task granularity.
    plan: list[int] = Field(default_factory=lambda: list(DEMO_PLAN))
    count: int = Field(default=1, ge=1, le=1000)


class ToolChaos(BaseModel):
    """`POST /chaos/tool` body."""

    name: str
    failure_rate: float = Field(ge=0.0, le=1.0)

    #: Accepted for contract compatibility and currently ignored: `set_tool`
    #: writes only `failure_rate` into `tool_overrides`. Injecting latency would
    #: mean changing chaos/harness.py, which belongs to another lane.
    latency_ms: int | None = None


class ConfigChaos(BaseModel):
    """
    `POST /chaos/config` body.

    Every field is optional and only the ones actually sent are written, so a
    caller can flip one flag without silently resetting the others.
    """

    retries_enabled: bool | None = None
    escalation_enabled: bool | None = None
    force_tier: str | None = None


# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------


@app.on_event("startup")
def _startup() -> None:
    # Open the pool now so the first request does not pay for it, and so a bad
    # DATABASE_URL fails at boot rather than under load.
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
def submit_agents(body: SubmitAgents) -> dict[str, list[str]]:
    """
    Create agents and ALL of their task rows, in one transaction, at the base tier.

    Two reasons this must not be done lazily, one row at a time:

    1. The claim query gates execution order with `t.seq = a.cursor`, so nothing
       runs before its predecessor commits. Creating every row up front is
       therefore safe -- the rows exist, but only one is claimable.

    2. It makes the cost invariant structural rather than a matter of care.
       Because the successor row ALREADY EXISTS at the base tier, a task that
       gets promoted physically cannot leak its tier forward. Created lazily,
       promotion would leak and the cost thesis would quietly stop holding.

    Nothing executes here. Workers discover the work by polling.
    """
    unknown = sorted({tid for tid in body.plan if tid not in TASK_DEFS})
    if unknown:
        # A plan referencing a task that does not exist is POISON -- no retry and
        # no bigger model fixes it. Rejecting at submission keeps it out of the
        # queue entirely, rather than letting it fail its way to the DLQ.
        raise HTTPException(
            status_code=422,
            detail=f"unknown task ids {unknown}; known ids are {sorted(TASK_DEFS)}",
        )
    if not body.plan:
        raise HTTPException(status_code=422, detail="plan must not be empty")

    # Normally the base tier -- promotion is scoped to a task, so work must
    # start cheap. `force_tier` pins every task to one tier instead, which is
    # how the all-junior and all-senior baselines are produced by this same
    # system rather than estimated.
    tier = force_tier() or base_tier()
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

    return {"agent_ids": agent_ids}


@app.get("/agents/{agent_id}")
def get_agent(agent_id: UUID) -> dict[str, Any]:
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
# observation
# ---------------------------------------------------------------------------


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    """
    Contract C4, returned verbatim.

    Deliberately not reassembled here. The demo CLI and the dashboard read the
    same function, so there is exactly one definition of every number and no way
    for two surfaces to contradict each other mid-demo.
    """
    return snapshot()


@app.get("/dlq")
def dead_letter_queue(limit: int = 100) -> list[dict[str, Any]]:
    """
    Terminal failures, newest first, each with the attempt trail that produced it.

    A dead-letter queue without the history is just a list of things that broke;
    the trail is what makes it possible to say why.
    """
    return fetchall(
        """SELECT id, agent_id, seq, task_def_id, failure_class, last_error,
                  attempt_trail, created_at
           FROM dlq
           ORDER BY created_at DESC
           LIMIT %s""",
        (limit,),
    )


# ---------------------------------------------------------------------------
# chaos
# ---------------------------------------------------------------------------
# These exist so the benchmark can run its own controls live. A metric only
# persuades next to its control: "we cost 0.14x all-senior" means nothing until
# the all-senior number is produced by the same system on the same stage.


@app.post("/chaos/tool")
def chaos_tool(body: ToolChaos) -> dict[str, Any]:
    """Set a mock tool's injected failure rate."""
    set_tool(body.name, body.failure_rate)
    return {
        "tool": body.name,
        "failure_rate": body.failure_rate,
        "latency_ms_ignored": body.latency_ms is not None,
    }


@app.post("/chaos/config")
def chaos_config(body: ConfigChaos) -> dict[str, Any]:
    """
    Flip runtime flags. Only the fields actually sent are written.

    `force_tier` is validated against the `tiers` table rather than a literal
    list, so adding a capability tier stays a single INSERT and never requires
    editing this file.
    """
    sent = body.model_dump(exclude_unset=True)
    if not sent:
        raise HTTPException(status_code=422, detail="no configuration fields supplied")

    if "force_tier" in sent and sent["force_tier"] is not None:
        valid = {t["name"] for t in all_tiers()}
        if sent["force_tier"] not in valid:
            raise HTTPException(
                status_code=422,
                detail=f"unknown tier {sent['force_tier']!r}; known tiers are {sorted(valid)}",
            )

    from json import dumps

    with pool().connection() as conn, conn.cursor() as cur:
        for key, value in sent.items():
            cur.execute(
                """INSERT INTO runtime_config (key, value)
                   VALUES (%s, %s::jsonb)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
                (key, dumps(value)),
            )

    return {"updated": sent}
