"""
Test harness for Lane C (worker).

Fixture task definitions live HERE, not in tasks/registry.py, so this suite
never collides with Lane A. The worker is coded against contract C2, so
whatever registry Lane A ships plugs straight in.
"""

from __future__ import annotations

import os
import time

os.environ.setdefault(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/rai"
)
os.environ.setdefault("LEASE_TTL_SECONDS", "3")

import pytest  # noqa: E402

from common.failures import CapabilityFailure, InfraFailure, PoisonFailure  # noqa: E402
from common.protocol import TaskContext, TaskDef  # noqa: E402
from db.pool import pool  # noqa: E402

# --- fixture task implementations -------------------------------------------

_CALLS: dict[str, int] = {}


def _count(name: str) -> int:
    _CALLS[name] = _CALLS.get(name, 0) + 1
    return _CALLS[name]


def t_ok(ctx: TaskContext) -> dict:
    return {"task": "ok", "seq": ctx.seq, "tier": ctx.tier, "saw": sorted(ctx.prior)}


def t_slow(ctx: TaskContext) -> dict:
    time.sleep(float(os.environ.get("SLOW_TASK_SECONDS", "8")))
    return {"task": "slow", "seq": ctx.seq}


def t_hard(ctx: TaskContext) -> dict:
    """Deterministically fails on junior, succeeds on senior."""
    if ctx.tier == "junior":
        raise CapabilityFailure("junior cannot solve this")
    return {"task": "hard", "solved_by": ctx.tier}


def t_infra(ctx: TaskContext) -> dict:
    raise InfraFailure("simulated connection reset")


def t_poison(ctx: TaskContext) -> dict:
    raise PoisonFailure("malformed input")


def t_side_effect(ctx: TaskContext) -> dict:
    """Counts real invocations so duplicate suppression is observable."""
    n = _count(f"side:{ctx.agent_id}:{ctx.seq}")
    return {"task": "side_effect", "invocations": n}


REGISTRY: dict[int, TaskDef] = {
    1: TaskDef(id=1, name="ok-a", run=t_ok),
    2: TaskDef(id=2, name="ok-b", run=t_ok),
    3: TaskDef(id=3, name="slow", run=t_slow),
    4: TaskDef(id=4, name="hard", run=t_hard, difficulty="hard"),
    5: TaskDef(id=5, name="infra", run=t_infra),
    6: TaskDef(id=6, name="poison", run=t_poison),
    7: TaskDef(id=7, name="side", run=t_side_effect, side_effecting=True, tool="jira"),
}


@pytest.fixture(autouse=True)
def clean_db():
    _CALLS.clear()
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE agents, task_instances, idempotency, attempts, dlq CASCADE")
    yield


@pytest.fixture
def registry():
    return REGISTRY


def seed_agent(plan: list[int], tiers: list[str] | None = None) -> str:
    """
    Create an agent and ALL of its task_instances in one transaction.

    This is the API's job (Lane B). Replicated here so the worker suite does not
    depend on that lane. Note every row starts at tier='junior' -- the claim
    query's `t.seq = a.cursor` predicate is what stops them all running at once.
    """
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO agents (plan, status) VALUES (%s, 'running') RETURNING id",
            (plan,),
        )
        agent_id = cur.fetchone()["id"]
        for seq, task_def_id in enumerate(plan):
            cur.execute(
                """INSERT INTO task_instances (agent_id, seq, task_def_id, tier)
                   VALUES (%s, %s, %s, %s)""",
                (agent_id, seq, task_def_id, (tiers or ["junior"] * len(plan))[seq]),
            )
    return str(agent_id)


def get_agent(agent_id: str) -> dict:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM agents WHERE id = %s", (agent_id,))
        return cur.fetchone()


def get_tasks(agent_id: str) -> list[dict]:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM task_instances WHERE agent_id = %s ORDER BY seq", (agent_id,)
        )
        return cur.fetchall()
