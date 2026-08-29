"""
Extensibility, as tests rather than as claims in a comment.

Each test performs a real extension and asserts the runtime picked it up with
NO change to worker, orchestrator or API code.
"""

from __future__ import annotations

import pytest

from tests.conftest import get_agent, get_tasks, seed_agent

from common import tiers
from common.protocol import TaskContext, TaskDef
from common.registry import Registry
from db.pool import pool
from worker.claim import claim_one
from worker.main import process_one


@pytest.fixture
def principal_tier():
    """Add a third capability tier. This INSERT is the entire change."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO tiers (name, rank, cost_units, tokens, latency_ms, p_success)
               VALUES ('principal', 3, 60, 8000, 4000, 0.99)
               ON CONFLICT (name) DO NOTHING"""
        )
    tiers.invalidate()
    yield "principal"
    with pool().connection() as conn, conn.cursor() as cur:
        # task_instances reference tiers, so clear them before removing the row.
        cur.execute("TRUNCATE agents, task_instances, attempts CASCADE")
        cur.execute("DELETE FROM tiers WHERE name='principal'")
    tiers.invalidate()


# --- extending the escalation ladder -----------------------------------------

def test_adding_a_tier_needs_no_code_change(principal_tier):
    """The ladder is data. One INSERT extends promotion, costing and routing."""
    assert [t["name"] for t in tiers.all_tiers(refresh=True)] == [
        "junior",
        "senior",
        "principal",
    ]
    # Promotion now walks one rung further than it used to.
    assert tiers.next_tier("junior") == "senior"
    assert tiers.next_tier("senior") == "principal"
    assert tiers.next_tier("principal") is None, "top of ladder must terminate"

    assert tiers.base_tier() == "junior"
    assert tiers.top_tier() == "principal"
    assert tiers.cost_of("principal") == (60, 8000)


def test_a_worker_pool_drains_the_new_tier(principal_tier, registry):
    """POOL_TIER=principal works with no code change: queues are data-driven."""
    agent_id = seed_agent([1], tiers=["principal"])

    # Existing pools correctly ignore it.
    assert claim_one("junior", "w1", 3) is None
    assert claim_one("senior", "s1", 3) is None

    assert process_one("principal", "p1", registry) is True
    assert get_tasks(agent_id)[0]["status"] == "succeeded"
    # Cost accounting picked up the new tier's price automatically.
    assert get_agent(agent_id)["cost_units"] == 60


def test_a_tier_can_be_inserted_between_existing_ones():
    """Ranks are ordered, not positional, so the ladder is open in the middle."""
    with pool().connection() as conn, conn.cursor() as cur:
        # Deferring the rank uniqueness check is what makes a mid-ladder
        # insertion possible: ranks collide transiently, then resolve on commit.
        cur.execute("SET CONSTRAINTS tiers_rank_uk DEFERRED")
        cur.execute("UPDATE tiers SET rank = 3 WHERE name = 'senior'")
        cur.execute(
            """INSERT INTO tiers (name, rank, cost_units, tokens, latency_ms)
               VALUES ('mid', 2, 4, 2000, 900)"""
        )
    tiers.invalidate()
    try:
        assert tiers.next_tier("junior") == "mid"
        assert tiers.next_tier("mid") == "senior"
    finally:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM tiers WHERE name='mid'")
            cur.execute("UPDATE tiers SET rank = 2 WHERE name = 'senior'")
        tiers.invalidate()


def test_unknown_tier_fails_loudly():
    """A typo must not silently cost nothing."""
    with pytest.raises(KeyError):
        tiers.cost_of("staff")


# --- extending the task set --------------------------------------------------

def test_adding_a_task_is_one_decorated_function():
    """No central dict to edit. Drop a file in, restart a worker."""
    reg = Registry()

    @reg.task(42, name="search-incidents", tool="logs")
    def search_incidents(ctx: TaskContext) -> dict:
        return {"hits": 3}

    td = reg.get(42)
    assert td is not None
    assert td.name == "search-incidents" and td.tool == "logs"
    # The function stays a plain function -- decoration is not invasive.
    assert search_incidents(TaskContext("a", 0, "junior")) == {"hits": 3}


def test_duplicate_task_id_is_a_hard_error():
    """Silent overwrite would make an agent run something it never asked for."""
    reg = Registry()
    reg.add(TaskDef(id=1, name="first", run=lambda ctx: {}))
    with pytest.raises(ValueError, match="already registered"):
        reg.add(TaskDef(id=1, name="second", run=lambda ctx: {}))


def test_both_registration_styles_coexist():
    """
    Decorators AND a legacy TASK_DEFS dict. Neither lane is blocked on the
    other adopting a new style mid-hackathon.
    """
    reg = Registry()

    @reg.task(10, name="decorated")
    def decorated(ctx: TaskContext) -> dict:
        return {}

    reg.merge({11: TaskDef(id=11, name="legacy", run=lambda ctx: {})})

    assert len(reg) == 2
    assert {t.name for t in reg} == {"decorated", "legacy"}


def test_new_task_runs_end_to_end_without_touching_the_worker(registry):
    """The real proof: register a task, and the runtime executes it."""
    reg = Registry()
    reg.merge(registry)

    @reg.task(77, name="brand-new")
    def brand_new(ctx: TaskContext) -> dict:
        return {"made_by": "an extension", "saw": sorted(ctx.prior)}

    agent_id = seed_agent([1, 77])
    assert process_one("junior", "w1", reg.as_dict()) is True
    assert process_one("junior", "w1", reg.as_dict()) is True

    agent = get_agent(agent_id)
    assert agent["status"] == "completed"
    assert agent["context"]["1"]["made_by"] == "an extension"
    assert agent["context"]["1"]["saw"] == [0], "new task still gets prior context"
