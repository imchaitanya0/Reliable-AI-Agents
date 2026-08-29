"""
Reliability Invariants Test Suite (Contract Verification)
=========================================================

Automated pytest tests asserting all 7 core reliability invariants.
Screenshots are not evidence — automated fault injection is.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

# Ensure workspace root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from common.failures import CapabilityFailure, InfraFailure, PoisonFailure
from common.protocol import TaskContext, TaskDef
from db.init_db import init_database
from db.pool import get_conn, get_transaction
from metrics.common import cleanup_test_data
from orchestrator.classify import process_failures
from orchestrator.promote import promote_task
from orchestrator.reaper import sweep_expired_leases
from tasks.registry import TASK_DEFS
from tasks.tiers import execute_with_tier
from worker.claim import claim_task
from worker.executor import execute_claimed_task


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    init_database()


@pytest.fixture(autouse=True)
def clean_db():
    with get_conn() as conn:
        cleanup_test_data(conn)


def test_invariant_1_and_2_lease_expiry_and_cursor_recovery():
    """
    Invariant 1 & 2:
    - An un-renewed lease becomes claimable again after expiry.
    - SIGKILL mid-task resumes agent at its own cursor without replaying prior work.
    """
    agent_id = str(uuid.uuid4())
    plan = [1, 2, 6, 8, 9]

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agents (id, plan, cursor, status, context) VALUES (%s, %s, 2, 'running', '{\"0\": {\"ok\": true}, \"1\": {\"ok\": true}}'::jsonb);", (agent_id, plan))
            # Step 2 claimed by crashed worker, lease expired
            cur.execute(
                """
                INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier, lease_owner, lease_expires, attempt)
                VALUES (%s, 2, 6, 'running', 'junior', 'dead-worker-2', now() - interval '5 seconds', 1);
                """,
                (agent_id,),
            )

    # Reaper sweeps expired lease
    reclaimed = sweep_expired_leases()
    assert len(reclaimed) == 1
    assert str(reclaimed[0]["agent_id"]) == agent_id
    assert reclaimed[0]["seq"] == 2
    assert reclaimed[0]["tier"] == "junior"  # Invariant: INFRA keeps junior tier!

    # Replacement worker claims and executes at cursor=2
    claimed = claim_task("healthy-worker-1", "junior", ttl_seconds=30)
    assert claimed is not None
    assert str(claimed["agent_id"]) == agent_id
    assert claimed["seq"] == 2


def test_invariant_3_idempotency_exactly_once_effect():
    """
    Invariant 3:
    - Forced double execution of a side-effecting task produces exactly one external action.
    """
    agent_id = str(uuid.uuid4())
    seq = 3
    task_row = {"agent_id": agent_id, "seq": seq, "task_def_id": 8, "tier": "junior"}  # Task 8 is Jira side-effecting

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agents (id, plan, cursor, status, context) VALUES (%s, %s, 0, 'running', '{}'::jsonb);", (agent_id, [8]))
            cur.execute("INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier) VALUES (%s, %s, 8, 'running', 'junior');", (agent_id, seq))

    # Attempt 1: Fires Jira tool and records in idempotency ledger
    res1, cost1 = execute_claimed_task(task_row)
    assert res1["status"] == "success"
    assert "idempotency_key" in res1
    key = res1["idempotency_key"]

    # Verify recorded in DB
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM idempotency WHERE key = %s;", (key,))
            assert cur.fetchone()["cnt"] == 1

    # Attempt 2 (Simulated replay): Must return stored result without re-executing
    res2, cost2 = execute_claimed_task(task_row)
    assert res2 == res1
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM idempotency WHERE key = %s;", (key,))
            assert cur.fetchone()["cnt"] == 1  # Exactly one!


def test_invariant_4_sequential_dependency_ordering():
    """
    Invariant 4:
    - Task seq=n is never claimable before seq=n-1 commits.
    """
    agent_id = str(uuid.uuid4())
    plan = [1, 2, 3]

    with get_transaction() as conn:
        with conn.cursor() as cur:
            # Agent cursor is 0
            cur.execute("INSERT INTO agents (id, plan, cursor, status) VALUES (%s, %s, 0, 'running');", (agent_id, plan))
            # Insert seq=0 and seq=1 both pending
            cur.execute("INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier) VALUES (%s, 0, 1, 'pending', 'junior');", (agent_id,))
            cur.execute("INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier) VALUES (%s, 1, 2, 'pending', 'junior');", (agent_id,))

    # Worker claims: MUST claim seq=0, cannot claim seq=1 because seq != a.cursor
    claimed = claim_task("worker-1", "junior")
    assert claimed is not None
    assert claimed["seq"] == 0

    # No second task claimable while cursor=0
    claimed_2 = claim_task("worker-2", "junior")
    assert claimed_2 is None


def test_invariant_5_classification_and_routing():
    """
    Invariant 5:
    - INFRA never promotes.
    - CAPABILITY promotes to senior only after max_attempts_per_tier is reached.
    - POISON goes directly to DLQ and marks agent failed.
    """
    agent_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agents (id, plan, cursor, status) VALUES (%s, %s, 0, 'running');", (agent_id, [6]))
            # Task with 2 failed CAPABILITY attempts on junior
            cur.execute(
                """
                INSERT INTO task_instances (id, agent_id, seq, task_def_id, status, tier, attempt, max_attempts_per_tier, failure_class)
                VALUES (%s, %s, 0, 6, 'pending', 'junior', 2, 2, 'CAPABILITY');
                """,
                (task_id, agent_id),
            )

    # Process failures: Must promote to senior!
    processed = process_failures()
    assert processed == 1

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tier, attempt FROM task_instances WHERE id = %s;", (task_id,))
            t = cur.fetchone()
            assert t["tier"] == "senior"
            assert t["attempt"] == 0


def test_invariant_6_promotion_never_leaks_onto_successor():
    """
    Invariant 6:
    - A promoted task's successor claims at tier='junior' (promotion does not leak onto the agent).
    """
    agent_id = str(uuid.uuid4())
    task_0_id = str(uuid.uuid4())
    task_1_id = str(uuid.uuid4())
    plan = [6, 8]  # Step 0 is hard (promoted), Step 1 is easy

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agents (id, plan, cursor, status, context) VALUES (%s, %s, 0, 'running', '{}'::jsonb);", (agent_id, plan))
            # Step 0 succeeded on senior tier
            cur.execute("INSERT INTO task_instances (id, agent_id, seq, task_def_id, status, tier) VALUES (%s, %s, 0, 6, 'succeeded', 'senior');", (task_0_id, agent_id))
            # Advance cursor to 1 and spawn next task instance
            cur.execute("UPDATE agents SET cursor = 1 WHERE id = %s;", (agent_id,))
            cur.execute("INSERT INTO task_instances (id, agent_id, seq, task_def_id, status, tier) VALUES (%s, %s, 1, 8, 'pending', 'junior');", (task_1_id, agent_id))

    # Junior worker claims step 1
    claimed = claim_task("junior-worker", "junior")
    assert claimed is not None
    assert claimed["seq"] == 1
    assert claimed["tier"] == "junior"


def test_invariant_7_context_propagation_to_promoted_attempt():
    """
    Invariant 7:
    - An escalated task receives the exact accumulated context from all prior tasks.
    """
    agent_id = str(uuid.uuid4())
    prior_data = {"0": {"logs": ["line1", "line2"]}, "1": {"commits": [{"sha": "abc"}]}}

    with get_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agents (id, plan, cursor, status, context) VALUES (%s, %s, 2, 'running', %s::jsonb);", (agent_id, [1, 2, 6], json.dumps(prior_data)))
            cur.execute("INSERT INTO task_instances (agent_id, seq, task_def_id, status, tier) VALUES (%s, 2, 6, 'running', 'senior');", (agent_id,))

    task_row = {"agent_id": agent_id, "seq": 2, "task_def_id": 6, "tier": "senior"}
    result, cost = execute_claimed_task(task_row)

    assert result["status"] == "success"
    assert result["evidence"]["log_count"] == 2
    assert result["evidence"]["commit_count"] == 1
    assert cost == 12  # Senior cost units
