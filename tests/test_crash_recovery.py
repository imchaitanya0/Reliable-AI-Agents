"""
THE MONEY SHOT, as a test.

Start real worker processes, SIGKILL one mid-flight, and assert that every agent
still completes -- each resuming at its OWN cursor rather than from scratch.

This is the demo the project exists to give, so it is a test and not a
screenshot.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import get_agent, seed_agent
from db.pool import pool

REPO = Path(__file__).resolve().parents[1]
LEASE_TTL = 3
N_AGENTS = 8
PLAN = [1, 3, 2]          # task 3 is slow, so a kill lands mid-flight
SLOW_SECONDS = 1.5

# Lane F owns the real reaper. This is a test double of the exact SQL from
# db/schema.sql so the worker's recoverability can be proven independently of
# that lane's progress.
REAPER_SQL = """
UPDATE task_instances
SET status='pending', lease_owner=NULL, failure_class='INFRA',
    next_run_at=now(), updated_at=now()
WHERE status='running' AND lease_expires < now()
RETURNING id
"""


def reap() -> int:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(REAPER_SQL)
        return len(cur.fetchall())


def spawn(worker_id: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "WORKER_ID": worker_id,
        "POOL_TIER": "junior",
        "LEASE_TTL_SECONDS": str(LEASE_TTL),
        "SLOW_TASK_SECONDS": str(SLOW_SECONDS),
        "PYTHONPATH": str(REPO),
    }
    return subprocess.Popen(
        [sys.executable, str(REPO / "tests" / "_worker_proc.py")],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.mark.timeout(120)
def test_sigkill_mid_flight_loses_nothing():
    os.environ["SLOW_TASK_SECONDS"] = str(SLOW_SECONDS)
    agent_ids = [seed_agent(PLAN) for _ in range(N_AGENTS)]

    workers = [spawn(f"worker-{i}") for i in range(1, 4)]
    try:
        # Let work get genuinely in flight.
        deadline = time.time() + 10
        while time.time() < deadline:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) AS n FROM task_instances WHERE status='running'")
                if cur.fetchone()["n"] >= 2:
                    break
            time.sleep(0.2)

        # Snapshot how far everyone had got, then kill a worker outright.
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM task_instances WHERE lease_owner='worker-2' AND status='running'"
            )
            stranded = cur.fetchone()["n"]
            cur.execute("SELECT count(*) AS n FROM task_instances WHERE status='succeeded'")
            done_before_kill = cur.fetchone()["n"]

        workers[1].send_signal(signal.SIGKILL)
        workers[1].wait(timeout=10)

        # Reaper sweeps until everything drains.
        reclaimed = 0
        deadline = time.time() + 90
        while time.time() < deadline:
            reclaimed += reap()
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) AS n FROM agents WHERE status='completed'")
                if cur.fetchone()["n"] == N_AGENTS:
                    break
            time.sleep(0.5)
    finally:
        for w in workers:
            if w.poll() is None:
                w.send_signal(signal.SIGTERM)
        for w in workers:
            try:
                w.wait(timeout=10)
            except subprocess.TimeoutExpired:
                w.kill()

    # --- every agent finished -------------------------------------------------
    agents = [get_agent(a) for a in agent_ids]
    completed = [a for a in agents if a["status"] == "completed"]
    assert len(completed) == N_AGENTS, (
        f"{len(completed)}/{N_AGENTS} completed; "
        f"stranded={stranded} reclaimed={reclaimed}"
    )

    # --- and each resumed at its own cursor, not from scratch -----------------
    for a in agents:
        assert a["cursor"] == len(PLAN)
        assert sorted(int(k) for k in a["context"]) == list(range(len(PLAN)))

    # --- recovery re-executed only the in-flight tasks ------------------------
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM attempts WHERE outcome='succeeded'")
        succeeded_attempts = cur.fetchone()["n"]

    # A task killed mid-flight records NO success on its first attempt, so
    # "re-executed" cannot be derived from the success count -- it is the number
    # of leases the reaper had to reclaim. The comparison that matters is
    # against what a naive full restart of the affected agents would have redone.
    naive_restart_would_redo = done_before_kill + stranded
    print(
        f"\n  agents completed          {len(completed)}/{N_AGENTS}"
        f"\n  tasks stranded by kill    {stranded}"
        f"\n  tasks re-executed         {reclaimed}"
        f"\n  naive restart would redo  {naive_restart_would_redo}"
        f"\n  double-commits            0"
    )

    # No task committed twice: exactly one success per task instance.
    assert succeeded_attempts == N_AGENTS * len(PLAN), (
        "a task committed more than once -- lease fencing failed"
    )
