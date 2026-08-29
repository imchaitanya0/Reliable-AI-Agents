"""
The demo, as a test.

Every scene the project presents on stage is asserted here, so the claims are
reproducible rather than a story told over a screenshot. If one of these fails,
the corresponding demo will fail in front of an audience.

    Scene 1  worker killed mid-flight   -> every agent still completes  (5.1, 5.2)
    Scene 2  orchestrator killed        -> nothing changes              (no leader)
    Scene 3  crash after a side effect  -> exactly one effect           (5.3)
    Scene 4  the three-way cost table   -> the control for the claim

These spawn REAL processes and SIGKILL them. Simulating a crash proves the
simulation works; killing a process proves the system does.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import get_agent, get_tasks, seed_agent

from common import runtime
from common.metrics import snapshot
from db.pool import pool
from orchestrator import ledger

REPO = Path(__file__).resolve().parents[1]

LEASE_TTL = 3          # short, so recovery is observable inside a test
ORCH_POLL = 0.5
EASY = [1, 2, 1]       # fixture registry: all fast, all succeed
HARD = [1, 4, 2]       # task 4 fails on junior, succeeds on senior


# --- process helpers ---------------------------------------------------------


def _env(**extra: str) -> dict[str, str]:
    return {
        **os.environ,
        "LEASE_TTL_SECONDS": str(LEASE_TTL),
        "ORCHESTRATOR_POLL_SECONDS": str(ORCH_POLL),
        "PYTHONPATH": str(REPO),
        **extra,
    }


def spawn_worker(worker_id: str, tier: str = "junior") -> subprocess.Popen:
    """A real worker process, driven by the fixture registry."""
    return subprocess.Popen(
        [sys.executable, str(REPO / "tests" / "_worker_proc.py")],
        env=_env(WORKER_ID=worker_id, POOL_TIER=tier, SLOW_TASK_SECONDS="1.0"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def spawn_orchestrator(name: str) -> subprocess.Popen:
    """A real orchestrator. No leader election, so N of these are equivalent."""
    return subprocess.Popen(
        [sys.executable, "-m", "orchestrator.main"],
        cwd=str(REPO),
        env=_env(ORCHESTRATOR_ID=name),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop_all(procs: list[subprocess.Popen]) -> None:
    """SIGTERM is a request; every escalation must actually be waited on."""
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)


def wait_until(predicate, timeout: float, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def completed_agents() -> int:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM agents WHERE status='completed'")
        return cur.fetchone()["n"]


def tasks_running() -> int:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM task_instances WHERE status='running'")
        return cur.fetchone()["n"]


# =============================================================================
# SCENE 1 -- a worker dies mid-flight and no work is lost
# =============================================================================


@pytest.mark.timeout(180)
class TestScene1WorkerFailure:
    """
    The money shot: kill a worker holding live leases, and every agent still
    finishes. The killed worker never gets to report anything -- it died -- so
    the ONLY thing that notices is the lease running out.
    """

    N_AGENTS = 8

    def test_every_agent_completes_after_a_worker_is_killed(self):
        agent_ids = [seed_agent(EASY) for _ in range(self.N_AGENTS)]
        procs = [spawn_worker(f"w{i}") for i in range(1, 4)]
        procs.append(spawn_orchestrator("orch-1"))

        try:
            # Let work get genuinely in flight before killing anything.
            assert wait_until(lambda: tasks_running() >= 1, timeout=20), (
                "no task ever started; the workers are not claiming"
            )

            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) AS n FROM task_instances "
                    "WHERE status='running' AND lease_owner='w1'"
                )
                stranded = cur.fetchone()["n"]

            procs[0].send_signal(signal.SIGKILL)
            procs[0].wait(timeout=10)

            done = wait_until(
                lambda: completed_agents() == self.N_AGENTS, timeout=120
            )
        finally:
            stop_all(procs)

        assert done, (
            f"only {completed_agents()}/{self.N_AGENTS} completed; "
            f"{stranded} task(s) were stranded by the kill"
        )

        # Not merely "finished" -- each agent resumed at its OWN cursor, with the
        # full accumulated context. A restart-from-scratch would also reach
        # 'completed' and would be wrong.
        for agent_id in agent_ids:
            agent = get_agent(agent_id)
            assert agent["cursor"] == len(EASY)
            assert sorted(int(k) for k in agent["context"]) == list(range(len(EASY)))

    def test_recovery_is_recorded_as_evidence(self):
        """
        A reclaim nobody recorded makes "% tasks recovered" a guess. The killed
        worker cannot write its own attempt row, so the reaper writes it.
        """
        seed_agent(EASY)
        procs = [spawn_worker("w1"), spawn_orchestrator("orch-1")]
        try:
            assert wait_until(lambda: tasks_running() >= 1, timeout=20)
            procs[0].send_signal(signal.SIGKILL)
            procs[0].wait(timeout=10)

            reclaimed = wait_until(
                lambda: snapshot()["recovery"]["leases_reclaimed"] >= 1, timeout=60
            )
        finally:
            stop_all(procs)

        assert reclaimed, "the reaper never recorded the reclaim"

    def test_no_task_is_left_stranded(self):
        """
        The 5.1 invariant, measured after the dust settles: nothing running past
        its lease, and no agent stuck with an empty queue.
        """
        for _ in range(4):
            seed_agent(EASY)
        procs = [spawn_worker("w1"), spawn_worker("w2"), spawn_orchestrator("o1")]
        try:
            assert wait_until(lambda: tasks_running() >= 1, timeout=20)
            procs[0].send_signal(signal.SIGKILL)
            procs[0].wait(timeout=10)
            wait_until(lambda: completed_agents() == 4, timeout=120)
        finally:
            stop_all(procs)

        recovery = snapshot()["recovery"]
        assert recovery["stalled_agents"] == 0, "an agent has no task in the queue"
        assert recovery["orphaned_leases"] == 0, "an expired lease was never reclaimed"


# =============================================================================
# SCENE 2 -- an orchestrator dies and nothing changes
# =============================================================================


@pytest.mark.timeout(180)
class TestScene2OrchestratorFailure:
    """
    There is no leader to lose. Every instance runs the identical loop and
    SKIP LOCKED keeps them off each other's rows, so killing one is a non-event.
    """

    def test_work_completes_with_an_orchestrator_killed_mid_run(self):
        n = 4
        for _ in range(n):
            seed_agent(EASY)

        workers = [spawn_worker("w1"), spawn_worker("w2")]
        orchestrators = [spawn_orchestrator("o1"), spawn_orchestrator("o2")]
        try:
            assert wait_until(lambda: tasks_running() >= 1, timeout=20)

            orchestrators[0].send_signal(signal.SIGKILL)
            orchestrators[0].wait(timeout=10)

            # Kill a worker too, so the surviving orchestrator has real work:
            # a dead orchestrator is only interesting if recovery still happens.
            workers[0].send_signal(signal.SIGKILL)
            workers[0].wait(timeout=10)

            done = wait_until(lambda: completed_agents() == n, timeout=120)
        finally:
            stop_all(workers + orchestrators)

        assert done, f"only {completed_agents()}/{n} completed after losing an orchestrator"

    def test_two_orchestrators_do_not_double_reclaim(self):
        """
        SKIP LOCKED is the entire mutual-exclusion protocol. If it were not
        holding, the same lease would be reclaimed twice and the recovery metric
        would over-report.
        """
        seed_agent(EASY)
        procs = [spawn_worker("w1"), spawn_orchestrator("o1"), spawn_orchestrator("o2")]
        try:
            assert wait_until(lambda: tasks_running() >= 1, timeout=20)
            procs[0].send_signal(signal.SIGKILL)
            procs[0].wait(timeout=10)
            wait_until(
                lambda: snapshot()["recovery"]["leases_reclaimed"] >= 1, timeout=60
            )
            time.sleep(2)   # give both orchestrators several ticks over the row
        finally:
            stop_all(procs)

        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT task_instance_id, attempt_no, count(*) AS n
                   FROM attempts WHERE outcome='reclaimed'
                   GROUP BY task_instance_id, attempt_no HAVING count(*) > 1"""
            )
            assert cur.fetchall() == [], "the same lease was reclaimed twice"


# =============================================================================
# SCENE 3 -- a crash AFTER the side effect, and still exactly one effect
# =============================================================================


class TestScene3Idempotency:
    """
    The counterintuitive scene, and the one worth narrating slowly: the worker
    does not crash instead of creating the ticket. It crashes AFTER creating it,
    before it could say so.
    """

    class Jira:
        def __init__(self) -> None:
            self.tickets: list[str] = []

        def create(self) -> dict:
            self.tickets.append(f"PAY-{len(self.tickets) + 1}")
            return {"ticket": self.tickets[-1]}

    def test_crash_after_success_creates_no_duplicate(self):
        agent_id = seed_agent(EASY)
        jira = self.Jira()

        # Worker A: reserve, act, die before acknowledging.
        status, _, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.PROCEED
        jira.create()

        # Worker B picks the task up after the lease expires.
        status, _, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.IN_FLIGHT, "B was cleared to act on an unknown effect"
        assert len(jira.tickets) == 1, "a second ticket was filed"

    def test_the_naive_check_would_have_missed_it(self):
        """
        Why the ledger is two-phase, in one assertion.

        A single-phase ledger records the id AFTER the action, so the crash
        window leaves NO row -- the retry duplicates, and `count(*)` still
        answers 1. The obvious audit passes while two tickets exist. Here the
        reservation exists from the start, so the ambiguity is visible.
        """
        agent_id = seed_agent(EASY)
        ledger.begin(agent_id, 0, "create_jira")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM idempotency")
            assert cur.fetchone()["n"] == 1, "the effect left no trace before it ran"

    def test_the_workflow_is_not_stuck_forever(self):
        """
        Refusing to duplicate is correct; refusing forever is not. Once the
        owner is provably gone the action is closed as unresolved -- never
        re-fired -- so one dead worker cannot strand the task.
        """
        agent_id = seed_agent(EASY)
        jira = self.Jira()
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        jira.create()

        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        assert len(ledger.reconcile()) == 1

        status, stored, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.DONE
        assert stored["status"] == "unresolved"
        assert len(jira.tickets) == 1, "reconciliation re-fired the action"

    def test_the_dashboard_number_is_honest(self):
        """
        `duplicates_prevented` must survive reconciliation, and an unknown
        effect must never be counted as a successful guard.
        """
        agent_id = seed_agent(EASY)
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        ledger.reconcile()

        m = snapshot()["idempotency"]
        assert m["duplicates_prevented"] == 1
        assert m["actions_guarded"] == 0


# =============================================================================
# SCENE 4 -- the three-way cost table
# =============================================================================


@pytest.mark.timeout(240)
class TestScene4CostComparison:
    """
    The claim is comparative, so it is worthless without its controls. All three
    baselines are produced by THIS system on THIS workload, live -- not
    estimated, and not measured on a different run.

    The workload deliberately mixes easy agents with ones containing a task that
    junior cannot do. That mix is what produces a realistic escalation rate; if
    every agent escalated, the rate would be an artefact of the fixture rather
    than a property of the runtime.
    """

    N_EASY = 6
    N_HARD = 2

    def _set(self, key: str, value: str) -> None:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO runtime_config (key, value) VALUES (%s, %s::jsonb) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, value),
            )
        runtime.invalidate()

    @pytest.fixture(autouse=True)
    def _restore_flags(self):
        yield
        self._set("retries_enabled", "true")
        self._set("escalation_enabled", "true")
        self._set("force_tier", "null")
        runtime.invalidate()

    def _run_workload(self, senior_pool: bool = True) -> dict:
        for _ in range(self.N_EASY):
            seed_agent(EASY)
        for _ in range(self.N_HARD):
            seed_agent(HARD)

        total = self.N_EASY + self.N_HARD
        procs = [spawn_worker("w1"), spawn_worker("w2"), spawn_orchestrator("o1")]
        if senior_pool:
            procs.append(spawn_worker("s1", tier="senior"))
        try:
            # Settled = nothing left that could still move.
            wait_until(
                lambda: completed_agents() + self._failed_agents() == total,
                timeout=150,
            )
        finally:
            stop_all(procs)
        return snapshot()

    def _failed_agents(self) -> int:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM agents WHERE status='failed'")
            return cur.fetchone()["n"]

    def test_tiered_finishes_everything(self):
        """The system as shipped: every agent completes."""
        m = self._run_workload()
        assert m["agents"]["completed"] == self.N_EASY + self.N_HARD
        assert m["escalation"]["promoted"] >= self.N_HARD, "the hard task never escalated"

    def test_all_junior_is_cheaper_and_leaves_work_unfinished(self):
        """
        The honest half of the cheap baseline. Quoting its cost without this is
        the difference between a comparison and a misleading one.
        """
        self._set("escalation_enabled", "false")
        m = self._run_workload(senior_pool=False)

        assert m["agents"]["completed"] == self.N_EASY
        assert m["agents"]["failed"] == self.N_HARD, (
            "with escalation off, the hard agents must NOT complete"
        )
        assert m["dlq"]["size"] >= self.N_HARD

    def test_all_senior_finishes_everything_at_full_price(self):
        """The expensive baseline: it works, and it pays top rate for everything."""
        self._set("force_tier", '"senior"')
        m = self._run_workload()

        assert m["agents"]["completed"] == self.N_EASY + self.N_HARD
        # Every task ran at 12 units rather than 1.
        assert m["cost"]["units_spent"] == m["cost"]["all_senior_baseline"]

    def test_tiered_costs_a_fraction_of_all_senior(self):
        """
        The claim itself. Same completion as all-senior, a fraction of the cost,
        because only the tasks that needed it ever paid senior rates.
        """
        m = self._run_workload()

        assert m["agents"]["completed"] == self.N_EASY + self.N_HARD
        assert m["cost"]["vs_all_senior"] < 0.5, (
            f"tiered cost is {m['cost']['vs_all_senior']}x all-senior -- the "
            f"escalation path is leaking onto tasks that did not need it"
        )
        assert m["cost"]["units_spent"] > m["cost"]["all_junior_baseline"], (
            "we cannot be cheaper than all-junior; that baseline does not finish"
        )
