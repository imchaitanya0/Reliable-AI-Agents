"""
The three distributed-systems guarantees, exercised by fault injection.

    5.1  Durable Task Queue   the task remains available when a worker vanishes
    5.2  Task Leasing         an unrenewed lease returns the task to the queue
    5.3  Idempotency          an already-executed action is never repeated

Integration tests by necessity. Every one of these guarantees is enforced by a
SQL predicate -- FOR UPDATE SKIP LOCKED, a fenced WHERE clause, an ON CONFLICT --
so a mocked database would test the mock and prove nothing.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.conftest import get_agent, get_tasks, seed_agent

from common.config import REAPER_BATCH
from db.pool import pool
from orchestrator import ledger, queue
from orchestrator.classify import capability_attempts, classify
from orchestrator.reaper import orphaned_leases, reap
from worker.claim import claim_one


# --- helpers -----------------------------------------------------------------


def _expire(agent_id: str) -> None:
    """A worker that stopped renewing. Avoids sleeping out a real lease TTL."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE task_instances SET lease_expires = now() - interval '1s' "
            "WHERE agent_id=%s",
            (agent_id,),
        )


def _clear_jitter() -> None:
    """The reaper spreads requeues over a couple of seconds; tests skip that."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE task_instances SET next_run_at = now()")


# =============================================================================
# 5.1  DURABLE TASK QUEUE
# =============================================================================


class TestTaskRemainsAvailable:
    """Task #123 -> queue -> worker A crashes -> still there -> worker B."""

    def test_task_survives_the_worker_holding_it(self):
        agent_id = seed_agent([1])
        claimed = claim_one("junior", "worker-A", 30)
        assert claimed is not None

        _expire(agent_id)          # worker A is gone, without writing anything
        reap()
        _clear_jitter()

        task = get_tasks(agent_id)[0]
        assert task["status"] == "pending", "the task must return to the queue"
        assert task["result"] is None

        picked_up = claim_one("junior", "worker-B", 30)
        assert picked_up is not None and str(picked_up["id"]) == str(task["id"])

    def test_a_leased_task_is_not_handed_to_a_second_worker(self):
        seed_agent([1])
        assert claim_one("junior", "worker-A", 30) is not None
        assert claim_one("junior", "worker-B", 30) is None


class TestStalledAgentRepair:
    """
    A running agent with no task at its cursor is the one failure mode that
    never resolves and never reports itself: every actor here is triggered by
    finding a row, and there is no row to find.
    """

    def _delete_current_task(self, agent_id: str) -> None:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM task_instances WHERE agent_id=%s", (agent_id,))

    def test_stalled_agent_is_detected(self):
        agent_id = seed_agent([1, 2])
        self._delete_current_task(agent_id)
        assert queue.stalled_agents() == 1

    def test_repair_enqueues_the_task_at_the_cursor(self):
        agent_id = seed_agent([7, 1, 2])
        self._delete_current_task(agent_id)

        created = queue.repair_stalled()
        assert len(created) == 1
        assert created[0]["seq"] == 0
        assert created[0]["task_def_id"] == 7, "must enqueue plan[cursor], not plan[0+n]"
        assert queue.stalled_agents() == 0

        assert claim_one("junior", "worker-A", 30) is not None

    def test_repair_resumes_at_the_cursor_not_the_start(self):
        """An agent that crashed two tasks in must resume at task three."""
        agent_id = seed_agent([1, 2, 7])
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET cursor = 2 WHERE id=%s", (agent_id,))
            cur.execute("DELETE FROM task_instances WHERE agent_id=%s AND seq=2",
                        (agent_id,))

        created = queue.repair_stalled()
        assert created[0]["seq"] == 2
        assert created[0]["task_def_id"] == 7

    def test_repaired_row_starts_at_the_base_tier(self):
        """
        Promotion is scoped to a task, never to an agent. A repaired row landing
        at a higher tier would make crash recovery a silent upgrade path, and
        the cost claim would decay every time a worker died.
        """
        agent_id = seed_agent([1])
        self._delete_current_task(agent_id)
        assert queue.repair_stalled()[0]["tier"] == "junior"

    def test_repair_is_idempotent(self):
        """N orchestrators run this concurrently with no leader."""
        agent_id = seed_agent([1, 2])
        self._delete_current_task(agent_id)
        for _ in range(3):
            queue.repair_stalled()
        assert len(get_tasks(agent_id)) == 1

    def test_repair_ignores_finished_agents(self):
        agent_id = seed_agent([1])
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET status='completed' WHERE id=%s", (agent_id,))
            cur.execute("DELETE FROM task_instances WHERE agent_id=%s", (agent_id,))
        assert queue.repair_stalled() == []


class TestAgentFinalisation:
    def test_exhausted_agent_is_completed(self):
        agent_id = seed_agent([1])
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET cursor=1 WHERE id=%s", (agent_id,))
            cur.execute(
                "UPDATE task_instances SET status='succeeded' WHERE agent_id=%s",
                (agent_id,),
            )
        assert queue.finalise_agents() == [agent_id]
        assert get_agent(agent_id)["status"] == "completed"

    def test_agent_with_an_unfinished_task_is_not_completed(self):
        """
        A cursor past the end of the plan is necessary but NOT sufficient.
        Without the succeeded check this sweep would report lost work as success.
        """
        agent_id = seed_agent([1])
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET cursor=1 WHERE id=%s", (agent_id,))
            cur.execute(
                "UPDATE task_instances SET status='dead' WHERE agent_id=%s", (agent_id,)
            )
        assert queue.finalise_agents() == []
        assert get_agent(agent_id)["status"] == "running"


# =============================================================================
# 5.2  TASK LEASING
# =============================================================================


class TestLeaseExpiry:
    def test_expired_lease_is_reclaimed_with_evidence(self):
        agent_id = seed_agent([1])
        claim_one("junior", "dead-worker", 3)
        _expire(agent_id)

        reclaimed = reap()
        assert len(reclaimed) == 1
        assert reclaimed[0]["lease_owner"] == "dead-worker"
        assert reclaimed[0]["overdue_seconds"] > 0

        task = get_tasks(agent_id)[0]
        assert task["status"] == "pending"
        assert task["lease_owner"] is None
        assert task["lease_expires"] is None, "a released lease must be cleared"

        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT outcome, worker_id FROM attempts WHERE agent_id=%s", (agent_id,)
            )
            rows = cur.fetchall()
        assert [r["outcome"] for r in rows] == ["reclaimed"]
        assert rows[0]["worker_id"] == "dead-worker", (
            "a killed worker never writes its own attempt row -- it died"
        )

    def test_unexpired_lease_is_never_reclaimed(self):
        seed_agent([1])
        claim_one("junior", "worker-A", 300)
        assert reap() == []

    def test_reclaim_does_not_reset_the_attempt_counter(self):
        """
        `attempt` is the honest count of handouts. Resetting it would hide a task
        being evicted over and over.
        """
        agent_id = seed_agent([1])
        claim_one("junior", "worker-A", 3)
        _expire(agent_id)
        reap()
        assert get_tasks(agent_id)[0]["attempt"] == 1


class TestReaperUnderLoad:
    """The reaper only matters when many workers fail at once."""

    def _flood(self, n: int) -> None:
        """
        `n` tasks abandoned by dead workers.

        The agent cursor is deliberately parked past the plan so `t.seq =
        a.cursor` never matches. These rows are therefore invisible to the claim
        query while remaining fully visible to the reaper, which keys off lease
        expiry alone. That keeps these tests measuring the reaper rather than
        racing whatever worker happens to be alive.
        """
        with pool().connection() as conn, conn.cursor() as cur:
            for i in range(n):
                cur.execute(
                    "INSERT INTO agents (plan, status, cursor) "
                    "VALUES ('{1}','running', 99) RETURNING id"
                )
                aid = cur.fetchone()["id"]
                cur.execute(
                    """INSERT INTO task_instances
                         (agent_id, seq, task_def_id, status, lease_owner,
                          lease_expires, attempt)
                       VALUES (%s, 0, 1, 'running', %s,
                               now() - interval '1s', 1)""",
                    (aid, f"worker-{i}"),
                )

    def test_sweep_is_bounded_by_the_batch(self):
        """A mass eviction must not be one statement locking every row."""
        self._flood(12)
        assert len(reap(batch=5)) == 5
        assert orphaned_leases() == 7

    def test_repeated_sweeps_drain_the_backlog(self):
        self._flood(12)
        total = 0
        for _ in range(10):
            batch = reap(batch=5)
            total += len(batch)
            if not batch:
                break
        assert total == 12
        assert orphaned_leases() == 0

    def test_requeue_is_jittered(self):
        """
        Returning every task at exactly now() does not remove the stampede, it
        relocates it to the claim query.
        """
        self._flood(10)
        reap()
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(DISTINCT next_run_at) AS n FROM task_instances")
            assert cur.fetchone()["n"] > 1, "all tasks requeued at the same instant"

    def test_concurrent_reapers_never_double_reclaim(self):
        """
        No leader election. SKIP LOCKED is the entire mutual-exclusion story, so
        three orchestrators must yield exactly one evidence row per reclaim.
        """
        self._flood(15)

        def sweep(_: int) -> int:
            done = 0
            for _ in range(10):
                batch = reap(batch=5)
                done += len(batch)
                if not batch:
                    break
            return done

        with ThreadPoolExecutor(max_workers=3) as pool_:
            results = list(pool_.map(sweep, range(3)))

        assert sum(results) == 15, "a task was reclaimed twice"
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM attempts WHERE outcome='reclaimed'")
            assert cur.fetchone()["n"] == 15


class TestInfraFailuresDoNotFundEscalation:
    """
    The regression test for the most expensive bug in this system.

    `task_instances.attempt` is incremented by the CLAIM query, so a reaper
    reclaim bumps it. If promotion read that counter, a task evicted twice by
    dying machines would be promoted on its FIRST real capability failure --
    having never had its second fair attempt. That is the common path in the
    very demo this runtime is built around.
    """

    def test_reclaims_do_not_count_as_capability_failures(self):
        from tests.test_orchestrator import _reclaim

        agent_id = seed_agent([4])
        _reclaim(agent_id, times=3)

        task = get_tasks(agent_id)[0]
        assert task["attempt"] == 3, "the claim counter did advance"

        with pool().connection() as conn, conn.cursor() as cur:
            assert capability_attempts(cur, task) == 0, (
                "three machine deaths taught us nothing about capability"
            )

    def test_a_kill_dash_9_never_promotes(self):
        from tests.test_orchestrator import _fail, _reclaim

        agent_id = seed_agent([4])
        _reclaim(agent_id, times=2)      # two worker deaths, budget is 2
        _fail(agent_id, "CAPABILITY", attempt=1)

        assert classify()["retry"] == 1, "promoted on its first capability failure"
        assert get_tasks(agent_id)[0]["tier"] == "junior"


# =============================================================================
# 5.3  IDEMPOTENCY
# =============================================================================


class FakeJira:
    """A side effect we can count. Every call creates a real ticket."""

    def __init__(self) -> None:
        self.tickets: list[str] = []

    def create(self) -> dict:
        self.tickets.append(f"PAY-{len(self.tickets) + 1}")
        return {"ticket": self.tickets[-1]}


@pytest.fixture
def agent_id() -> str:
    return seed_agent([1])


class TestActionId:
    def test_id_is_deterministic_across_workers(self):
        """
        The original attempt and every recovery attempt must derive the same id
        from the task's identity alone. That property IS the mechanism.
        """
        assert ledger.action_id("a", 3, "jira") == ledger.action_id("a", 3, "jira")

    def test_distinct_actions_get_distinct_ids(self):
        assert ledger.action_id("a", 1, "jira") != ledger.action_id("a", 1, "slack")
        assert ledger.action_id("a", 1, "jira") != ledger.action_id("a", 2, "jira")
        assert ledger.action_id("a", 1, "jira") != ledger.action_id("b", 1, "jira")


class TestCrashAfterSuccess:
    """The exact sequence in 5.3: the crash happens AFTER the action succeeds."""

    def test_retry_after_crash_creates_no_duplicate(self, agent_id):
        jira = FakeJira()

        status, _, key = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.PROCEED
        jira.create()                      # ticket exists; worker dies here

        status, _, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.IN_FLIGHT
        assert len(jira.tickets) == 1, "the retry created a second ticket"

    def test_settled_action_replays_its_result(self, agent_id):
        jira = FakeJira()
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        result = jira.create()
        assert ledger.settle(key, result) is True

        status, stored, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.DONE
        assert stored == {"ticket": "PAY-1"}
        assert len(jira.tickets) == 1

    def test_many_retries_produce_one_effect(self, agent_id):
        jira = FakeJira()
        for _ in range(5):
            status, _, key = ledger.begin(agent_id, 0, "create_jira")
            if status == ledger.PROCEED:
                ledger.settle(key, jira.create())
        assert len(jira.tickets) == 1

    def test_racing_workers_yield_one_effect(self, agent_id):
        """Lease recovery means two workers legitimately run the same task."""
        jira = FakeJira()

        def attempt(_: int) -> bool:
            status, _, key = ledger.begin(agent_id, 0, "create_jira")
            if status == ledger.PROCEED:
                ledger.settle(key, jira.create())
                return True
            return False

        with ThreadPoolExecutor(max_workers=8) as pool_:
            cleared = list(pool_.map(attempt, range(8)))

        assert sum(cleared) == 1
        assert len(jira.tickets) == 1


class TestSettlement:
    def test_double_settle_does_not_overwrite(self, agent_id):
        """The first result is the one the action actually produced."""
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        assert ledger.settle(key, {"ticket": "PAY-1"}) is True
        assert ledger.settle(key, {"ticket": "WRONG"}) is False
        assert ledger.lookup(key)["result"] == {"ticket": "PAY-1"}

    def test_release_frees_an_action_that_never_ran(self, agent_id):
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        assert ledger.release(key) is True
        status, _, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.PROCEED

    def test_release_cannot_erase_a_settled_action(self, agent_id):
        """Releasing a completed action would license a duplicate."""
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        ledger.settle(key, {"ticket": "PAY-1"})
        assert ledger.release(key) is False

    def test_settled_row_must_carry_a_result(self, agent_id):
        """
        A settled row with no result would let a replay hand back NULL as if it
        were the answer. The schema forbids it.
        """
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        with pytest.raises(Exception):
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE idempotency SET state='done' WHERE key=%s", (key,)
                )


class TestAudit:
    def test_stranded_reservation_is_reported_not_deleted(self, agent_id):
        """
        An unsettled action id is the ledger doing its job. Deleting it to tidy
        a number would reopen the window the two-phase design closes.
        """
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        assert ledger.audit() == 1
        assert ledger.lookup(key) is not None

    def test_action_in_progress_is_not_flagged(self, agent_id):
        ledger.begin(agent_id, 0, "create_jira")
        assert ledger.audit() == 0


class TestReconciliation:
    """
    Refusing to duplicate is correct. Refusing forever is not.

    A worker that dies between reserving an action id and settling it leaves a
    reservation nobody will ever settle. Without reconciliation every retry sees
    IN_FLIGHT, refuses to act, and fails again until the task dead-letters --
    one crashed worker turned into permanently lost work, which is worse than
    either honest answer.
    """

    def test_a_fresh_reservation_is_not_reconciled(self, agent_id):
        """A merely slow worker still holds a renewable lease. Leave it alone."""
        ledger.begin(agent_id, 0, "create_jira")
        assert ledger.reconcile() == []

    def test_an_orphaned_reservation_is_closed_as_unresolved(self, agent_id):
        jira = FakeJira()
        status, _, key = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.PROCEED
        jira.create()                       # effect exists; worker dies here

        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )

        closed = ledger.reconcile()
        assert len(closed) == 1

        # The task can now proceed instead of retrying forever...
        status, stored, _ = ledger.begin(agent_id, 0, "create_jira")
        assert status == ledger.DONE
        # ...and the effect is reported as unknown rather than invented.
        assert stored["status"] == "unresolved"
        # Above all: it was NOT re-fired.
        assert len(jira.tickets) == 1

    def test_reconciliation_never_re_runs_the_action(self, agent_id):
        """
        The whole point. Re-running would trade a stuck task for exactly the
        duplicate side effect 5.3 exists to prevent.
        """
        jira = FakeJira()
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        jira.create()
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        for _ in range(3):
            ledger.reconcile()
            ledger.begin(agent_id, 0, "create_jira")
        assert len(jira.tickets) == 1

    def test_reconciled_action_does_not_overwrite_a_real_result(self, agent_id):
        """A result that arrived before the grace elapsed must survive."""
        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        ledger.settle(key, {"ticket": "PAY-1"})
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        assert ledger.reconcile() == []
        assert ledger.lookup(key)["result"] == {"ticket": "PAY-1"}


class TestInfraReclaimsDoNotDeadLetter:
    """
    The companion to TestInfraFailuresDoNotFundEscalation, and the worse of the
    two failure modes: an over-promoted task still completes, a dead-lettered
    one is simply lost.

    `attempt` counts every handout, reclaims included. If the INFRA budget read
    it, killing workers often enough would dead-letter perfectly healthy work --
    and the demo kills workers on purpose.
    """

    def test_repeated_reclaims_do_not_exhaust_the_infra_budget(self):
        from orchestrator.classify import MAX_INFRA_ATTEMPTS, infra_attempts
        from tests.test_orchestrator import _reclaim

        agent_id = seed_agent([1])
        _reclaim(agent_id, times=MAX_INFRA_ATTEMPTS + 2)

        task = get_tasks(agent_id)[0]
        assert task["attempt"] > MAX_INFRA_ATTEMPTS, "the claim counter did advance"

        with pool().connection() as conn, conn.cursor() as cur:
            assert infra_attempts(cur, task) == 0, (
                "machine deaths are not infra failures reported by a worker"
            )

    def test_a_much_killed_task_still_retries(self):
        from orchestrator.classify import MAX_INFRA_ATTEMPTS
        from tests.test_orchestrator import _fail, _reclaim

        agent_id = seed_agent([1])
        _reclaim(agent_id, times=MAX_INFRA_ATTEMPTS + 2)
        _fail(agent_id, "INFRA", attempt=1)

        assert classify()["retry"] == 1, "healthy work dead-lettered by worker kills"
        assert get_tasks(agent_id)[0]["status"] == "pending"

    def test_a_genuinely_broken_tool_still_dead_letters(self):
        """The budget must still do its job: stop consuming worker slots."""
        from orchestrator.classify import MAX_INFRA_ATTEMPTS
        from tests.test_orchestrator import _fail

        agent_id = seed_agent([1])
        _fail(agent_id, "INFRA", attempt=MAX_INFRA_ATTEMPTS)
        assert classify()["dlq"] == 1


class TestRuntimeFlags:
    """
    The chaos switches must actually switch something.

    They were written by /chaos/config and read by nothing, which made the
    controls the benchmark depends on silent no-ops -- the worst kind of broken,
    because the demo appears to work.
    """

    def _set(self, key: str, value: str) -> None:
        from common import runtime

        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO runtime_config (key, value) VALUES (%s, %s::jsonb) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, value),
            )
        runtime.invalidate()

    @pytest.fixture(autouse=True)
    def _restore(self):
        yield
        self._set("retries_enabled", "true")
        self._set("escalation_enabled", "true")
        self._set("force_tier", "null")

    def test_retries_disabled_makes_a_failure_terminal(self):
        from tests.test_orchestrator import _fail

        self._set("retries_enabled", "false")
        agent_id = seed_agent([1])
        _fail(agent_id, "INFRA", attempt=1)

        assert classify()["dlq"] == 1
        assert get_tasks(agent_id)[0]["status"] == "dead"

    def test_retries_enabled_is_the_control(self):
        from tests.test_orchestrator import _fail

        self._set("retries_enabled", "true")
        agent_id = seed_agent([1])
        _fail(agent_id, "INFRA", attempt=1)
        assert classify()["retry"] == 1

    def test_escalation_disabled_dead_letters_instead_of_promoting(self):
        """
        The all-junior baseline: cheaper, and it leaves work unfinished. The DLQ
        entry is what makes the second half visible rather than assumed.
        """
        from tests.test_orchestrator import _fail

        self._set("escalation_enabled", "false")
        agent_id = seed_agent([4])
        _fail(agent_id, "CAPABILITY", attempt=2)

        assert classify()["dlq"] == 1
        assert get_tasks(agent_id)[0]["tier"] == "junior", "must not have promoted"

    def test_forced_tier_cannot_escalate(self):
        from tests.test_orchestrator import _fail

        self._set("force_tier", '"senior"')
        agent_id = seed_agent([4], tiers=["senior"])
        _fail(agent_id, "CAPABILITY", attempt=2, tier="senior")
        assert classify()["dlq"] == 1


class TestRunawayGuard:
    def test_a_task_handed_out_forever_still_terminates(self):
        from orchestrator.classify import MAX_TOTAL_ATTEMPTS
        from tests.test_orchestrator import _fail

        agent_id = seed_agent([1])
        _fail(agent_id, "INFRA", attempt=MAX_TOTAL_ATTEMPTS)
        assert classify()["dlq"] == 1


class TestLedgerMetrics:
    """
    reconcile() closes an orphaned reservation with state='done'. Counting the
    ledger by state alone therefore reports an unknown effect as a successful
    guard, and loses the prevented-duplicate evidence precisely when it becomes
    permanent.
    """

    def test_a_settled_action_counts_as_guarded(self, agent_id):
        from common.metrics import snapshot

        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        ledger.settle(key, {"ticket": "PAY-1"})

        m = snapshot()["idempotency"]
        assert m["actions_guarded"] == 1
        assert m["duplicates_prevented"] == 0

    def test_a_reconciled_action_counts_as_prevented_not_guarded(self, agent_id):
        from common.metrics import snapshot

        _, _, key = ledger.begin(agent_id, 0, "create_jira")
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE idempotency SET created_at = now() - interval '10 min' "
                "WHERE key=%s",
                (key,),
            )
        ledger.reconcile()

        m = snapshot()["idempotency"]
        assert m["duplicates_prevented"] == 1, "evidence vanished once reconciled"
        assert m["actions_guarded"] == 0, "an unknown effect is not a successful guard"


class TestFailedAgentLeavesNoOrphans:
    """
    A dead-lettered task fails its agent, and the claim query requires
    `a.status='running'` -- so every remaining task of that agent becomes
    unclaimable. Left pending they are permanent garbage the queue still counts
    as work, which on a dashboard reads as a backlog that never drains.
    """

    def _poison_first_task(self, plan: list[int]) -> str:
        agent_id = seed_agent(plan)
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE task_instances SET status='failed', failure_class='POISON' "
                "WHERE agent_id=%s AND seq=0",
                (agent_id,),
            )
        return agent_id

    def test_remaining_tasks_are_cancelled(self):
        agent_id = self._poison_first_task([6, 1, 2])
        assert classify()["dlq"] == 1

        statuses = [t["status"] for t in get_tasks(agent_id)]
        assert statuses == ["dead", "dead", "dead"]
        assert get_agent(agent_id)["status"] == "failed"

    def test_queue_reports_no_claimable_work(self):
        agent_id = self._poison_first_task([6, 1, 2])
        classify()
        assert queue.depth()["claimable"] == 0

    def test_depth_ignores_tasks_of_stopped_agents(self):
        """
        Belt and braces: even if a row were somehow left pending under a stopped
        agent, it must not be reported as work.
        """
        agent_id = seed_agent([1, 2])
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET status='failed' WHERE id=%s", (agent_id,))
        assert queue.depth()["claimable"] == 0

    def test_a_healthy_agent_is_untouched(self):
        """Cancellation must be scoped to the failing agent only."""
        healthy = seed_agent([1, 2])
        self._poison_first_task([6, 1])
        classify()
        assert [t["status"] for t in get_tasks(healthy)] == ["pending", "pending"]
        # One claimable (seq 0, at the cursor); the other waits on it. That
        # split is `t.seq = a.cursor` enforcing dependency order.
        depth = queue.depth()
        assert depth["claimable"] == 1
        assert depth["waiting"] == 1
