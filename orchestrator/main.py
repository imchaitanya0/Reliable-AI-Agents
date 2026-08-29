"""
Orchestrator loop.

Run as many instances as you like. They are identical and stateless: every one
runs the same two sweeps, and FOR UPDATE SKIP LOCKED guarantees no two ever
process the same row. There is no leader election, no Raft, no Zookeeper --
coordination lives in the transaction layer.

Kill any instance mid-demo and nothing changes. That is the point.

The orchestrator NEVER executes tasks. It repairs queue state and routes
failures. Escalation is re-enqueueing at a higher tier, which a separate worker
pool drains. If this process executed, it would be slow, stateful and a
bottleneck -- undoing the no-single-point-of-failure property it exists to
provide.

WHAT EACH SWEEP GUARANTEES
--------------------------
    reaper      5.2  an unrenewed lease returns its task to the queue
    repair      5.1  every running agent has a task in the queue at its cursor
    finalise    5.1  an agent whose plan is done is marked done
    classify    5.5  failures are routed by class, never retried blindly
    audit       5.3  unsettled action ids are surfaced, never silently dropped
    reconcile   5.3  an action whose owner died is closed, never re-fired

SWEEP ORDER MATTERS EXACTLY ONCE
--------------------------------
The reaper runs first. It is the only sweep with a deadline attached -- a
stranded task is unavailable to the whole system until it runs -- so it must
never queue behind bookkeeping. Repair runs before classify so a task the
repair just enqueued is visible immediately rather than a tick later. The rest
are order-independent.

Every sweep is idempotent and bounded, which is what allows N instances with no
leader: `SKIP LOCKED` means two instances never touch the same row, and a sweep
that is only correct when run once would never be correct here.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from common.config import ORCHESTRATOR_POLL_SECONDS
from orchestrator import ledger, queue
from orchestrator.classify import classify
from orchestrator.reaper import reap

log = logging.getLogger("orchestrator")

_shutdown = False


def _stop(signum: int, frame: object) -> None:
    global _shutdown
    _shutdown = True


def tick() -> dict[str, int]:
    """
    One pass of every sweep.

    Each sweep owns its own transaction. Sharing one would mean a failure in the
    audit query rolling back a completed reclaim -- letting a reporting bug
    cause the data loss the runtime exists to prevent.

    A sweep that raises is logged and skipped rather than killing the loop: an
    orchestrator that exits on the first transient database error is one that
    stops reclaiming leases at exactly the moment the system is unhealthy.
    """
    counts = {"reclaimed": 0, "repaired": 0, "finalised": 0,
              "retry": 0, "promote": 0, "dlq": 0,
              "stranded": 0, "unresolved": 0}

    for name, run in (
        ("reclaimed", lambda: len(reap())),               # 5.2
        ("repaired", lambda: len(queue.repair_stalled())), # 5.1
        ("finalised", lambda: len(queue.finalise_agents())),
        ("routed", classify),                              # 5.5
        ("stranded", ledger.audit),                        # 5.3
        ("unresolved", lambda: len(ledger.reconcile())),   # 5.3
    ):
        try:
            outcome = run()
        except Exception:
            log.exception("sweep %s failed; continuing", name)
            continue
        if name == "routed":
            counts.update(outcome)
        else:
            counts[name] = outcome

    return counts


def main() -> int:
    instance = os.getenv("ORCHESTRATOR_ID", f"orch-{os.getpid()}")
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)-7s [{instance}] %(message)s",
    )
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("orchestrator up poll=%ss", ORCHESTRATOR_POLL_SECONDS)

    while not _shutdown:
        try:
            counts = tick()
            if any(counts.values()):
                # Steady state is silent, so a non-empty log always means
                # something actually happened.
                log.info(
                    "reclaimed=%(reclaimed)s repaired=%(repaired)s "
                    "finalised=%(finalised)s retry=%(retry)s "
                    "promote=%(promote)s dlq=%(dlq)s stranded=%(stranded)s "
                    "unresolved=%(unresolved)s",
                    counts,
                )
        except Exception:
            log.exception("orchestrator tick failed")
        time.sleep(ORCHESTRATOR_POLL_SECONDS)

    log.info("orchestrator stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
