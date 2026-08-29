"""
Lane F — Orchestrator Main Process Loop.
Stateless recovery & escalation daemon running reaper sweeps, failure classification, and queue repairs.
Zero leader election — runs N identical instances safely with SKIP LOCKED.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from common.config import ORCHESTRATOR_POLL_SECONDS, REAPER_INTERVAL_SECONDS
from orchestrator import ledger, queue
from orchestrator.classify import classify, process_failures
from orchestrator.reaper import reap, sweep_expired_leases

log = logging.getLogger("orchestrator")

_shutdown = False


def _stop(signum: int, frame: object) -> None:
    global _shutdown
    _shutdown = True


def tick() -> dict[str, int]:
    """One pass of every sweep."""
    counts = {
        "reclaimed": 0, "repaired": 0, "finalised": 0,
        "retry": 0, "promote": 0, "dlq": 0,
        "stranded": 0, "unresolved": 0
    }

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


def run_orchestrator_loop(interval_seconds: float = ORCHESTRATOR_POLL_SECONDS, once: bool = False) -> None:
    if once:
        tick()
        return

    while not _shutdown:
        try:
            tick()
        except Exception:
            pass
        time.sleep(interval_seconds)


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
