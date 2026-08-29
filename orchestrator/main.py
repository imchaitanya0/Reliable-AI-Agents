"""
Orchestrator loop.

Run as many instances as you like. They are identical and stateless: every one
runs the same two sweeps, and FOR UPDATE SKIP LOCKED guarantees no two ever
process the same row. There is no leader election, no Raft, no Zookeeper --
coordination lives in the transaction layer.

Kill any instance mid-demo and nothing changes. That is the point.

The orchestrator NEVER executes tasks. It classifies and routes. Escalation is
re-enqueueing at a higher tier, which a separate worker pool drains. If this
process executed, it would be slow, stateful and a bottleneck -- undoing the
no-single-point-of-failure property it exists to provide.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from common.config import ORCHESTRATOR_POLL_SECONDS
from orchestrator.classify import classify
from orchestrator.reaper import reap

log = logging.getLogger("orchestrator")

_shutdown = False


def _stop(signum: int, frame: object) -> None:
    global _shutdown
    _shutdown = True


def tick() -> dict[str, int]:
    """One sweep. Reap dead leases, then route whatever failed."""
    reclaimed = reap()
    routed = classify()
    return {"reclaimed": len(reclaimed), **routed}


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
                    "reclaimed=%(reclaimed)s retry=%(retry)s "
                    "promote=%(promote)s dlq=%(dlq)s", counts,
                )
        except Exception:
            log.exception("orchestrator tick failed")
        time.sleep(ORCHESTRATOR_POLL_SECONDS)

    log.info("orchestrator stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
