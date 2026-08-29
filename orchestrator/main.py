"""
Lane F — Orchestrator Main Process Loop
=======================================

Stateless recovery & escalation daemon running reaper sweeps and failure classification.
Zero leader election — runs N identical instances safely with SKIP LOCKED.
"""

from __future__ import annotations

import logging
import signal
import sys
import time

from common.config import REAPER_INTERVAL_SECONDS
from orchestrator.classify import process_failures
from orchestrator.reaper import sweep_expired_leases

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Orchestrator] %(message)s",
)
logger = logging.getLogger("Orchestrator")


def run_orchestrator_loop(interval_seconds: float = REAPER_INTERVAL_SECONDS, once: bool = False) -> None:
    """Continuous orchestrator loop."""
    logger.info("Starting Orchestrator daemon...")
    running = True

    def _handle_signal(signum, frame):
        nonlocal running
        logger.info("Received termination signal. Shutting down Orchestrator gracefully...")
        running = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while running:
        try:
            # 1. Sweep expired leases (Recovery path)
            reclaimed = sweep_expired_leases()

            # 2. Process failed tasks (Escalation / DLQ path)
            processed = process_failures()

        except Exception as exc:
            logger.error(f"Error in orchestrator loop: {exc}", exc_info=True)

        if once:
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_orchestrator_loop()
