"""
Standalone worker process driven by the fixture registry.

Used by the crash-recovery test so it can SIGKILL a REAL process rather than
simulating one. Identical to `python -m worker.main` except that it supplies the
test registry instead of tasks/registry.py.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.conftest import REGISTRY  # noqa: E402
from worker.main import process_one  # noqa: E402

_stop = False


def _sigterm(signum, frame):
    global _stop
    _stop = True


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    signal.signal(signal.SIGTERM, _sigterm)
    tier = os.environ.get("POOL_TIER", "junior")
    worker_id = os.environ.get("WORKER_ID", "worker")
    while not _stop:
        try:
            if not process_one(tier, worker_id, REGISTRY):
                time.sleep(0.2)
        except Exception:
            logging.exception("loop error")
            time.sleep(0.3)
    return 0


if __name__ == "__main__":
    sys.exit(main())
