"""
Lane C — Background Task Lease Renewal (Heartbeat)
==================================================

Renews task leases periodically while execution is actively running.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Generator

from common.config import HEARTBEAT_INTERVAL_SECONDS, LEASE_TTL_SECONDS
from db.pool import get_conn


class HeartbeatThread(threading.Thread):
    def __init__(self, task_id: str, ttl_seconds: int = LEASE_TTL_SECONDS, interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS) -> None:
        super().__init__(daemon=True)
        self.task_id = task_id
        self.ttl_seconds = ttl_seconds
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE task_instances
                            SET lease_expires = now() + make_interval(secs => %(ttl)s),
                                updated_at = now()
                            WHERE id = %(task_id)s AND status = 'running';
                            """,
                            {"task_id": self.task_id, "ttl": self.ttl_seconds},
                        )
            except Exception:
                # If connection dropped temporarily, continue until stop event
                pass

    def stop(self) -> None:
        self._stop_event.set()


@contextmanager
def task_heartbeat(task_id: str, ttl_seconds: int = LEASE_TTL_SECONDS, interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS) -> Generator[None, None, None]:
    """Context manager to start and cleanly stop background heartbeats for a task."""
    hb = HeartbeatThread(task_id, ttl_seconds, interval_seconds)
    hb.start()
    try:
        yield
    finally:
        hb.stop()
        hb.join(timeout=2.0)
