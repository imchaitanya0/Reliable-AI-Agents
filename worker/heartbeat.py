"""
Lease renewal (Lane C).
A worker renews every LEASE_TTL/3; the reaper reclaims anything whose lease_expires has passed.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Generator

from common.config import HEARTBEAT_INTERVAL, LEASE_TTL_SECONDS
from db.pool import pool

log = logging.getLogger(__name__)

RENEW_SQL = """
UPDATE task_instances
SET lease_expires = now() + make_interval(secs => %(ttl)s),
    updated_at    = now()
WHERE id          = %(task_id)s
  AND (lease_owner = %(worker_id)s OR %(worker_id)s IS NULL)
  AND status      = 'running'
"""


class Heartbeat:
    """
    Renews the lease on a background thread until stopped.
    """

    def __init__(self, task_id: str, worker_id: str | None = None, ttl_seconds: int = LEASE_TTL_SECONDS, interval: float = HEARTBEAT_INTERVAL):
        self.task_id = task_id
        self.worker_id = worker_id
        self.ttl = ttl_seconds
        self.interval = interval
        self.lost = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                with pool().connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        RENEW_SQL,
                        {
                            "ttl": self.ttl,
                            "task_id": self.task_id,
                            "worker_id": self.worker_id,
                        },
                    )
                    if cur.rowcount == 0:
                        self.lost = True
                        log.warning("lease lost task=%s worker=%s -- abandoning", self.task_id, self.worker_id)
                        return
            except Exception:
                log.exception("heartbeat renewal failed task=%s", self.task_id)

    def start(self) -> "Heartbeat":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def __enter__(self) -> "Heartbeat":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


@contextmanager
def task_heartbeat(task_id: str, ttl_seconds: int = LEASE_TTL_SECONDS, interval_seconds: float = HEARTBEAT_INTERVAL) -> Generator[Heartbeat, None, None]:
    hb = Heartbeat(task_id=task_id, worker_id=None, ttl_seconds=ttl_seconds, interval=interval_seconds)
    hb.start()
    try:
        yield hb
    finally:
        hb.stop()
