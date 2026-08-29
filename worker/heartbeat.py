"""
Lease renewal.

A worker must keep saying "still alive" or the reaper takes its work away. This
is the only failure detector in the system: you cannot distinguish a crashed
worker from a slow one, so the runtime picks a timeout, accepts that it will
sometimes reclaim from a worker that is merely slow, and defends against the
consequence with lease fencing and idempotency keys.
"""

from __future__ import annotations

import logging
import threading

from db.pool import pool

log = logging.getLogger(__name__)

RENEW_SQL = """
UPDATE task_instances
SET lease_expires = now() + make_interval(secs => %(ttl)s),
    updated_at    = now()
WHERE id          = %(task_id)s
  AND lease_owner = %(worker_id)s
  AND status      = 'running'
"""


class Heartbeat:
    """
    Renews the lease on a background thread until stopped.

    If a renewal ever updates zero rows, this worker no longer owns the task --
    the reaper reclaimed it while we were slow, and someone else may already be
    running it. `lost` goes True and the worker MUST abandon its checkpoint.
    Committing anyway would double-advance the agent's cursor.
    """

    def __init__(self, task_id: str, worker_id: str, ttl_seconds: int, interval: float):
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
                        log.warning(
                            "lease lost task=%s worker=%s -- abandoning",
                            self.task_id,
                            self.worker_id,
                        )
                        return
            except Exception:  # pragma: no cover - transient DB blip
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
