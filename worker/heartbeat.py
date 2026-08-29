from __future__ import annotations

import threading
import time

from db.pool import RuntimeDB


class Heartbeat:
    def __init__(self, db: RuntimeDB, task_id: str, worker_id: str, attempt: int, lease_ttl: int) -> None:
        self.db = db
        self.task_id = task_id
        self.worker_id = worker_id
        self.attempt = attempt
        self.lease_ttl = lease_ttl
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "Heartbeat":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._stop.set()
        self._thread.join(timeout=1)

    def _run(self) -> None:
        interval = max(1, self.lease_ttl / 3)
        while not self._stop.wait(interval):
            if not self.db.heartbeat(self.task_id, self.worker_id, self.attempt, self.lease_ttl):
                self._stop.set()
