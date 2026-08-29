from __future__ import annotations

from db.pool import RuntimeDB


def reap_expired_leases(db: RuntimeDB, batch_size: int = 100, jitter_seconds: float = 5.0):
    return db.reap_expired(batch_size=batch_size, jitter_seconds=jitter_seconds)
