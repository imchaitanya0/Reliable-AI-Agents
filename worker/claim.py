from __future__ import annotations

from db.pool import RuntimeDB


def claim_next(db: RuntimeDB, pool_tier: str, worker_id: str, lease_ttl: int = 30):
    return db.claim_task(pool_tier=pool_tier, worker_id=worker_id, lease_ttl=lease_ttl)
