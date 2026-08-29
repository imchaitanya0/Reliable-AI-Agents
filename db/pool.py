"""Connection pool. Shared plumbing -- Lane 0."""

from __future__ import annotations

import os
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common.config import DATABASE_URL

# Postgres connections are a GLOBAL budget, not a per-process one. Every replica
# of every service draws on the same `max_connections`, so a pool size that is
# harmless at one instance becomes an outage at fifty: the database starts
# refusing connections, and it refuses them to the orchestrators that would have
# recovered the situation.
#
# Sized per role. An orchestrator runs its sweeps sequentially and never needs
# more than one connection at a time, so it sets DB_POOL_MAX=2 and can be scaled
# hard. A worker holds one for the length of a task.
POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

_pool: ConnectionPool | None = None


def pool() -> ConnectionPool:
    """Process-wide lazy pool."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            DATABASE_URL,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def fetchone(sql: str, params: dict | tuple | None = None) -> dict[str, Any] | None:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetchall(sql: str, params: dict | tuple | None = None) -> list[dict[str, Any]]:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def execute(sql: str, params: dict | tuple | None = None) -> int:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount
