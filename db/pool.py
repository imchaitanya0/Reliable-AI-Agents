"""Connection pool. Shared plumbing -- Lane 0."""

from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common.config import DATABASE_URL

_pool: ConnectionPool | None = None


def pool() -> ConnectionPool:
    """Process-wide lazy pool."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            DATABASE_URL,
            min_size=1,
            max_size=10,
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
