"""Connection pool and transaction utilities (Lane 0)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common.config import DATABASE_URL

_pool: ConnectionPool | None = None


def get_pool(min_size: int = 1, max_size: int = 20) -> ConnectionPool:
    """Return the global connection pool singleton."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


def pool() -> ConnectionPool:
    """Alias for get_pool."""
    return get_pool()


def close_pool() -> None:
    """Close the global connection pool."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def get_conn() -> Generator[psycopg.Connection, None, None]:
    """Provide a connection from the pool."""
    with pool().connection() as conn:
        yield conn


@contextmanager
def get_transaction() -> Generator[psycopg.Connection, None, None]:
    """Provide a connection with transaction management (commits on exit, rolls back on exception)."""
    with pool().connection() as conn:
        with conn.transaction():
            yield conn


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
        conn.commit()
        return cur.rowcount
