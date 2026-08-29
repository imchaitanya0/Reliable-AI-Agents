"""
Postgres Connection Pool & Transaction Management.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from common.config import DATABASE_URL

_pool: ConnectionPool | None = None


def get_pool(min_size: int = 2, max_size: int = 20) -> ConnectionPool:
    """Return the global connection pool singleton."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=True,
        )
    return _pool


def close_pool() -> None:
    """Close the global connection pool."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def get_conn() -> Generator[psycopg.Connection, None, None]:
    """Provide a connection from the pool."""
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


@contextmanager
def get_transaction() -> Generator[psycopg.Connection, None, None]:
    """Provide a connection with transaction management (commits on exit, rolls back on exception)."""
    pool = get_pool()
    with pool.connection() as conn:
        # Turn off autocommit for explicit transaction block
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = True
