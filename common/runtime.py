"""
Runtime flags -- the chaos and benchmark switches, read from the database.

Separate from `common/config.py` for two reasons. One is mechanical: config.py
is imported by db/pool.py, so it cannot import the pool back without a cycle.
The other is conceptual. Environment settings are process-shaped and fixed at
startup; these are runtime-shaped, shared by every process, and changed WHILE
THE DEMO IS RUNNING. A metric only persuades next to its control, so the ability
to flip `escalation_enabled` live and re-run the same workload is the point.

Cached briefly. Without a cache every orchestrator tick pays an extra round
trip; with an unbounded one the /chaos endpoints stop taking effect and the
live benchmark silently stops being live.
"""

from __future__ import annotations

import time
from typing import Any

from db.pool import pool

CACHE_SECONDS = 2.0

DEFAULTS: dict[str, Any] = {
    # False => a failed task is terminal. This is the control for
    # "failure rate with retries vs without".
    "retries_enabled": True,
    # False => capability failures dead-letter instead of promoting. This is the
    # all-junior baseline: cheaper, and it leaves work unfinished.
    "escalation_enabled": True,
    # Pin every task to one tier. Set to the top tier for the all-senior
    # baseline: everything completes, at maximum cost.
    "force_tier": None,
    "lease_ttl_seconds": 30,
    "tool_overrides": {},
}

_cache: dict[str, Any] = {}
_cached_at: float = 0.0


def flags(fresh: bool = False) -> dict[str, Any]:
    """Every runtime flag, merged over DEFAULTS."""
    global _cache, _cached_at

    now = time.monotonic()
    if not fresh and _cache and (now - _cached_at) < CACHE_SECONDS:
        return _cache

    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT key, value FROM runtime_config")
        rows = {r["key"]: r["value"] for r in cur.fetchall()}

    _cache = {**DEFAULTS, **rows}
    _cached_at = now
    return _cache


def retries_enabled() -> bool:
    return bool(flags().get("retries_enabled", True))


def escalation_enabled() -> bool:
    return bool(flags().get("escalation_enabled", True))


def force_tier() -> str | None:
    """The tier every task is pinned to, or None for normal tiered operation."""
    value = flags().get("force_tier")
    return str(value) if value else None


def invalidate() -> None:
    """Drop the cache. Tests flip flags between assertions."""
    global _cache, _cached_at
    _cache = {}
    _cached_at = 0.0
