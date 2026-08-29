"""
The escalation ladder, read from the `tiers` table.

SINGLE SOURCE OF TRUTH. Nothing else in the codebase may hardcode a tier name.
Adding a capability tier is one INSERT:

    INSERT INTO tiers VALUES ('principal', 3, 60, 8000, 4000, 0.99, NULL);

Promotion, cost accounting and pool routing all pick it up with no code change.
Start a worker with POOL_TIER=principal and it drains the new queue.
"""

from __future__ import annotations

import threading
from typing import Any

from db.pool import pool

_cache: list[dict[str, Any]] | None = None
_lock = threading.Lock()

LOAD_SQL = """
SELECT name, rank, cost_units, tokens, latency_ms, p_success, model
FROM tiers ORDER BY rank
"""


def all_tiers(refresh: bool = False) -> list[dict[str, Any]]:
    """Every tier, ascending by rank. Cached; call refresh=True after an INSERT."""
    global _cache
    with _lock:
        if _cache is None or refresh:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(LOAD_SQL)
                _cache = cur.fetchall()
        return list(_cache)


def invalidate() -> None:
    """Drop the cache. Call after inserting a tier at runtime."""
    global _cache
    with _lock:
        _cache = None


def tier(name: str) -> dict[str, Any]:
    for t in all_tiers():
        if t["name"] == name:
            return t
    # A tier that is not in the table cannot be costed or promoted from.
    raise KeyError(f"unknown tier {name!r}; known: {[t['name'] for t in all_tiers()]}")


def base_tier() -> str:
    """The cheapest tier. Every task starts here."""
    return all_tiers()[0]["name"]


def top_tier() -> str:
    """The most capable tier. A task failing here has nowhere left to go."""
    return all_tiers()[-1]["name"]


def next_tier(current: str) -> str | None:
    """
    The tier above `current`, or None if already at the top.

    This is the whole escalation ladder. Ranks come from the table, so inserting
    a tier between two existing ones works without touching this function.
    """
    ladder = all_tiers()
    for i, t in enumerate(ladder):
        if t["name"] == current:
            return ladder[i + 1]["name"] if i + 1 < len(ladder) else None
    return None


def cost_of(name: str) -> tuple[int, int]:
    """(cost_units, tokens) for one execution at this tier."""
    t = tier(name)
    return int(t["cost_units"]), int(t["tokens"])
