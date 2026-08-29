"""
The metrics snapshot -- contract C4.

Every number here is derived from the `attempts` table, which is why that table
is described as evidence rather than as a log. The API's GET /metrics should
return exactly snapshot(); the dashboard and the demo CLI read the same shape.
"""

from __future__ import annotations

from typing import Any

from common.config import LEASE_TTL_SECONDS
from common.tiers import all_tiers, base_tier, top_tier
from db.pool import pool

SQL = {
    "agents": """
        SELECT status, count(*) AS n FROM agents GROUP BY status
    """,
    "tasks": """
        SELECT status, count(*) AS n FROM task_instances GROUP BY status
    """,
    "by_tier": """
        SELECT tier, outcome, count(*) AS n, coalesce(sum(cost_units),0) AS cost
        FROM attempts GROUP BY tier, outcome
    """,
    "promotions": """
        SELECT count(*) AS n FROM task_instances WHERE tier <> %(base)s
    """,
    "reclaimed": """
        SELECT count(*) AS n FROM attempts WHERE outcome = 'reclaimed'
    """,
    "cost": "SELECT coalesce(sum(cost_units),0) AS c FROM agents",
    # Split by state: a settled id is an action that completed exactly once; an
    # unsettled one older than a lease is an action whose outcome is unknown --
    # i.e. a duplicate the ledger prevented. Counting them together would hide
    # the number that actually demonstrates 5.3.
    "dupes": """
        SELECT count(*) FILTER (WHERE state = 'done') AS settled,
               count(*) FILTER (
                   WHERE state = 'in_flight'
                     AND created_at < now() - make_interval(secs => %(ttl)s)
               ) AS prevented
        FROM idempotency
    """,
    "dlq": "SELECT count(*) AS n FROM dlq",
    "totals": "SELECT count(*) AS n FROM task_instances",
    # 5.1: agents whose cursor points at a task that does not exist. Must be 0 --
    # a non-zero reading is work that can never be picked up by anything.
    "stalled": """
        SELECT count(*) AS n FROM agents a
        WHERE a.status = 'running'
          AND a.cursor < coalesce(array_length(a.plan, 1), 0)
          AND NOT EXISTS (
              SELECT 1 FROM task_instances t
              WHERE t.agent_id = a.id AND t.seq = a.cursor
          )
    """,
    # 5.2: expired leases the reaper has not yet reclaimed -- its backlog.
    "orphaned": """
        SELECT count(*) AS n FROM task_instances
        WHERE status = 'running' AND lease_expires < now()
    """,
    "latency": """
        SELECT
          coalesce(percentile_disc(0.5) WITHIN GROUP (
            ORDER BY extract(epoch FROM (ended_at - started_at))), 0) AS p50,
          coalesce(percentile_disc(0.99) WITHIN GROUP (
            ORDER BY extract(epoch FROM (ended_at - started_at))), 0) AS p99
        FROM attempts WHERE ended_at IS NOT NULL
    """,
}


def _rows(cur, key: str, params: dict | None = None) -> list[dict]:
    cur.execute(SQL[key], params or {})
    return cur.fetchall()


def snapshot() -> dict[str, Any]:
    with pool().connection() as conn, conn.cursor() as cur:
        agents = {r["status"]: r["n"] for r in _rows(cur, "agents")}
        tasks = {r["status"]: r["n"] for r in _rows(cur, "tasks")}
        tier_rows = _rows(cur, "by_tier")
        promoted = _rows(cur, "promotions", {"base": base_tier()})[0]["n"]
        reclaimed = _rows(cur, "reclaimed")[0]["n"]
        spent = _rows(cur, "cost")[0]["c"]
        ledger_row = _rows(cur, "dupes", {"ttl": LEASE_TTL_SECONDS})[0]
        dead = _rows(cur, "dlq")[0]["n"]
        total_tasks = _rows(cur, "totals")[0]["n"]
        stalled = _rows(cur, "stalled")[0]["n"]
        orphaned = _rows(cur, "orphaned")[0]["n"]
        lat = _rows(cur, "latency")[0]

    ladder = all_tiers()
    cheap = next(t for t in ladder if t["name"] == base_tier())
    dear = next(t for t in ladder if t["name"] == top_tier())

    succeeded = sum(r["n"] for r in tier_rows if r["outcome"] == "succeeded")
    senior_attempts = sum(r["n"] for r in tier_rows if r["tier"] != base_tier())
    senior_ok = sum(
        r["n"] for r in tier_rows
        if r["tier"] != base_tier() and r["outcome"] == "succeeded"
    )

    # The three-way comparison. Baselines are what the SAME completed work would
    # have cost had every task run on one tier throughout.
    all_cheap = succeeded * int(cheap["cost_units"])
    all_dear = succeeded * int(dear["cost_units"])

    return {
        "agents": {
            "running": agents.get("running", 0),
            "completed": agents.get("completed", 0),
            "failed": agents.get("failed", 0),
        },
        "tasks": {
            "pending": tasks.get("pending", 0),
            "running": tasks.get("running", 0),
            "succeeded": tasks.get("succeeded", 0),
            "failed": tasks.get("failed", 0),
            "dead": tasks.get("dead", 0),
            "total": total_tasks,
        },
        "recovery": {
            "leases_reclaimed": reclaimed,
            "tasks_reexecuted": reclaimed,
            # Both must read 0 in a healthy runtime. `stalled_agents` is the
            # 5.1 invariant -- work that exists but nothing can ever claim --
            # and `orphaned_leases` is the reaper's outstanding backlog.
            "stalled_agents": stalled,
            "orphaned_leases": orphaned,
        },
        "escalation": {
            "promoted": promoted,
            "promotion_rate": round(promoted / total_tasks, 4) if total_tasks else 0.0,
            "senior_attempts": senior_attempts,
            "senior_success_rate": round(senior_ok / senior_attempts, 3)
            if senior_attempts else 0.0,
        },
        "cost": {
            "units_spent": spent,
            "all_junior_baseline": all_cheap,
            # Honest caveat: the cheap baseline is not actually achievable. Every
            # task that had to be promoted would never have completed on the base
            # tier, so all-junior is cheaper AND broken. Quoting its cost without
            # this number would be misleading.
            "all_junior_would_never_finish": promoted,
            "all_senior_baseline": all_dear,
            "vs_all_senior": round(spent / all_dear, 3) if all_dear else 0.0,
        },
        "idempotency": {
            "actions_guarded": ledger_row["settled"],
            # Actions whose outcome was unknown after a crash and which were
            # therefore NOT performed a second time.
            "duplicates_prevented": ledger_row["prevented"],
        },
        "dlq": {"size": dead},
        "latency": {"p50_s": round(float(lat["p50"]), 3),
                    "p99_s": round(float(lat["p99"]), 3)},
    }
