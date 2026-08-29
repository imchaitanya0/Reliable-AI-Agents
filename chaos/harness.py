#!/usr/bin/env python3
"""
Demo + fault-injection CLI.

    python -m chaos.harness seed 20          # create 20 agents on the demo plan
    python -m chaos.harness tool jira 1.0    # take a tool to 100% failure
    python -m chaos.harness tool jira 0.1    # put it back
    python -m chaos.harness status           # one metrics snapshot
    python -m chaos.harness watch            # live, refreshes every second

Exists so the system is demoable without waiting on the API. The API's
GET /metrics should return common.metrics.snapshot() -- the same shape.
"""

from __future__ import annotations

import json
import sys
import time

from common.metrics import snapshot
from db.pool import pool
from tasks.registry import DEMO_PLAN, EASY_PLAN


def seed(n: int = 20, plan: list[int] | None = None, hard_share: float = 0.3) -> list[str]:
    """
    Create agents and ALL their task rows in one transaction, at the base tier.

    This is what POST /agents must do. Nothing runs yet -- workers discover the
    work by polling, and `t.seq = a.cursor` gates the order.

    By default 30% of agents get a plan containing a hard task. That is what
    produces a realistic ~6-7% escalation RATE across all tasks. If every agent
    escalated, the rate would be 20% and the cost thesis would not hold -- the
    rate is an output of the workload, not a number we get to assert.
    """
    ids = []
    plans = ([plan] * n) if plan else [
        DEMO_PLAN if i < int(n * hard_share) else EASY_PLAN for i in range(n)
    ]
    with pool().connection() as conn, conn.cursor() as cur:
        for p in plans:
            cur.execute(
                "INSERT INTO agents (plan, status) VALUES (%s, 'running') RETURNING id",
                (p,),
            )
            aid = cur.fetchone()["id"]
            for seq, tdid in enumerate(p):
                cur.execute(
                    """INSERT INTO task_instances (agent_id, seq, task_def_id)
                       VALUES (%s, %s, %s)""",
                    (aid, seq, tdid),
                )
            ids.append(str(aid))
    return ids


def set_tool(name: str, failure_rate: float) -> None:
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE runtime_config
               SET value = jsonb_set(value, %s, %s::jsonb, true)
               WHERE key = 'tool_overrides'""",
            ([name], json.dumps({"failure_rate": failure_rate})),
        )


def render(m: dict) -> str:
    a, t, c, e = m["agents"], m["tasks"], m["cost"], m["escalation"]
    bar = "=" * 58
    return f"""
{bar}
 RELIABLE AI AGENTS
{bar}
 agents     running {a['running']:<5} completed {a['completed']:<5} failed {a['failed']:<5}
 tasks      pending {t['pending']:<5} running   {t['running']:<5} done   {t['succeeded']:<5}
            failed  {t['failed']:<5} dead      {t['dead']:<5}
{'-' * 58}
 RECOVERY   leases reclaimed          {m['recovery']['leases_reclaimed']}
            tasks re-executed         {m['recovery']['tasks_reexecuted']}
{'-' * 58}
 ESCALATION promoted to senior        {e['promoted']}
            promotion rate            {e['promotion_rate'] * 100:.1f}%   (target ~7%)
            senior success rate       {e['senior_success_rate'] * 100:.1f}%
{'-' * 58}
 COST       ours                      {c['units_spent']:>7} units
            all-junior baseline       {c['all_junior_baseline']:>7} units  ({c['all_junior_would_never_finish']} tasks never finish)
            all-senior baseline       {c['all_senior_baseline']:>7} units
            ours / all-senior         {c['vs_all_senior']:>7.2f}x
{'-' * 58}
 GUARDS     idempotent actions        {m['idempotency']['actions_guarded']}
            dead letter queue         {m['dlq']['size']}
 LATENCY    p50 {m['latency']['p50_s']}s   p99 {m['latency']['p99_s']}s
{bar}"""


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 1
    cmd = argv[1]

    if cmd == "seed":
        n = int(argv[2]) if len(argv) > 2 else 20
        ids = seed(n)
        print(f"seeded {len(ids)} agents on plan {DEMO_PLAN}")
    elif cmd == "tool":
        set_tool(argv[2], float(argv[3]))
        print(f"{argv[2]} failure_rate -> {argv[3]}")
    elif cmd == "status":
        print(render(snapshot()))
    elif cmd == "json":
        print(json.dumps(snapshot(), indent=2))
    elif cmd == "watch":
        try:
            while True:
                print("\033[2J\033[H" + render(snapshot()), flush=True)
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
