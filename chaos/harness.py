#!/usr/bin/env python3
"""
Control plane for demos, fault injection and pipeline composition.

PIPELINES -- compose your own workflow at runtime
    python -m chaos.harness pipelines
    python -m chaos.harness pipeline create my-flow 1,10,21,40 "whatever I need"
    python -m chaos.harness seed 20 --pipeline deep-investigation
    python -m chaos.harness seed 5  --plan 1,13,23,40

WORKER POOLS -- spawn as many of each tier as you want
    python -m chaos.harness workers junior=5 senior=2
    python -m chaos.harness workers junior=10 senior=3 --orchestrators 2

TOOLS -- switch between mock and real, break them on purpose
    python -m chaos.harness tools
    python -m chaos.harness mode live            # all tools real
    python -m chaos.harness mode mock github     # just this one back to mock
    python -m chaos.harness tool jira 1.0        # 100% failure rate

STATUS
    python -m chaos.harness status
    python -m chaos.harness watch
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time

from common.metrics import snapshot
from db.pool import pool
from tasks import tools
from tasks.registry import DEFAULT_PIPELINES, TASK_DEFS

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(REPO, ".venv", "bin", "python")
if not os.path.exists(PY):
    PY = sys.executable


# --- pipelines ---------------------------------------------------------------

def ensure_default_pipelines() -> None:
    with pool().connection() as conn, conn.cursor() as cur:
        for name, (plan, desc) in DEFAULT_PIPELINES.items():
            cur.execute(
                """INSERT INTO pipelines (name, plan, description)
                   VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING""",
                (name, plan, desc),
            )


def list_pipelines() -> list[dict]:
    ensure_default_pipelines()
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT name, plan, description FROM pipelines ORDER BY name")
        return cur.fetchall()


def create_pipeline(name: str, plan: list[int], description: str = "") -> None:
    unknown = [t for t in plan if t not in TASK_DEFS]
    if unknown:
        raise SystemExit(
            f"unknown task ids {unknown}. Known: {sorted(TASK_DEFS)}"
        )
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO pipelines (name, plan, description) VALUES (%s,%s,%s)
               ON CONFLICT (name) DO UPDATE
               SET plan = EXCLUDED.plan, description = EXCLUDED.description""",
            (name, plan, description),
        )


def resolve_plan(pipeline: str | None, plan: str | None) -> tuple[list[int], str]:
    if plan:
        ids = [int(x) for x in plan.replace(" ", "").split(",") if x]
        unknown = [t for t in ids if t not in TASK_DEFS]
        if unknown:
            raise SystemExit(f"unknown task ids {unknown}")
        return ids, "(inline)"
    name = pipeline or "investigation"
    ensure_default_pipelines()
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT plan FROM pipelines WHERE name = %s", (name,))
        row = cur.fetchone()
    if row is None:
        raise SystemExit(
            f"no pipeline {name!r}. Try: "
            f"{[p['name'] for p in list_pipelines()]}"
        )
    return row["plan"], name


# --- seeding -----------------------------------------------------------------

def seed(n: int, plan: list[int]) -> list[str]:
    """
    Create agents and ALL their task rows in one transaction, at the base tier.

    This is exactly what POST /agents must do. Nothing runs yet -- workers
    discover the work by polling, and `t.seq = a.cursor` gates the order.
    """
    ids = []
    with pool().connection() as conn, conn.cursor() as cur:
        for _ in range(n):
            cur.execute(
                "INSERT INTO agents (plan, status) VALUES (%s,'running') RETURNING id",
                (plan,),
            )
            aid = cur.fetchone()["id"]
            for seq, tdid in enumerate(plan):
                cur.execute(
                    "INSERT INTO task_instances (agent_id, seq, task_def_id) "
                    "VALUES (%s,%s,%s)",
                    (aid, seq, tdid),
                )
            ids.append(str(aid))
    return ids


# --- worker pools ------------------------------------------------------------

def spawn_pools(spec: dict[str, int], orchestrators: int = 1) -> list:
    """Start worker processes per tier plus N orchestrators. Ctrl-C stops all."""
    procs = []
    for i in range(orchestrators):
        procs.append(subprocess.Popen(
            [PY, "-m", "orchestrator.main"],
            cwd=REPO,
            env={**os.environ, "PYTHONPATH": REPO, "ORCHESTRATOR_ID": f"orch-{i+1}"},
        ))
    for tier, count in spec.items():
        for i in range(count):
            procs.append(subprocess.Popen(
                [PY, "-m", "worker.main"],
                cwd=REPO,
                env={**os.environ, "PYTHONPATH": REPO,
                     "POOL_TIER": tier, "WORKER_ID": f"{tier}-{i+1}"},
            ))
    return procs


# --- rendering ---------------------------------------------------------------

def render(m: dict) -> str:
    a, t, c, e = m["agents"], m["tasks"], m["cost"], m["escalation"]
    bar = "=" * 60
    return f"""
{bar}
 RELIABLE AI AGENTS
{bar}
 agents     running {a['running']:<5} completed {a['completed']:<5} failed {a['failed']:<5}
 tasks      pending {t['pending']:<5} running   {t['running']:<5} done   {t['succeeded']:<5}
            failed  {t['failed']:<5} dead      {t['dead']:<5}
{'-' * 60}
 RECOVERY   leases reclaimed          {m['recovery']['leases_reclaimed']}
            tasks re-executed         {m['recovery']['tasks_reexecuted']}
{'-' * 60}
 ESCALATION promoted to senior        {e['promoted']}
            promotion rate            {e['promotion_rate'] * 100:.1f}%
            senior success rate       {e['senior_success_rate'] * 100:.1f}%
{'-' * 60}
 COST       ours                      {c['units_spent']:>7} units
            all-junior baseline       {c['all_junior_baseline']:>7} units  ({c['all_junior_would_never_finish']} tasks never finish)
            all-senior baseline       {c['all_senior_baseline']:>7} units
            ours / all-senior         {c['vs_all_senior']:>7.2f}x
{'-' * 60}
 GUARDS     idempotent actions        {m['idempotency']['actions_guarded']}
            dead letter queue         {m['dlq']['size']}
 LATENCY    p50 {m['latency']['p50_s']}s   p99 {m['latency']['p99_s']}s
{bar}"""


# --- cli ---------------------------------------------------------------------

def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="chaos.harness", add_help=True)
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("seed", help="create agents")
    p.add_argument("count", type=int, nargs="?", default=20)
    p.add_argument("--pipeline", "-P", help="named pipeline")
    p.add_argument("--plan", help="comma-separated task ids, e.g. 1,10,21,40")

    sub.add_parser("pipelines", help="list pipelines")

    p = sub.add_parser("pipeline", help="manage pipelines")
    p.add_argument("action", choices=["create", "delete"])
    p.add_argument("name")
    p.add_argument("plan", nargs="?", help="comma-separated task ids")
    p.add_argument("description", nargs="?", default="")

    sub.add_parser("tasks", help="list every registered task")
    sub.add_parser("tools", help="list tools and their mode")

    p = sub.add_parser("mode", help="switch tools between mock and live")
    p.add_argument("mode", choices=["mock", "live"])
    p.add_argument("tool", nargs="?", help="omit to set every tool")

    p = sub.add_parser("tool", help="set a tool's failure rate")
    p.add_argument("name")
    p.add_argument("failure_rate", type=float)

    p = sub.add_parser("workers", help="spawn worker pools, e.g. junior=5 senior=2")
    p.add_argument("spec", nargs="+")
    p.add_argument("--orchestrators", type=int, default=1)

    sub.add_parser("status")
    sub.add_parser("json")
    sub.add_parser("watch")

    a = ap.parse_args(argv[1:])

    if a.cmd == "seed":
        plan, label = resolve_plan(a.pipeline, a.plan)
        ids = seed(a.count, plan)
        print(f"seeded {len(ids)} agents  pipeline={label}  plan={plan}")

    elif a.cmd == "pipelines":
        for r in list_pipelines():
            print(f"  {r['name']:22} {str(r['plan']):40} {r['description'] or ''}")

    elif a.cmd == "pipeline":
        if a.action == "create":
            ids = [int(x) for x in (a.plan or "").replace(" ", "").split(",") if x]
            create_pipeline(a.name, ids, a.description)
            print(f"pipeline {a.name!r} -> {ids}")
        else:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM pipelines WHERE name=%s", (a.name,))
            print(f"deleted {a.name!r}")

    elif a.cmd == "tasks":
        for tid in sorted(TASK_DEFS):
            t = TASK_DEFS[tid]
            flags = " ".join(filter(None, [
                "HARD" if t.difficulty == "hard" else "",
                "SIDE-EFFECT" if t.side_effecting else "",
                f"tool={t.tool}" if t.tool else "",
            ]))
            print(f"  {tid:3}  {t.name:24} {flags}")

    elif a.cmd == "tools":
        for name in tools.TOOLS:
            d = tools.describe(name)
            print(f"  {name:12} mode={d['mode']:5} live_available="
                  f"{'yes' if d['has_live'] else 'no ':4} "
                  f"fail={d['failure_rate']:<5} latency={d['latency_ms']}ms")

    elif a.cmd == "mode":
        tools.set_mode(a.tool, a.mode)
        print(f"{a.tool or 'all tools'} -> {a.mode}")

    elif a.cmd == "tool":
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE runtime_config SET value=jsonb_set(value,%s,%s::jsonb,true)
                   WHERE key='tool_overrides'""",
                ([a.name], json.dumps({"failure_rate": a.failure_rate})),
            )
        print(f"{a.name} failure_rate -> {a.failure_rate}")

    elif a.cmd == "workers":
        spec = {}
        for s in a.spec:
            tier, _, n = s.partition("=")
            spec[tier] = int(n or 1)
        procs = spawn_pools(spec, a.orchestrators)
        print(f"spawned {a.orchestrators} orchestrator(s) + "
              + ", ".join(f"{n} {t}" for t, n in spec.items())
              + "  (Ctrl-C to stop)")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            for pr in procs:
                pr.send_signal(signal.SIGTERM)
            for pr in procs:
                try:
                    pr.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pr.kill()
            print("\nstopped")

    elif a.cmd == "status":
        print(render(snapshot()))
    elif a.cmd == "json":
        print(json.dumps(snapshot(), indent=2))
    elif a.cmd == "watch":
        try:
            while True:
                print("\033[2J\033[H" + render(snapshot()), flush=True)
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
    else:
        ap.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
