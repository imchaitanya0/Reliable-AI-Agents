from __future__ import annotations

import os
import time
from typing import Any, Optional

from db.pool import open_runtime_db, utc_now
from orchestrator.main import run_once as orchestrator_step
from tasks.registry import TASK_DEFS
from worker.main import run_once as worker_step

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse
except ImportError:  # pragma: no cover
    FastAPI = None
    HTTPException = Exception
    HTMLResponse = None


db = open_runtime_db(os.getenv("RUNTIME_DB", "runtime.sqlite3"))
app = FastAPI(title="Reliable AI Agents") if FastAPI else None


if app:

    @app.get("/", response_class=HTMLResponse)
    def home() -> str:
        return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reliable AI Agents</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; background: #f7f8fb; color: #172033; }
    header { background: #172033; color: white; padding: 18px 24px; }
    main { padding: 20px; display: grid; gap: 16px; }
    section { background: white; border: 1px solid #dde2eb; border-radius: 8px; padding: 16px; }
    button, input { font-size: 14px; padding: 9px 11px; margin: 4px; }
    button { background: #2264d1; color: white; border: 0; border-radius: 6px; cursor: pointer; }
    button.secondary { background: #526071; }
    button.danger { background: #b42318; }
    pre { background: #0f172a; color: #d8e2ff; padding: 14px; border-radius: 8px; overflow: auto; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }
    .metric { border: 1px solid #e3e8f0; border-radius: 6px; padding: 12px; }
    .metric b { display: block; font-size: 22px; margin-top: 6px; }
    table { width: 100%; border-collapse: collapse; }
    td, th { padding: 8px; border-bottom: 1px solid #e3e8f0; text-align: left; }
  </style>
</head>
<body>
  <header>
    <h1>Reliable AI Agents Runtime</h1>
    <div>Submit agents, run workers, watch retries, promotion, fencing, idempotency, and cost metrics.</div>
  </header>
  <main>
    <section>
      <h2>Create Work</h2>
      <input id="query" value="Fix payment outage" size="36" />
      <input id="plan" value="1,2,6,8,9" size="14" />
      <input id="count" value="1" size="4" />
      <button onclick="createAgents()">Create Agent</button>
      <button class="secondary" onclick="createDuplicate()">Create Duplicate Query</button>
      <button class="danger" onclick="resetRuntime()">Reset Demo Data</button>
    </section>
    <section>
      <h2>Run Runtime Steps</h2>
      <button onclick="step('junior')">Run Junior Worker</button>
      <button onclick="step('senior')">Run Senior Worker</button>
      <button onclick="step('both')">Run Both Workers</button>
      <button class="secondary" onclick="orchestrate()">Run Orchestrator</button>
      <button class="secondary" onclick="step('both', 20)">Run 20 Steps</button>
    </section>
    <section>
      <h2>Prove Failure Guarantees</h2>
      <button onclick="prove('zombie')">Prove Zombie Fencing</button>
      <button onclick="prove('idempotency')">Prove Idempotency</button>
      <button onclick="prove('reaper')">Prove Batched Reaper</button>
    </section>
    <section>
      <h2>Metrics</h2>
      <div id="metrics" class="grid"></div>
    </section>
    <section>
      <h2>Agents</h2>
      <div id="agents"></div>
    </section>
    <section>
      <h2>Recent Attempts</h2>
      <div id="attempts"></div>
    </section>
    <section>
      <h2>Raw State</h2>
      <pre id="raw">Loading...</pre>
    </section>
  </main>
  <script>
    async function post(url, body) {
      const res = await fetch(url, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(body || {})});
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    }
    async function get(url) {
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    }
    async function createAgents() {
      const plan = document.getElementById('plan').value.split(',').map(x => Number(x.trim())).filter(Boolean);
      const count = Number(document.getElementById('count').value || 1);
      const query = document.getElementById('query').value;
      await post('/agents', {plan, count, query});
      await refresh();
    }
    async function createDuplicate() {
      await createAgents();
      await createAgents();
    }
    async function step(tier, count) {
      await post('/run/step', {tier, count: count || 1, fast_forward_backoff: true});
      await refresh();
    }
    async function orchestrate() {
      await post('/run/orchestrator', {});
      await refresh();
    }
    async function resetRuntime() {
      await post('/reset', {});
      await refresh();
    }
    async function prove(name) {
      const result = await post(`/chaos/prove/${name}`, {});
      document.getElementById('raw').textContent = JSON.stringify(result, null, 2);
      await refresh();
    }
    function metric(label, value) {
      return `<div class="metric">${label}<b>${value}</b></div>`;
    }
    function renderAgents(agents) {
      if (!agents.length) return 'No agents yet.';
      return `<table><tr><th>ID</th><th>Status</th><th>Cursor</th><th>Plan</th><th>Cost</th></tr>${agents.map(a =>
        `<tr><td>${a.id.slice(0,8)}</td><td>${a.status}</td><td>${a.cursor}</td><td>${a.plan.join(',')}</td><td>${a.cost_units}</td></tr>`
      ).join('')}</table>`;
    }
    function renderAttempts(attempts) {
      if (!attempts.length) return 'No attempts yet.';
      return `<table><tr><th>Task</th><th>Seq</th><th>Attempt</th><th>Tier</th><th>Outcome</th><th>Failure</th></tr>${attempts.map(a =>
        `<tr><td>${a.task_def_id}</td><td>${a.seq}</td><td>${a.attempt_no}</td><td>${a.tier}</td><td>${a.outcome}</td><td>${a.failure_class || ''}</td></tr>`
      ).join('')}</table>`;
    }
    async function refresh() {
      const [metrics, agents, attempts] = await Promise.all([get('/metrics'), get('/agents'), get('/attempts')]);
      document.getElementById('metrics').innerHTML = [
        metric('Escalation Rate', `${(metrics.escalation_rate * 100).toFixed(1)}%`),
        metric('Completed Tasks', metrics.total_tasks_completed),
        metric('Senior Completed', metrics.senior_tasks_completed),
        metric('Zombie Writes Blocked', metrics.zombie_writes_blocked),
        metric('Duplicate Actions Blocked', metrics.duplicate_actions_blocked),
        metric('Semantic Dedupes', metrics.tasks_deduplicated),
        metric('Tiered Cost', metrics.cost_comparison.tiered),
        metric('All Senior Cost', metrics.cost_comparison.all_senior)
      ].join('');
      document.getElementById('agents').innerHTML = renderAgents(agents);
      document.getElementById('attempts').innerHTML = renderAttempts(attempts);
      document.getElementById('raw').textContent = JSON.stringify({metrics, agents, attempts}, null, 2);
    }
    refresh();
    setInterval(refresh, 1500);
  </script>
</body>
</html>
"""

    @app.post("/agents")
    def create_agents(payload: dict[str, Any]) -> dict[str, Any]:
        plan = payload.get("plan")
        if not isinstance(plan, list) or not plan:
            raise HTTPException(status_code=400, detail="plan must be a non-empty list")
        count = int(payload.get("count", 1))
        query = payload.get("query")
        ids = [db.create_agent(plan, query_text=query) for _ in range(count)]
        return {"agent_ids": ids}

    @app.get("/agents")
    def list_agents() -> list[dict[str, Any]]:
        return db.list_agents()

    @app.get("/agents/{agent_id}")
    def get_agent(agent_id: str) -> dict[str, Any]:
        agent = db.get_agent(agent_id)
        if agent is None:
            raise HTTPException(status_code=404, detail="agent not found")
        return agent

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return db.metrics()

    @app.get("/tasks")
    def task_defs() -> list[dict[str, Any]]:
        return [
            {
                "id": task.id,
                "name": task.name,
                "difficulty": task.difficulty,
                "side_effecting": task.side_effecting,
                "tool": task.tool,
            }
            for task in TASK_DEFS.values()
        ]

    @app.get("/attempts")
    def attempts(limit: int = 50) -> list[dict[str, Any]]:
        return db.recent_attempts(limit=limit)

    @app.get("/dlq")
    def dlq() -> list[dict[str, Any]]:
        return [dict(row) for row in db.conn.execute("SELECT * FROM dlq ORDER BY created_at DESC").fetchall()]

    @app.get("/config")
    def config() -> dict[str, Any]:
        keys = {"retries_enabled", "escalation_enabled", "force_tier", "lease_ttl_seconds", "tool_overrides"}
        return {key: db.get_config(key) for key in keys}

    @app.post("/run/step")
    def run_step(payload: dict[str, Any]) -> dict[str, Any]:
        tier = payload.get("tier", "both")
        count = int(payload.get("count", 1))
        fast_forward = bool(payload.get("fast_forward_backoff", False))
        if tier not in {"junior", "senior", "both"}:
            raise HTTPException(status_code=400, detail="tier must be junior, senior, or both")
        results = []
        for i in range(count):
            did_junior = worker_step(db, "junior", f"api-junior-{i}") if tier in {"junior", "both"} else False
            did_senior = worker_step(db, "senior", f"api-senior-{i}") if tier in {"senior", "both"} else False
            routed = orchestrator_step(db)
            if fast_forward:
                db.conn.execute("UPDATE task_instances SET next_run_at=0 WHERE status='pending'")
            results.append({"junior_did_work": did_junior, "senior_did_work": did_senior, **routed})
        return {"steps": results, "metrics": db.metrics()}

    @app.post("/run/orchestrator")
    def run_orchestrator() -> dict[str, Any]:
        return {"orchestrator": orchestrator_step(db), "metrics": db.metrics()}

    @app.post("/reset")
    def reset() -> dict[str, Any]:
        db.reset_runtime()
        return {"ok": True, "metrics": db.metrics()}

    @app.post("/chaos/tool")
    def chaos_tool(payload: dict[str, Any]) -> dict[str, Any]:
        name = payload["name"]
        overrides = db.get_config("tool_overrides") or {}
        overrides[name] = {k: v for k, v in payload.items() if k != "name"}
        db.set_config("tool_overrides", overrides)
        return {"tool_overrides": overrides}

    @app.post("/chaos/config")
    def chaos_config(payload: dict[str, Any]) -> dict[str, Any]:
        allowed = {"retries_enabled", "escalation_enabled", "force_tier", "lease_ttl_seconds"}
        for key, value in payload.items():
            if key in allowed:
                db.set_config(key, value)
        return {key: db.get_config(key) for key in allowed}

    @app.post("/chaos/prove/zombie")
    def prove_zombie() -> dict[str, Any]:
        agent_id = db.create_agent([1])
        stale = db.claim_task("junior", "chaos-stale-worker", lease_ttl=1)
        if stale is None:
            raise HTTPException(status_code=409, detail="no task was claimable for zombie proof")
        db.conn.execute("UPDATE task_instances SET lease_expires=? WHERE id=?", (utc_now() - 1, stale["id"]))
        reclaimed = db.reap_expired(batch_size=100, jitter_seconds=0)
        fresh = db.claim_task("junior", "chaos-fresh-worker", lease_ttl=30)
        if fresh is None:
            raise HTTPException(status_code=409, detail="reclaimed task was not claimable")
        fresh_write = db.complete_task(fresh, "chaos-fresh-worker", {"winner": "fresh"})
        stale_write = db.complete_task(stale, "chaos-stale-worker", {"winner": "stale"})
        return {
            "agent_id": agent_id,
            "expired_leases_reclaimed": len(reclaimed),
            "fresh_worker_write_committed": fresh_write,
            "stale_zombie_write_committed": stale_write,
            "agent": db.get_agent(agent_id),
            "metrics": db.metrics(),
        }

    @app.post("/chaos/prove/idempotency")
    def prove_idempotency() -> dict[str, Any]:
        agent_id = db.create_agent([8], query_text=f"idempotency proof {time.time()}")
        key = f"proof:{agent_id}:jira"
        state, _ = db.reserve_idempotency(key, agent_id, 0, "jira")
        fired = {"count": 0}

        def fire() -> dict[str, Any]:
            fired["count"] += 1
            return {"ticket": "JIRA-PROOF"}

        duplicate_state, duplicate_result = db.run_idempotent(key, agent_id, 0, "jira", fire)
        return {
            "agent_id": agent_id,
            "first_reservation": state,
            "duplicate_state_seen": duplicate_state,
            "duplicate_result": duplicate_result,
            "actual_tool_fires_on_duplicate": fired["count"],
            "metrics": db.metrics(),
        }

    @app.post("/chaos/prove/reaper")
    def prove_reaper(payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        payload = payload or {}
        count = int(payload.get("count", 500))
        batch_size = int(payload.get("batch_size", 100))
        task_ids = []
        for i in range(count):
            db.create_agent([1])
            task = db.claim_task("junior", f"chaos-reaper-{i}", lease_ttl=1)
            if task is not None:
                task_ids.append(task["id"])
                db.conn.execute("UPDATE task_instances SET lease_expires=? WHERE id=?", (utc_now() - 1, task["id"]))
        started = time.time()
        reclaimed = 0
        while reclaimed < len(task_ids):
            batch = db.reap_expired(batch_size=batch_size, jitter_seconds=5)
            if not batch:
                break
            reclaimed += len(batch)
        elapsed = time.time() - started
        spread_row = db.conn.execute(
            "SELECT MAX(next_run_at)-MIN(next_run_at) AS spread FROM task_instances WHERE id IN (%s)"
            % ",".join("?" for _ in task_ids),
            task_ids,
        ).fetchone() if task_ids else {"spread": 0}
        return {
            "created_expired_leases": len(task_ids),
            "reclaimed": reclaimed,
            "batch_size": batch_size,
            "elapsed_seconds": elapsed,
            "next_run_at_spread_seconds": spread_row["spread"] or 0,
            "metrics": db.metrics(),
        }
