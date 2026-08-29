from __future__ import annotations

import os
from typing import Any

from db.pool import open_runtime_db

try:
    from fastapi import FastAPI, HTTPException
except ImportError:  # pragma: no cover
    FastAPI = None
    HTTPException = Exception


db = open_runtime_db(os.getenv("RUNTIME_DB", "runtime.sqlite3"))
app = FastAPI(title="Reliable AI Agents") if FastAPI else None


if app:

    @app.post("/agents")
    def create_agents(payload: dict[str, Any]) -> dict[str, Any]:
        plan = payload.get("plan")
        if not isinstance(plan, list) or not plan:
            raise HTTPException(status_code=400, detail="plan must be a non-empty list")
        count = int(payload.get("count", 1))
        query = payload.get("query")
        ids = [db.create_agent(plan, query_text=query) for _ in range(count)]
        return {"agent_ids": ids}

    @app.get("/agents/{agent_id}")
    def get_agent(agent_id: str) -> dict[str, Any]:
        agent = db.get_agent(agent_id)
        if agent is None:
            raise HTTPException(status_code=404, detail="agent not found")
        return agent

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return db.metrics()

    @app.get("/dlq")
    def dlq() -> list[dict[str, Any]]:
        return [dict(row) for row in db.conn.execute("SELECT * FROM dlq ORDER BY created_at DESC").fetchall()]

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
