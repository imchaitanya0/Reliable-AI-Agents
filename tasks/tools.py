"""
Mock tools: fixed latency plus an injectable failure rate.

Deliberately not real APIs. The demo has to be fast, reproducible and
independent of network conditions on stage -- a flaky third party during the
presentation would look like our bug.

Failure rates are read from runtime_config.tool_overrides, so the chaos harness
can take a tool to 100% failure live:

    POST /chaos/tool {"name": "jira", "failure_rate": 1.0}
"""

from __future__ import annotations

import json
import random
import threading
import time
from typing import Any

from common.failures import InfraFailure
from db.pool import pool

# Baseline behaviour. Overrides from runtime_config are layered on top.
TOOLS: dict[str, dict[str, Any]] = {
    "github": {"latency_ms": 300, "failure_rate": 0.02},
    "logs":   {"latency_ms": 800, "failure_rate": 0.05},
    "jira":   {"latency_ms": 500, "failure_rate": 0.10},
}

_overrides: dict[str, dict] = {}
_overrides_at: float = 0.0
_lock = threading.Lock()
_TTL = 1.0  # seconds; short so chaos changes land within a demo beat


def _load_overrides() -> dict[str, dict]:
    """Cached read of the chaos knobs."""
    global _overrides, _overrides_at
    with _lock:
        if time.time() - _overrides_at < _TTL:
            return _overrides
        try:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM runtime_config WHERE key = 'tool_overrides'"
                )
                row = cur.fetchone()
            val = (row or {}).get("value") or {}
            _overrides = val if isinstance(val, dict) else json.loads(val)
        except Exception:
            _overrides = {}
        _overrides_at = time.time()
        return _overrides


def tool_config(name: str) -> dict[str, Any]:
    cfg = dict(TOOLS.get(name, {"latency_ms": 200, "failure_rate": 0.0}))
    cfg.update(_load_overrides().get(name, {}))
    return cfg


def call(name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Invoke a mock tool.

    A tool failure is INFRA, never CAPABILITY: the upstream broke, which says
    nothing about whether the model was smart enough. Escalating here would
    spend senior tokens on a network blip.
    """
    cfg = tool_config(name)
    time.sleep(float(cfg.get("latency_ms", 200)) / 1000.0)

    if random.random() < float(cfg.get("failure_rate", 0.0)):
        raise InfraFailure(f"{name} unavailable (injected)")

    return {"tool": name, "ok": True, "payload": payload or {}}
