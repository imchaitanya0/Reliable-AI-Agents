"""
Mock tools with fixed latency and injectable failure rates (Lane A).
Failure rates are read from runtime_config.tool_overrides dynamically.
"""

from __future__ import annotations

import json
import random
import threading
import time
from typing import Any

from common.failures import InfraFailure
from db.pool import pool

TOOLS: dict[str, dict[str, Any]] = {
    "github": {"latency_ms": 300, "failure_rate": 0.02, "side_effecting": False},
    "logs": {"latency_ms": 800, "failure_rate": 0.05, "side_effecting": False},
    "jira": {"latency_ms": 500, "failure_rate": 0.10, "side_effecting": True},
}

_overrides: dict[str, dict] = {}
_overrides_at: float = 0.0
_lock = threading.Lock()
_TTL = 0.5


def _load_overrides() -> dict[str, dict]:
    global _overrides, _overrides_at
    with _lock:
        if time.time() - _overrides_at < _TTL:
            return _overrides
        try:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT value FROM runtime_config WHERE key = 'tool_overrides'")
                row = cur.fetchone()
            val = (row or {}).get("value") or {}
            _overrides = val if isinstance(val, dict) else json.loads(val)
        except Exception:
            _overrides = {}
        _overrides_at = time.time()
        return _overrides


def tool_config(name: str) -> dict[str, Any]:
    cfg = dict(TOOLS.get(name, {"latency_ms": 100, "failure_rate": 0.0, "side_effecting": False}))
    cfg.update(_load_overrides().get(name, {}))
    return cfg


def get_tool_config(name: str) -> dict[str, Any]:
    return tool_config(name)


def call(name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Invoke a mock tool.
    A tool failure is INFRA, never CAPABILITY: the upstream broke, which says
    nothing about whether the model was smart enough.
    """
    cfg = tool_config(name)
    latency_ms = float(cfg.get("latency_ms", 100))
    if latency_ms > 0:
        time.sleep(latency_ms / 1000.0)

    if float(cfg.get("failure_rate", 0.0)) > 0.0 and random.random() < float(cfg.get("failure_rate", 0.0)):
        raise InfraFailure(f"{name} unavailable (injected)", retryable_hint=True)

    return {"tool": name, "ok": True, "payload": payload or {}}


def call_mock_tool(tool_name: str, action: str, params: dict[str, Any]) -> dict[str, Any]:
    """Execute mock tool call with semantic responses."""
    cfg = tool_config(tool_name)
    latency_ms = float(cfg.get("latency_ms", 100))
    if latency_ms > 0:
        time.sleep(latency_ms / 1000.0)

    if float(cfg.get("failure_rate", 0.0)) > 0.0 and random.random() < float(cfg.get("failure_rate", 0.0)):
        raise InfraFailure(f"Tool '{tool_name}' failed during '{action}': 503 Service Unavailable / Timeout", retryable_hint=True)

    if tool_name == "github":
        return {
            "tool": "github",
            "action": action,
            "commits": [
                {"sha": "a1b2c3d", "message": "fix: update db timeout settings", "author": "dev@company.com"},
                {"sha": "e4f5g6h", "message": "feat: rollout new payment gateway", "author": "eng@company.com"},
            ],
            "repo": params.get("repo", "org/main-repo"),
        }
    elif tool_name == "logs":
        return {
            "tool": "logs",
            "action": action,
            "log_lines": [
                "2026-08-29 13:45:01 ERROR [PaymentSvc] HTTP 504 Gateway Timeout on POST /checkout",
                "2026-08-29 13:45:02 WARN  [DatabasePool] Connection pool exhausted (20/20 active)",
                "2026-08-29 13:45:05 ERROR [PaymentSvc] Upstream gateway circuit breaker OPEN",
            ],
            "service": params.get("service", "payment-service"),
        }
    elif tool_name == "jira":
        return {
            "tool": "jira",
            "action": action,
            "ticket_id": f"INC-{random.randint(1000, 9999)}",
            "summary": params.get("summary", "Payment Incident Investigation"),
            "status": "OPEN",
            "priority": "P1-CRITICAL",
        }

    return {"tool": tool_name, "action": action, "output": "ok"}
