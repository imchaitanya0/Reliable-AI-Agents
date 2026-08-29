"""
Lane A — Mock Tools with Injectable Latency, Failure Rates & Idempotency
========================================================================

Implements mock GitHub, Logs, and Jira with dynamic runtime overrides.
"""

from __future__ import annotations

import json
import random
import time
from typing import Any

from common.failures import InfraFailure
from db.pool import get_conn

# Default Tool Parameters
DEFAULT_TOOL_SPECS = {
    "github": {"latency_ms": 300, "failure_rate": 0.02, "side_effecting": False},
    "logs": {"latency_ms": 800, "failure_rate": 0.05, "side_effecting": False},
    "jira": {"latency_ms": 500, "failure_rate": 0.10, "side_effecting": True},
}


def get_tool_config(tool_name: str) -> dict[str, Any]:
    """Fetch tool configuration with live overrides from runtime_config."""
    base_spec = dict(DEFAULT_TOOL_SPECS.get(tool_name, {"latency_ms": 100, "failure_rate": 0.0, "side_effecting": False}))
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM runtime_config WHERE key = 'tool_overrides';")
                row = cur.fetchone()
                if row and row["value"]:
                    overrides = row["value"]
                    if tool_name in overrides:
                        base_spec.update(overrides[tool_name])
    except Exception:
        # If DB not yet ready, use defaults
        pass
    return base_spec


def call_mock_tool(tool_name: str, action: str, params: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a mock tool call with simulated latency and fault injection.
    Raises InfraFailure if simulated network/service error occurs.
    """
    cfg = get_tool_config(tool_name)
    latency_ms = cfg.get("latency_ms", 100)
    failure_rate = cfg.get("failure_rate", 0.0)

    # Simulate network latency (scaled down for fast test execution)
    time.sleep(latency_ms / 1000.0)

    # Fault injection check
    if failure_rate > 0.0 and random.random() < failure_rate:
        raise InfraFailure(
            f"Tool '{tool_name}' failed during '{action}': 503 Service Unavailable / Timeout",
            retryable_hint=True,
        )

    # Tool execution results
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
