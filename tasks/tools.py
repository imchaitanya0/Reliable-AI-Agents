from __future__ import annotations

import random
import time
from typing import Any

from common.failures import InfraFailure


DEFAULT_TOOL_CONFIG: dict[str, dict[str, Any]] = {
    "github": {"latency_ms": 30, "failure_rate": 0.0, "side_effecting": False},
    "logs": {"latency_ms": 40, "failure_rate": 0.0, "side_effecting": False},
    "jira": {"latency_ms": 30, "failure_rate": 0.0, "side_effecting": True},
}


def call_tool(name: str, payload: dict[str, Any], overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config = dict(DEFAULT_TOOL_CONFIG.get(name, {"latency_ms": 10, "failure_rate": 0.0}))
    if overrides and name in overrides:
        config.update(overrides[name])
    time.sleep(config.get("latency_ms", 0) / 1000)
    if random.random() < float(config.get("failure_rate", 0.0)):
        raise InfraFailure(f"{name} returned a transient failure")
    return {"tool": name, "payload": payload, "ok": True}
