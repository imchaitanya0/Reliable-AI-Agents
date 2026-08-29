"""
Lane E — Chaos Injection Harness (Contract C3)
==============================================

Programmatically injects tool failures, latency, and worker process faults.
"""

from __future__ import annotations

import json
from typing import Any

import requests

from common.config import API_HOST, API_PORT

BASE_URL = f"http://127.0.0.1:{API_PORT}"


def set_tool_chaos(tool_name: str, failure_rate: float, latency_ms: int = 300) -> dict[str, Any]:
    """Inject failure rate and latency into a mock tool via the API."""
    url = f"{BASE_URL}/chaos/tool"
    payload = {"name": tool_name, "failure_rate": failure_rate, "latency_ms": latency_ms}
    try:
        resp = requests.post(url, json=payload, timeout=2.0)
        return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


def set_runtime_config(retries: bool = True, escalation: bool = True, force_tier: str | None = None) -> dict[str, Any]:
    """Configure global runtime flags."""
    url = f"{BASE_URL}/chaos/config"
    payload = {"retries_enabled": retries, "escalation_enabled": escalation, "force_tier": force_tier}
    try:
        resp = requests.post(url, json=payload, timeout=2.0)
        return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


def submit_agent_batch(plan: list[int] = [1, 2, 6, 8, 9], count: int = 20) -> list[str]:
    """Submit a batch of agent workflows to the runtime."""
    url = f"{BASE_URL}/agents"
    payload = {"plan": plan, "count": count}
    try:
        resp = requests.post(url, json=payload, timeout=5.0)
        return resp.json().get("agent_ids", [])
    except Exception as exc:
        return []
