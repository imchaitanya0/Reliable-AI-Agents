"""Shared configuration. Every process reads the same env vars."""

from __future__ import annotations

import os
import uuid

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('USER', 'postgres')}@localhost:5432/postgres",
)

# Lease TTL. A worker renews every LEASE_TTL/3; the reaper reclaims anything
# whose lease_expires has passed.
LEASE_TTL_SECONDS = int(os.getenv("LEASE_TTL_SECONDS", "30"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", str(LEASE_TTL_SECONDS / 3.0)))
HEARTBEAT_INTERVAL_SECONDS = HEARTBEAT_INTERVAL

# Worker configuration
POOL_TIER = os.getenv("POOL_TIER", "junior")
WORKER_ID = os.getenv("WORKER_ID", f"{POOL_TIER}-worker-{os.getpid()}-{str(uuid.uuid4())[:6]}")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))
WORKER_POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", str(POLL_INTERVAL_SECONDS)))

# Orchestrator configuration
REAPER_INTERVAL_SECONDS = float(os.getenv("REAPER_INTERVAL_SECONDS", "2.0"))
ORCHESTRATOR_POLL_SECONDS = float(os.getenv("ORCHESTRATOR_POLL_SECONDS", str(REAPER_INTERVAL_SECONDS)))
REAPER_BATCH = int(os.getenv("REAPER_BATCH", "100"))
REAPER_JITTER_SECONDS = float(os.getenv("REAPER_JITTER_SECONDS", "2.0"))
ORCHESTRATOR_BATCH = int(os.getenv("ORCHESTRATOR_BATCH", "100"))

# Retry & Escalation Thresholds
MAX_ATTEMPTS_PER_TIER = int(os.getenv("MAX_ATTEMPTS_PER_TIER", "2"))

# Tokenomics Cost Multipliers
JUNIOR_COST_UNIT = 1
SENIOR_COST_UNIT = 12

# HTTP / API Settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Mock tools: fixed latency plus an injectable failure rate.
TOOLS: dict[str, dict] = {
    "github": {"latency_ms": 300, "failure_rate": 0.02, "side_effecting": False},
    "logs": {"latency_ms": 800, "failure_rate": 0.05, "side_effecting": False},
    "jira": {"latency_ms": 500, "failure_rate": 0.10, "side_effecting": True},
}
