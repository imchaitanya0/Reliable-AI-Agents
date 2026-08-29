"""
Shared runtime configuration and environment settings.
"""

from __future__ import annotations

import os
import uuid

# Database
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('USER', 'postgres')}@localhost:5432/postgres",
)

# Leasing & Timeouts
LEASE_TTL_SECONDS: int = int(os.getenv("LEASE_TTL_SECONDS", "30"))
HEARTBEAT_INTERVAL_SECONDS: float = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", str(LEASE_TTL_SECONDS / 3)))
REAPER_INTERVAL_SECONDS: float = float(os.getenv("REAPER_INTERVAL_SECONDS", "2.0"))

# Worker Settings
POOL_TIER: str = os.getenv("POOL_TIER", "junior")  # 'junior' | 'senior'
WORKER_ID: str = os.getenv("WORKER_ID", f"{POOL_TIER}-worker-{os.getpid()}-{str(uuid.uuid4())[:6]}")
POLL_INTERVAL_SECONDS: float = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))

# Retry & Escalation Thresholds
MAX_ATTEMPTS_PER_TIER: int = int(os.getenv("MAX_ATTEMPTS_PER_TIER", "2"))

# Tokenomics Cost Multipliers
JUNIOR_COST_UNIT: int = 1
SENIOR_COST_UNIT: int = 12

# HTTP / API Settings
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
