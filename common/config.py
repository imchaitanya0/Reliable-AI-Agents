"""Shared configuration. Every process reads the same env vars."""

from __future__ import annotations

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/rai"
)

# Lease TTL. A worker renews every LEASE_TTL/3; the reaper reclaims anything
# whose lease_expires has passed. This is the ONLY failure detector in the
# system -- you cannot distinguish a crashed worker from a slow one, so the
# runtime does not try. It reclaims on expiry and defends against the
# consequence with idempotency keys.
LEASE_TTL_SECONDS = int(os.getenv("LEASE_TTL_SECONDS", "30"))
HEARTBEAT_INTERVAL = LEASE_TTL_SECONDS / 3.0

# Which tier this worker process drains. Junior and senior pools scale
# independently: only ~7% of tasks escalate, so 2 senior to 10 junior is right.
POOL_TIER = os.getenv("POOL_TIER", "junior")
WORKER_ID = os.getenv("WORKER_ID", f"worker-{os.getpid()}")

WORKER_POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "0.5"))
ORCHESTRATOR_POLL_SECONDS = float(os.getenv("ORCHESTRATOR_POLL_SECONDS", "2.0"))

# How many expired leases one reaper sweep may reclaim. Bounds the blast radius
# of a mass eviction: the sweep is short, holds few locks, and simply runs again
# on the next tick rather than locking every affected row at once.
REAPER_BATCH = int(os.getenv("REAPER_BATCH", "100"))

# Reclaimed tasks are spread over [0, jitter) seconds. Requeueing a thousand
# tasks at exactly now() does not remove the stampede, it just moves it from the
# reaper to the claim query.
REAPER_JITTER_SECONDS = float(os.getenv("REAPER_JITTER_SECONDS", "2.0"))

# Batch bound shared by the queue-repair sweeps, for the same reason.
ORCHESTRATOR_BATCH = int(os.getenv("ORCHESTRATOR_BATCH", "100"))

# Admission control: the most agents allowed to be running at once.
#
# Backpressure is a reliability feature, not a limitation. Without a cap a spike
# is accepted in full and then fails slowly and invisibly -- every agent
# admitted, none finishing on time, queue depth climbing, and no signal that
# anything is wrong. Refusing work at the door turns that into an immediate,
# legible 429 that a caller can retry.
#
# Sized to the autoscaler: 100 agents at one orchestrator per 10 is exactly the
# 10-replica ceiling, so the system is full and fully scaled at the same moment.
MAX_ACTIVE_AGENTS = int(os.getenv("MAX_ACTIVE_AGENTS", "100"))

# Tier definitions live in the `tiers` TABLE (see db/schema.sql and
# common/tiers.py) so that adding a capability tier is one INSERT rather than a
# code change. Nothing here may duplicate them -- two sources of truth drift.

# Mock tools: fixed latency plus an injectable failure rate.
TOOLS: dict[str, dict] = {
    "github": {"latency_ms": 300, "failure_rate": 0.02, "side_effecting": False},
    "logs": {"latency_ms": 800, "failure_rate": 0.05, "side_effecting": False},
    "jira": {"latency_ms": 500, "failure_rate": 0.10, "side_effecting": True},
}
