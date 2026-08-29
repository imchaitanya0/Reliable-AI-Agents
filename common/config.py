from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TierConfig:
    cost_units: int
    token_units: int


TIERS: dict[str, TierConfig] = {
    "junior": TierConfig(cost_units=1, token_units=100),
    "senior": TierConfig(cost_units=12, token_units=300),
}

DEFAULT_PLAN = [1, 2, 6, 8, 9]
DEFAULT_LEASE_TTL_SECONDS = int(os.getenv("LEASE_TTL_SECONDS", "30"))
SEMANTIC_DUP_THRESHOLD = float(os.getenv("SEMANTIC_DUP_THRESHOLD", "0.92"))
