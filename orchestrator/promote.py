from __future__ import annotations

from common.failures import next_tier


def promotion_target(current_tier: str) -> str | None:
    return next_tier(current_tier)
