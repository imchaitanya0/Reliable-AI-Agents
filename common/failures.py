"""
CONTRACT C1 — FAILURE TAXONOMY
==============================

The seam between the worker (which raises) and the orchestrator (which routes).
Lane C and Lane F agree on these three strings and nothing else. Neither lane
needs to read the other's code.

WHY THIS EXISTS
---------------
Celery, Temporal, SQS and Airflow all retry with the identical configuration.
For deterministic code that is correct — same input, same code, transient fault,
it will probably work now.

For a capability-bounded workload it is wrong. If a model failed because it
wasn't strong enough, running the same model again buys you the same failure at
full price. So this runtime classifies before it retries.

Classification is also what makes the cost argument defensible. Escalating on
INFRA would mean a `kill -9` promotes work to the expensive model for zero
benefit. Escalating on POISON burns senior compute on something no model can
fix. Only CAPABILITY failures are worth paying more to retry.
"""

from __future__ import annotations


class TaskFailure(Exception):
    """Base class. Never raise this directly — raise one of the three below."""

    failure_class: str = "INFRA"

    def __init__(self, detail: str = "", retryable_hint: bool = True) -> None:
        super().__init__(detail)
        self.detail = detail
        self.retryable_hint = retryable_hint

    def __repr__(self) -> str:  # pragma: no cover - debugging affordance
        return f"<{type(self).__name__} {self.failure_class}: {self.detail}>"


class InfraFailure(TaskFailure):
    """
    The machine broke. The task itself never got a fair chance.

    Examples: worker SIGKILLed, lease expired, tool timeout, upstream 5xx,
    connection reset, OOM.

    Routing: retry at the SAME tier with exponential backoff. A dead machine
    says nothing about model capability, so promoting here would spend money
    for no reason.
    """

    failure_class = "INFRA"


class CapabilityFailure(TaskFailure):
    """
    The task ran and failed on its own merits. An identical retry fails
    identically.

    Examples: output failed validation, the agent gave up, the result was
    unusable, the model could not construct a workable query.

    Routing: retry at the same tier up to `max_attempts_per_tier`, THEN promote
    to senior. This is the only path that spends extra money, and it should
    reach single-digit percentages of all tasks.
    """

    failure_class = "CAPABILITY"


class PoisonFailure(TaskFailure):
    """
    Nothing fixes this. Not a retry, not a bigger model.

    Examples: malformed task definition, schema violation, unknown task id,
    4xx from a tool, a plan referencing a task that does not exist.

    Routing: straight to the dead-letter queue. No retry at any tier.
    """

    failure_class = "POISON"

    def __init__(self, detail: str = "") -> None:
        super().__init__(detail, retryable_hint=False)


# ---------------------------------------------------------------------------
# The escalation ladder lives in the `tiers` TABLE, not here. See common/tiers.py.
# Adding a tier must never require editing code, so these are thin shims kept
# only so existing imports keep working.
# ---------------------------------------------------------------------------


def next_tier(current: str) -> str | None:
    """Deprecated shim. Import from common.tiers instead."""
    from common.tiers import next_tier as _next

    return _next(current)


def backoff_seconds(attempt: int, base: float = 2.0, cap: float = 30.0) -> float:
    """
    Exponential backoff for the retry path.

    Deliberately capped: an uncapped backoff on a ten-minute demo makes a
    recovered task look hung, which reads as a bug on stage.
    """
    return min(cap, base ** max(0, attempt))
