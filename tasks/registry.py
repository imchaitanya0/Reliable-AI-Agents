"""
The task registry -- Lane A.

Adding a capability is ONE decorated function. Nothing in the worker,
orchestrator or API changes. Drop a new file in this package and `discover()`
picks it up on the next worker restart.

DIFFICULTY IS THE ESCALATION LEVER
----------------------------------
Tasks marked difficulty="hard" fail DETERMINISTICALLY on the base tier and
succeed on a higher one. Probability would make the escalation demo a coin flip
on stage; determinism makes it reproducible, which is what you want when a room
is watching.
"""

from __future__ import annotations

from common.failures import CapabilityFailure
from common.protocol import TaskContext
from common.registry import registry, task
from common.tiers import base_tier
from tasks import tools


def _needs_a_bigger_model(ctx: TaskContext, what: str) -> None:
    """A hard task is beyond the base tier, by construction."""
    if ctx.tier == base_tier():
        raise CapabilityFailure(f"{what}: base tier could not produce a usable answer")


# --- the investigation workflow ---------------------------------------------

@task(1, name="parse-request")
def parse_request(ctx: TaskContext) -> dict:
    return {"intent": "investigate_payment_failure", "entities": ["payments-api"]}


@task(2, name="search-logs", tool="logs")
def search_logs(ctx: TaskContext) -> dict:
    tools.call("logs", {"query": "payments-api ERROR"})
    return {"matches": 42, "top_error": "UpstreamTimeout on charge()"}


@task(3, name="check-github", tool="github")
def check_github(ctx: TaskContext) -> dict:
    tools.call("github", {"repo": "payments-api", "since": "24h"})
    return {"recent_deploys": 2, "suspect_pr": "#8814"}


@task(4, name="read-jira", tool="jira")
def read_jira(ctx: TaskContext) -> dict:
    tools.call("jira", {"jql": "project=PAY AND status!=Done"})
    return {"open_incidents": 3}


@task(5, name="correlate-incidents", difficulty="hard")
def correlate_incidents(ctx: TaskContext) -> dict:
    _needs_a_bigger_model(ctx, "correlation across incidents")
    return {"correlated": True, "solved_by": ctx.tier}


@task(6, name="root-cause-analysis", difficulty="hard")
def root_cause_analysis(ctx: TaskContext) -> dict:
    """The escalation showcase. Sees everything prior tasks found."""
    _needs_a_bigger_model(ctx, "root cause analysis")
    return {
        "root_cause": "connection pool exhaustion after deploy #8814",
        "confidence": 0.91,
        "solved_by": ctx.tier,
        "evidence_from_steps": sorted(ctx.prior),
    }


@task(7, name="draft-summary")
def draft_summary(ctx: TaskContext) -> dict:
    return {"summary": "Payment failures traced to pool exhaustion.",
            "based_on": sorted(ctx.prior)}


@task(8, name="create-jira-ticket", tool="jira", side_effecting=True)
def create_jira_ticket(ctx: TaskContext) -> dict:
    """
    The idempotency showcase. Reclaim-on-timeout means this WILL sometimes run
    twice; the key in the executor is what stops two tickets existing.
    """
    tools.call("jira", {"action": "create"})
    return {"ticket": "PAY-4471", "created": True}


@task(9, name="notify-team")
def notify_team(ctx: TaskContext) -> dict:
    return {"notified": ["#payments-oncall"], "steps_seen": len(ctx.prior)}


# Legacy-style export so anything importing TASK_DEFS keeps working.
TASK_DEFS = registry.as_dict()

# The demo plan: parse -> search logs -> root cause (HARD, escalates)
#                      -> create ticket (SIDE-EFFECTING) -> notify
DEMO_PLAN = [1, 2, 6, 8, 9]

# Most real work needs no escalation. This mix is what makes the observed
# escalation rate land near 7% rather than being asserted into existence.
EASY_PLAN = [1, 2, 3, 7, 9]
