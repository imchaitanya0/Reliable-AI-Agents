"""
The task library.

Adding a capability is ONE decorated function. Nothing in the worker,
orchestrator or API changes -- `discover()` imports this package and the
decorators register themselves.

IDS ARE GROUPED BY PHASE so the space stays extensible:
    1-9    intake        30-39  decide
   10-19   observe       40-49  act (side-effecting)
   20-29   analyse       50-59  verify

DIFFICULTY IS THE ESCALATION LEVER
----------------------------------
difficulty="hard" fails DETERMINISTICALLY on the base tier and succeeds higher
up. Probability would make the escalation demo a coin flip on stage.
"""

from __future__ import annotations

from common.failures import CapabilityFailure
from common.protocol import TaskContext
from common.registry import registry, task
from common.tiers import base_tier
from tasks import tools


def _hard(ctx: TaskContext, what: str) -> None:
    """A hard task is beyond the base tier, by construction."""
    if ctx.tier == base_tier():
        raise CapabilityFailure(f"{what}: base tier produced no usable answer")


# --- 1-9 intake --------------------------------------------------------------

@task(1, name="parse-request")
def parse_request(ctx: TaskContext) -> dict:
    return {"intent": "investigate_payment_failure", "service": "payments-api"}


@task(2, name="classify-severity")
def classify_severity(ctx: TaskContext) -> dict:
    return {"severity": "sev2", "customer_facing": True}


@task(3, name="extract-entities")
def extract_entities(ctx: TaskContext) -> dict:
    return {"services": ["payments-api", "ledger"], "regions": ["us-east-1"]}


# --- 10-19 observe (these call tools) ----------------------------------------

@task(10, name="search-logs", tool="logs")
def search_logs(ctx: TaskContext) -> dict:
    return {"logs": tools.call("logs", {"query": "payments-api ERROR"})}


@task(11, name="query-metrics", tool="metrics_db")
def query_metrics(ctx: TaskContext) -> dict:
    return {"metrics": tools.call("metrics_db", {"window": "1h"})}


@task(12, name="check-deploys", tool="github")
def check_deploys(ctx: TaskContext) -> dict:
    return {"github": tools.call("github", {"repo": "psycopg/psycopg"})}


@task(13, name="scan-codebase", tool="files")
def scan_codebase(ctx: TaskContext) -> dict:
    return {"scan": tools.call("files", {"pattern": "SKIP LOCKED"})}


@task(14, name="run-diagnostics", tool="shell")
def run_diagnostics(ctx: TaskContext) -> dict:
    return {"diagnostics": tools.call("shell", {"cmd": "git-log"})}


@task(15, name="fetch-status-page", tool="http")
def fetch_status_page(ctx: TaskContext) -> dict:
    return {"status_page": tools.call("http", {"url": "https://api.github.com/zen"})}


@task(16, name="read-jira", tool="jira")
def read_jira(ctx: TaskContext) -> dict:
    return {"jira": tools.call("jira", {"action": "read"})}


# --- 20-29 analyse (the expensive thinking) ----------------------------------

@task(20, name="correlate-signals", difficulty="hard")
def correlate_signals(ctx: TaskContext) -> dict:
    _hard(ctx, "correlating signals across sources")
    return {"correlated": True, "solved_by": ctx.tier, "sources": sorted(ctx.prior)}


@task(21, name="root-cause-analysis", difficulty="hard")
def root_cause_analysis(ctx: TaskContext) -> dict:
    _hard(ctx, "root cause analysis")
    return {"root_cause": "connection pool exhaustion after deploy #8814",
            "confidence": 0.91, "solved_by": ctx.tier,
            "evidence_from_steps": sorted(ctx.prior)}


@task(22, name="assess-blast-radius")
def assess_blast_radius(ctx: TaskContext) -> dict:
    return {"affected_users": 12400, "regions": ["us-east-1"]}


@task(23, name="rank-hypotheses", difficulty="hard")
def rank_hypotheses(ctx: TaskContext) -> dict:
    _hard(ctx, "ranking competing hypotheses")
    return {"top": "pool exhaustion", "alternatives": 3, "solved_by": ctx.tier}


# --- 30-39 decide ------------------------------------------------------------

@task(30, name="draft-summary")
def draft_summary(ctx: TaskContext) -> dict:
    return {"summary": "Payment failures traced to pool exhaustion.",
            "based_on_steps": sorted(ctx.prior)}


@task(31, name="recommend-remediation", difficulty="hard")
def recommend_remediation(ctx: TaskContext) -> dict:
    _hard(ctx, "recommending a remediation")
    return {"action": "raise pool size to 60 and roll back #8814",
            "solved_by": ctx.tier}


@task(32, name="estimate-risk")
def estimate_risk(ctx: TaskContext) -> dict:
    return {"rollback_risk": "low", "eta_minutes": 15}


# --- 40-49 act (SIDE-EFFECTING: every one needs an idempotency key) ----------

@task(40, name="create-jira-ticket", tool="jira", side_effecting=True)
def create_jira_ticket(ctx: TaskContext) -> dict:
    return {"jira": tools.call("jira", {"action": "create"})}


@task(41, name="notify-slack", tool="slack", side_effecting=True)
def notify_slack(ctx: TaskContext) -> dict:
    return {"slack": tools.call("slack", {"channel": "#payments-oncall"})}


@task(42, name="page-oncall", tool="pagerduty", side_effecting=True)
def page_oncall(ctx: TaskContext) -> dict:
    return {"page": tools.call("pagerduty", {"severity": "sev2"})}


@task(43, name="post-status-update", tool="http", side_effecting=True)
def post_status_update(ctx: TaskContext) -> dict:
    return {"posted": tools.call("http", {"url": "https://api.github.com/zen"})}


# --- 50-59 verify ------------------------------------------------------------

@task(50, name="verify-resolution")
def verify_resolution(ctx: TaskContext) -> dict:
    return {"resolved": True, "checks_passed": 4}


@task(51, name="write-postmortem", difficulty="hard")
def write_postmortem(ctx: TaskContext) -> dict:
    _hard(ctx, "writing the postmortem")
    return {"postmortem": "PM-118", "steps_covered": len(ctx.prior),
            "solved_by": ctx.tier}


TASK_DEFS = registry.as_dict()


# --- default pipelines -------------------------------------------------------
# Seeded into the `pipelines` table on first run. Compose your own at runtime;
# nothing here is privileged.
DEFAULT_PIPELINES: dict[str, tuple[list[int], str]] = {
    "smoke":              ([1, 30], "two steps, no tools -- fastest sanity check"),
    "quick-triage":       ([1, 2, 10, 30, 41],
                           "cheap path, no hard tasks, never escalates"),
    "investigation":      ([1, 10, 21, 40, 41],
                           "the demo plan: one hard task, one side effect"),
    "deep-investigation": ([1, 3, 10, 11, 12, 20, 21, 31, 40, 42],
                           "many observations, three hard steps"),
    "code-audit":         ([1, 13, 14, 23, 30, 40],
                           "filesystem + shell tools, one hard ranking step"),
    "full-incident":      ([1, 2, 3, 10, 11, 12, 15, 20, 21, 22, 31, 32,
                            40, 42, 41, 50, 51],
                           "17 steps end to end -- the long-running showcase"),
}

# Kept for the existing demo scripts.
DEMO_PLAN = DEFAULT_PIPELINES["investigation"][0]
EASY_PLAN = DEFAULT_PIPELINES["quick-triage"][0]
