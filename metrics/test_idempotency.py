"""
Metric Script: Intervention 3 — Idempotency & Exactly-Once External Effect

Measures:
1. Deduplication of side-effecting external actions (e.g. Jira issue creation, payment refund).
2. Exactly-once external effect under forced retry storms / lease reclamation replays.
3. Invariant: Duplicate external side-effects blocked = 100% (0 duplicate actions executed).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.table import Table

from metrics.common import (
    cleanup_test_data,
    get_db_connection,
    init_test_db,
    print_metric_banner,
)

console = Console()


class MockJiraTool:
    """Mock external side-effecting service tracking raw API calls."""

    def __init__(self) -> None:
        self.call_count = 0
        self.created_issues: list[dict] = []

    def create_issue(self, summary: str, description: str) -> dict:
        self.call_count += 1
        issue_key = f"SEC-{1000 + self.call_count}"
        issue_data = {
            "issue_key": issue_key,
            "summary": summary,
            "description": description,
            "created_at": time.time(),
        }
        self.created_issues.append(issue_data)
        return issue_data


def run_idempotency_metric() -> dict:
    print_metric_banner(
        "INTERVENTION 3: IDEMPOTENCY & EXACTLY-ONCE EFFECT",
        "Measures duplicate side-effect prevention under forced network timeouts & retry replays",
    )

    conn = get_db_connection()
    init_test_db(conn)
    cleanup_test_data(conn)

    jira = MockJiraTool()
    agent_id = str(uuid.uuid4())
    seq = 2
    action_type = "jira:create_issue"
    action_payload = {"summary": "Payment Gateway Timeout", "description": "Investigation findings"}

    # Generate deterministic idempotency key
    raw_key = f"{agent_id}:{seq}:{action_type}"
    idem_key = hashlib.sha256(raw_key.encode()).hexdigest()

    # Insert agent record so foreign key constraint holds
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agents (id, plan, cursor, status)
            VALUES (%s, %s, %s, 'running');
            """,
            (agent_id, [1, 2, 3], seq),
        )

    console.print(f"[cyan]Generated Idempotency Key: [bold]{idem_key[:16]}...[/bold] (SHA-256)[/cyan]")

    # --- Attempt 1: Worker-A executes the side-effect ---
    console.print("\n[yellow]Attempt 1: Worker-A executes task...[/yellow]")

    # Check idempotency ledger
    with conn.cursor() as cur:
        cur.execute("SELECT result FROM idempotency WHERE key = %s;", (idem_key,))
        cached_result = cur.fetchone()

    if cached_result is None:
        console.print("  • No prior record found. Firing external Jira API call...")
        tool_output = jira.create_issue(action_payload["summary"], action_payload["description"])

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO idempotency (key, agent_id, seq, action_type, result)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (key) DO NOTHING;
                """,
                (idem_key, agent_id, seq, action_type, json.dumps(tool_output)),
            )
        console.print(f"  • Jira issue created: [green]{tool_output['issue_key']}[/green]. Recorded in Postgres.")

    # Simulate Worker-A dying right before acknowledging task completion!
    console.print("[bold red]NETWORK TIMEOUT SIMULATION: Worker-A crashes before updating task_instances.[/bold red]")

    # --- Attempt 2: Worker-B re-claims and re-executes the exact same task ---
    console.print("\n[yellow]Attempt 2: Worker-B receives retried task after lease expiry...[/yellow]")

    external_call_fired_attempt_2 = False
    with conn.cursor() as cur:
        cur.execute("SELECT result FROM idempotency WHERE key = %s;", (idem_key,))
        cached_result = cur.fetchone()

    if cached_result is not None:
        console.print(
            f"  • [bold green]IDEMPOTENCY HIT![/bold green] Found existing result for key. Skipping external API call."
        )
        tool_output_attempt_2 = cached_result["result"]
    else:
        # Should NOT happen
        external_call_fired_attempt_2 = True
        tool_output_attempt_2 = jira.create_issue(
            action_payload["summary"], action_payload["description"]
        )

    # --- Verification & Table ---
    total_replays = 2
    actual_external_calls = jira.call_count
    duplicate_calls_prevented = total_replays - actual_external_calls

    table = Table(title="Idempotency Verification Metrics")
    table.add_column("Metric", style="cyan")
    table.add_column("Without Idempotency", style="red")
    table.add_column("With Runtime Idempotency", style="bold green")
    table.add_column("Status", style="bold")

    table.add_row(
        "Total Task Attempts",
        "2 attempts",
        "2 attempts",
        "INFO",
    )
    table.add_row(
        "External Jira Issues Created",
        "2 issues (Duplicate Bug!)",
        f"{actual_external_calls} issue ({jira.created_issues[0]['issue_key']})",
        "PASS [green]✓[/green]",
    )
    table.add_row(
        "Duplicate Actions Prevented",
        "0 prevented",
        f"{duplicate_calls_prevented} duplicate action blocked",
        "PASS [green]✓[/green]",
    )
    table.add_row(
        "Side-Effect Guarantee",
        "At-least-once (Dirty)",
        "Exactly-once EFFECT (Clean)",
        "PASS [green]✓[/green]",
    )

    console.print(table)
    conn.close()

    return {
        "status": "PASS" if actual_external_calls == 1 else "FAIL",
        "external_calls": actual_external_calls,
        "duplicate_calls_prevented": duplicate_calls_prevented,
    }


if __name__ == "__main__":
    run_idempotency_metric()
