from __future__ import annotations

import json
import math
import os
import random
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from common.config import SEMANTIC_DUP_THRESHOLD, TIERS
from common.failures import backoff_seconds, next_tier


def utc_now() -> float:
    return time.time()


def encode_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def decode_json(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def embed_text(text: str, dims: int = 16) -> list[float]:
    buckets = [0.0] * dims
    for token in text.lower().split():
        buckets[hash(token) % dims] += 1.0
    norm = math.sqrt(sum(v * v for v in buckets)) or 1.0
    return [v / norm for v in buckets]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


class RuntimeDB:
    def __init__(self, path: str | os.PathLike[str] = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.initialize()

    @contextmanager
    def transaction(self, mode: str = "IMMEDIATE") -> Iterator[sqlite3.Connection]:
        self.conn.execute(f"BEGIN {mode}")
        try:
            yield self.conn
        except Exception:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def initialize(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                plan TEXT NOT NULL,
                cursor INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                context TEXT NOT NULL DEFAULT '{}',
                cost_units INTEGER NOT NULL DEFAULT 0,
                tokens_used INTEGER NOT NULL DEFAULT 0,
                query_text TEXT,
                query_embedding TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_instances (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                task_def_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                tier TEXT NOT NULL DEFAULT 'junior',
                attempt INTEGER NOT NULL DEFAULT 0,
                max_attempts_per_tier INTEGER NOT NULL DEFAULT 2,
                lease_owner TEXT,
                lease_expires REAL,
                next_run_at REAL NOT NULL,
                result TEXT,
                last_error TEXT,
                failure_class TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(agent_id, seq)
            );
            CREATE INDEX IF NOT EXISTS task_instances_claim_idx
                ON task_instances (tier, next_run_at, status);
            CREATE INDEX IF NOT EXISTS task_instances_lease_idx
                ON task_instances (lease_expires, status);
            CREATE TABLE IF NOT EXISTS idempotency (
                key TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'in_flight',
                result TEXT,
                error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_instance_id TEXT NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
                agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL,
                tier TEXT NOT NULL,
                worker_id TEXT,
                outcome TEXT NOT NULL,
                failure_class TEXT,
                cost_units INTEGER NOT NULL DEFAULT 0,
                tokens INTEGER NOT NULL DEFAULT 0,
                started_at REAL NOT NULL,
                ended_at REAL
            );
            CREATE TABLE IF NOT EXISTS dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                task_def_id INTEGER NOT NULL,
                failure_class TEXT NOT NULL,
                last_error TEXT,
                attempt_trail TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metrics_counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        defaults = {
            "retries_enabled": True,
            "escalation_enabled": True,
            "force_tier": None,
            "lease_ttl_seconds": 30,
            "tool_overrides": {},
        }
        counters = {
            "zombie_writes_blocked": 0,
            "duplicate_actions_blocked": 0,
            "semantic_deduplications": 0,
            "semantic_loop_blocks": 0,
        }
        with self.transaction():
            for key, value in defaults.items():
                self.conn.execute(
                    "INSERT OR IGNORE INTO runtime_config(key, value) VALUES(?, ?)",
                    (key, encode_json(value)),
                )
            for key, value in counters.items():
                self.conn.execute(
                    "INSERT OR IGNORE INTO metrics_counters(key, value) VALUES(?, ?)",
                    (key, value),
                )

    def increment_counter(self, key: str, amount: int = 1) -> None:
        self.conn.execute(
            "INSERT INTO metrics_counters(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=value+excluded.value",
            (key, amount),
        )

    def set_config(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO runtime_config(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, encode_json(value)),
        )

    def get_config(self, key: str) -> Any:
        row = self.conn.execute("SELECT value FROM runtime_config WHERE key=?", (key,)).fetchone()
        return decode_json(row["value"]) if row else None

    def create_agent(self, plan: list[int], query_text: str | None = None) -> str:
        now = utc_now()
        embedding = embed_text(query_text or " ".join(map(str, plan)))
        if query_text:
            duplicate = self.find_semantic_duplicate(query_text, embedding)
            if duplicate is not None:
                self.increment_counter("semantic_deduplications")
                return duplicate
        agent_id = str(uuid.uuid4())
        with self.transaction():
            self.conn.execute(
                "INSERT INTO agents(id, plan, query_text, query_embedding, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (agent_id, encode_json(plan), query_text, encode_json(embedding), now, now),
            )
            for seq, task_def_id in enumerate(plan):
                self.conn.execute(
                    "INSERT INTO task_instances(id, agent_id, seq, task_def_id, created_at, updated_at, next_run_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (str(uuid.uuid4()), agent_id, seq, task_def_id, now, now, now),
                )
        return agent_id

    def find_semantic_duplicate(self, query_text: str, embedding: list[float]) -> str | None:
        rows = self.conn.execute(
            "SELECT id, query_embedding FROM agents WHERE status IN ('pending', 'running') AND query_text IS NOT NULL"
        ).fetchall()
        for row in rows:
            if cosine(embedding, decode_json(row["query_embedding"], [])) >= SEMANTIC_DUP_THRESHOLD:
                return row["id"]
        return None

    def claim_task(self, pool_tier: str, worker_id: str, lease_ttl: int = 30) -> sqlite3.Row | None:
        now = utc_now()
        force_tier = self.get_config("force_tier")
        effective_tier = force_tier or pool_tier
        with self.transaction():
            row = self.conn.execute(
                """
                SELECT t.* FROM task_instances t
                JOIN agents a ON a.id=t.agent_id
                WHERE t.status='pending'
                  AND t.next_run_at <= ?
                  AND t.tier = ?
                  AND a.status='running'
                  AND t.seq = a.cursor
                ORDER BY t.next_run_at, t.created_at
                LIMIT 1
                """,
                (now, effective_tier),
            ).fetchone()
            if row is None:
                return None
            attempt = row["attempt"] + 1
            self.conn.execute(
                """
                UPDATE task_instances
                SET status='running', lease_owner=?, lease_expires=?, attempt=?,
                    failure_class=NULL, last_error=NULL, updated_at=?
                WHERE id=? AND status='pending'
                """,
                (worker_id, now + lease_ttl, attempt, now, row["id"]),
            )
            self.conn.execute(
                """
                INSERT INTO attempts(task_instance_id, agent_id, seq, attempt_no, tier, worker_id,
                                     outcome, started_at)
                VALUES(?, ?, ?, ?, ?, ?, 'started', ?)
                """,
                (row["id"], row["agent_id"], row["seq"], attempt, row["tier"], worker_id, now),
            )
            return self.conn.execute("SELECT * FROM task_instances WHERE id=?", (row["id"],)).fetchone()

    def heartbeat(self, task_id: str, worker_id: str, attempt: int, lease_ttl: int = 30) -> bool:
        now = utc_now()
        result = self.conn.execute(
            """
            UPDATE task_instances SET lease_expires=?, updated_at=?
            WHERE id=? AND lease_owner=? AND attempt=? AND status='running'
            """,
            (now + lease_ttl, now, task_id, worker_id, attempt),
        )
        return result.rowcount == 1

    def complete_task(self, task: sqlite3.Row, worker_id: str, result: dict[str, Any]) -> bool:
        now = utc_now()
        tier_cost = TIERS[task["tier"]]
        with self.transaction():
            update = self.conn.execute(
                """
                UPDATE task_instances
                SET status='succeeded', result=?, lease_owner=NULL, lease_expires=NULL, updated_at=?
                WHERE id=? AND lease_owner=? AND attempt=? AND status='running'
                """,
                (encode_json(result), now, task["id"], worker_id, task["attempt"]),
            )
            if update.rowcount != 1:
                self.increment_counter("zombie_writes_blocked")
                return False
            agent = self.conn.execute("SELECT * FROM agents WHERE id=?", (task["agent_id"],)).fetchone()
            context = decode_json(agent["context"], {})
            context[str(task["seq"])] = result
            plan = decode_json(agent["plan"], [])
            next_cursor = task["seq"] + 1
            status = "completed" if next_cursor >= len(plan) else "running"
            self.conn.execute(
                """
                UPDATE agents
                SET cursor=?, status=?, context=?, cost_units=cost_units+?,
                    tokens_used=tokens_used+?, updated_at=?
                WHERE id=?
                """,
                (
                    next_cursor,
                    status,
                    encode_json(context),
                    tier_cost.cost_units,
                    tier_cost.token_units,
                    now,
                    task["agent_id"],
                ),
            )
            self.conn.execute(
                """
                UPDATE attempts SET outcome='succeeded', cost_units=?, tokens=?, ended_at=?
                WHERE task_instance_id=? AND attempt_no=? AND worker_id=? AND outcome='started'
                """,
                (tier_cost.cost_units, tier_cost.token_units, now, task["id"], task["attempt"], worker_id),
            )
        return True

    def fail_task(self, task: sqlite3.Row, worker_id: str, failure_class: str, detail: str) -> bool:
        now = utc_now()
        tier_cost = TIERS[task["tier"]]
        with self.transaction():
            update = self.conn.execute(
                """
                UPDATE task_instances
                SET status='failed', failure_class=?, last_error=?, lease_owner=NULL,
                    lease_expires=NULL, updated_at=?
                WHERE id=? AND lease_owner=? AND attempt=? AND status='running'
                """,
                (failure_class, detail, now, task["id"], worker_id, task["attempt"]),
            )
            if update.rowcount != 1:
                self.increment_counter("zombie_writes_blocked")
                return False
            self.conn.execute(
                """
                UPDATE attempts
                SET outcome='failed', failure_class=?, cost_units=?, tokens=?, ended_at=?
                WHERE task_instance_id=? AND attempt_no=? AND worker_id=? AND outcome='started'
                """,
                (
                    failure_class,
                    tier_cost.cost_units,
                    tier_cost.token_units,
                    now,
                    task["id"],
                    task["attempt"],
                    worker_id,
                ),
            )
        return True

    def route_failures(self) -> int:
        rows = self.conn.execute("SELECT * FROM task_instances WHERE status='failed'").fetchall()
        routed = 0
        for task in rows:
            routed += self._route_one_failure(task)
        return routed

    def _route_one_failure(self, task: sqlite3.Row) -> int:
        failure_class = task["failure_class"]
        now = utc_now()
        if failure_class == "POISON":
            self.dead_letter(task)
            return 1
        if failure_class == "INFRA" or not self.get_config("retries_enabled"):
            self.conn.execute(
                """
                UPDATE task_instances
                SET status='pending', next_run_at=?, updated_at=?
                WHERE id=? AND status='failed'
                """,
                (now + backoff_seconds(task["attempt"]), now, task["id"]),
            )
            return 1
        capability_attempts = self.conn.execute(
            """
            SELECT COUNT(*) AS n FROM attempts
            WHERE task_instance_id=? AND outcome='failed' AND failure_class='CAPABILITY'
            """,
            (task["id"],),
        ).fetchone()["n"]
        if capability_attempts >= task["max_attempts_per_tier"]:
            promoted = next_tier(task["tier"]) if self.get_config("escalation_enabled") else None
            if promoted:
                self.conn.execute(
                    """
                    UPDATE task_instances
                    SET status='pending', tier=?, attempt=0, next_run_at=?, updated_at=?
                    WHERE id=? AND status='failed'
                    """,
                    (promoted, now, now, task["id"]),
                )
            else:
                self.dead_letter(task)
        else:
            self.conn.execute(
                """
                UPDATE task_instances
                SET status='pending', next_run_at=?, updated_at=?
                WHERE id=? AND status='failed'
                """,
                (now + backoff_seconds(task["attempt"]), now, task["id"]),
            )
        return 1

    def dead_letter(self, task: sqlite3.Row) -> None:
        trail = [
            dict(row)
            for row in self.conn.execute(
                "SELECT attempt_no, tier, outcome, failure_class, ended_at FROM attempts WHERE task_instance_id=?",
                (task["id"],),
            ).fetchall()
        ]
        now = utc_now()
        self.conn.execute(
            """
            INSERT INTO dlq(agent_id, seq, task_def_id, failure_class, last_error, attempt_trail, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task["agent_id"],
                task["seq"],
                task["task_def_id"],
                task["failure_class"],
                task["last_error"],
                encode_json(trail),
                now,
            ),
        )
        self.conn.execute("UPDATE task_instances SET status='dead', updated_at=? WHERE id=?", (now, task["id"]))
        self.conn.execute("UPDATE agents SET status='failed', updated_at=? WHERE id=?", (now, task["agent_id"]))

    def reap_expired(self, batch_size: int = 100, jitter_seconds: float = 5.0) -> list[dict[str, Any]]:
        now = utc_now()
        rows = self.conn.execute(
            "SELECT * FROM task_instances WHERE status='running' AND lease_expires < ? LIMIT ?",
            (now, batch_size),
        ).fetchall()
        reclaimed: list[dict[str, Any]] = []
        with self.transaction():
            for task in rows:
                next_run = now + random.random() * jitter_seconds
                self.conn.execute(
                    """
                    UPDATE task_instances
                    SET status='pending', lease_owner=NULL, lease_expires=NULL,
                        failure_class='INFRA', next_run_at=?, updated_at=?
                    WHERE id=? AND status='running'
                    """,
                    (next_run, now, task["id"]),
                )
                self.conn.execute(
                    """
                    INSERT INTO attempts(task_instance_id, agent_id, seq, attempt_no, tier, worker_id,
                                         outcome, failure_class, started_at, ended_at)
                    VALUES(?, ?, ?, ?, ?, ?, 'reclaimed', 'INFRA', ?, ?)
                    """,
                    (
                        task["id"],
                        task["agent_id"],
                        task["seq"],
                        task["attempt"],
                        task["tier"],
                        task["lease_owner"],
                        now,
                        now,
                    ),
                )
                reclaimed.append(dict(task))
        return reclaimed

    def reserve_idempotency(self, key: str, agent_id: str, seq: int, action_type: str) -> tuple[str, Any]:
        now = utc_now()
        with self.transaction():
            row = self.conn.execute("SELECT * FROM idempotency WHERE key=?", (key,)).fetchone()
            if row is None:
                self.conn.execute(
                    """
                    INSERT INTO idempotency(key, agent_id, seq, action_type, state, created_at, updated_at)
                    VALUES(?, ?, ?, ?, 'in_flight', ?, ?)
                    """,
                    (key, agent_id, seq, action_type, now, now),
                )
                return "reserved", None
            self.increment_counter("duplicate_actions_blocked")
            return row["state"], decode_json(row["result"], None)

    def settle_idempotency(self, key: str, result: dict[str, Any]) -> None:
        now = utc_now()
        self.conn.execute(
            "UPDATE idempotency SET state='done', result=?, updated_at=? WHERE key=? AND state='in_flight'",
            (encode_json(result), now, key),
        )

    def fail_idempotency(self, key: str, error: str) -> None:
        now = utc_now()
        self.conn.execute(
            "UPDATE idempotency SET state='failed', error=?, updated_at=? WHERE key=? AND state='in_flight'",
            (error, now, key),
        )

    def run_idempotent(
        self,
        key: str,
        agent_id: str,
        seq: int,
        action_type: str,
        fn: Callable[[], dict[str, Any]],
    ) -> tuple[str, dict[str, Any] | None]:
        state, result = self.reserve_idempotency(key, agent_id, seq, action_type)
        if state == "done":
            return "done", result
        if state == "in_flight":
            return "in_flight", None
        if state == "failed":
            return "failed", None
        try:
            fresh = fn()
        except Exception as exc:
            self.fail_idempotency(key, str(exc))
            raise
        self.settle_idempotency(key, fresh)
        return "done", fresh

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
        if row is None:
            return None
        tasks = [dict(r) for r in self.conn.execute(
            "SELECT * FROM task_instances WHERE agent_id=? ORDER BY seq", (agent_id,)
        ).fetchall()]
        agent = dict(row)
        agent["plan"] = decode_json(agent["plan"], [])
        agent["context"] = decode_json(agent["context"], {})
        agent["query_embedding"] = decode_json(agent["query_embedding"], [])
        agent["tasks"] = tasks
        return agent

    def metrics(self) -> dict[str, Any]:
        counters = {
            row["key"]: row["value"]
            for row in self.conn.execute("SELECT key, value FROM metrics_counters").fetchall()
        }
        completed = self.conn.execute("SELECT COUNT(*) AS n FROM task_instances WHERE status='succeeded'").fetchone()["n"]
        senior_completed = self.conn.execute(
            "SELECT COUNT(*) AS n FROM task_instances WHERE status='succeeded' AND tier='senior'"
        ).fetchone()["n"]
        attempts = self.conn.execute("SELECT tier, COUNT(*) AS n FROM attempts WHERE outcome='succeeded' GROUP BY tier").fetchall()
        tier_runs = {row["tier"]: row["n"] for row in attempts}
        tiered_cost = sum(TIERS[tier].cost_units * count for tier, count in tier_runs.items())
        return {
            "agents": {
                row["status"]: row["n"]
                for row in self.conn.execute("SELECT status, COUNT(*) AS n FROM agents GROUP BY status").fetchall()
            },
            "tasks": {
                row["status"]: row["n"]
                for row in self.conn.execute("SELECT status, COUNT(*) AS n FROM task_instances GROUP BY status").fetchall()
            },
            "escalation_rate": senior_completed / completed if completed else 0.0,
            "senior_tasks_completed": senior_completed,
            "total_tasks_completed": completed,
            "zombie_writes_blocked": counters.get("zombie_writes_blocked", 0),
            "duplicate_actions_blocked": counters.get("duplicate_actions_blocked", 0),
            "tasks_deduplicated": counters.get("semantic_deduplications", 0),
            "semantic_loop_blocks": counters.get("semantic_loop_blocks", 0),
            "cost_units": tiered_cost,
            "cost_comparison": {
                "all_junior": completed * TIERS["junior"].cost_units,
                "all_senior": completed * TIERS["senior"].cost_units,
                "tiered": tiered_cost,
            },
        }


def open_runtime_db(path: str | None = None) -> RuntimeDB:
    return RuntimeDB(path or os.getenv("RUNTIME_DB", "runtime.sqlite3"))
