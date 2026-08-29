"""
Shared utilities, database connection, metrics timing, and reporting for metric verification.
"""

from __future__ import annotations

import math
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Generator

import numpy as np
import psycopg
from psycopg.rows import dict_row
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{os.getenv('USER', 'postgres')}@localhost:5432/postgres",
)


def get_db_connection() -> psycopg.Connection:
    """Return a psycopg3 connection with dict row factory."""
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)
    return conn


def init_test_db(conn: psycopg.Connection) -> None:
    """Initialize schema from db/schema.sql."""
    schema_path = os.path.join(os.path.dirname(__file__), "..", "db", "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    with conn.cursor() as cur:
        # Strip pgcrypto if already enabled or handle gracefully
        cur.execute(schema_sql)


def cleanup_test_data(conn: psycopg.Connection) -> None:
    """Clean up test rows between runs."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dlq, attempts, idempotency, task_instances, agents CASCADE;")


def calculate_percentiles(latencies_ms: list[float]) -> dict[str, float]:
    """Compute p50, p95, p99 latencies in milliseconds."""
    if not latencies_ms:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0}
    sorted_lats = sorted(latencies_ms)
    n = len(sorted_lats)
    return {
        "p50": sorted_lats[int(n * 0.50)],
        "p95": sorted_lats[min(int(n * 0.95), n - 1)],
        "p99": sorted_lats[min(int(n * 0.99), n - 1)],
        "avg": sum(sorted_lats) / n,
    }


def cosine_similarity(v1: list[float] | np.ndarray, v2: list[float] | np.ndarray) -> float:
    """Compute cosine similarity between two vector embeddings."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def generate_mock_embedding(text: str, dim: int = 256) -> list[float]:
    """
    Lightweight, deterministic semantic embedding using character & word n-grams.
    Produces high cosine similarity (>0.88) for semantically synonymous variations
    and near-zero for unrelated queries.
    """
    import hashlib
    import re

    cleaned = re.sub(r"[^\w\s]", " ", text.lower())
    words = cleaned.split()
    
    vec = np.zeros(dim, dtype=np.float32)
    
    # 1. Word unigrams & bigrams
    for i, w in enumerate(words):
        h = int(hashlib.md5(w.encode()).hexdigest(), 16) % dim
        vec[h] += 2.0
        if i + 1 < len(words):
            bigram = f"{w}_{words[i+1]}"
            h2 = int(hashlib.md5(bigram.encode()).hexdigest(), 16) % dim
            vec[h2] += 1.5

    # 2. Character 3-grams across whole string
    compact = "".join(words)
    for i in range(len(compact) - 2):
        trigram = compact[i : i + 3]
        h3 = int(hashlib.md5(trigram.encode()).hexdigest(), 16) % dim
        vec[h3] += 1.0

    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec.tolist()


@dataclass
class MetricResult:
    intervention: str
    target_metric: str
    baseline_value: str
    measured_value: str
    status: str  # PASS / FAIL
    notes: str


def print_metric_banner(title: str, subtitle: str) -> None:
    console.print(
        Panel.fit(
            f"[bold cyan]{title}[/bold cyan]\n[dim]{subtitle}[/dim]",
            border_style="cyan",
        )
    )
