"""
CLI utility and helper to initialize the database schema.
"""

from __future__ import annotations

import os
import sys

import psycopg

from common.config import DATABASE_URL


def init_database() -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    print(f"Connecting to {DATABASE_URL}...")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
    print("Database schema successfully applied.")


if __name__ == "__main__":
    init_database()
