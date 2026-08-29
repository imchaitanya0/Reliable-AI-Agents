from __future__ import annotations

from db.pool import RuntimeDB


def route_reported_failures(db: RuntimeDB) -> int:
    return db.route_failures()
