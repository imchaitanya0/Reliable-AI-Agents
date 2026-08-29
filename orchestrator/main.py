from __future__ import annotations

import os
import time

from db.pool import open_runtime_db
from orchestrator.classify import route_reported_failures
from orchestrator.reaper import reap_expired_leases


def run_once(db, batch_size: int = 100) -> dict[str, int]:
    reclaimed = reap_expired_leases(db, batch_size=batch_size)
    routed = route_reported_failures(db)
    return {"reclaimed": len(reclaimed), "routed": routed}


def main() -> None:
    db = open_runtime_db()
    interval = float(os.getenv("ORCHESTRATOR_INTERVAL_SECONDS", "2"))
    print(f"[orchestrator] running db={db.path}", flush=True)
    while True:
        result = run_once(db)
        if result["reclaimed"] or result["routed"]:
            print(f"[orchestrator] reclaimed={result['reclaimed']} routed={result['routed']}", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
