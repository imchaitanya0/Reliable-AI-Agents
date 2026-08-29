from __future__ import annotations

import json
import os
import time
from pathlib import Path

from db.pool import open_runtime_db


def render(metrics: dict) -> str:
    return "\n".join(
        [
            "Reliable AI Agent Runtime",
            f"Escalation rate: {metrics['escalation_rate']:.1%}",
            f"Zombie writes blocked: {metrics['zombie_writes_blocked']}",
            f"Duplicate actions blocked: {metrics['duplicate_actions_blocked']}",
            f"Semantic deduplications: {metrics['tasks_deduplicated']}",
            f"Cost units: {metrics['cost_units']} "
            f"(junior={metrics['cost_comparison']['all_junior']}, "
            f"senior={metrics['cost_comparison']['all_senior']}, "
            f"tiered={metrics['cost_comparison']['tiered']})",
        ]
    )


def main() -> None:
    if os.getenv("DASH_FIXTURE"):
        metrics = json.loads(Path(os.getenv("DASH_FIXTURE", "dash/fixture.json")).read_text())
        print(render(metrics))
        return
    db = open_runtime_db()
    while True:
        print("\033[2J\033[H" + render(db.metrics()))
        time.sleep(1)


if __name__ == "__main__":
    main()
