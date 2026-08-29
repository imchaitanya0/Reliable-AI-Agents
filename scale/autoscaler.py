#!/usr/bin/env python3
"""
Orchestrator autoscaler -- one orchestrator per N active agents.

    python -m scale.autoscaler                 # watch and scale
    python -m scale.autoscaler --once          # one decision, then exit
    python -m scale.autoscaler --per-agent 5   # tighter ratio

WHY THE ORCHESTRATOR IS WORTH SCALING AT ALL
--------------------------------------------
It is not CPU-bound on agent count -- it is a sweep loop running a handful of
bounded queries every couple of seconds. What it IS bound by is the batch limit:
each instance reclaims at most REAPER_BATCH expired leases per tick. When a
spike strands more leases than that, recovery falls behind, and the only way to
widen the pipe is more instances sweeping in parallel. `FOR UPDATE SKIP LOCKED`
means they never collide, so this is safe to do without coordination.

THE CONSTRAINT THAT MAKES A NAIVE RATIO DANGEROUS
--------------------------------------------------
Postgres connections are a global budget shared by every replica of every
service. One-per-ten-agents with no ceiling means 1,000 agents asks for 100
orchestrators; at even a small pool each that is thousands of connections
against a default `max_connections` of 100. The database then starts refusing
connections -- including the ones the orchestrators need to recover the very
backlog that triggered the scale-up. The autoscaler would have caused the
outage it was meant to absorb.

So the ratio is the policy and MAX_REPLICAS is the safety limit, and the limit
is derived from the connection budget rather than guessed. Scaling is also
deliberately asymmetric: up fast (a spike is happening now), down slow (a lull
may be a gap between bursts, and thrashing containers helps nobody).
"""

from __future__ import annotations

import argparse
import logging
import math
import shutil
import subprocess
import sys
import time

from db.pool import pool

log = logging.getLogger("autoscaler")

DEFAULTS = {
    "per_agent": 10,      # one orchestrator per N active agents
    "min_replicas": 1,    # never zero: with none, nothing reclaims a lease
    "max_replicas": 10,   # the connection budget, see below
    "interval": 5.0,
    "cooldown_down": 30.0,  # seconds a lull must persist before scaling in
    "service": "orchestrator",
}

# An orchestrator sweeps sequentially, so DB_POOL_MAX=2 is ample. 10 replicas is
# then ~20 connections. Beside the worker pools and the API that lands near the
# default max_connections of 100, which is why compose raises it to 200: the
# ceiling and the connection budget must move together, or scaling up is what
# takes the database down.
CONNECTIONS_PER_REPLICA = 2

# Admission control caps active agents, so the autoscaler ceiling is reached
# exactly when the system is full: 100 agents / 10 per orchestrator = 10.
# Keeping them consistent means the ceiling is never the thing that silently
# limits recovery -- the agent cap is.
MAX_ACTIVE_AGENTS = 100

ACTIVE_AGENTS_SQL = "SELECT count(*) AS n FROM agents WHERE status = 'running'"

# The signal that actually means "recovery is falling behind": leases that have
# expired and not yet been reclaimed. Agent count is the requested policy;
# this is the evidence that the policy is working.
BACKLOG_SQL = """
SELECT count(*) AS n FROM task_instances
WHERE status = 'running' AND lease_expires < now()
"""


class Autoscaler:
    def __init__(self, **opts) -> None:
        self.cfg = {**DEFAULTS, **{k: v for k, v in opts.items() if v is not None}}
        self._last_scale_down = 0.0
        self._current: int | None = None

    # -- observation ----------------------------------------------------------

    def active_agents(self) -> int:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(ACTIVE_AGENTS_SQL)
            return cur.fetchone()["n"]

    def reaper_backlog(self) -> int:
        with pool().connection() as conn, conn.cursor() as cur:
            cur.execute(BACKLOG_SQL)
            return cur.fetchone()["n"]

    def running_replicas(self) -> int:
        out = subprocess.run(
            ["docker", "compose", "ps", "-q", self.cfg["service"]],
            capture_output=True, text=True, check=False,
        )
        return len([line for line in out.stdout.splitlines() if line.strip()])

    # -- decision -------------------------------------------------------------

    def desired_replicas(self, agents: int, backlog: int) -> int:
        """
        One orchestrator per `per_agent` active agents, clamped to the
        connection budget.

        Rounded UP, so any active work guarantees at least one instance: with
        zero orchestrators nothing reclaims an expired lease, and the runtime
        keeps working while quietly losing the ability to heal.
        """
        wanted = math.ceil(agents / self.cfg["per_agent"]) if agents else 0
        wanted = max(self.cfg["min_replicas"], wanted)

        # A backlog means the reaper is already behind, so do not wait for the
        # agent count to justify the capacity that is needed right now.
        if backlog > 0:
            wanted = max(wanted, math.ceil(backlog / 50) + 1)

        return min(wanted, self.cfg["max_replicas"])

    def apply(self, replicas: int) -> bool:
        """
        Scale the service. Returns True if anything changed.

        --no-recreate matters: without it, adjusting the orchestrator count
        would also recreate the workers, which in the middle of a demo looks
        exactly like the crash we are supposed to be recovering from.
        """
        current = self.running_replicas()
        self._current = current
        if replicas == current:
            return False

        now = time.monotonic()
        if replicas < current:
            if (now - self._last_scale_down) < self.cfg["cooldown_down"]:
                log.debug("scale-in suppressed: cooling down")
                return False
            self._last_scale_down = now

        log.info(
            "scaling %s: %d -> %d", self.cfg["service"], current, replicas
        )
        subprocess.run(
            ["docker", "compose", "up", "-d", "--no-recreate",
             f"--scale", f"{self.cfg['service']}={replicas}"],
            capture_output=True, text=True, check=False,
        )
        return True

    def tick(self) -> dict:
        agents = self.active_agents()
        backlog = self.reaper_backlog()
        want = self.desired_replicas(agents, backlog)
        changed = self.apply(want)
        return {
            "agents": agents,
            "backlog": backlog,
            "replicas": self._current,
            "desired": want,
            "changed": changed,
            "capped": want == self.cfg["max_replicas"],
        }

    def run(self) -> int:
        log.info(
            "autoscaler up: 1 orchestrator per %d agents, %d..%d replicas "
            "(~%d connections at ceiling)",
            self.cfg["per_agent"], self.cfg["min_replicas"],
            self.cfg["max_replicas"],
            self.cfg["max_replicas"] * CONNECTIONS_PER_REPLICA,
        )
        while True:
            try:
                state = self.tick()
                if state["changed"]:
                    log.info("%s", state)
                if state["capped"] and state["backlog"] > 0:
                    log.warning(
                        "at the replica ceiling (%d) with a backlog of %d -- "
                        "raise max_replicas AND the Postgres connection budget "
                        "together, never one alone",
                        self.cfg["max_replicas"], state["backlog"],
                    )
            except Exception:
                log.exception("autoscaler tick failed; continuing")
            time.sleep(self.cfg["interval"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--per-agent", type=int, help="agents per orchestrator")
    ap.add_argument("--min-replicas", type=int)
    ap.add_argument("--max-replicas", type=int)
    ap.add_argument("--interval", type=float)
    ap.add_argument("--once", action="store_true", help="one decision, then exit")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s autoscaler: %(message)s"
    )

    if shutil.which("docker") is None:
        log.error("docker not on PATH -- the autoscaler runs on the host, not in a container")
        return 1

    scaler = Autoscaler(
        per_agent=args.per_agent,
        min_replicas=args.min_replicas,
        max_replicas=args.max_replicas,
        interval=args.interval,
    )

    if args.once:
        print(scaler.tick())
        return 0
    return scaler.run()


if __name__ == "__main__":
    sys.exit(main())
