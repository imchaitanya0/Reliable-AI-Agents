"""
CONTRACT C2 — TASK DEFINITION PROTOCOL
======================================

The seam between Lane A (writes task implementations) and Lane C (executes
them). Lane A never imports the worker; Lane C never imports the registry
except through `TASK_DEFS`.

THE CENTRAL IDEA
----------------
An agent is a PLAN OF TASK IDS — `agent([1, 2, 6, 8, 9])` — executed in
sequence. Because the plan is data rather than code, the agent is fully
serializable and resumable at exact task granularity.

That is what lets this project skip the hardest problem in durable execution.
Temporal has to replay nondeterministic code and needs a deterministic-execution
sandbox to do it. We don't, because our "program" is a static list of ids.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

Tier = Literal["junior", "senior"]
Difficulty = Literal["easy", "hard"]


@dataclass
class TaskContext:
    """
    Everything a task implementation is allowed to see.

    `prior` carries the accumulated results of every completed task in the plan,
    keyed by seq. A PROMOTED task receives the SAME `prior` dict as the attempt
    that failed — senior must solve the identical problem, not a different one.
    This is an invariant worth testing explicitly.
    """

    agent_id: str
    seq: int
    tier: Tier
    prior: dict[int, dict[str, Any]] = field(default_factory=dict)

    # Injected by the executor. Wraps sha256(agent_id:seq:action_type) so a task
    # never has to know how keys are derived.
    idem_key: Callable[[str], str] | None = None

    def key_for(self, action_type: str) -> str:
        """
        Deterministic idempotency key for an externally visible action.

        Both the original attempt and any recovery attempt compute the SAME key,
        which is what turns at-least-once delivery into exactly-once effect.
        """
        if self.idem_key is not None:
            return self.idem_key(action_type)
        raw = f"{self.agent_id}:{self.seq}:{action_type}"
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class TaskDef:
    """
    One entry in the registry. Extending the system with a new capability is a
    single addition to `TASK_DEFS` — no runtime change anywhere. That is the
    extensibility requirement.
    """

    id: int
    name: str
    run: Callable[[TaskContext], dict[str, Any]]

    # "hard" tasks DETERMINISTICALLY fail on junior and succeed on senior.
    # Probability would make the escalation demo a coin flip on stage; this
    # makes it reproducible, which matters when presenting live.
    difficulty: Difficulty = "easy"

    # True => the implementation MUST guard its external action with
    # ctx.key_for(...). Reclaim-on-timeout guarantees that a slow-but-alive
    # worker and its replacement will sometimes run the same task concurrently.
    side_effecting: bool = False

    # Which mock tool this task drives, if any. Used by the chaos harness to
    # target failure injection.
    tool: str | None = None
