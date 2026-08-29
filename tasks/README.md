# Lane A — Tasks, tools and tiers

**Owner:** _unclaimed_ · **Starts:** T0, immediately · **Depends on:** C2 only

Pure functions. **Zero database access.** This lane is unblocked from minute one.

## Deliverable
```
registry.py   TASK_DEFS: dict[int, TaskDef] — extend by adding an entry
tools.py      mock github / logs / jira with injectable failure + latency
tiers.py      simulated junior / senior execution
```

## Mock tools
| Tool | Latency | Failure rate | Side-effecting |
|---|---|---|---|
| github | 300 ms | 2% | no |
| logs | 800 ms | 5% | no |
| jira | 500 ms | 10% | **yes** — needs an idempotency key |

## The one thing that must be deterministic
Tasks marked `difficulty="hard"` must **always** fail on junior and **always**
succeed on senior. Probability would make the escalation demo a coin flip on
stage; determinism makes it reproducible.

## Done when
9 task defs run standalone, and `hard` deterministically fails on junior.
