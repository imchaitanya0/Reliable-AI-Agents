# Lane C — Worker

**Owner:** karthik · **Status:** built, 15 invariant tests green · **Depends on:** C1, C2, C5

```
claim.py      the SKIP LOCKED claim query, scoped to POOL_TIER
heartbeat.py  background lease renewal every LEASE_TTL/3
executor.py   runs a TaskDef; guards side-effecting actions with an idem key
main.py       claim -> heartbeat -> execute -> checkpoint -> repeat
```

## The rule that makes recovery work

**The worker never holds state that is not committed.** Each task ends in one
transaction that marks the row succeeded, merges the result into
`agents.context` and advances `agents.cursor`. Die at any instruction and the
committed prefix is intact; the lease expires and another worker resumes at
exactly the next task.

## Two fences on the checkpoint

Reclaim-on-timeout means a slow-but-alive worker and its replacement can run the
same task at once. Both writes are therefore conditional:

| Fence | Guards against |
|---|---|
| `AND lease_owner = %(worker_id)s` | a worker whose task was reaped committing anyway |
| `AND cursor = %(seq)s` | a replayed checkpoint double-advancing the cursor |

If either affects zero rows the transaction rolls back and the result is
discarded. `test_stale_worker_checkpoint_is_discarded` proves it.

## What the worker does NOT do

It never decides retries or promotions. It moves rows into `succeeded` or
`failed` and records which of the three classes in C1 occurred. Every routing
decision belongs to the orchestrator — that separation is what keeps that
component stateless and horizontally scalable.

## Contract note for Lane B (API)

The worker's checkpoint never INSERTs a task row. **The API must create the
agent AND every `task_instance` of its plan in one transaction, all at
`tier='junior'`.** The claim query's `t.seq = a.cursor` predicate is what gates
execution order, so nothing runs early. This also makes invariant 6 structural:
a promoted task cannot leak its tier onto its successor, because the successor
row already exists at `junior`.

## Tests

```bash
docker compose up -d postgres
pytest tests/ -q -s
```

`tests/conftest.py` carries a fixture registry so this suite never collides with
Lane A. `tests/test_crash_recovery.py` spawns real worker processes and
`SIGKILL`s one mid-flight — the demo, as a test.
