# Lane C — Worker

**Owner:** _unclaimed_ · **Starts:** after the schema merges · **Depends on:** C1, C2, C5

## Deliverable
```
claim.py      the SKIP LOCKED claim query, scoped to POOL_TIER
heartbeat.py  background lease renewal every LEASE_TTL/3
executor.py   runs a TaskDef against the agent's accumulated context
main.py       claim -> heartbeat -> execute -> checkpoint -> repeat
```

## The rule that makes recovery work
**The worker never holds state that is not committed.** After every task:
write the result, advance `agents.cursor`, merge into `agents.context`, and
create the next `task_instance` at `tier='junior'` — all in one transaction.

If the process dies at any instruction, the committed prefix is intact and the
lease simply expires.

## Failure handling
Raise from `common/failures.py` and let Lane F route it. The worker does not
decide retries or promotions — it only reports which of the three classes
occurred.

## Done when
`kill -9` mid-task loses nothing: the agent resumes at its own cursor.
