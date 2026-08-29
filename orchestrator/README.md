# Lane F — Orchestrator

**Owner:** _unclaimed_ · **Starts:** after the schema merges · **Depends on:** C1, C5

## Deliverable
```
reaper.py     lease-expiry sweep            <- the RECOVERY path
classify.py   INFRA | CAPABILITY | POISON
promote.py    tier promotion + re-enqueue   <- the ESCALATION path
main.py       runs every loop; N identical instances
```

## Two rules
1. **The orchestrator never executes tasks.** It classifies and routes.
   Escalation is re-enqueueing at `tier='senior'`, which a separate pool drains.
   If the orchestrator executed, it would be slow, stateful and a bottleneck —
   which would undo the no-single-point-of-failure property.

2. **No leader election.** Every instance runs the identical loop.
   `FOR UPDATE SKIP LOCKED` guarantees two instances never grab the same row.
   Run three; kill one on stage; nothing changes.

## Routing
| Class | Response |
|---|---|
| `INFRA` | retry same tier, exponential backoff |
| `CAPABILITY` | retry same tier up to `max_attempts_per_tier`, then promote |
| `POISON` | dead-letter queue, no retry at any tier |

## Done when
Expired leases requeue; capability failures promote; poison never reaches senior.
