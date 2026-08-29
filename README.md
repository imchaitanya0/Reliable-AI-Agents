# Reliable AI Agents

A distributed runtime that makes AI-agent workflows survive failure — and that
knows the difference between a failure worth retrying and a failure worth
*paying more* to fix.

```
An agent is a plan of task IDs:  agent([1, 2, 6, 8, 9])
Executed in sequence. All state in Postgres. Nothing in worker memory.

Therefore: resumable at exact task granularity, and failure becomes something
you CLASSIFY rather than blindly retry.
```

---

## The problem

An agent workflow is long-running, stateful and side-effecting. When the worker
executing it dies four minutes into a five-minute run, naive systems either lose
all progress or duplicate the external side effect on retry.

Separately: when a task fails because the model *wasn't capable enough*,
retrying it identically fails identically. Celery, Temporal, SQS and Airflow all
retry with the same configuration. That is correct for deterministic code and
wrong for a capability-bounded workload.

## The two paths

Every failure is classified and routed down exactly one of these.

| | **Recovery** | **Escalation** |
|---|---|---|
| What broke | the machine | the attempt |
| Did the agent finish reasoning? | no — killed mid-thought | yes, and failed |
| Detected by | lease expiry, no heartbeat | task returned an error |
| What changes | *who* runs it | ***how*** it runs — bigger model |
| Extra cost | none, same tier | 12×, senior tier |
| Share of failures | ~93% | ~7% |

The test is one question: *would running this identically again probably
succeed?* Yes → recovery. No → escalation.

Recovery is the substrate. Escalation is the contribution.

---

## Architecture

```
                          ┌──────────────────┐
                          │    Task API      │   stateless, N replicas
                          │    FastAPI       │   behind a load balancer
                          └────────┬─────────┘
                                   │ INSERT agents + task_instances
                                   ▼
   ┌────────────────┐    ┌──────────────────────┐    ┌────────────────────┐
   │ Orchestrator 1 │───▶│                      │◀───│  Junior pool × N   │
   │ Orchestrator 2 │───▶│      POSTGRES        │◀───│  cheap, fails 25%  │
   │ Orchestrator 3 │───▶│                      │    └────────────────────┘
   └────────────────┘    │  queue + leases      │
     stateless,          │  checkpoints         │    ┌────────────────────┐
     identical loops,    │  idempotency         │◀───│  Senior pool × 2   │
     no leader election  │  attempt log         │    │  costly, drains    │
                         └──────────────────────┘    │  escalations only  │
                            THE ONLY SHARED STATE    └────────────────────┘
```

**No component calls any other component.** Everything talks only to Postgres.
That single property delivers all three requirements at once:

- **Fault tolerant** — orchestrators are stateless and identical; kill any one
  and the rest cover. There is no leader to lose.
- **Scalable** — every process type is horizontally scalable, and the senior
  pool scales independently of the junior pool.
- **Parallel to build** — each lane's only integration surface is the schema, so
  the team works simultaneously without coordinating.

### Escalation is task-scoped, never agent-scoped

```
 agent.plan = [1, 2, 6, 8, 9]

 JUNIOR  ──①──▶──②──▶──⑥ fail ──▶──⑥ fail            ──▶──⑧──▶──⑨
                             │                      ▲
                      promote │                      │ resume
                             ▼                      │
 SENIOR                      └────────▶ ⑥ ok ───────┘
                                        ▲
                             only THIS task pays 12×
```

After the promoted task succeeds, its result is written into `agent.context`,
the cursor advances, and **task 8 claims at `tier='junior'` again.** The agent is
never permanently upgraded.

This is the entire cost argument. If promotion ever leaks onto the agent row,
cost silently converges on the all-senior baseline and the project's central
claim evaporates. There is an explicit test for it.

---

## Fault tolerance

Kill anything. What happens:

| What dies | Detected by | Recovery | Data lost |
|---|---|---|---|
| Worker mid-task | lease expiry (30s) | reaper requeues at same tier; another worker claims | none — resumes at cursor |
| Senior worker | lease expiry | same, tier preserved | none |
| Orchestrator | nothing — the other instances already run the identical loop | automatic | none |
| API replica | load balancer | another replica | none — submission is one transaction |
| Postgres | everything stalls | HA replica / restart | none committed |
| Network partition | lease expiry | task double-runs; idempotency key makes the second a no-op | none — exactly-once *effect* |

**The key admission:** you cannot distinguish a crashed worker from a slow one.
That is a real impossibility result, not a gap in the design. So the runtime
chooses **at-least-once execution** and defends against the consequence with
idempotency keys, upgrading it to **exactly-once effect**.

## Scalability

| Dimension | How it scales | Limit |
|---|---|---|
| Workers | add processes; they self-coordinate via `SKIP LOCKED`, zero config | Postgres connections |
| Orchestrators | add processes; identical loops, `SKIP LOCKED` prevents double-sweep | none practically |
| API | stateless → horizontal | none |
| Expensive compute | senior pool scales **independently**; ~7% of tasks reach it | your choice |

The scheduler is not a process. It is one SQL statement, so there is nothing to
scale.

---

## The claim query is the entire scheduler

```sql
UPDATE task_instances SET
  status='running', lease_owner=$worker,
  lease_expires=now()+$ttl, attempt=attempt+1
WHERE id = (
  SELECT t.id FROM task_instances t
  JOIN agents a ON a.id = t.agent_id
  WHERE t.status='pending'
    AND t.next_run_at <= now()      -- backoff gate
    AND t.tier = $pool_tier         -- junior pool ignores escalated work
    AND a.status = 'running'
    AND t.seq = a.cursor            -- <- the sequential dependency
  ORDER BY t.next_run_at
  FOR UPDATE SKIP LOCKED LIMIT 1    -- <- mutual exclusion, never blocks
) RETURNING *;
```

Two lines carry the design:

- **`t.seq = a.cursor`** — no task is claimable until its predecessor commits and
  advances the cursor. Dependency ordering, free. Swap this predicate for a
  `deps_satisfied` check and you have full DAG support.
- **`FOR UPDATE SKIP LOCKED`** — two workers never claim the same row and never
  wait on each other. This replaces an entire consensus protocol, which is why
  there is no leader election and no single point of failure.

## The reaper is crash recovery, in full

```sql
UPDATE task_instances SET
  status='pending', lease_owner=NULL, failure_class='INFRA',
  next_run_at = now() + backoff(attempt)
WHERE status='running' AND lease_expires < now();
```

Ten lines, running in every orchestrator instance every 2s. It requeues at the
**same tier** — a dead machine says nothing about model capability.

---

## Failure classification

```
                     task 6 fails
                          │
                          ▼
                   ┌─────────────┐
                   │  CLASSIFY   │
                   └──┬───┬───┬──┘
          ┌───────────┘   │   └───────────┐
          ▼               ▼               ▼
       INFRA         CAPABILITY        POISON
          │               │               │
          ▼               ▼               ▼
  retry SAME tier   retry same tier   dead-letter queue
  exponential       up to N, then     no retry, ever
  backoff           PROMOTE→senior    (nothing fixes it)
          │               │               │
  worker died,      model too weak,   malformed input,
  timeout, 5xx      invalid output    4xx, bad task id
          │               │               │
     cost: 0        cost: 12×         cost: 0
```

Escalating on `INFRA` would mean a `kill -9` promotes work to the expensive
model for zero benefit. Escalating on `POISON` burns senior compute on something
no model can fix. Classification is what makes the cost claim defensible.

---

## Repository layout

```
Reliable-AI-Agents/
├── common/
│   ├── failures.py        C1 · failure taxonomy      (worker ↔ orchestrator)
│   ├── protocol.py        C2 · TaskDef + TaskContext (registry ↔ executor)
│   └── config.py          shared env + tier/tool tables
├── db/
│   ├── schema.sql         C5 · THE blocking contract
│   └── pool.py            connection pool
├── api/                   Lane B · POST /agents, /metrics, /chaos/*
├── orchestrator/          Lane F · reaper, classify, promote
│   ├── reaper.py            lease expiry sweep      (recovery path)
│   ├── classify.py          INFRA | CAPABILITY | POISON
│   └── promote.py           tier promotion          (escalation path)
├── worker/                Lane C · claim, heartbeat, execute, checkpoint
├── tasks/                 Lane A · registry, mock tools, simulated tiers
├── dash/                  Lane D · dashboard (builds against a JSON fixture)
├── chaos/                 Lane E · fault-injection harness
└── tests/                 reliability invariants
```

## Contracts — freeze these first

Five interfaces. Freeze them and nobody needs to coordinate again; everything
else is an implementation detail owned by exactly one person.

| | Contract | File | Seam |
|---|---|---|---|
| **C1** | Failure taxonomy | `common/failures.py` | worker raises ↔ orchestrator routes |
| **C2** | TaskDef protocol | `common/protocol.py` | task authors ↔ executor |
| **C3** | HTTP surface | `api/main.py` | API ↔ chaos harness |
| **C4** | `/metrics` payload | `dash/fixture.json` | API ↔ dashboard |
| **C5** | Database schema | `db/schema.sql` | **everyone** |

**Integration rule.** A lane is finished when its contract holds, not when it
looks right. Lane C and Lane F never read each other's code — they meet only at
the three strings in C1. If you need to know how another lane works internally,
a contract is missing: add it here rather than reaching across the boundary.

## Workstreams

```
 T0 ─────────────────────── schema frozen (~45min) ──────────────────────▶

 Lane 0  schema · db pool        ████████
 Lane A  tasks · tools · tiers   ████████████████████████        no DB needed
 Lane D  dashboard               ██████████████████████████      mocked metrics
 Lane E  chaos · tests           ████████████████████████████    HTTP contract
 Lane B  task API                        ░░░░░░░░░░░░░░░░░░
 Lane C  worker                          ░░░░░░░░░░░░░░░░░░░░░░
 Lane F  orchestrator                    ░░░░░░░░░░░░░░░░░░░░░░░░
```

Lanes A, D and E need nothing from anybody and start immediately. Only three of
seven people ever wait on the schema.

## Build order

| # | Deliverable | Done when |
|---|---|---|
| 1 | schema + API + registry + claim loop | 20 agents complete the happy path |
| 2 | **leases + heartbeat + reaper + resume** | **`kill -9` a worker → 20/20 ✓ — stop and verify here** |
| 3 | classifier + backoff + DLQ | forced tool failure classifies correctly |
| 4 | **tier promotion + senior pool** | hard task → senior → agent resumes on junior |
| 5 | idempotency keys | duplicate actions = 0 |
| 6 | dashboard + three-way benchmark | the cost comparison runs live |
| 7 | *stretch* multi-orchestrator | kill one, nothing changes |
| 8 | *stretch* real models · DAG plans | Haiku junior, Opus senior |

Phase 2 de-risks everything. A working crash-recovery demo early beats a
half-finished everything late.

---

## What we have to prove

| Strategy | Completion | Cost units | Reading |
|---|---|---|---|
| all junior | low | 1× | cheap, unreliable |
| all senior | high | ~12× | reliable, wasteful |
| **tiered (ours)** | **= all senior** | **~2×** | only ~7% escalated |

### Invariants — automated fault injection, not screenshots

1. A task whose lease stops being renewed becomes claimable again.
2. `SIGKILL` a worker mid-task → no agent lost, each resumed at its own cursor.
3. Forced double execution of a side-effecting task → exactly one external action.
4. `seq=n` never starts before `seq=n-1` commits, asserted from `attempts` timestamps.
5. `INFRA` never promotes; `CAPABILITY` promotes only after same-tier attempts are exhausted; `POISON` never reaches senior.
6. A promoted task's successor claims at `tier='junior'` — promotion does not leak onto the agent.
7. An escalated task receives the full accumulated context from every prior task.

### Demo sequence

1. Submit 20 agents, plan `[1,2,6,8,9]`.
2. `docker kill worker-2` mid-flight.
3. Reaper fires at T+30s → each resumes at **its own** cursor.
   → *4 tasks re-executed, 47 avoided.*
4. Hard task fails twice on junior → promotes → senior succeeds → back to junior.
5. Three-way cost table, live via `/chaos/config`.
6. `jira` failure rate to 1.0 → retries, backoff, dead-letter queue.
7. `docker kill orchestrator-1` → nothing changes.

---

## Getting started

```bash
docker compose up -d postgres
psql "$DATABASE_URL" -f db/schema.sql

pip install -r requirements.txt

uvicorn api.main:app --reload          # Task API
python -m orchestrator.main            # reaper + classifier
POOL_TIER=junior python -m worker.main # junior worker
POOL_TIER=senior python -m worker.main # senior worker
```

---

## Design notes

- **Postgres over Redis, deliberately.** Real transactions are what make
  exactly-once-effect and ordering guarantees true rather than merely likely.
- **Mock tools over real APIs, deliberately.** The demo must be fast,
  reproducible and independent of network conditions on stage.
- **Budgets are accounting only.** `cost_units` and `tokens_used` accumulate so
  the benchmark has real numbers; nothing terminates an agent for exceeding them.
- **Keep the agent thin.** The infrastructure is the product. The agent is a
  workload generator that happens to fail in interesting ways.

> The agent did not become smarter. The system learned which failures were worth
> paying to fix.
