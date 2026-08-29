# Reliable AI Agents
**A distributed runtime that keeps AI-agent workflows alive through failure — and knows which failures are worth paying to fix.**

Repo: https://github.com/imchaitanya0/Reliable-AI-Agents · Track: Distributed Systems · 93 automated tests passing

---

## The Problem

An agent workflow is a distributed workload, not a request: long-running, stateful, side-effecting. When a worker dies four minutes into a five-minute run, naive systems either lose all progress or duplicate the side effect on retry.

Worse, **every existing runtime — Celery, Temporal, SQS, Airflow — retries with the identical configuration.** For deterministic code that is correct. For a capability-bounded workload it is wrong: if a model failed because it wasn't strong enough, running it again buys the same failure at full price — while retrying a `kill -9` on an expensive model wastes money on a problem that was never about capability. Nobody distinguishes these cases.

## The Solution

An agent is a **plan of task IDs** — `agent([1, 10, 21, 40, 41])` — executed in sequence, with all state in Postgres. Because the plan is data rather than code, an agent is one database row: serializable and resumable at exact task granularity. Temporal must replay non-deterministic code and needs a sandbox to do it; we don't, because our program is a list of integers.

On that substrate, **failures are classified before they are retried**:

| Class | Cause | Action | Cost |
|---|---|---|---|
| `INFRA` | worker killed, timeout, 5xx | retry **same tier**, backoff | 0 |
| `CAPABILITY` | invalid output, model too weak | retry, then **promote to senior tier** | 12× |
| `POISON` | malformed input, 4xx | dead-letter immediately | 0 |

Escalating on `INFRA` would promote a crashed machine to the expensive model for zero benefit. Escalating on `POISON` burns premium compute on something no model can fix. **This classification is what makes the cost claim defensible.**

Critically, **promotion is scoped to the task, not the agent.** When task 21 escalates, the senior tier runs that one task with the same accumulated context — then task 40 claims back at junior. The agent is never permanently upgraded. That single decision is the entire cost argument.

## Architecture

```
                          +--------------------+
                          |     Task API       |  stateless, N replicas
                          +---------+----------+
                                    | INSERT agents + task_instances
                                    v
  +-----------------+    +--------------------------+    +---------------------+
  | Orchestrator 1  |--->|                          |<---|  JUNIOR POOL x N    |
  | Orchestrator 2  |--->|        POSTGRES          |<---|  cheap tier         |
  | Orchestrator 3  |--->|                          |    +---------------------+
  +-----------------+    |   durable queue          |
    stateless,           |   leases + checkpoints   |    +---------------------+
    identical loops,     |   idempotency ledger     |<---|  SENIOR POOL x 2    |
    NO leader election   |   attempt evidence log   |    |  escalations only   |
                         +--------------------------+    +---------------------+
                            THE ONLY SHARED STATE
```

**No component calls any other component.** Everything talks only to Postgres. That one property gives fault tolerance (orchestrators are interchangeable — kill any one), horizontal scalability (the senior pool scales independently of the junior pool), and parallel development (the schema is the only integration surface).

**The scheduler is a single SQL statement** — there is no scheduler process:

```sql
UPDATE task_instances SET status='running', lease_owner=$w, lease_expires=now()+$ttl
WHERE id = (SELECT t.id FROM task_instances t JOIN agents a ON a.id=t.agent_id
  WHERE t.status='pending' AND t.tier=$pool_tier AND t.seq = a.cursor
  ORDER BY t.next_run_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *;
```

Two lines carry the design. **`t.seq = a.cursor`** gives dependency ordering for free — no task is claimable until its predecessor commits. **`FOR UPDATE SKIP LOCKED`** means two workers never claim the same row and never block each other — **this replaces an entire consensus layer.** No Raft, no Zookeeper, no leader election.

**Exactly-once effect.** You cannot distinguish a crashed worker from a slow one — a real impossibility result — so we don't try. We reclaim on lease expiry and defend in two layers: the checkpoint is fenced on `lease_owner` and on `cursor` (a reaped worker cannot commit; a replayed checkpoint cannot double-advance), and side-effecting tasks guard on `sha256(agent_id:seq:action)`. At-least-once delivery becomes exactly-once effect.

## Implementation Plan

Eight phases, each gated on a demonstrable outcome. The schema was frozen first as the only integration surface, so three workstreams needing no database access began immediately.

| # | Deliverable | Verified by |
|---|---|---|
| 1 | Schema (8 tables), contracts, connection pool | Applies cleanly on Postgres 16 |
| 2 | Worker: claim, lease, heartbeat, fenced checkpoint | `kill -9` mid-task loses nothing |
| 3 | Failure classifier, backoff, dead-letter queue | Forced tool failure classifies correctly |
| 4 | Tier promotion + senior worker pool | Hard task escalates, succeeds, agent resumes at junior |
| 5 | Idempotency keys on side-effecting tasks | Forced double execution → one action |
| 6 | Task API, metrics, live dashboard | 20 agents submitted over HTTP complete |
| 7 | 23 tasks, 9 tools (5 real), runtime-composable pipelines | Custom pipeline composed and executed |
| 8 | Orchestrator autoscaling within a connection budget | Scales on reaper backlog, capped by connections |

**Extensibility is data, not code.** Add a capability tier with one `INSERT`; add a task with one decorator; compose a pipeline at runtime with no code at all.

## Results

**93 automated tests** against real Postgres — including one that spawns real worker processes and `SIGKILL`s one mid-flight. Verified invariants: an unrenewed lease becomes claimable; a reaped worker cannot commit a stale result; task *n* never starts before *n−1* commits; concurrent workers never claim the same row; forced double execution yields exactly one external action; `INFRA` never promotes and `POISON` never reaches senior; promotion never leaks onto the next task.

Measured on a live 20-agent run with a worker killed mid-flight:

```
agents completed  20/20      cost (tiered, ours)    166 units
leases reclaimed      1      all-senior baseline   1200 units  -> 0.14x
promoted to senior    6      all-junior baseline    100 units  -> but 6 tasks never finish
```

Same completion rate as all-senior, at 14% of the price. The all-junior baseline is cheaper **and broken** — the escalated tasks would never have completed on it.

## Known Limitations

**Capability tiers are simulated** — there is no LLM in the system; `junior`/`senior` are cost and capability stand-ins, and the `tiers.model` column exists and is null. **Idempotency has a reserve-then-act window** — a crash between reserving the key and completing the action loses that action; closing it properly needs a transactional outbox. **The ~7% escalation rate is a workload property, not a constant.** Also unimplemented: multi-tenancy, admission control, and true DAG fan-out (plans are sequential; that is a one-predicate change).

---

**The agent did not become smarter. The system around it became reliable, and learned which failures were worth paying to fix.**
