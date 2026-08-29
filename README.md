# Reliable AI Agents

A distributed runtime that makes AI-agent workflows survive failure — and that
knows the difference between a failure worth retrying and a failure worth
**paying more** to fix.

```
An agent is a plan of task IDs:  agent([1, 10, 21, 40, 41])
Executed in sequence. All state in Postgres. Nothing in worker memory.

Therefore: resumable at exact task granularity, and failure becomes something
you CLASSIFY rather than blindly retry.
```

**Status:** 93 tests passing · 23 tasks · 9 tools (5 with real implementations) ·
6 pipelines · 8 tables

---

## Quickstart

```bash
docker compose up -d postgres            # schema auto-applies on first boot
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q     # 93 tests

# run it
python -m chaos.harness workers junior=4 senior=2 --orchestrators 2 &
python -m chaos.harness seed 20 --pipeline full-incident
python -m chaos.harness watch            # live dashboard
```

Then `kill -9` any worker and watch every agent still finish.

Or via the API:

```bash
uvicorn api.main:app --port 8000
curl -X POST localhost:8000/agents -H 'content-type: application/json' \
     -d '{"plan":[1,10,21,40,41],"count":20}'
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

The test is one question: *would running this identically again probably
succeed?* Yes → recovery. No → escalation.

Recovery is the substrate. Escalation is the contribution.

---

## Architecture

```
                          ┌──────────────────┐
                          │    Task API      │   stateless, N replicas
                          │    FastAPI       │
                          └────────┬─────────┘
                                   │ INSERT agents + task_instances
                                   ▼
   ┌────────────────┐    ┌──────────────────────┐    ┌────────────────────┐
   │ Orchestrator 1 │───▶│                      │◀───│  Junior pool × N   │
   │ Orchestrator 2 │───▶│      POSTGRES        │◀───│  cheap tier        │
   │ Orchestrator 3 │───▶│                      │    └────────────────────┘
   └────────────────┘    │  queue + leases      │
     stateless,          │  checkpoints         │    ┌────────────────────┐
     identical loops,    │  idempotency         │◀───│  Senior pool × 2   │
     no leader election  │  attempt log         │    │  escalations only  │
                         └──────────────────────┘    └────────────────────┘
                            THE ONLY SHARED STATE
```

**No component calls any other component.** Everything talks only to Postgres.
That single property delivers all three requirements at once:

- **Fault tolerant** — orchestrators are stateless and identical; kill any one
  and the rest cover. There is no leader to lose.
- **Scalable** — every process type is horizontally scalable, and the senior
  pool scales independently of the junior pool.
- **Parallel to build** — each lane's only integration surface is the schema.

### Escalation is task-scoped, never agent-scoped

```
 agent.plan = [1, 10, 21, 40, 41]

 JUNIOR  ──①──▶──⑩──▶──㉑ fail ──▶──㉑ fail            ──▶──㊵──▶──㊶
                              │                       ▲
                       promote │                       │ resume
                              ▼                       │
 SENIOR                       └────────▶ ㉑ ok ───────┘
                                          ▲
                               only THIS task pays 12×
```

After the promoted task succeeds, its result is written into `agent.context`,
the cursor advances, and **the next task claims at `tier='junior'` again.**

This is the entire cost argument. If promotion ever leaked onto the agent row,
cost would silently converge on the all-senior baseline. There is an explicit
test for it (`test_promotion_does_not_leak_to_successor`).

---

## Design choices, and why

| Choice | Why | Cost of the alternative |
|---|---|---|
| **Postgres as the only shared state** | Real transactions make exactly-once-effect and ordering guarantees *true*, not merely likely | Redis is faster but you lose the transactional fencing the whole design rests on |
| **`FOR UPDATE SKIP LOCKED` instead of consensus** | Two workers never claim the same row and never block each other. Coordination lives in the transaction layer | Raft/Zookeeper = a leader to elect, lose and debug |
| **The scheduler is a SQL query, not a process** | Nothing to scale, nothing to fail over | A scheduler process is a single point of failure |
| **Orchestrator classifies but never executes** | Keeps it stateless, so N instances are interchangeable | An executing orchestrator becomes a stateful bottleneck |
| **Worker reports, orchestrator decides** | Separates "what happened" from "what it means" | Routing logic in the worker means every worker carries policy |
| **Tier ladder in a table, not in code** | Adding a capability tier is one `INSERT` | Hardcoding drifts across three files |
| **Mock tools by default, live opt-in** | A live third party during a demo is a dependency you don't control | A rate limit on stage looks like your bug |
| **`difficulty="hard"` fails deterministically** | The escalation demo is reproducible | Probabilistic failure makes the demo a coin flip |
| **All task rows created up front** | Makes "promotion doesn't leak" *structural*, not a rule to remember | Lazy creation lets tier leak forward silently |
| **Budgets are accounting only** | Out of scope by decision; the numbers still feed the benchmark | — |

### The two lines doing the heavy lifting

```sql
AND t.seq = a.cursor          -- can't run task 3 until task 2 is committed
FOR UPDATE SKIP LOCKED        -- two workers never grab the same task
```

The first gives dependency ordering for free. The second gives mutual exclusion
across any number of machines **with no coordinator**. Replacing the first
predicate with a `deps_satisfied` check is the entire migration path to full DAG
support.

### The checkpoint has two fences

Reclaim-on-timeout guarantees a slow-but-alive worker and its replacement will
sometimes both run a task. So both writes are conditional:

| Fence | Prevents |
|---|---|
| `AND lease_owner = %(worker_id)s` | a reaped worker committing anyway |
| `AND cursor = %(seq)s` | a replayed checkpoint double-advancing |

Either matching zero rows → rollback, result discarded.

---

## Implementation

### Data model — 8 tables

| Table | Role |
|---|---|
| `agents` | one row per run: `plan INT[]`, `cursor`, `context JSONB`, `cost_units` |
| `task_instances` | the queue, the lease and the checkpoint in one row |
| `tiers` | **the escalation ladder, as data** — add a tier with one INSERT |
| `pipelines` | **named plans** — compose workflows at runtime |
| `idempotency` | `sha256(agent_id:seq:action)` → exactly-once effect |
| `attempts` | not a log — **this is the evidence** behind every metric |
| `dlq` | terminal failures with the full attempt trail |
| `runtime_config` | chaos + benchmark flags (`force_tier`, tool overrides) |

### Task instance state machine

```
pending  --claim------------------> running     worker takes the lease
running  --success----------------> succeeded   result committed, cursor++
running  --raises TaskFailure-----> failed      awaiting orchestrator routing
running  --lease expired----------> pending     reaper, INFRA, same tier
failed   --INFRA / retries left---> pending     same tier, backoff
failed   --CAPABILITY, tier spent-> pending     tier promoted, attempt reset
failed   --POISON / top tier------> dead        written to dlq
```

The worker only ever moves rows into `succeeded` or `failed`. Every routing
decision belongs to the orchestrator.

### Failure classification

| Class | Trigger | Action | Cost |
|---|---|---|---|
| `INFRA` | lease expiry, timeout, 5xx, worker killed | retry **same tier**, exponential backoff, then DLQ after 5 | 0 |
| `CAPABILITY` | invalid output, agent gave up | retry same tier, then **promote** | real |
| `POISON` | schema violation, 4xx, unknown task id | **dead-letter immediately** | 0 |

Escalating on `INFRA` would mean a `kill -9` promotes work to the expensive
model for zero benefit. Escalating on `POISON` burns senior compute on something
no model can fix. `failure_class` is a deliberately **closed** set — these three
are exhaustive, and a fourth would have no distinct routing.

### Components

```
api/main.py           POST /agents · GET /agents/{id} · /metrics · /dlq · /chaos/*
worker/               claim.py · heartbeat.py · executor.py · main.py
orchestrator/         reaper.py · classify.py · queue.py · ledger.py · main.py
common/               protocol · failures · tiers · registry · metrics · runtime · config
tasks/                registry.py (23 tasks) · tools.py (9 tools)
chaos/harness.py      seed · pipelines · tools · workers · watch
```

---

## Extensibility

Everything you'd want to extend is **data or one decorated function** — never a
code change across multiple files.

**Add a capability tier** — one INSERT. Promotion, cost accounting and pool
routing all pick it up:

```sql
INSERT INTO tiers VALUES ('principal', 3, 60, 8000, 4000, 0.99, NULL);
```
```bash
POOL_TIER=principal python -m worker.main    # drains the new queue
```

`tiers.rank` is `UNIQUE DEFERRABLE` specifically so a tier can be inserted
*between* two existing ones — shifting ranks collides transiently, which a
non-deferrable constraint would reject.

**Add a task** — one decorated function in any file under `tasks/`:

```python
@task(60, name="check-dns", tool="http")
def check_dns(ctx):
    return {"resolved": True, "saw": sorted(ctx.prior)}
```

**Compose a pipeline** — at runtime, no code:

```bash
python -m chaos.harness pipeline create my-flow 1,13,14,23,31,40,41 "my own line"
python -m chaos.harness seed 20 --pipeline my-flow
python -m chaos.harness seed 5  --plan 1,10,21,40      # or inline
```

**Switch tools between mock and real:**

```bash
python -m chaos.harness mode live         # everything real
python -m chaos.harness mode live files   # just one
python -m chaos.harness tool jira 1.0     # 100% failure injection
```

5 of the 9 tools do genuine work in live mode: `files` (real repo grep), `shell`
(allowlisted commands), `metrics_db` (real Postgres query), `http` (real
network), `github` (real API, public endpoints need no credentials).

---

## What is proven

93 tests against real Postgres. Screenshots are not evidence.

| Invariant | Test |
|---|---|
| Unrenewed lease becomes claimable again | `test_expired_lease_becomes_claimable_again` |
| `SIGKILL` mid-task loses nothing, resumes at its own cursor | `test_sigkill_mid_flight_loses_nothing` |
| A reaped worker cannot commit a stale result | `test_stale_worker_checkpoint_is_discarded` |
| `seq=n` never starts before `seq=n-1` commits | `test_task_not_claimable_before_predecessor_commits` |
| Concurrent workers never claim the same task | `test_concurrent_workers_never_claim_the_same_task` |
| Forced double execution → exactly one external action | `test_side_effecting_task_runs_once_under_double_execution` |
| `INFRA` never promotes; `POISON` never reaches senior | `test_infra_retries_at_the_same_tier`, `test_poison_never_reaches_senior` |
| Promotion does not leak onto the successor | `test_promotion_does_not_leak_to_successor` |
| Adding a tier needs no code change | `test_adding_a_tier_needs_no_code_change` |

Measured on a live 20-agent run with a `kill -9` mid-flight:

```
agents completed    20/20
leases reclaimed        1
promoted to senior      6   (6.0% of tasks)
cost                  166 units
all-senior baseline  1200 units  -> 0.14x
all-junior baseline   100 units  (6 tasks never finish)
```

---

## Known issues and limitations

Stated plainly, because a limitation you name is a design decision and one you
hide is a bug.

**1. Idempotency has a reserve-then-act window.**
A side-effecting task inserts its key *before* acting. A crash between the
reserve and the action loses that action — it will not be retried, because the
key is already present. Closing this properly needs a transactional outbox,
which is out of scope. The metric we claim (duplicate actions prevented) is
correct under this design; "no action is ever lost" is not claimed.

**2. Escalation rate is a property of the workload, not a constant.**
The ~7% figure holds for a mix where roughly 30% of agents contain a hard task.
Seed only `full-incident` pipelines and it rises past 19%. The rate is an
*output*, so quote it alongside the workload that produced it.

**3. Fast tasks make crash windows hard to hit.**
With mock latency, tasks finish in milliseconds and a `kill -9` often lands
*between* tasks — stranding nothing and reclaiming nothing. This is why
`tests/test_demo_scenarios.py` uses a plan containing a deliberately slow task.
**For a live demo, use a long pipeline** or the headline moment may show zero.

**4. `INFRA` retries are capped at 5, globally.**
A permanently dead tool dead-letters rather than retrying forever. That cap is a
constant in `orchestrator/classify.py`, not per-tool policy.

**5. Tiers are simulated.**
There is no LLM in the system. `junior`/`senior` are cost/latency/capability
stand-ins. The `tiers.model` column exists and is null; wiring real models is one
INSERT plus one function. **Say "capability tier", not "model", when presenting.**

**6. No multi-tenancy.**
There is no `tenant_id` anywhere. Per-tenant limits and fair scheduling are not
implemented and should not be claimed.

**7. No admission control or load shedding.**
The queue absorbs whatever is submitted. Under extreme spike the pending depth
grows; nothing returns 503.

**8. Live `shell` is allowlisted, not sandboxed.**
A task definition is data that a submitted plan can point at, so an open shell
would let any plan run anything on a worker. Only four commands are permitted.

**9. Connection pooling is not configured for large fleets.**
Each worker holds a small pool. Beyond roughly 40 processes you would want
pgbouncer. Not needed at demo scale, and transaction-pooling mode would break
the `SET CONSTRAINTS ... DEFERRED` the tier ladder uses.

**10. Sequential plans only.**
`t.seq = a.cursor` enforces a chain. True DAG fan-out is a one-predicate change
but is not implemented.

---

## Repository layout

```
api/          Task API (FastAPI)
common/       contracts: protocol, failures, tiers, registry, metrics, runtime
db/           schema.sql — the load-bearing contract — and the connection pool
worker/       claim → heartbeat → execute → checkpoint
orchestrator/ reaper (recovery) · classify (routing) · queue · ledger
tasks/        registry.py (23 tasks) · tools.py (9 tools, 5 with live impls)
chaos/        harness.py (CLI) · demo.sh · verify.sh
tests/        93 tests
```

---

> The agent did not become smarter. The system learned which failures were worth
> paying to fix.
