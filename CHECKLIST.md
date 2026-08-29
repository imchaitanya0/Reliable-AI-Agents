# Reliable AI Agent Runtime — Project Checklist & USP Roadmap

> **Thesis:** *"We built a distributed scheduler that understands the unique resource, tool, capability, and dependency characteristics of AI-agent workloads."*
> **Key Metrics:** **Throughput** + **P95 Latency** + **Tool Overload/Contention** + **Cost/Task** + **Worker Utilization**

---

## 1. USP Scoring & Evaluation Matrix (0–2 Scale)

**Scoring Guide:**
- **0 = Already Common / Not Interesting** (standard commodity queuing/scheduling pattern)
- **1 = Somewhat Differentiated** (useful, but incremental or existing in adjacent fields)
- **2 = Clearly Differentiated + Measurable** (unique to AI-agent workloads; high-impact, directly measurable)

| # | Potential USP | What you could build / test | Target Metric | Score (0–2) | Rationale & AI-Agent Specificity |
|:---|:---|:---|:---|:---:|:---|
| **1** | **Resource-aware scheduling** | Choose worker based on CPU, local queue depth, and worker affinity | P95 latency | **1** | Standard in Ray/K8s/Celery. Beneficial for agent workloads, but well-understood in general distributed computing. |
| **2** | **Tool-aware scheduling** | Avoid dispatching agent steps when required downstream tools (e.g. GitHub/Jira/DB) are near capacity or rate-limited | Tool contention & 429 errors | **2** | **Core differentiator.** Traditional schedulers treat tasks as opaque. AI agent tasks bottleneck heavily on rate-limited third-party APIs and downstream services. Dispatching based on tool concurrency limits directly prevents 429 thrashing and cascading retries. |
| **3** | **Cost-aware scheduling (Tiered Escalation)** | Execute on cheap junior models first; classify failures and escalate to senior models only on capability failure | Cost per task / Cost per workflow | **2** | **Flagship differentiator.** Deterministic compute retries identically; stochastic AI agents fail either due to infrastructure (retry same tier) or capability bounds (promote to expensive tier). Achieves ~80–90% cost savings over all-senior baselines. |
| **4** | **Priority-aware scheduling** | High-priority agent workflows and critical remediation tasks preempt or jump ahead in queue | Priority task latency | **1** | Priority queues are common in message brokers, but priority propagation across long multi-step agent dependency chains adds moderate differentiation. |
| **5** | **Dependency-aware scheduling (DAG)** | Execute independent investigation steps concurrently (fan-out/fan-in) rather than sequential FIFO | Workflow completion time | **2** | **Core differentiator.** Agents often investigate multiple independent data sources (Logs, GitHub, Jira) before reasoning. Running non-dependent steps in parallel via DAG scheduling drastically cuts end-to-end wall-clock latency. |
| **6** | **Adaptive concurrency** | Dynamically adjust tool/worker concurrency via AIMD/PID feedback based on downstream error rates and latency | Throughput + failure rate | **2** | **Strong differentiator.** Downstream tools degrade under agent bursts. Dynamically modulating concurrency protects fragile downstream services without manual rate-limit configuration. |
| **7** | **Agent-specific backpressure** | Throttle or pause runaway agents generating excessive tool traffic or stuck in reasoning loops | Blast radius & tool error rate | **2** | **Strong differentiator.** Prevents rogue/hallucinating agents from exhausting shared organizational API quotas and starving other healthy agent workflows. |
| **8** | **Failure-aware scheduling** | Dynamically route around flapping workers and degrade tool dispatch via circuit breaking | Recovery time & wasted retries | **1** | Circuit breakers are standard in microservices, but applying them to agent tool endpoints and worker node health prevents continuous retry thrashing during outages. |
| **9** | **Checkpoint-aware recovery** | Resume failed agents from the exact committed task cursor and accumulated context rather than replaying from scratch | Re-computation avoided / recovery time | **2** | **Foundational differentiator.** In multi-step agent pipelines, restarting from scratch wastes expensive LLM tokens and external side-effects. Cursor + context checkpoints allow exact-step resumption with zero lost work. |
| **10** | **Speculative execution** | Fire hedged/alternative agent reasoning paths or fallback tool queries when primary branch exceeds P95 latency | P99 tail latency | **1** | Proven in MapReduce/Hedged Requests. High value for smoothing LLM tail latency variance, though trades off increased token cost. |
| **11** | **Budget-aware execution** | Enforce hard bounds on execution time, tool call counts, step limits, and token budgets with safe termination | Cost saved / rogue loops stopped | **2** | **Essential AI safety control.** Prevents infinite agent reasoning loops, runaway recursive calls, and catastrophic bill shock. Measurable by dollars/tokens saved on runaway workloads. |
| **12** | **Deadline-aware scheduling** | Schedule tasks using Earliest Deadline First (EDF) based on remaining time budget and estimated step time | SLA / deadline success % | **1** | Classic real-time scheduling principle applied to agent task queues with SLA time constraints. |
| **13** | **Multi-tenant fairness** | Deficit round-robin or fair-share allocation preventing one tenant's 500-agent batch from starving urgent single agents | Fairness index & queue wait time | **1** | Standard multi-tenant queuing pattern adapted to agent orchestration pools. |
| **14** | **Tool reliability scoring** | Continuously track tool success rates and dynamically select alternative tools/fallback paths | Workflow success rate | **2** | **Strong differentiator.** When one search tool or data provider degrades, the runtime dynamically routes agent tool calls to fallback integrations. |
| **15** | **Agent workload classification** | Bin-pack tasks onto specialized worker pools based on profile (I/O-heavy, LLM-heavy, CPU-heavy) | Worker utilization & throughput | **2** | **Strong differentiator.** Prevents high-latency I/O tool calls (waiting 1s on logs) from hogging workers meant for intensive compute or streaming LLM operations. |
| **16** | **Explainable scheduling** | Record structured decision traces explaining why every task was claimed, queued, promoted, or delayed | Decision trace auditability | **1** | High operational and diagnostic value for agent observability, giving operators full visibility into orchestration decisions. |
| **17** | **Predictive scheduling** | Estimate task and tool execution duration to schedule optimal worker batching | P95/P99 latency | **1** | Valuable but difficult to predict reliably given LLM stochasticity and variable agent reasoning steps. |
| **18** | **Carbon/energy-aware scheduling** | Shift non-urgent batch agent tasks to off-peak / lower-cost energy windows | Energy cost / carbon index | **0** | Generic cloud scheduling concept, disconnected from the core challenges of agent reliability and tool orchestration. |
| **19** | **Dynamic worker allocation** | Autoscale junior vs senior worker pools independently based on queue backlog and promotion rate | Cost + throughput | **1** | Horizontal autoscaling is standard, though scaling senior pools independently based on capability failure rates is a neat operational feature. |
| **20** | **Reliability-performance tradeoff** | Adjust retry policies and checkpointing granularity based on task criticality (side-effecting vs read-only) | Reliability vs DB overhead | **1** | Selectively enforcing idempotency and synchronous checkpointing for side-effecting operations while fast-pathing read-only steps. |

---

## 2. Recommended Top 5 USP Bundle

To clearly demonstrate: *"I built a scheduler that understands the unique resource and dependency characteristics of AI-agent workloads"*, focus the project narrative and benchmarks on this top-tier bundle:

1. **USP 3: Cost-Aware Tiered Escalation** (Classify `INFRA` vs `CAPABILITY` vs `POISON` $\rightarrow$ spend 12× only when model strength is the root cause).
2. **USP 9: Checkpoint-Aware Cursor Resumption** (Resume at exact step index with full `context`; 4 tasks re-executed vs 47 avoided).
3. **USP 2: Tool-Aware Concurrency & Scheduling** (Prevent downstream tool saturation and 429 cascading failures).
4. **USP 5: Dependency-Aware DAG Execution** (Parallelize independent tool reads before synthesis).
5. **USP 11: Controlled Autonomy & Budget Enforcement** (Bound max time, steps, tool calls, and token spend with safe termination).

---

## 3. Architecture & Implementation Status: Done vs. Need to Be Done

### Overview of Architectural Contracts

```
Reliable-AI-Agents/
├── common/
│   ├── failures.py        [DONE] Contract C1: Failure Taxonomy (INFRA, CAPABILITY, POISON)
│   ├── protocol.py        [DONE] Contract C2: TaskDef + TaskContext protocol
│   └── config.py          [TODO] Shared settings, database URLs, lease TTLs
├── db/
│   ├── schema.sql         [DONE] Contract C5: Database Schema (agents, task_instances, idempotency, attempts, dlq, runtime_config)
│   └── pool.py            [TODO] Async/sync Postgres connection pool & lifecycle management
├── api/                   Lane B: Task API & Chaos Endpoints (Contract C3)
│   ├── main.py            [TODO] FastAPI application, /agents, /metrics, /dlq, /chaos/*
│   └── models.py          [TODO] Pydantic request/response schemas
├── orchestrator/          Lane F: Orchestrator & Recovery Engine
│   ├── reaper.py          [TODO] Lease expiry reaper sweep (Recovery path)
│   ├── classify.py        [TODO] Failure classifier and routing logic
│   ├── promote.py         [TODO] Tier promotion & senior queue re-enqueue
│   └── main.py            [TODO] Orchestrator main loop (stateless, SKIP LOCKED)
├── worker/                Lane C: Worker Execution Engine
│   ├── claim.py           [TODO] SKIP LOCKED claim query scoped to POOL_TIER
│   ├── heartbeat.py       [TODO] Background lease renewal loop (every TTL / 3)
│   ├── executor.py        [TODO] Task execution against TaskContext with idempotency enforcement
│   └── main.py            [TODO] Worker claim-heartbeat-execute-checkpoint loop
├── tasks/                 Lane A: Task Registry, Mock Tools & Simulated Tiers
│   ├── registry.py        [TODO] TASK_DEFS registry (9 benchmark tasks)
│   ├── tools.py           [TODO] Mock GitHub, Logs, Jira with injectable failure & latency
│   └── tiers.py           [TODO] Junior vs Senior execution simulation
├── dash/                  Lane D: Metrics & Observability Dashboard (Contract C4)
│   ├── fixture.json       [TODO] Static mock metrics contract fixture
│   └── app.py / index.html[TODO] Real-time dashboard showing 3-way cost comparison & recovery stats
├── metrics/               Intervention Metrics & Invariants Verification Suite
│   ├── common.py          [DONE] Shared DB helpers, timing, percentiles, Rich UI
│   ├── test_durable_execution.py    [DONE] Intervention 1: State preservation across crash
│   ├── test_leasing_recovery.py     [DONE] Intervention 2: Reaper recovery & tasks avoided (4 vs 47)
│   ├── test_idempotency.py          [DONE] Intervention 3: Exactly-once external side-effects
│   ├── test_controlled_autonomy.py  [DONE] Intervention 4: Runaway loop bounds & token protection
│   ├── test_tiered_escalation.py    [DONE] Intervention 5: 3-way tokenomics & cost benchmark
│   └── run_all_metrics.py           [DONE] Master test runner & executive summary table
├── chaos/                 Lane E: Fault Injection Harness & Chaos Scripts
│   ├── harness.py         [DONE] Automated chaos injection (worker SIGKILL, tool degradation)
│   └── demo_runner.py     [DONE] 7-step live benchmark & demo runner
└── tests/                 Reliability Invariant Test Suite
    ├── test_invariants.py [DONE] Pytest suite testing all 7 core reliability invariants
    └── conftest.py        [DONE] Test fixtures, DB setup/teardown, mock workers
```

---

## 4. Granular Component Checklist

### 4.1 Database Layer (Lane 0 — Contract C5)
- [x] **`db/schema.sql`**: Define `agents` table with plan, cursor, status, context, and cost accounting.
- [x] **`db/schema.sql`**: Define `task_instances` table with leasing, tier, attempt count, backoff gate, and failure class.
- [x] **`db/schema.sql`**: Define partial indexes (`task_instances_claim_idx`, `task_instances_lease_idx`) for lock-free `SKIP LOCKED` queries.
- [x] **`db/schema.sql`**: Define `idempotency` table for deduplicating side-effecting actions.
- [x] **`db/schema.sql`**: Define `attempts` evidence table for computing recovery, escalation, and cost metrics.
- [x] **`db/schema.sql`**: Define `dlq` table for unrecoverable poison failures.
- [x] **`db/schema.sql`**: Define `runtime_config` table for live chaos and benchmark flags.
- [x] **`db/pool.py`**: Implement database connection pool (`psycopg_pool.ConnectionPool`) with transaction managers.
- [x] **`db/init_db.py`**: CLI utility to apply `schema.sql` cleanly against local/Docker Postgres instance.

---

### 4.2 Common Protocols & Taxonomy (Contracts C1 & C2)
- [x] **`common/failures.py`**: Implement `TaskFailure`, `InfraFailure`, `CapabilityFailure`, `PoisonFailure` hierarchy.
- [x] **`common/failures.py`**: Implement `TIER_LADDER`, `next_tier()` helper, and capped `backoff_seconds()` calculator.
- [x] **`common/protocol.py`**: Implement `TaskContext` dataclass with `prior` context dict and deterministic `key_for()` idempotency hashing.
- [x] **`common/protocol.py`**: Implement `TaskDef` dataclass with difficulty (`easy` / `hard`), side-effecting flags, and tool bindings.
- [x] **`common/config.py`**: Implement unified configuration management (`DATABASE_URL`, `LEASE_TTL_SECONDS`, `HEARTBEAT_INTERVAL`, `POOL_TIER`).

---

### 4.3 Task Registry, Tools & Tiers (Lane A — Contract C2)
- [x] **`tasks/tools.py`**: Implement mock tools:
  - Mock `github` (read-only, ~300ms latency, configurable failure rate).
  - Mock `logs` (read-only, ~800ms latency, configurable failure rate).
  - Mock `jira` (side-effecting, ~500ms latency, requires idempotency key).
  - Dynamic override hooks reading from `runtime_config`.
- [x] **`tasks/tiers.py`**: Implement tier execution profiles:
  - `junior`: fast & cheap (1× cost units), fails deterministically on `difficulty="hard"` tasks with `CapabilityFailure`.
  - `senior`: capable & expensive (12× cost units), succeeds on `difficulty="hard"` tasks.
- [x] **`tasks/registry.py`**: Define standard 9-task benchmark library (`TASK_DEFS` mapping `1..9` to `TaskDef` implementations).

---

### 4.4 Worker Pool (Lane C — Contracts C1, C2, C5)
- [x] **`worker/claim.py`**: Implement atomic `SKIP LOCKED` claim query:
  - Filter by `status='pending'`, `next_run_at <= now()`, `tier=POOL_TIER`, `seq=agents.cursor`.
  - Atomically update to `status='running'`, assign `lease_owner`, set `lease_expires = now() + TTL`.
- [x] **`worker/heartbeat.py`**: Background thread/task periodically updating `lease_expires = now() + TTL` every `TTL / 3` seconds while task is active.
- [x] **`worker/executor.py`**:
  - Build `TaskContext` with accumulated `agents.context`.
  - Check idempotency table before executing side-effecting tasks (`jira`).
  - Execute `TaskDef.run(ctx)` inside try-catch block classifying exceptions to C1 taxonomy.
- [x] **`worker/main.py`**: Worker process entrypoint:
  - Claim $\rightarrow$ Heartbeat $\rightarrow$ Execute $\rightarrow$ Commit Checkpoint (in single transaction: update task result, advance `agent.cursor`, merge context, create next task instance at `junior` tier).
  - Support `POOL_TIER=junior` and `POOL_TIER=senior` environment flags.

---

### 4.5 Orchestrator & Failure Routing (Lane F — Contracts C1, C5)
- [x] **`orchestrator/reaper.py`**: Sweep expired leases (`status='running' AND lease_expires < now()`):
  - Reset task to `status='pending'`, set `failure_class='INFRA'`, set exponential backoff `next_run_at`.
  - Log reclaim event to `attempts` table.
  - Preserve existing `tier` (dead worker $\neq$ capability failure).
- [x] **`orchestrator/classify.py`**: Inspect failed attempts and route accordingly:
  - `INFRA` $\rightarrow$ Requeue at current tier with exponential backoff.
  - `CAPABILITY` $\rightarrow$ If attempts < `max_attempts_per_tier`, retry at current tier; if exhausted, trigger promotion.
  - `POISON` $\rightarrow$ Route immediately to `dlq` table; mark task `dead` and agent `failed`.
- [x] **`orchestrator/promote.py`**: Escalate capability failures:
  - Promote task to `tier='senior'`, reset `attempt=0`, set `next_run_at = now()`.
  - Ensure promotion remains scoped strictly to the current `task_instance` (never modify the parent agent tier).
- [x] **`orchestrator/main.py`**: Orchestrator loop running reaper, classifier, and promotion checks every 1–2s. Support multiple identical stateless instances without leader election.

---

### 4.6 Task API & Chaos Control (Lane B — Contracts C3, C4, C5)
- [x] **`api/main.py`**: Implement FastAPI application with CORS and connection lifecycle.
- [x] **`api/models.py`**: Pydantic request/response schemas.
- [x] **`api/routes` & Endpoints**:
  - `POST /agents`: Atomically insert new agent row and initialize `task_instances` for plan sequence.
  - `GET /agents/{id}`: Return agent status, plan progress, cursor, context, cost units, and task details.
  - `GET /metrics`: Return live dashboard metrics matching Contract C4 schema (active tasks, throughput, recovery count, tasks re-executed vs avoided, 3-way cost comparison).
  - `GET /dlq`: List dead-lettered tasks with full attempt trails.
  - `POST /chaos/tool`: Set latency and failure rate overrides on mock tools.
  - `POST /chaos/config`: Toggle `retries_enabled`, `escalation_enabled`, `force_tier`.

---

### 4.7 Metrics Dashboard (Lane D — Contract C4)
- [x] **`dash/fixture.json`**: Define canonical static JSON fixture of the `/metrics` contract for offline UI development.
- [x] **`dash/app.py`**: Implement real-time monitoring terminal dashboard displaying:
  - Active, Succeeded, Failed, and Recovered task counters.
  - **The Recovery Headline Metric:** "Tasks Re-executed vs Tasks Avoided" (e.g. 4 re-executed vs 47 avoided after worker crash).
  - **The Escalation Headline Metric:** Promotion rate % (~7% expected).
  - **Three-Way Live Cost Comparison:** All-Junior (1×, low completion) vs All-Senior (12×, expensive) vs Tiered Adaptive (~2×, high completion).
  - P50 / P95 / P99 latency charts and throughput (tasks/sec).
  - Zero duplicate actions counter.

---

### 4.8 Chaos Harness & Live Demos (Lane E)
- [x] **`chaos/harness.py`**: Automated fault injection helpers (tool failure overrides, runtime configuration).
- [x] **`chaos/demo_runner.py`**: Automated demo script executing the 7-step sequence:
  1. Submit 20 agents with plan `[1, 2, 6, 8, 9]`.
  2. Kill worker mid-flight $\rightarrow$ verify reaper recovery at cursor without full restart.
  3. Hard task fails twice on junior $\rightarrow$ promotes to senior $\rightarrow$ completes $\rightarrow$ next task returns to junior.
  4. Display 3-way cost comparison live.
  5. Inject Jira tool outage $\rightarrow$ demonstrate exponential backoff & DLQ.
  6. Terminate orchestrator instance $\rightarrow$ verify seamless failover without leader election.

---

### 4.9 Automated Invariant Test Suite (Contract Invariants)
- [x] **`tests/test_invariants.py`**:
  - **Invariant 1 & 2:** Task whose lease expires is reclaimed; `SIGKILL` mid-task resumes agent at its own cursor without losing state or re-executing prior steps.
  - **Invariant 3:** Forced double execution of side-effecting task (`jira`) produces exactly one external action.
  - **Invariant 4:** `seq=n` never starts before `seq=n-1` commits, verified via attempt timestamps.
  - **Invariant 5:** `INFRA` never promotes; `CAPABILITY` promotes only after `max_attempts_per_tier`; `POISON` goes directly to DLQ.
  - **Invariant 6:** Promoted task's successor claims at `tier='junior'` (promotion never leaks onto agent row).
  - **Invariant 7:** Promoted senior attempt receives exact accumulated `context` from prior steps.

---

## 5. Phase-by-Phase Build & Verification Milestones

| Phase | Milestone | Deliverable | Status |
|:---|:---|:---|:---:|
| **Phase 0** | **Contracts & Architecture** | Freeze C1 (`failures.py`), C2 (`protocol.py`), C5 (`schema.sql`), Repository Structure | ✅ **DONE** |
| **Phase 1** | **Storage & Registry** | `db/pool.py`, `tasks/registry.py`, `tasks/tools.py`, `tasks/tiers.py` | ✅ **DONE** |
| **Phase 2** | **Task API & Claim Loop** | `api/main.py` (`POST /agents`), `worker/claim.py`, basic execution loop | ✅ **DONE** |
| **Phase 3** | **Leases & Crash Recovery** | `worker/heartbeat.py`, `orchestrator/reaper.py`, cursor resumption after `kill -9` | ✅ **DONE** |
| **Phase 4** | **Classification & Tier Escalation** | `orchestrator/classify.py`, `promote.py`, junior + senior worker pools | ✅ **DONE** |
| **Phase 5** | **Idempotency & Exactly-Once Effect** | `idempotency` table checks in `worker/executor.py`, Jira tool deduplication | ✅ **DONE** |
| **Phase 6** | **Dead-Letter Queue & Backoff** | Exponential backoff gate (`next_run_at`), DLQ logging, poison task routing | ✅ **DONE** |
| **Phase 7** | **Observability Dashboard** | `api/main.py` (`/metrics`), `dash/app.py`, live 3-way cost comparison UI | ✅ **DONE** |
| **Phase 8** | **Chaos Harness & Demos** | `chaos/harness.py`, `chaos/demo_runner.py`, 7-step fault injection script | ✅ **DONE** |
| **Phase 9** | **Invariant Test Suite** | Full `pytest tests/ -v` testing all 7 reliability invariants (100% passing) | ✅ **DONE** |
| **Phase 10** | **Master Metrics Suite** | `python3 metrics/run_all_metrics.py` validating 5 core interventions | ✅ **DONE** |

---

## 6. How to Run the Entire Runtime Live

```bash
# 1. Initialize the PostgreSQL Database Schema
python3 -m db.init_db

# 2. Run the Automated Invariants Test Suite (All 7 Invariants Asserted)
pytest tests/ -v

# 3. Run the Master Metrics Benchmark (5 Core Interventions)
python3 metrics/run_all_metrics.py

# 4. Run the 7-Step Hackathon Demo Runner
python3 -m chaos.demo_runner

# 5. Start the Live Services (in separate terminal tabs)
uvicorn api.main:app --port 8000 --reload          # API Gateway
python3 -m orchestrator.main                      # Stateless Orchestrator
POOL_TIER=junior python3 -m worker.main           # Junior Worker Pool
POOL_TIER=senior python3 -m worker.main           # Senior Escalation Pool
python3 -m dash.app                               # Real-Time Terminal Dashboard
```
