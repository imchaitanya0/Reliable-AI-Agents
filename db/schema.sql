-- =============================================================================
-- CONTRACT C5 — DATABASE SCHEMA
-- =============================================================================
-- This is the load-bearing contract. Every lane codes against it, and it is the
-- only file that must land before parallel work begins.
--
-- Postgres is the durable queue, the lease table, the checkpoint store and the
-- idempotency ledger all at once. Using one transactional system is what makes
-- the reliability claims true rather than merely likely.
--
-- CHANGING THIS FILE MID-HACKATHON COSTS THE WHOLE TEAM. Discuss first.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()


-- -----------------------------------------------------------------------------
-- agents — one row per agent run
-- -----------------------------------------------------------------------------
-- An agent is a PLAN OF TASK IDS executed in sequence. Because the plan is data
-- and not code, the agent is fully serializable and resumable at exact task
-- granularity: `cursor` is the resume point, `context` is everything learned so
-- far.
--
-- cost_units / tokens_used are ACCOUNTING ONLY. Nothing enforces them, and
-- nothing terminates an agent for exceeding them. They exist so the three-way
-- cost benchmark has real numbers.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agents (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    plan         INT[]       NOT NULL,
    cursor       INT         NOT NULL DEFAULT 0,      -- index into plan[]; the resume point
    status       TEXT        NOT NULL DEFAULT 'running',
    context      JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- {seq: result} accumulated
    cost_units   INT         NOT NULL DEFAULT 0,
    tokens_used  INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT agents_status_ck
        CHECK (status IN ('pending', 'running', 'completed', 'failed'))
);


-- -----------------------------------------------------------------------------
-- tiers — the escalation ladder, as DATA
-- -----------------------------------------------------------------------------
-- Adding a capability tier is one INSERT, not a migration and not a code change.
-- `rank` ascending means more capable and more expensive; promotion walks to the
-- next rank up, and the top rank is where a task stops being retryable.
--
-- This table is the SINGLE SOURCE OF TRUTH for the ladder. Nothing else may
-- hardcode tier names.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tiers (
    name       TEXT PRIMARY KEY,
    -- DEFERRABLE so a tier can be inserted BETWEEN two existing ones: shifting
    -- the ranks above it transiently duplicates a value, which a non-deferrable
    -- constraint would reject mid-transaction. Extending the ladder in the
    -- middle is a legitimate operation, so the schema has to allow it.
    rank       INT  NOT NULL,
    CONSTRAINT tiers_rank_uk UNIQUE (rank) DEFERRABLE INITIALLY IMMEDIATE,
    cost_units INT  NOT NULL,
    tokens     INT  NOT NULL,
    latency_ms INT  NOT NULL,
    p_success  REAL NOT NULL DEFAULT 1.0,
    model      TEXT                        -- null while tiers are simulated
);

INSERT INTO tiers (name, rank, cost_units, tokens, latency_ms, p_success) VALUES
    ('junior', 1,  1, 1200,  400, 0.75),
    ('senior', 2, 12, 3000, 1800, 0.95)
ON CONFLICT (name) DO NOTHING;

-- To add a third tier, this is the entire change:
--   INSERT INTO tiers VALUES ('principal', 3, 60, 8000, 4000, 0.99, NULL);
-- Promotion, cost accounting and the worker pools all pick it up with no code
-- change. Start a pool with POOL_TIER=principal and it drains.


-- -----------------------------------------------------------------------------
-- task_instances — the queue, the lease and the checkpoint, in one row
-- -----------------------------------------------------------------------------
-- tier is the escalation axis and the single most important column in the
-- system. A worker pool only claims rows matching its own tier, so promoting a
-- task is nothing more than an UPDATE of this field.
--
-- INVARIANT: promotion is scoped to the TASK, never the agent. When a promoted
-- task succeeds, the NEXT task_instance is created at tier='junior' again. If
-- promotion ever leaks onto the agent row, cost silently converges on the
-- all-senior baseline and the project's central claim evaporates.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS task_instances (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id              UUID        NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    seq                   INT         NOT NULL,       -- position in agents.plan
    task_def_id           INT         NOT NULL,       -- key into tasks/registry.py
    status                TEXT        NOT NULL DEFAULT 'pending',

    tier                  TEXT        NOT NULL DEFAULT 'junior',
    attempt               INT         NOT NULL DEFAULT 0,   -- attempts AT THE CURRENT TIER
    max_attempts_per_tier INT         NOT NULL DEFAULT 2,

    -- leasing: a worker must renew before lease_expires or the reaper reclaims
    lease_owner           TEXT,
    lease_expires         TIMESTAMPTZ,

    next_run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),  -- exponential backoff gate

    result                JSONB,
    last_error            TEXT,
    failure_class         TEXT,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT task_instances_agent_seq_uk UNIQUE (agent_id, seq),
    -- 'failed' means: an attempt finished unsuccessfully and is AWAITING
    -- ORCHESTRATOR ROUTING. The worker reports what happened; the orchestrator
    -- decides whether that means retry, promote, or dead-letter. Keeping those
    -- two responsibilities apart is what lets the orchestrator stay stateless.
    CONSTRAINT task_instances_status_ck
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'dead')),
    -- FK, not a CHECK: the ladder is data, so a new tier needs no DDL.
    CONSTRAINT task_instances_tier_fk
        FOREIGN KEY (tier) REFERENCES tiers(name),
    -- Deliberately CLOSED, unlike tier. These three are exhaustive: the machine
    -- broke, the attempt failed on its merits, or nothing can fix it. A fourth
    -- would have no distinct routing, so this is a real taxonomy and not a gap.
    CONSTRAINT task_instances_failure_class_ck
        CHECK (failure_class IS NULL
               OR failure_class IN ('INFRA', 'CAPABILITY', 'POISON'))
);

-- Drives the claim query. Partial index keeps it small: only pending rows are
-- ever scanned for work.
CREATE INDEX IF NOT EXISTS task_instances_claim_idx
    ON task_instances (tier, next_run_at)
    WHERE status = 'pending';

-- Drives the reaper sweep.
CREATE INDEX IF NOT EXISTS task_instances_lease_idx
    ON task_instances (lease_expires)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS task_instances_agent_idx
    ON task_instances (agent_id);

-- Drives the orchestrator's classification sweep: rows a worker has reported on
-- but nobody has routed yet.
CREATE INDEX IF NOT EXISTS task_instances_failed_idx
    ON task_instances (updated_at)
    WHERE status = 'failed';


-- -----------------------------------------------------------------------------
-- idempotency — turns at-least-once delivery into exactly-once EFFECT
-- -----------------------------------------------------------------------------
-- You cannot distinguish a crashed worker from a slow one. That is a real
-- impossibility result, not a gap in the design. So the runtime does not try:
-- it reclaims on lease expiry and accepts that a slow-but-alive worker and its
-- replacement will SOMETIMES run the same task concurrently.
--
-- This table is the defence. Before any externally visible action, the task
-- reserves sha256(agent_id:seq:action) here. A retry that finds the key knows
-- the action already happened and returns the stored result instead of doing it
-- twice.
--
-- THE LEDGER IS TWO-PHASE, AND IT HAS TO BE
-- -----------------------------------------
-- Read the failure this table exists for closely:
--
--     create Jira ticket -> SUCCESS -> worker dies before acknowledging
--       -> retry -> check action id -> already executed -> do not duplicate
--
-- The crash is AFTER the action succeeds. So a ledger row written after the
-- fact cannot help: the window between "ticket created" and "row committed" is
-- exactly where the worker dies, the retry finds no key, and a second ticket is
-- created. Worse, it fails silently -- `SELECT count(*) FROM idempotency` still
-- answers 1 while two tickets exist, so the obvious check passes.
--
-- A single-phase ledger cannot close that window, because the result does not
-- exist until the action has already had its effect. Hence two phases:
--
--   state='in_flight'  reserved BEFORE the action. It may or may not have run.
--   state='done'       settled AFTER it succeeded; `result` is authoritative.
--
-- A retry that finds 'in_flight' must NOT act: either a twin is mid-action, or
-- a worker died in the window. Both mean the effect may already exist. That
-- ambiguity is real, and orchestrator/ledger.py reports it rather than guessing.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS idempotency (
    key         TEXT        PRIMARY KEY,     -- sha256(agent_id:seq:action_type)
    agent_id    UUID        NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    seq         INT         NOT NULL,
    action_type TEXT        NOT NULL,

    state       TEXT        NOT NULL DEFAULT 'in_flight',
    result      JSONB,                       -- NULL until settled
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at  TIMESTAMPTZ,

    CONSTRAINT idempotency_state_ck
        CHECK (state IN ('in_flight', 'done')),
    -- A settled row with no result would let a replay hand back NULL as if it
    -- were the answer the action produced.
    CONSTRAINT idempotency_settled_has_result_ck
        CHECK (state <> 'done' OR result IS NOT NULL)
);

-- Drives the audit sweep: reservations that outlived the lease that would have
-- let their owner settle them.
CREATE INDEX IF NOT EXISTS idempotency_in_flight_idx
    ON idempotency (created_at)
    WHERE state = 'in_flight';


-- -----------------------------------------------------------------------------
-- attempts — not a log. This table IS the evidence.
-- -----------------------------------------------------------------------------
-- Every number on the dashboard is computed from here: escalation rate, cost
-- versus the all-senior baseline, recovery time, tasks re-executed after a
-- crash versus what a naive full restart would have redone.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS attempts (
    id               BIGSERIAL   PRIMARY KEY,
    task_instance_id UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    agent_id         UUID        NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    seq              INT         NOT NULL,
    attempt_no       INT         NOT NULL,
    tier             TEXT        NOT NULL,
    worker_id        TEXT,
    outcome          TEXT        NOT NULL,   -- 'succeeded' | 'failed' | 'reclaimed'
    failure_class    TEXT,
    cost_units       INT         NOT NULL DEFAULT 0,
    tokens           INT         NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS attempts_agent_idx    ON attempts (agent_id);
CREATE INDEX IF NOT EXISTS attempts_outcome_idx  ON attempts (outcome);
CREATE INDEX IF NOT EXISTS attempts_tier_idx     ON attempts (tier);


-- -----------------------------------------------------------------------------
-- dlq — terminal failures, with the history that got them there
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dlq (
    id            BIGSERIAL   PRIMARY KEY,
    agent_id      UUID        NOT NULL,
    seq           INT         NOT NULL,
    task_def_id   INT         NOT NULL,
    failure_class TEXT        NOT NULL,
    last_error    TEXT,
    attempt_trail JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- -----------------------------------------------------------------------------
-- runtime_config — chaos + benchmark flags, readable by every process
-- -----------------------------------------------------------------------------
-- A metric only persuades next to its control. These flags let the demo run the
-- all-junior and all-senior baselines live, and inject tool failures on stage.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runtime_config (
    key   TEXT PRIMARY KEY,
    value JSONB NOT NULL
);

INSERT INTO runtime_config (key, value) VALUES
    ('retries_enabled',    'true'::jsonb),
    ('escalation_enabled', 'true'::jsonb),
    ('force_tier',         'null'::jsonb),   -- null | "junior" | "senior"
    ('lease_ttl_seconds',  '30'::jsonb),
    ('tool_overrides',     '{}'::jsonb)      -- {"jira": {"failure_rate": 1.0}}
ON CONFLICT (key) DO NOTHING;


-- -----------------------------------------------------------------------------
-- pipelines — named plans, as DATA
-- -----------------------------------------------------------------------------
-- A pipeline is a reusable ordered list of task ids. Naming them means you
-- compose new workflows at RUNTIME instead of hardcoding [1,2,6,8,9] at a call
-- site:
--
--   INSERT INTO pipelines (name, plan, description)
--   VALUES ('my-workflow', ARRAY[1,10,21,40], 'whatever I need today');
--
-- The API and the CLI both accept either a pipeline name or a raw plan array,
-- so nothing is locked to a fixed set.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipelines (
    name        TEXT PRIMARY KEY,
    plan        INT[] NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- =============================================================================
-- THE CLAIM QUERY — this is the entire scheduler
-- =============================================================================
-- Kept here as documentation; worker/claim.py executes it.
--
--   UPDATE task_instances SET
--     status='running', lease_owner=%(worker)s,
--     lease_expires=now() + make_interval(secs => %(ttl)s),
--     attempt=attempt+1, updated_at=now()
--   WHERE id = (
--     SELECT t.id FROM task_instances t
--     JOIN agents a ON a.id = t.agent_id
--     WHERE t.status='pending'
--       AND t.next_run_at <= now()      -- backoff gate
--       AND t.tier = %(pool_tier)s      -- junior pool ignores escalated work
--       AND a.status = 'running'
--       AND t.seq = a.cursor            -- <- the sequential dependency
--     ORDER BY t.next_run_at
--     FOR UPDATE SKIP LOCKED LIMIT 1    -- <- mutual exclusion, never blocks
--   ) RETURNING *;
--
-- Two lines carry the whole design:
--
--   t.seq = a.cursor         No task is claimable until its predecessor commits
--                            and advances the cursor. Dependency ordering, free.
--                            Swap this for a deps_satisfied check and you have
--                            full DAG support.
--
--   FOR UPDATE SKIP LOCKED   Two workers never claim the same row and never wait
--                            on each other. This replaces an entire consensus
--                            protocol — which is why there is no leader election
--                            and no single point of failure.
-- =============================================================================


-- =============================================================================
-- THE REAPER — crash recovery in full
-- =============================================================================
-- Runs in every orchestrator instance every 2s. Note it requeues at the SAME
-- tier: a dead machine says nothing about model capability.
--
--   UPDATE task_instances SET
--     status='pending', lease_owner=NULL, failure_class='INFRA',
--     next_run_at = now() + make_interval(secs => backoff(attempt)),
--     updated_at=now()
--   WHERE status='running' AND lease_expires < now()
--   RETURNING id, agent_id, seq;
-- =============================================================================


-- =============================================================================
-- TASK_INSTANCE STATE MACHINE
-- =============================================================================
--   pending   --claim-------------------> running     worker took the lease
--   running   --success-----------------> succeeded   result committed, cursor++
--   running   --raises TaskFailure------> failed      awaiting orchestrator
--   running   --lease expired-----------> pending     reaper, INFRA, same tier
--   failed    --INFRA or retries left---> pending     same tier, backoff
--   failed    --CAPABILITY, tier spent--> pending     tier='senior', attempt=0
--   failed    --POISON or top tier------> dead        written to dlq
--
-- The worker only ever moves rows into 'succeeded' or 'failed'. Every routing
-- decision belongs to the orchestrator. That separation is what keeps the
-- orchestrator stateless and horizontally scalable.
--
-- NOTE ON WHO CREATES TASK ROWS: the API inserts the agent AND every
-- task_instance of its plan in one transaction, all at tier='junior'. The
-- claim query's `t.seq = a.cursor` predicate is what gates execution order, so
-- nothing runs early. This means the worker's checkpoint never INSERTs -- it
-- only marks succeeded and advances the cursor -- and a promoted task cannot
-- leak its tier onto its successor, because the successor row already exists
-- at 'junior'.
-- =============================================================================
