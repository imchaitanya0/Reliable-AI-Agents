# Lane B — Task API

**Owner:** _unclaimed_ · **Starts:** after the schema merges · **Depends on:** C3, C5

## Deliverable
```
POST /agents        {"plan":[1,2,6,8,9], "count":20}  -> {"agent_ids":[...]}
GET  /agents/{id}   -> {id, plan, cursor, status, context, cost_units, tasks[]}
GET  /metrics       -> exactly the shape in dash/fixture.json  (contract C4)
GET  /dlq           -> [{agent_id, seq, failure_class, attempts[]}]

POST /chaos/tool    {"name":"jira","failure_rate":1.0,"latency_ms":500}
POST /chaos/config  {"retries_enabled":false,"escalation_enabled":true,
                     "force_tier":null}
```

`POST /agents` inserts the agent row and its `task_instances` in ONE transaction,
then returns. Nothing executes yet — workers discover the work by polling.

The `/chaos/*` endpoints write to `runtime_config`. They exist so the benchmark
can run the all-junior and all-senior baselines live: a metric only persuades
next to its control.

## Done when
20 agents submitted and the rows are visible in Postgres.
