# Lane E — Chaos harness and invariant tests

**Owner:** _unclaimed_ · **Starts:** T0, immediately · **Depends on:** C3 only

Script against the HTTP contract; stub the server until Lane B lands.

## Demo sequence
```
1. submit 20 agents, plan [1,2,6,8,9]
2. docker kill worker-2 mid-flight
3. reaper fires at T+30s -> each resumes at ITS OWN cursor
   "4 tasks re-executed, 47 avoided"
4. hard task fails 2x junior -> promotes -> senior succeeds -> back to junior
5. three-way cost table, live via /chaos/config
6. jira failure_rate=1.0 -> retries, backoff, DLQ
7. docker kill orchestrator-1 -> nothing changes
```

## Invariants (tests/, not screenshots)
1. An unrenewed lease becomes claimable again.
2. `SIGKILL` mid-task -> no agent lost, each resumed at its cursor.
3. Forced double execution -> exactly one external action.
4. `seq=n` never starts before `seq=n-1` commits.
5. `INFRA` never promotes; `POISON` never reaches senior.
6. A promoted task's successor claims at `tier='junior'`.
7. An escalated task receives the full prior context.
