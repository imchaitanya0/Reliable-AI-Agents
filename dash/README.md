# Lane D — Dashboard

**Owner:** _unclaimed_ · **Starts:** T0, immediately · **Depends on:** C4 only

Build the entire dashboard against `fixture.json` — a static file of exactly the
`/metrics` shape. No waiting on the API.

## The headline numbers
- **tasks re-executed vs. what a naive restart would redo** (e.g. 4 vs 47) —
  the cost of recovery, made concrete
- **promotion rate** — expect single digits; this is the whole argument
- **three-way cost comparison** — all-junior / all-senior / tiered
- duplicate actions prevented, recovery time p50/p99, throughput

Build this early, not last. A recovery that only shows up in log files did not
happen as far as the room is concerned.
