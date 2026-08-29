#!/bin/bash
cd "/home/karthik/Signal Hackathon /Reliable-AI-Agents" || exit 1
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/rai"
export LEASE_TTL_SECONDS=10 ORCHESTRATOR_POLL_SECONDS=1 PYTHONPATH=.

# Never orphan children: kill the whole process group on any exit path.
cleanup() { pkill -9 -P $$ 2>/dev/null; }
trap cleanup EXIT INT TERM
PG=reliable-ai-agents-postgres-1

docker exec -i $PG psql -U postgres -d rai -q -c \
  "TRUNCATE agents, task_instances, idempotency, attempts, dlq CASCADE;" >/dev/null

.venv/bin/python -m chaos.harness seed 20

ORCHESTRATOR_ID=orch-1 .venv/bin/python -m orchestrator.main >/tmp/orch1.log 2>&1 & O1=$!
ORCHESTRATOR_ID=orch-2 .venv/bin/python -m orchestrator.main >/tmp/orch2.log 2>&1 & O2=$!
POOL_TIER=junior WORKER_ID=junior-1 .venv/bin/python -m worker.main >/tmp/jw1.log 2>&1 & J1=$!
POOL_TIER=junior WORKER_ID=junior-2 .venv/bin/python -m worker.main >/tmp/jw2.log 2>&1 & J2=$!
POOL_TIER=junior WORKER_ID=junior-3 .venv/bin/python -m worker.main >/tmp/jw3.log 2>&1 & J3=$!
POOL_TIER=senior WORKER_ID=senior-1 .venv/bin/python -m worker.main >/tmp/sw1.log 2>&1 & S1=$!
echo "up: 2 orchestrators, 3 junior, 1 senior"

sleep 7
INFLIGHT=$(docker exec -i $PG psql -U postgres -d rai -tAc \
  "SELECT count(*) FROM task_instances WHERE status='running' AND lease_owner='junior-2'" | tr -d ' ')
echo ">>> kill -9 junior-2 (pid $J2), holding $INFLIGHT task(s)"
kill -9 $J2 2>/dev/null

for i in $(seq 1 60); do
  n=$(docker exec -i $PG psql -U postgres -d rai -tAc \
      "SELECT count(*) FROM agents WHERE status IN ('completed','failed')" 2>/dev/null | tr -d ' ')
  [ "$n" = "20" ] && { echo ">>> all 20 settled"; break; }
  sleep 2
done

.venv/bin/python -m chaos.harness status
echo "=== orchestrator decisions ==="
grep -hE "PROMOTE|reclaimed" /tmp/orch1.log /tmp/orch2.log | tail -6
kill $O1 $O2 $J1 $J3 $S1 2>/dev/null
sleep 1
echo "(stopped)"
