#!/bin/bash
cd "/home/karthik/Signal Hackathon /Reliable-AI-Agents" || exit 1
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/rai"
export LEASE_TTL_SECONDS=10 ORCHESTRATOR_POLL_SECONDS=1 PYTHONPATH=.
PG=reliable-ai-agents-postgres-1
cleanup() { pkill -9 -P $$ 2>/dev/null; }
trap cleanup EXIT INT TERM
P=.venv/bin/python

docker exec -i $PG psql -U postgres -d rai -q -c \
 "TRUNCATE agents, task_instances, idempotency, attempts, dlq CASCADE;" >/dev/null

echo "### 1. compose a brand-new pipeline at runtime"
$P -m chaos.harness pipeline create karthik-flow 1,13,14,23,31,40,41 "my own line"

echo
echo "### 2. seed three DIFFERENT pipelines at once"
$P -m chaos.harness seed 6  --pipeline karthik-flow
$P -m chaos.harness seed 8  --pipeline quick-triage
$P -m chaos.harness seed 4  --pipeline full-incident
$P -m chaos.harness seed 2  --plan 1,10,21,40

echo
echo "### 3. put the tools that CAN be real into live mode"
$P -m chaos.harness mode live files
$P -m chaos.harness mode live shell
$P -m chaos.harness mode live metrics_db
sleep 1.2
$P -m chaos.harness tools

echo
echo "### 4. spawn pools + orchestrators"
for i in 1 2; do ORCHESTRATOR_ID=orch-$i $P -m orchestrator.main >/tmp/o$i.log 2>&1 & done
for i in 1 2 3 4; do POOL_TIER=junior WORKER_ID=junior-$i $P -m worker.main >/tmp/j$i.log 2>&1 & done
J2=$!
for i in 1 2; do POOL_TIER=senior WORKER_ID=senior-$i $P -m worker.main >/tmp/s$i.log 2>&1 & done
echo "2 orchestrators, 4 junior, 2 senior"

sleep 8
echo
echo "### 5. kill -9 a junior worker mid-flight"
kill -9 $J2 2>/dev/null; echo "killed pid $J2"

for i in $(seq 1 60); do
  n=$(docker exec -i $PG psql -U postgres -d rai -tAc \
     "SELECT count(*) FROM agents WHERE status IN ('completed','failed')" | tr -d ' ')
  [ "$n" = "20" ] && { echo ">>> all 20 agents settled"; break; }
  sleep 2
done

$P -m chaos.harness status
echo "### live tool output actually captured in an agent's context:"
docker exec -i $PG psql -U postgres -d rai -tAc \
 "SELECT jsonb_pretty(context->'1') FROM agents WHERE context->'1'->'scan' IS NOT NULL LIMIT 1;"
