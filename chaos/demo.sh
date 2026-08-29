#!/bin/bash
set -e

# Dynamically locate the repository root
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

# Select python executable
if [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
elif [ -f "venv/bin/python" ]; then
    PYTHON="venv/bin/python"
else
    PYTHON="python3"
fi

export PYTHONPATH="."
export LEASE_TTL_SECONDS=10
export ORCHESTRATOR_POLL_SECONDS=1

# Clean up all spawned background processes on exit
cleanup() {
    pkill -9 -P $$ 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "=========================================================="
echo " Reliable AI Agent Runtime — Live Fault & Recovery Demo"
echo "=========================================================="

echo "1. Initializing database schema..."
$PYTHON -m db.init_db >/dev/null 2>&1

$PYTHON -c "
from db.pool import pool
with pool().connection() as conn, conn.cursor() as cur:
    cur.execute('TRUNCATE agents, task_instances, idempotency, attempts, dlq CASCADE;')
"

echo "2. Seeding 20 agents (6 hard capability plans, 14 standard)..."
$PYTHON -m chaos.harness seed 20

echo "3. Starting distributed runtime components..."
ORCHESTRATOR_ID=orch-1 $PYTHON -m orchestrator.main >/tmp/orch1.log 2>&1 & O1=$!
ORCHESTRATOR_ID=orch-2 $PYTHON -m orchestrator.main >/tmp/orch2.log 2>&1 & O2=$!
WORKER_TIER=junior POOL_TIER=junior WORKER_ID=junior-1 $PYTHON -m worker.main >/tmp/jw1.log 2>&1 & J1=$!
WORKER_TIER=junior POOL_TIER=junior WORKER_ID=junior-2 $PYTHON -m worker.main >/tmp/jw2.log 2>&1 & J2=$!
WORKER_TIER=junior POOL_TIER=junior WORKER_ID=junior-3 $PYTHON -m worker.main >/tmp/jw3.log 2>&1 & J3=$!
WORKER_TIER=senior POOL_TIER=senior WORKER_ID=senior-1 $PYTHON -m worker.main >/tmp/sw1.log 2>&1 & S1=$!

echo "   -> Active: 2 Stateless Orchestrators, 3 Junior Workers, 1 Senior Worker"
echo "4. Letting workers claim initial tasks (sleeping 4s)..."
sleep 4

echo "5. [CHAOS INJECTION] Simulating worker crash: kill -9 junior-2 (pid $J2)..."
kill -9 $J2 2>/dev/null || true

echo "6. Waiting for Orchestrator Reaper to reclaim lease and complete all workflows..."
for i in $(seq 1 45); do
    SETTLED=$($PYTHON -c "
from db.pool import fetchone
row = fetchone(\"SELECT count(*) AS cnt FROM agents WHERE status IN ('completed','failed')\")
print(row['cnt'] if row else 0)
" 2>/dev/null || echo "0")

    if [ "$SETTLED" = "20" ]; then
        echo "   -> All 20 agents settled successfully!"
        break
    fi
    sleep 1
done

echo ""
echo "=========================================================="
echo " FINAL SYSTEM METRICS REPORT"
echo "=========================================================="
$PYTHON -m chaos.harness status

echo ""
echo "=== RECENT ORCHESTRATOR REAPER & PROMOTION EVENTS ==="
grep -hE "PROMOTE|reclaimed|DEAD" /tmp/orch1.log /tmp/orch2.log 2>/dev/null | tail -10 || echo "(No failure events)"

kill $O1 $O2 $J1 $J3 $S1 2>/dev/null || true
echo ""
echo "(Demo completed successfully. All background processes cleaned up.)"
