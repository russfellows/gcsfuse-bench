#!/bin/bash
# run-read-all.sh — All 4 hosts read ALL 4 prefix namespaces simultaneously
#
# Each host runs 4 gcs-bench processes in parallel, one per host prefix
# (resnet2-50/host-0/ through host-3/), each with CONCURRENCY/4 goroutines.
# Total goroutines per host = CONCURRENCY (same as a single-prefix run).
#
# Usage:
#   bash run-read-all.sh <WID> <EPOCH> [CONCURRENCY]
#
#   WID          Host/worker ID: 0, 1, 2, or 3  (used only for output path naming)
#   EPOCH        Synchronized Unix epoch start time (seconds)
#   CONCURRENCY  Total goroutines per host (default: 256 → 64 per prefix)
#
# Example:
#   EPOCH=$(date -d '+3 minutes' +%s)
#   bash run-read-all.sh 0 $EPOCH    # vm1
#   bash run-read-all.sh 1 $EPOCH    # vm2
#   bash run-read-all.sh 2 $EPOCH    # vm3
#   bash run-read-all.sh 3 $EPOCH    # vm4

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing ----------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [CONCURRENCY]" >&2
    echo "  WID         : worker/host ID (0-3) — used for output path naming" >&2
    echo "  EPOCH       : synchronized Unix start time (seconds)" >&2
    echo "  CONCURRENCY : total goroutines per host, default 256 (64 per prefix)" >&2
    exit 1
fi

WID="${1}"
EPOCH="${2}"
CONCURRENCY="${3:-256}"

# ---------- validate ----------
if [[ ! "$WID" =~ ^[0-3]$ ]]; then
    echo "ERROR: WID must be 0, 1, 2, or 3 (got: $WID)" >&2
    exit 1
fi

if [[ ! "$EPOCH" =~ ^[0-9]+$ ]]; then
    echo "ERROR: EPOCH must be a positive integer (got: $EPOCH)" >&2
    exit 1
fi

if [[ ! "$CONCURRENCY" =~ ^[0-9]+$ || "$CONCURRENCY" -lt 4 ]]; then
    echo "ERROR: CONCURRENCY must be >= 4 (got: $CONCURRENCY)" >&2
    exit 1
fi

NOW=$(date +%s)
if [[ "$EPOCH" -le "$NOW" ]]; then
    echo "WARNING: EPOCH ($EPOCH) is in the past — hosts may not start simultaneously." >&2
fi

PER_PREFIX=$(( CONCURRENCY / 4 ))

echo "=== resnet50 read-all (4 prefixes) ==="
echo "  Host/WID         : ${WID}"
echo "  Prefixes         : resnet2-50/host-{0,1,2,3}/"
echo "  Total concurrency: ${CONCURRENCY} goroutines (${PER_PREFIX} per prefix)"
echo "  Start-at         : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH}))"
echo "  Output path      : ~/results/worker-${WID}-read-all/host-{0,1,2,3}"
echo "======================================="

# ---------- launch 4 parallel readers ----------
mkdir -p ~/results/worker-${WID}-read-all

PIDS=()
for H in 0 1 2 3; do
    LOG=~/results/worker-${WID}-read-all-host-${H}.log
    "${GCS_BENCH}" bench \
        --config "${CONFIG_DIR}/resnet50.yaml" \
        --bucket sig65-rapid1 \
        --object-prefix "resnet2-50/host-${H}/" \
        --concurrency "${PER_PREFIX}" \
        --warmup 60s \
        --start-at "${EPOCH}" \
        --rapid-mode on \
        --output-path ~/results/worker-${WID}-read-all/host-${H} \
        --output-format both \
        2>&1 | sed -u "s/^/[host-${H}] /" | tee "${LOG}" &
    PIDS+=($!)
    echo "  Launched reader for host-${H} prefix (PID ${PIDS[-1]})"
done

echo ""
echo "All 4 readers running. Waiting for completion..."

# ---------- wait and report ----------
FAILED=0
for i in "${!PIDS[@]}"; do
    PID="${PIDS[$i]}"
    H=$i
    if wait "$PID"; then
        echo "  host-${H}: DONE"
    else
        echo "  host-${H}: FAILED (exit $?)" >&2
        FAILED=$(( FAILED + 1 ))
    fi
done

echo ""
echo "Logs: ~/results/worker-${WID}-read-all-host-{0,1,2,3}.log"

if [[ "$FAILED" -gt 0 ]]; then
    echo "ERROR: $FAILED reader(s) failed." >&2
    exit 1
fi
echo "All readers completed successfully."
