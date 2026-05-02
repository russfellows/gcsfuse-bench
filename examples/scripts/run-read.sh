#!/bin/bash
# run-read.sh — Parameterized resnet50 read (GET) benchmark for one host
#
# Usage:
#   bash run-read.sh <WID> <EPOCH> [CONCURRENCY]
#
#   WID          Host/worker ID: 0, 1, 2, or 3
#   EPOCH        Synchronized Unix epoch start time (seconds)
#   CONCURRENCY  Number of goroutines (default: 256)
#
# Prerequisites: objects must already exist under resnet2-50/host-<WID>/
#   Run run-prepare.sh first if needed.
#
# Example — generate EPOCH once, then paste on each host:
#   EPOCH=$(date -d '+3 minutes' +%s)   # on any one host
#   bash run-read.sh 0 $EPOCH           # vm1
#   bash run-read.sh 1 $EPOCH           # vm2
#   bash run-read.sh 2 $EPOCH           # vm3
#   bash run-read.sh 3 $EPOCH           # vm4

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing ----------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [CONCURRENCY]" >&2
    echo "  WID         : worker/host ID (0-3)" >&2
    echo "  EPOCH       : synchronized Unix start time (seconds)" >&2
    echo "  CONCURRENCY : goroutines, default 256" >&2
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

if [[ ! "$CONCURRENCY" =~ ^[0-9]+$ || "$CONCURRENCY" -lt 1 ]]; then
    echo "ERROR: CONCURRENCY must be a positive integer (got: $CONCURRENCY)" >&2
    exit 1
fi

NOW=$(date +%s)
if [[ "$EPOCH" -le "$NOW" ]]; then
    echo "WARNING: EPOCH ($EPOCH) is in the past — hosts may not start simultaneously." >&2
fi

# ---------- run ----------
echo "=== resnet50 read ==="
echo "  Host/WID    : ${WID}"
echo "  Prefix      : resnet2-50/host-${WID}/"
echo "  Concurrency : ${CONCURRENCY}"
echo "  Start-at    : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH}))"
echo "  Output path : ~/results/worker-${WID}-read"
echo "====================="

"${GCS_BENCH}" bench \
    --config "${CONFIG_DIR}/resnet50.yaml" \
    --bucket sig65-rapid1 \
    --object-prefix "resnet2-50/host-${WID}/" \
    --concurrency "${CONCURRENCY}" \
    --warmup 60s \
    --start-at "${EPOCH}" \
    --rapid-mode on \
    --output-path ~/results/worker-${WID}-read \
    --output-format both
