#!/bin/bash
# run-unet3d-read.sh — Parameterized UNet3D read (GET) benchmark for one host
#
# Reads from unet3d/host-<WID>/ — the per-host corpus written by
# run-unet3d-prepare.sh.  Full-object GETs, random access, 5-minute run.
#
# Usage:
#   bash run-unet3d-read.sh <WID> <EPOCH> [CONCURRENCY]
#
#   WID          Host/worker ID: 0, 1, 2, or 3
#   EPOCH        Synchronized Unix epoch start time (seconds)
#   CONCURRENCY  Number of goroutines (default: 128)
#
# Prerequisites: objects must already exist under unet3d/host-<WID>/
#   Run run-unet3d-prepare.sh first if needed.
#
# Example — generate EPOCH once, then paste on each host:
#   EPOCH=$(date -d '+3 minutes' +%s)   # on any one host
#   bash run-unet3d-read.sh 0 $EPOCH    # vm1
#   bash run-unet3d-read.sh 1 $EPOCH    # vm2
#   bash run-unet3d-read.sh 2 $EPOCH    # vm3
#   bash run-unet3d-read.sh 3 $EPOCH    # vm4

set -euo pipefail

# ---------- locate binary and configs (works from any working directory) ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing ----------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [CONCURRENCY]" >&2
    echo "  WID         : worker/host ID (0-3)" >&2
    echo "  EPOCH       : synchronized Unix start time (seconds)" >&2
    echo "  CONCURRENCY : goroutines, default 128" >&2
    exit 1
fi

WID="${1}"
EPOCH="${2}"
CONCURRENCY="${3:-128}"

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
echo "=== unet3d read ==="
echo "  Host/WID    : ${WID}"
echo "  Prefix      : unet3d/host-${WID}/"
echo "  Objects     : 100,352 (28² × 128), ~678 GiB"
echo "  Concurrency : ${CONCURRENCY}"
echo "  Start-at    : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH}))"
echo "  Output path : ~/results/unet3d-worker-${WID}-read"
echo "==================="

"${GCS_BENCH}" bench \
    --config "${CONFIG_DIR}/unet3d-rapid.yaml" \
    --object-prefix "unet3d/host-${WID}/" \
    --concurrency "${CONCURRENCY}" \
    --warmup 60s \
    --start-at "${EPOCH}" \
    --output-path ~/results/unet3d-worker-${WID}-read \
    --output-format both
