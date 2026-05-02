#!/bin/bash
# run-dlrm-prepare.sh — Write the DLRM embedding table dataset (one host)
#
# Creates 5000 synthetic embedding table files (Parquet, lognormal 64 MiB –
# 1 GiB, mean ≈ 256 MiB) under dlrm-embeddings/host-<WID>/ in the target bucket.
# Total data per host: ~1.25 TiB.  Estimated write time: 30–120 s at ~5 GiB/s.
#
# MULTI-HOST USAGE (recommended — run simultaneously on each VM):
#   EPOCH=$(date -d '+2 minutes' +%s)   # generate once, share the value
#   bash run-dlrm-prepare.sh 0 $EPOCH   # vm1 → dlrm-embeddings/host-0/
#   bash run-dlrm-prepare.sh 1 $EPOCH   # vm2 → dlrm-embeddings/host-1/
#   bash run-dlrm-prepare.sh 2 $EPOCH   # vm3 → dlrm-embeddings/host-2/
#   bash run-dlrm-prepare.sh 3 $EPOCH   # vm4 → dlrm-embeddings/host-3/
#
# SINGLE-HOST USAGE:
#   EPOCH=$(date -d '+1 minute' +%s)
#   bash run-dlrm-prepare.sh 0 $EPOCH
#
# Usage:
#   bash run-dlrm-prepare.sh <WID> <EPOCH> [CONCURRENCY] [BUCKET]
#
#   WID          Host/worker ID: 0, 1, 2, or 3
#   EPOCH        Synchronized Unix epoch start time (seconds)
#   CONCURRENCY  Number of write goroutines (default: 16)
#   BUCKET       GCS bucket name (default: sig65-rapid1)

set -euo pipefail

# ---------- locate binary and configs (works from any working directory) ------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing --------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [CONCURRENCY] [BUCKET]" >&2
    echo "  WID         : worker/host ID (0-3)" >&2
    echo "  EPOCH       : synchronized Unix start time (seconds)" >&2
    echo "  CONCURRENCY : write goroutines, default 16" >&2
    echo "  BUCKET      : GCS bucket, default sig65-rapid1" >&2
    exit 1
fi

WID="${1}"
EPOCH="${2}"
CONCURRENCY="${3:-16}"
BUCKET="${4:-sig65-rapid1}"

# ---------- validate ----------------------------------------------------------
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

# ---------- print run summary -------------------------------------------------
echo "=== DLRM embedding table prepare ==="
echo "  Host/WID    : ${WID}"
echo "  Prefix      : dlrm-embeddings/host-${WID}/"
echo "  Objects     : 5000 embedding tables (lognormal, 64 MiB – 1 GiB, mean ≈ 256 MiB)"
echo "  Dataset     : ~1.25 TiB"
echo "  Bucket      : ${BUCKET}"
echo "  Concurrency : ${CONCURRENCY}"
echo "  Start-at    : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH} 2>/dev/null || echo 'date conversion unavailable'))"
echo "  Output      : ~/results/dlrm-worker-${WID}-prepare"
echo "====================================="

# ---------- run ---------------------------------------------------------------
"${GCS_BENCH}" bench \
    --config "${CONFIG_DIR}/dlrm-prepare.yaml" \
    --bucket "${BUCKET}" \
    --object-prefix "dlrm-embeddings/host-${WID}/" \
    --concurrency "${CONCURRENCY}" \
    --start-at "${EPOCH}" \
    --output-path ~/results/dlrm-worker-${WID}-prepare \
    --output-format both

echo ""
echo "Prepare complete.  Dataset at gs://${BUCKET}/dlrm-embeddings/host-${WID}/"
echo "Next steps:"
echo "  Single-host comparison : bash ${SCRIPT_DIR}/run-dlrm-compare.sh ${WID} <EPOCH>"
echo "  Multi-host  comparison : bash ${SCRIPT_DIR}/run-dlrm-compare-all.sh ${WID} <EPOCH>"
