#!/bin/bash
# run-dlrm-compare.sh — DLRM single-prefix: MRD vs traditional comparison
#
# Reads from dlrm-embeddings/host-<WID>/ and runs two sequential benchmark
# phases against that prefix: first the RAPID MRD path, then the traditional
# S3-style byte-range path.  Use this to compare reader architectures on a
# single host's portion of the embedding table dataset.
#
# For a cluster-wide read across ALL host prefixes simultaneously, use
# run-dlrm-compare-all.sh instead.
#
# MULTI-HOST USAGE (all VMs start MRD phase simultaneously, traditional
# phase is auto-synchronized via pre-calculated TRAD_EPOCH):
#   EPOCH=$(date -d '+3 minutes' +%s)
#   bash run-dlrm-compare.sh 0 $EPOCH    # vm1 → reads host-0/
#   bash run-dlrm-compare.sh 1 $EPOCH    # vm2 → reads host-1/
#   bash run-dlrm-compare.sh 2 $EPOCH    # vm3 → reads host-2/
#   bash run-dlrm-compare.sh 3 $EPOCH    # vm4 → reads host-3/
#
# SINGLE-HOST USAGE:
#   EPOCH=$(date -d '+1 minute' +%s)
#   bash run-dlrm-compare.sh 0 $EPOCH
#
# Usage:
#   bash run-dlrm-compare.sh <WID> <EPOCH> [CONCURRENCY] [DURATION] [BUCKET]
#
#   WID          Host/worker ID: 0, 1, 2, or 3
#   EPOCH        Synchronized Unix epoch start time for MRD phase (seconds)
#   CONCURRENCY  Number of goroutines per run (default: 64)
#   DURATION     Measurement duration per phase in Go format (default: 5m)
#   BUCKET       GCS bucket name (default: sig65-rapid1)
#
# Prerequisites:
#   Objects must exist under dlrm-embeddings/host-<WID>/
#   Run run-dlrm-prepare.sh <WID> <EPOCH> first if needed.

set -euo pipefail

# ---------- locate binary and configs (works from any working directory) ------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing --------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [CONCURRENCY] [DURATION] [BUCKET]" >&2
    echo "  WID         : worker/host ID (0-3)" >&2
    echo "  EPOCH       : synchronized Unix start time for MRD phase (seconds)" >&2
    echo "  CONCURRENCY : goroutines per phase, default 64" >&2
    echo "  DURATION    : measurement duration, default 5m (Go duration string)" >&2
    echo "  BUCKET      : GCS bucket, default sig65-rapid1" >&2
    exit 1
fi

WID="${1}"
EPOCH="${2}"
CONCURRENCY="${3:-64}"
DURATION="${4:-5m}"
BUCKET="${5:-sig65-rapid1}"

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

# ---------- calculate traditional phase start epoch --------------------------
# TRAD_EPOCH is pre-calculated so that all VMs in a multi-host run start their
# traditional phase at the same wall-clock time without any manual coordination.
# Formula: MRD_start + warmup(60s) + measurement + 90s_grace
#
# Convert DURATION string to seconds (supports: Xs, Xm, Xh, XmYs).
duration_to_seconds() {
    local d="${1}"
    if [[ "$d" =~ ^([0-9]+)h([0-9]+)m([0-9]+)s$ ]]; then
        echo $(( ${BASH_REMATCH[1]}*3600 + ${BASH_REMATCH[2]}*60 + ${BASH_REMATCH[3]} ))
    elif [[ "$d" =~ ^([0-9]+)h([0-9]+)m$ ]]; then
        echo $(( ${BASH_REMATCH[1]}*3600 + ${BASH_REMATCH[2]}*60 ))
    elif [[ "$d" =~ ^([0-9]+)h$ ]]; then
        echo $(( ${BASH_REMATCH[1]}*3600 ))
    elif [[ "$d" =~ ^([0-9]+)m([0-9]+)s$ ]]; then
        echo $(( ${BASH_REMATCH[1]}*60 + ${BASH_REMATCH[2]} ))
    elif [[ "$d" =~ ^([0-9]+)m$ ]]; then
        echo $(( ${BASH_REMATCH[1]}*60 ))
    elif [[ "$d" =~ ^([0-9]+)s$ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo 300  # fallback: 5m
    fi
}

WARMUP_S=60
MEASURE_S=$(duration_to_seconds "${DURATION}")
TRAD_EPOCH=$(( EPOCH + WARMUP_S + MEASURE_S + 90 ))

# Output directories stamped with WID + epoch for easy archiving.
MRD_OUT="${HOME}/results/dlrm-worker-${WID}-mrd-${EPOCH}"
TRAD_OUT="${HOME}/results/dlrm-worker-${WID}-traditional-${EPOCH}"

# ---------- print run summary -------------------------------------------------
echo "======================================================="
echo " DLRM Inferencing: MRD vs Traditional  (host-${WID})"
echo "======================================================="
echo "  Bucket       : ${BUCKET}"
echo "  Prefix       : dlrm-embeddings/host-${WID}/"
echo "  Dataset      : 5000 embedding tables, 64 MiB – 1 GiB"
echo "  Concurrency  : ${CONCURRENCY} goroutines (each phase)"
echo "  Duration     : ${DURATION} measurement + 60s warmup"
echo "  Access       : 1 footer (32 KiB) + 16 embedding reads (8–128 KiB)"
echo ""
echo "  MRD   phase start : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH} 2>/dev/null || echo 'n/a'))"
echo "  Trad  phase start : ${TRAD_EPOCH}  (auto-calculated, ~$(( WARMUP_S + MEASURE_S + 90 ))s after MRD)"
echo ""
echo "  MRD   output : ${MRD_OUT}"
echo "  Trad  output : ${TRAD_OUT}"
echo "======================================================="
echo ""

# ---------- phase 1: RAPID MRD -----------------------------------------------
echo "--- Phase 1/2: RAPID MultiRangeDownloader (MRD) ---"

"${GCS_BENCH}" bench \
    --config "${CONFIG_DIR}/dlrm-compare-mrd.yaml" \
    --bucket "${BUCKET}" \
    --object-prefix "dlrm-embeddings/host-${WID}/" \
    --concurrency "${CONCURRENCY}" \
    --duration "${DURATION}" \
    --warmup 60s \
    --start-at "${EPOCH}" \
    --output-path "${MRD_OUT}" \
    --output-format both

echo ""
echo "  MRD phase complete → ${MRD_OUT}"
echo ""

# ---------- phase 2: traditional S3-style ------------------------------------
echo "--- Phase 2/2: Traditional byte-range (baseline) ---"
echo "  Waiting for synchronized start at ${TRAD_EPOCH} ..."

"${GCS_BENCH}" bench \
    --config "${CONFIG_DIR}/dlrm-compare-traditional.yaml" \
    --bucket "${BUCKET}" \
    --object-prefix "dlrm-embeddings/host-${WID}/" \
    --concurrency "${CONCURRENCY}" \
    --duration "${DURATION}" \
    --warmup 60s \
    --start-at "${TRAD_EPOCH}" \
    --output-path "${TRAD_OUT}" \
    --output-format both

echo ""
echo "  Traditional phase complete → ${TRAD_OUT}"
echo ""

# ---------- summary ----------------------------------------------------------
echo "======================================================="
echo " Comparison complete."
echo "   MRD:         ${MRD_OUT}/bench.txt"
echo "   Traditional: ${TRAD_OUT}/bench.txt"
echo ""
echo " Key metrics to compare:"
echo "   dlrm-embedding-mrd track  vs  dlrm-traditional track"
echo "   Samples/sec  (higher is better)"
echo "   P50 / P95 / P99 latency   (lower is better)"
echo "======================================================="
