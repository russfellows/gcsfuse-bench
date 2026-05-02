#!/bin/bash
# run-dlrm-compare-all.sh — DLRM all-prefixes: MRD vs traditional comparison
#
# Each host reads ALL NUM_WORKERS host prefixes simultaneously, running
# NUM_WORKERS parallel gcs-bench processes per phase.  This matches the
# cluster-wide aggregate load pattern of run-unet3d-read-all.sh.
#
# Two sequential phases per run:
#   Phase 1 — MRD:         NUM_WORKERS parallel gcs-bench processes,
#              each reading one dlrm-embeddings/host-{0..N-1}/ prefix using
#              read-type: multirange (bidi-gRPC, negative-offset footer support)
#   Phase 2 — Traditional: same NUM_WORKERS parallel processes using
#              read-type: traditional-parquet (HEAD + footer GET + N range GETs)
#
# The traditional phase start time (TRAD_EPOCH) is pre-calculated from the
# MRD EPOCH so all VMs in a multi-host run enter the traditional phase at
# exactly the same wall-clock time, without any manual coordination.
#
# MULTI-HOST USAGE (recommended — run simultaneously on each VM):
#   EPOCH=$(date -d '+3 minutes' +%s)
#   bash run-dlrm-compare-all.sh 0 $EPOCH    # vm1
#   bash run-dlrm-compare-all.sh 1 $EPOCH    # vm2
#   bash run-dlrm-compare-all.sh 2 $EPOCH    # vm3
#   bash run-dlrm-compare-all.sh 3 $EPOCH    # vm4
#
# SINGLE-HOST USAGE (reads all prefixes from one VM):
#   EPOCH=$(date -d '+1 minute' +%s)
#   bash run-dlrm-compare-all.sh 0 $EPOCH
#
# Usage:
#   bash run-dlrm-compare-all.sh <WID> <EPOCH> [NUM_WORKERS] [CONCURRENCY] [TRAD_CONCURRENCY] [DURATION] [BUCKET]
#
#   WID               This host's ID (0-3) — used for output path naming only
#   EPOCH             Synchronized Unix epoch start time for MRD phase (seconds)
#   NUM_WORKERS       Number of host prefixes to read simultaneously (1-4, default: 4)
#                     Must match the number of hosts that ran run-dlrm-prepare.sh
#   CONCURRENCY       Total goroutines for the MRD phase (split equally across prefixes,
#                     default: 256 → 64 per prefix with NUM_WORKERS=4)
#   TRAD_CONCURRENCY  Total goroutines for the traditional phase (default: CONCURRENCY/4).
#                     Traditional ops issue 18 GCS requests each (stat+footer+16 ranges),
#                     so this must be kept lower than CONCURRENCY to avoid throttling.
#                     With defaults: 64 total → 16 per prefix with NUM_WORKERS=4.
#   DURATION          Measurement duration per phase in Go format (default: 5m)
#   BUCKET            GCS bucket name (default: sig65-rapid1)
#
# Prerequisites:
#   dlrm-embeddings/host-{0..NUM_WORKERS-1}/ must be populated.
#   Run run-dlrm-prepare.sh on each host first:
#     EPOCH=$(date -d '+2 minutes' +%s)
#     bash run-dlrm-prepare.sh 0 $EPOCH   # vm1
#     bash run-dlrm-prepare.sh 1 $EPOCH   # vm2
#     ...

set -euo pipefail

# ---------- locate binary and configs (works from any working directory) ------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../benchmark-configs"
GCS_BENCH="${HOME}/gcs-bench"

# ---------- argument parsing --------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <WID> <EPOCH> [NUM_WORKERS] [CONCURRENCY] [TRAD_CONCURRENCY] [DURATION] [BUCKET]" >&2
    echo "  WID              : this host's ID (0-3); used for output path naming" >&2
    echo "  EPOCH            : synchronized Unix start time for MRD phase (seconds)" >&2
    echo "  NUM_WORKERS      : prefixes to read simultaneously (1-4, default 4)" >&2
    echo "  CONCURRENCY      : total goroutines for MRD phase, default 256 (64/prefix)" >&2
    echo "  TRAD_CONCURRENCY : total goroutines for traditional phase (default CONCURRENCY/4)" >&2
    echo "                     lower because each op issues 18 GCS requests" >&2
    echo "  DURATION         : measurement duration, default 5m (Go duration string)" >&2
    echo "  BUCKET           : GCS bucket, default sig65-rapid1" >&2
    exit 1
fi

WID="${1}"
EPOCH="${2}"
NUM_WORKERS="${3:-4}"
CONCURRENCY="${4:-256}"
_TRAD_DEFAULT=$(( CONCURRENCY / 4 < NUM_WORKERS ? NUM_WORKERS : CONCURRENCY / 4 ))
TRAD_CONCURRENCY="${5:-${_TRAD_DEFAULT}}"
DURATION="${6:-5m}"
BUCKET="${7:-sig65-rapid1}"

# ---------- validate ----------------------------------------------------------
if [[ ! "$WID" =~ ^[0-3]$ ]]; then
    echo "ERROR: WID must be 0, 1, 2, or 3 (got: $WID)" >&2
    exit 1
fi

if [[ ! "$EPOCH" =~ ^[0-9]+$ ]]; then
    echo "ERROR: EPOCH must be a positive integer (got: $EPOCH)" >&2
    exit 1
fi

if [[ ! "$NUM_WORKERS" =~ ^[1-4]$ ]]; then
    echo "ERROR: NUM_WORKERS must be 1, 2, 3, or 4 (got: $NUM_WORKERS)" >&2
    exit 1
fi

if [[ ! "$CONCURRENCY" =~ ^[0-9]+$ || "$CONCURRENCY" -lt "$NUM_WORKERS" ]]; then
    echo "ERROR: CONCURRENCY must be >= NUM_WORKERS ($NUM_WORKERS) (got: $CONCURRENCY)" >&2
    exit 1
fi

if [[ ! "$TRAD_CONCURRENCY" =~ ^[0-9]+$ || "$TRAD_CONCURRENCY" -lt "$NUM_WORKERS" ]]; then
    echo "ERROR: TRAD_CONCURRENCY must be >= NUM_WORKERS ($NUM_WORKERS) (got: $TRAD_CONCURRENCY)" >&2
    exit 1
fi

NOW=$(date +%s)
if [[ "$EPOCH" -le "$NOW" ]]; then
    echo "WARNING: EPOCH ($EPOCH) is in the past — hosts may not start simultaneously." >&2
fi

# ---------- derive per-prefix concurrency ------------------------------------
PER_PREFIX=$(( CONCURRENCY / NUM_WORKERS ))
if [[ "$PER_PREFIX" -lt 1 ]]; then
    PER_PREFIX=1
fi

TRAD_PER_PREFIX=$(( TRAD_CONCURRENCY / NUM_WORKERS ))
if [[ "$TRAD_PER_PREFIX" -lt 1 ]]; then
    TRAD_PER_PREFIX=1
fi

# ---------- calculate traditional phase start epoch --------------------------
# TRAD_EPOCH is identical across all VMs (derived from the same EPOCH + fixed
# arithmetic), ensuring synchronized traditional-phase start without any
# manual coordination between hosts.
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

# Output root for this host's results.
OUT_ROOT="${HOME}/results/dlrm-worker-${WID}-compare-all-${EPOCH}"
MRD_ROOT="${OUT_ROOT}/mrd"
TRAD_ROOT="${OUT_ROOT}/traditional"

# ---------- print run summary ------------------------------------------------
echo "============================================================"
echo " DLRM Inferencing: MRD vs Traditional  (all-prefixes)"
echo "============================================================"
echo "  This host / WID  : ${WID}"
echo "  Bucket           : ${BUCKET}"
echo "  Prefixes         : dlrm-embeddings/host-{0..$(( NUM_WORKERS - 1 ))}/"
echo "  NUM_WORKERS      : ${NUM_WORKERS}"
  echo "  MRD goroutines   : ${CONCURRENCY} total (${PER_PREFIX} per prefix)"
  echo "  Trad goroutines  : ${TRAD_CONCURRENCY} total (${TRAD_PER_PREFIX} per prefix)  [lower — each op issues 18 GCS requests]"
echo "  Duration         : ${DURATION} measurement + 60s warmup"
echo "  Access           : 1 footer (32 KiB) + 16 embedding reads (8–128 KiB)"
echo ""
echo "  MRD   phase start : ${EPOCH}  ($(date -d @${EPOCH} 2>/dev/null || date -r ${EPOCH} 2>/dev/null || echo 'n/a'))"
echo "  Trad  phase start : ${TRAD_EPOCH}  (auto-calculated)"
echo ""
echo "  MRD   output root : ${MRD_ROOT}"
echo "  Trad  output root : ${TRAD_ROOT}"
echo "============================================================"
echo ""

mkdir -p "${MRD_ROOT}" "${TRAD_ROOT}"

# ==========================================================================
# Phase 1 — RAPID MRD
# ==========================================================================
echo "--- Phase 1/2: RAPID MultiRangeDownloader (MRD) ---"
echo "  Launching ${NUM_WORKERS} parallel reader(s), one per prefix ..."
echo ""

MRD_PIDS=()
for H in $(seq 0 $(( NUM_WORKERS - 1 ))); do
    LOG="${MRD_ROOT}/host-${H}.log"
    "${GCS_BENCH}" bench \
        --config "${CONFIG_DIR}/dlrm-compare-mrd.yaml" \
        --bucket "${BUCKET}" \
        --object-prefix "dlrm-embeddings/host-${H}/" \
        --concurrency "${PER_PREFIX}" \
        --duration "${DURATION}" \
        --warmup 60s \
        --start-at "${EPOCH}" \
        --output-path "${MRD_ROOT}/host-${H}" \
        --output-format both \
        2>&1 | sed -u "s/^/[mrd-host-${H}] /" | tee "${LOG}" &
    MRD_PIDS+=($!)
    echo "  Launched MRD reader for host-${H}/ (PID ${MRD_PIDS[-1]})"
done
echo ""
echo "  Waiting for all MRD readers to complete ..."

MRD_FAILED=0
for i in "${!MRD_PIDS[@]}"; do
    PID="${MRD_PIDS[$i]}"
    H=$i
    if wait "$PID"; then
        echo "  MRD host-${H}: DONE"
    else
        echo "  MRD host-${H}: FAILED (exit $?)" >&2
        MRD_FAILED=$(( MRD_FAILED + 1 ))
    fi
done

if [[ "$MRD_FAILED" -gt 0 ]]; then
    echo "ERROR: ${MRD_FAILED} MRD reader(s) failed.  Aborting before traditional phase." >&2
    exit 1
fi
echo ""
echo "  MRD phase complete.  Logs: ${MRD_ROOT}/host-{0..$(( NUM_WORKERS - 1 ))}.log"
echo ""

# ==========================================================================
# Phase 2 — Traditional S3-style
# ==========================================================================
echo "--- Phase 2/2: Traditional byte-range (baseline) ---"
echo "  Launching ${NUM_WORKERS} parallel reader(s) ..."
echo "  Waiting for synchronized start at ${TRAD_EPOCH} ..."
echo ""

TRAD_PIDS=()
for H in $(seq 0 $(( NUM_WORKERS - 1 ))); do
    LOG="${TRAD_ROOT}/host-${H}.log"
    "${GCS_BENCH}" bench \
        --config "${CONFIG_DIR}/dlrm-compare-traditional.yaml" \
        --bucket "${BUCKET}" \
        --object-prefix "dlrm-embeddings/host-${H}/" \
        --concurrency "${TRAD_PER_PREFIX}" \
        --duration "${DURATION}" \
        --warmup 60s \
        --start-at "${TRAD_EPOCH}" \
        --output-path "${TRAD_ROOT}/host-${H}" \
        --output-format both \
        2>&1 | sed -u "s/^/[trad-host-${H}] /" | tee "${LOG}" &
    TRAD_PIDS+=($!)
    echo "  Launched traditional reader for host-${H}/ (PID ${TRAD_PIDS[-1]})"
done
echo ""
echo "  Waiting for all traditional readers to complete ..."

TRAD_FAILED=0
for i in "${!TRAD_PIDS[@]}"; do
    PID="${TRAD_PIDS[$i]}"
    H=$i
    if wait "$PID"; then
        echo "  Traditional host-${H}: DONE"
    else
        echo "  Traditional host-${H}: FAILED (exit $?)" >&2
        TRAD_FAILED=$(( TRAD_FAILED + 1 ))
    fi
done

# ==========================================================================
# Summary
# ==========================================================================
echo ""
echo "============================================================"
echo " Comparison complete."
echo ""
echo "  MRD   logs : ${MRD_ROOT}/host-{0..$(( NUM_WORKERS - 1 ))}.log"
echo "  Trad  logs : ${TRAD_ROOT}/host-{0..$(( NUM_WORKERS - 1 ))}.log"
echo ""
echo " Key metrics to compare (per prefix output in bench.txt files):"
echo "   dlrm-embedding-mrd track  vs  dlrm-traditional track"
echo "   Samples/sec  (higher is better)"
echo "   P50 / P95 / P99 latency   (lower is better)"
echo "============================================================"

if [[ "$TRAD_FAILED" -gt 0 ]]; then
    echo "ERROR: ${TRAD_FAILED} traditional reader(s) failed." >&2
    exit 1
fi
