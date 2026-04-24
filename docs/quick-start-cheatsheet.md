# gcs-bench Quick-Start Cheatsheet

A condensed reminder of the most common workflows.  
Full details: [`bench-user-guide.md`](bench-user-guide.md).

---

## Build

```bash
cd /path/to/gcsfuse-bench
make bench                     # builds ./gcs-bench with version stamp
# or manually:
GOTOOLCHAIN=auto go build -o gcs-bench .
```

---

## Single-host workflow

### Step 1 — Validate your config (no GCS touch)

```bash
./gcs-bench bench --config examples/benchmark-configs/unet3d-like.yaml --dry-run
```

### Step 2 — Populate the bucket (prepare)

```bash
./gcs-bench bench \
  --config examples/benchmark-configs/unet3d-like-prepare.yaml \
  --concurrency 64
```

Objects are written once. Re-running prepare on an already-populated prefix is
safe — objects are overwritten. To start from a clean slate, run cleanup first
(see below).

### Step 3 — Run the benchmark

```bash
./gcs-bench bench \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m \
  --concurrency 32 \
  --output-path ./results \
  --output-format both
```

Results land in `./results/bench-YYYYMMDD-HHMMSS/`:  
`bench.txt` (human), `bench.yaml` (machine), `bench.tsv`, `*.hgrm` (histograms).

### RAPID/zonal buckets — add `--rapid-mode auto`

```bash
./gcs-bench bench \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --rapid-mode auto          # detects zonal bucket, enables bidi-gRPC
```

---

## Multi-host workflow (4 or 8 hosts)

### Object sharding vs per-host corpora

| Script | Object layout | Shared FS needed? |
|--------|--------------|-------------------|
| `run-distributed.sh` | All hosts share one prefix; each gets a shard (modulo partition) | Yes — NFS/FUSE for result files |
| `run-distributed-per-host.sh` | Each host has its own sub-prefix (`host-0/`, `host-1/`, …); full corpus per host | No — results uploaded to GCS |

---

### 4-host run — shared NFS (run-distributed.sh)

Run the same command on **all 4 hosts simultaneously**, changing only `WORKER_ID`.

```bash
# On host 0 (also acts as coordinator — merges results after all done)
WORKER_ID=0 NUM_WORKERS=4 OUTPUT_DIR=/mnt/shared/results \
  ./examples/scripts/run-distributed.sh \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m

# On host 1
WORKER_ID=1 NUM_WORKERS=4 OUTPUT_DIR=/mnt/shared/results \
  ./examples/scripts/run-distributed.sh \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m

# On host 2
WORKER_ID=2 NUM_WORKERS=4 OUTPUT_DIR=/mnt/shared/results \
  ./examples/scripts/run-distributed.sh \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m

# On host 3
WORKER_ID=3 NUM_WORKERS=4 OUTPUT_DIR=/mnt/shared/results \
  ./examples/scripts/run-distributed.sh \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m
```

Worker 0 waits for all result files to appear in `OUTPUT_DIR/worker-N/`, then
automatically runs `gcs-bench merge-results` and writes the combined summary to
`OUTPUT_DIR/merged/`.

**Prepare phase (4 hosts):**

```bash
WORKER_ID=0 NUM_WORKERS=4 OUTPUT_DIR=/mnt/shared/results \
  ./examples/scripts/run-distributed.sh \
  --config examples/benchmark-configs/unet3d-like-prepare.yaml \
  --mode prepare
# Repeat with WORKER_ID=1,2,3 simultaneously.
# Each host writes ~12,544 of the 50,176 objects.
```

---

### 8-host run — per-host corpora (run-distributed-per-host.sh)

Each host gets its own full corpus under `unet3d/host-N/`. No shared filesystem
needed — results are uploaded to a GCS path.

```bash
# Phase 1 — populate each host's prefix (run on all 8 hosts simultaneously)
PHASE=prepare WORKER_ID=0 NUM_WORKERS=8 \
  OBJECT_PREFIX_BASE=unet3d/ \
  RESULTS_GCS_PATH=gs://my-bucket/bench-results/ \
  GCS_BENCH=./gcs-bench \
  ./examples/scripts/run-distributed-per-host.sh \
  --config examples/benchmark-configs/unet3d-like-prepare.yaml
# Repeat with WORKER_ID=1 … 7 on each respective host.

# Phase 2 — run the benchmark (all 8 hosts simultaneously)
PHASE=bench WORKER_ID=0 NUM_WORKERS=8 \
  OBJECT_PREFIX_BASE=unet3d/ \
  RESULTS_GCS_PATH=gs://my-bucket/bench-results/ \
  GCS_BENCH=./gcs-bench \
  ./examples/scripts/run-distributed-per-host.sh \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --duration 5m
# Repeat with WORKER_ID=1 … 7 on each respective host.
# Worker 0 polls RESULTS_GCS_PATH, downloads all 8 YAMLs, merges, and exits.
```

---

### Manual multi-host (no helper script)

```bash
# Compute a start time 90 s from now (run on one host, share the value)
START=$(date -d "+90 seconds" +%s)

# On each host (0-based WORKER_ID, total NUM_WORKERS):
./gcs-bench bench \
  --config examples/benchmark-configs/unet3d-like.yaml \
  --worker-id  0 \           # change per host
  --num-workers 4 \          # or 8
  --start-at   ${START} \
  --output-path ./results/worker-0 \
  --output-format yaml

# After ALL workers complete, merge on one host:
./gcs-bench merge-results \
  ./results/worker-{0,1,2,3}/bench-*.yaml \
  --output-path ./results/merged \
  --output-format both
```

---

## Cleanup / delete — removing benchmark objects

Use cleanup to wipe a prefix before re-running prepare, or for end-of-test teardown.  
`cleanup` and `delete` are identical aliases.

### Dry-run first (always a good idea)

```bash
./gcs-bench cleanup \
  --bucket my-bucket \
  --object-prefix resnet50/ \
  --dry-run
```

### Delete a prefix

```bash
./gcs-bench cleanup \
  --bucket my-bucket \
  --object-prefix resnet50/

# Higher concurrency for large prefixes
./gcs-bench cleanup \
  --bucket my-bucket \
  --object-prefix resnet50/ \
  --concurrency 256

# With a service-account key
./gcs-bench cleanup \
  --bucket my-bucket \
  --object-prefix resnet50/ \
  --key-file /path/to/key.json
```

### Clean a distributed run's per-host shards in parallel

```bash
for HOST in 0 1 2 3 4 5 6 7; do
    ./gcs-bench cleanup \
      --bucket my-bucket \
      --object-prefix "unet3d/host-${HOST}/" \
      --concurrency 128 &
done
wait
echo 'All host shards cleaned.'
```

### Sample cleanup output

```
[cleanup] Starting producer/consumer cleanup of gs://my-bucket/resnet50/
[cleanup] delete-workers=64  list-page=1000  channel-buffer=5000

[cleanup] elapsed=5s   listed=18000  deleted=15200  3040/s  queue=2800  errs=0
[cleanup] elapsed=10s  listed=36000  deleted=33400  3640/s  queue=2600  errs=0
[cleanup] elapsed=15s  listed=50176  deleted=49100  3140/s  queue=1076  errs=0
[cleanup] COMPLETE — deleted 50176/50176 objects  elapsed=17s  avg=2951/s  errs=0
```

### Cleanup flags reference

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket` | *(required)* | GCS bucket name |
| `--object-prefix` | *(required)* | Prefix to delete (non-empty required — whole-bucket deletion not supported) |
| `--concurrency` | `64` | Parallel DELETE workers |
| `--dry-run` | `false` | List without deleting |
| `--key-file` | ADC | Service account key JSON |
| `--rapid-mode` | `off` | `auto`/`on`/`off` for RAPID/zonal buckets |

---

## Plotting histograms

```bash
# Single track
./gcs-bench plot-hgrm results/bench-20260329-184200/unet3d-read-total-latency.hgrm

# Compare two runs
./gcs-bench plot-hgrm \
  results/bench-20260329-184200/unet3d-read-total-latency.hgrm \
  results/bench-20260329-185500/unet3d-read-total-latency.hgrm \
  --output comparison.svg
```

---

## Quick-reference flags

| Flag | What it does |
|------|-------------|
| `--dry-run` | Validate config, print plan, no GCS touch |
| `--duration 5m` | Measurement window (default 30s) |
| `--warmup 30s` | Warm-up period, stats discarded (default 5s) |
| `--concurrency 64` | Total I/O goroutines |
| `--rapid-mode auto` | Auto-detect RAPID/zonal and enable bidi-gRPC |
| `--output-path /tmp/r` | Where to write result directories |
| `--output-format both` | Write YAML + TSV (default: yaml only) |
| `--worker-id N` | 0-based index for distributed runs |
| `--num-workers N` | Total hosts (enables sharding + raw histogram export) |
| `--start-at EPOCH` | Sleep until this Unix timestamp (synchronized start) |
| `-v` / `-vv` / `-vvv` | Increase verbosity: INFO / DEBUG / TRACE |
