# gcs-bench User Guide: Parquet Dataloader Benchmarks

This guide covers the Parquet-like workload support in `gcs-bench`, including
all configuration fields, read-path internals, example configs, and unit tests.
It is the companion reference to [parquet_dataloader.md](parquet_dataloader.md).

---

## Table of contents

1. [Background](#background)
2. [New configuration fields](#new-configuration-fields)
3. [Read paths](#read-paths)
   - [new-reader (default)](#new-reader-default)
   - [multirange (MRD)](#multirange-mrd)
   - [traditional-parquet](#traditional-parquet)
4. [Example configs](#example-configs)
   - [Prepare (write) dataset](#prepare-write-dataset)
   - [MRD read (footer + row groups)](#mrd-read-footer--row-groups)
   - [MRD per-range read with random sizes](#mrd-per-range-read-with-random-sizes)
   - [MRD vs traditional comparison](#mrd-vs-traditional-comparison)
5. [Running benchmarks](#running-benchmarks)
6. [Understanding Samples/sec](#understanding-samplessec)
7. [Unit tests](#unit-tests)
   - [Running the tests](#running-the-tests)
   - [Test inventory](#test-inventory)
8. [Configuration reference](#configuration-reference)

---

## Background

A Parquet dataloader reads files in three distinct phases:

```
Phase 1  HEAD/stat ──────────────► learn objectSize
Phase 2  Footer GET ─────────────► read last ~32 KiB (schema + row-group map)
Phase 3  Row-group GETs (×N) ────► read N column chunks in parallel
```

`gcs-bench` models both the **optimized path** (GCS MultiRangeDownloader, MRD)
and the **traditional path** (stat → sequential footer GET → N parallel range
GETs) so you can measure the latency difference directly.

Key insight: the MRD API supports **negative byte offsets** (server resolves
`offset = objectSize + read_offset` without a prior stat) and **multiplexes all
ranges over a single bidi-gRPC stream**, eliminating the sequential stat+footer
round trips and per-request connection overhead of the traditional approach.

---

## New configuration fields

The following fields were added to `BenchmarkTrack` in `cfg/benchmark_config.go`
to support the Parquet simulation workloads:

| Field | Type | Default | Description |
|---|---|---|---|
| `read-type` | string | `"new-reader"` | Selects the GCS read path. See [Read paths](#read-paths). |
| `read-offset` | int64 | `0` | Byte offset to start each read. Negative values read from end of object (MRD only). |
| `read-offset-random` | bool | `false` | Pick a uniformly random offset within `[0, objectSize − readSize]` per read. Overrides `read-offset`. |
| `reads-per-object` | int | `1` | Number of concurrent range reads per operation (MRD paths). Models N row groups per training step. |
| `read-size-min` | int64 | `0` | Minimum per-range size in bytes. When `> 0`, activates the per-range latency tracking path. |
| `read-size-max` | int64 | `0` | Maximum per-range size in bytes (paired with `read-size-min`). |
| `samples-per-object` | int | `0` | Credits this many samples per completed op on the `new-reader` path. Enables `Samples/sec` output for full-object GET workloads. |
| `read-footer-size` | int64 | `32768` | Footer byte-range size for `traditional-parquet` Phase 2. Defaults to 32 KiB when zero. |

Previously-existing fields used heavily in Parquet configs:

| Field | Description |
|---|---|
| `read-size` | Bytes per range read (aggregate MRD path and `traditional-parquet` row-group GETs). |
| `object-size-min` / `object-size-max` | Object size range. Used for random-offset safe-window calculation. |
| `access-pattern` | `"random"` or `"sequential"` within the object list. |
| `object-count` | Number of distinct objects the track selects from. |

---

## Read paths

### new-reader (default)

`read-type: new-reader` (or omitted)

Issues a single `NewReaderWithReadHandle` call per operation. Each call opens a
new HTTP/2 connection. Suitable for sequential or random single-range reads.

- `read-offset` must be `≥ 0`; negative offsets return a counted error.
- `reads-per-object` is ignored.
- `samples-per-object > 0` credits N samples per op for `Samples/sec` output.

```yaml
- name: full-object-read
  op-type: read
  read-type: new-reader   # or omit — same thing
  read-size: 0            # 0 = read entire object
  object-count: 1000
  samples-per-object: 64  # each file has 64 row groups → credits 64 samples/op
```

### multirange (MRD)

`read-type: multirange`

Uses the GCS `MultiRangeDownloader` bidi-gRPC API. Key properties:

- A connection per unique object is **cached** in an LRU (singleflight-deduped),
  so repeated reads of the same file reuse the same stream.
- `reads-per-object: N` dispatches N concurrent `Add()` calls over that stream.
- `read-offset: -N` is passed directly to the server; no stat required.

**Aggregate path** (`read-size-min` not set or `<= 0`):
One histogram entry per operation (covers all N ranges combined).
`TotalSamples` and `Samples/sec` are reported when `reads-per-object > 1`.

**Per-range path** (`read-size-min > 0`):
One histogram entry per individual `Add()` callback. With `reads-per-object: 8`
there are 8 histogram data points per operation call. Each range independently
draws a size from `[read-size-min, read-size-max]`.

```yaml
# Footer: negative offset, single range per op
- name: parquet-footer
  op-type: read
  read-type: multirange
  read-offset: -32768       # last 32 KiB — no stat needed
  read-size: 32768
  reads-per-object: 1

# Row groups: per-range latency, random sizes
- name: parquet-rowgroups
  op-type: read
  read-type: multirange
  read-offset-random: true
  read-size-min: 8388608    # 8 MiB min
  read-size-max: 32768000   # ~31 MiB max
  reads-per-object: 8
```

### traditional-parquet

`read-type: traditional-parquet`

Simulates a conventional object-storage client that cannot use negative offsets
or stream multiplexing. Three sequential/partially-parallel phases:

```
Phase 1 — StatObject (HEAD)
  Discovers objectSize. Required to compute footer offset.
  Latency: T_stat ≈ 20–60 ms on GCS.

Phase 2 — Byte-range GET of footer (sequential after stat)
  Range: [objectSize - read-footer-size, objectSize]
  Cannot begin until stat completes.
  Latency: T_footer ≈ 30–120 ms for 32 KiB from GCS.

Phase 3 — N parallel byte-range GETs for row groups
  Each uses a separate NewReaderWithReadHandle (independent connections).
  N = reads-per-object. Range size = read-size.
  Latency: max(T_rg_0 … T_rg_{N-1}) ≈ 100–500 ms.

Total per-file latency = T_stat + T_footer + max(T_rg_i)
```

`reads-per-object` samples are credited per operation for `Samples/sec`
comparison against the MRD path.

```yaml
- name: parquet-traditional
  op-type: read
  read-type: traditional-parquet
  read-footer-size: 32768   # 32 KiB footer (default if omitted)
  read-size: 16777216       # 16 MiB per row-group GET
  read-offset-random: true  # random row-group position
  reads-per-object: 8       # N parallel row-group GETs in Phase 3
  object-count: 1000
```

> **Note**: `read-footer-size: 0` is silently promoted to `32768` (32 KiB).
> When `reads-per-object` is `0` or unset, defaults to `1`.

---

## Example configs

All configs are in `examples/benchmark-configs/`.

### Prepare (write) dataset

**File**: `parquet-like-mrd-prepare.yaml`

Write 10,000 lognormal-sized objects to the bucket once before benchmarking:

```
mean ≈ 128 MiB, std-dev ≈ 64 MiB, clamped to [32 MiB, 512 MiB]
prefix: parquet-dataset/
```

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd-prepare.yaml \
    --bucket my-bucket-name
```

This takes several minutes the first time. Objects are reused for all read configs
that specify `object-prefix: "parquet-dataset/"`.

### MRD read (footer + row groups)

**File**: `parquet-like-mrd.yaml`

Two-track workload modelling the real Parquet dataloader access pattern:

| Track | `read-type` | What it does |
|---|---|---|
| `parquet-footer` (weight 1) | `multirange` | Reads last 32 KiB via negative offset on MRD stream |
| `parquet-rowgroup` (weight 8) | `multirange` | Reads 4 random 16 MiB row groups per op, all on one MRD stream |

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd.yaml \
    --bucket my-bucket-name
```

### MRD per-range read with random sizes

**File**: `parquet-like-mrd-perrange.yaml`

Activates `doReadMultiRangePerRange` by setting `read-size-min > 0`. Each row
group independently draws a size from `[read-size-min, read-size-max]`. The
histogram shows per-range (not per-operation) latency distribution.

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd-perrange.yaml \
    --bucket my-bucket-name
```

### MRD vs traditional comparison

These two configs are matched on `reads-per-object`, `object-size-min/max`,
`total-concurrency`, and `duration` for a direct side-by-side comparison.

**Optimized path** — `parquet-compare-mrd.yaml`:

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-compare-mrd.yaml \
    --bucket my-bucket-name --output-dir results/mrd/
```

**Traditional path** — `parquet-compare-traditional.yaml`:

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-compare-traditional.yaml \
    --bucket my-bucket-name --output-dir results/traditional/
```

Compare the `Samples/sec` and P50/P90/P99 latency rows in both result YAMLs.
The traditional path will show higher per-op latency (stat + sequential footer
overhead adds to every file visit) with typically lower `Samples/sec` at the
same concurrency.

Example comparison table (illustrative, not from a live run):

| Metric | MRD path | Traditional path | Delta |
|---|---|---|---|
| Samples/sec | ~960 | ~420 | ~2.3× |
| P50 op latency (ms) | ~42 | ~195 | stat+footer cost |
| P99 op latency (ms) | ~180 | ~620 | |
| Throughput (MB/s) | ~1,100 | ~950 | roughly equal |

> Throughput is similar because the same bytes are transferred; the gap is in
> **latency** (the stat + sequential footer round trips are pure overhead).

---

## Running benchmarks

### Prerequisites

1. Build the binary:

   ```bash
   cd /path/to/gcsfuse-bench
   make          # or: go build ./...
   ```

2. Authenticate:

   ```bash
   gcloud auth application-default login
   # or set GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   ```

3. Write the dataset (once):

   ```bash
   gcs-bench bench \
       --config examples/benchmark-configs/parquet-like-mrd-prepare.yaml \
       --bucket YOUR_BUCKET_NAME
   ```

### Running a single benchmark

```bash
gcs-bench bench \
    --config examples/benchmark-configs/parquet-like-mrd.yaml \
    --bucket YOUR_BUCKET_NAME \
    --output-dir ./results/ \
    --output-format yaml
```

### Running the comparison pair

```bash
# Run MRD path
gcs-bench bench \
    --config examples/benchmark-configs/parquet-compare-mrd.yaml \
    --bucket YOUR_BUCKET_NAME \
    --output-dir ./results/mrd/

# Run traditional path (use the same bucket/dataset)
gcs-bench bench \
    --config examples/benchmark-configs/parquet-compare-traditional.yaml \
    --bucket YOUR_BUCKET_NAME \
    --output-dir ./results/traditional/
```

### RAPID / zonal buckets

If your bucket is a RAPID (zonal) bucket, set `rapid-mode: on` or leave it at
`auto` (the engine calls `GetStorageLayout` once to detect). Do **not** use
`rapid-mode: off` against a RAPID bucket — all I/O will fail.

---

## Understanding Samples/sec

A "sample" in `gcs-bench` corresponds to one row group (or column chunk) read.
Because a single file visit delivers multiple row groups, `ops/sec` understates
throughput for AI/ML workloads. `Samples/sec` = `ops/sec × reads-per-object`
and is the primary comparison metric.

**How samples are credited per read path:**

| Path | Mechanism |
|---|---|
| `multirange` + `read-size-min ≤ 0` | `totalRanges += reads-per-object` per op |
| `multirange` + `read-size-min > 0` | `totalRanges += 1` per completed `Add()` callback |
| `traditional-parquet` | `totalRanges += reads-per-object` per op (Phase 3 count) |
| `new-reader` + `samples-per-object > 0` | `totalRanges += samples-per-object` per op |
| `new-reader` + `samples-per-object == 0` | `Samples/sec` not reported (single-range semantics) |

The `AvgSampleSizeBytes` field in the result summary is `totalBytes / totalRanges`
and approximates the mean row-group size seen during the run.

---

## Unit tests

The Parquet workload tests live in
`internal/benchmark/parquet_test.go`. They use in-memory mock buckets and fake
MRD instances so that every test completes in milliseconds without network access.

### Running the tests

Run the full benchmark + cfg test suites:

```bash
cd /path/to/gcsfuse-bench

# All benchmark + cfg tests
go test ./internal/benchmark/... ./cfg/... -count=1 -timeout 120s

# Parquet tests only (faster iteration)
go test ./internal/benchmark/... -run TestParquet -v -count=1

# Per-range tests only
go test ./internal/benchmark/... -run TestPerRange -v -count=1

# Traditional path tests only
go test ./internal/benchmark/... -run TestTraditional -v -count=1

# computeReadOffset / computeReadSize unit tests
go test ./internal/benchmark/... -run 'TestCompute|TestNegativeOffset' -v -count=1
```

Expected output (all 17 tests in the file):

```
=== RUN   TestComputeReadOffsetStatic
--- PASS: TestComputeReadOffsetStatic (0.00s)
=== RUN   TestComputeReadOffsetRandom
--- PASS: TestComputeReadOffsetRandom (0.01s)
...
PASS
ok      github.com/googlecloudplatform/gcsfuse/v3/internal/benchmark  ~23s
```

### Test infrastructure

Two mock bucket types power all tests:

**`mrdMockBucket`** — wraps `mockBucket` and overrides
`NewMultiRangeDownloader` to return a `fake.FakeMultiRangeDownloader` backed by
in-memory zero data. Used for all MRD path tests.

```go
mb := newMRDBucket(objectSize)  // objectSize bytes of zero data
```

**`traditionalMockBucket`** — wraps `mockBucket` and overrides `StatObject` to
return a `gcs.MinObject{Size: statSize}` (the base mock returns `nil`).
Required because `doReadTraditionalParquet` dereferences the `MinObject` to read
`objectSize`.

```go
mb := newTraditionalBucket(objectSize)  // StatObject returns Size = objectSize
```

### Test inventory

All 17 tests in `internal/benchmark/parquet_test.go`:

#### computeReadOffset tests

| Test | What it verifies |
|---|---|
| `TestComputeReadOffsetStatic` | Positive, zero, and negative static offsets pass through unchanged |
| `TestComputeReadOffsetRandom` | Random offsets always land in `[0, objectSizeMax − readSize]` |
| `TestComputeReadOffsetRandomClampSmallObject` | Object exactly equal to readSize → offset always 0 |
| `TestComputeReadOffsetRandomUsesReadSizeMaxForWindow` | When `ReadSizeMax > ReadSize`, safe-window uses `ReadSizeMax` |
| `TestNegativeOffsetRejectedForNewReader` | Negative `read-offset` on `new-reader` path counts an error |

#### computeReadSize tests

| Test | What it verifies |
|---|---|
| `TestComputeReadSizeFallback` | `ReadSizeMin ≤ 0` returns `ReadSize` unchanged |
| `TestComputeReadSizeFixed` | `ReadSizeMin == ReadSizeMax` always returns that exact value |
| `TestComputeReadSizeRandom` | All draws land in `[ReadSizeMin, ReadSizeMax]` over 10,000 iterations |

#### Engine / integration tests

| Test | What it verifies |
|---|---|
| `TestParquetLikePrepare` | Prepare mode writes exactly `objectCount` objects with zero errors |
| `TestParquetLikeFooterRead` | MRD negative-offset footer read: zero errors, avg bytes ≈ footer size |
| `TestParquetLikeRowGroupRead` | MRD random-offset, `reads-per-object:4`: zero errors, avg bytes ≈ 4 × readSize |
| `TestParquetLikeTwoTrackEngine` | Two-track engine (footer + row-group): both tracks complete with zero errors |
| `TestPerRangeLatencyFixed` | `read-size-min == read-size-max > 0`: per-range path, zero errors, correct byte totals |
| `TestPerRangeLatencyRandomSize` | `read-size-min < read-size-max > 0`: random sizes, totals within expected range |
| `TestTraditionalParquetBasic` | `traditional-parquet`: zero errors, `TotalSamples > 0`, `SamplesPerSec > 0`, samples/op ≈ reads-per-object |
| `TestTraditionalParquetDefaultFooterSize` | `read-footer-size: 0` promoted to 32 KiB: no errors |
| `TestSamplesPerObjectNewReader` | `samples-per-object > 0` on `new-reader` track: `TotalSamples ≈ TotalOps × N` |

---

## Configuration reference

Full listing of all Parquet-relevant fields with their types and defaults.
(All fields live under `benchmark.tracks[*]` in the YAML.)

```yaml
benchmark:
  tracks:
    - name: my-parquet-track
      op-type: read

      # ── Read path selector ──────────────────────────────────────────────────
      read-type: multirange       # "new-reader" | "multirange" | "traditional-parquet"

      # ── Object selection ────────────────────────────────────────────────────
      object-count: 1000
      object-size-min: 33554432   # 32 MiB — used for random-offset safe window
      object-size-max: 536870912  # 512 MiB

      # ── Read sizing ─────────────────────────────────────────────────────────
      read-size: 16777216         # bytes per range (aggregate path / traditional Phase 3)
      read-size-min: 0            # > 0 activates per-range latency path (MRD only)
      read-size-max: 0            # upper bound for random range sizes

      # ── Offset control ──────────────────────────────────────────────────────
      read-offset: 0              # >= 0: absolute; < 0: from end of object (MRD only)
      read-offset-random: false   # true: uniform random in [0, objectSize - effectiveReadSize]

      # ── Per-op parallelism ──────────────────────────────────────────────────
      reads-per-object: 8         # N concurrent ranges per op (MRD paths)

      # ── Sample accounting ───────────────────────────────────────────────────
      samples-per-object: 0       # new-reader: credits N samples per op for Samples/sec
      read-footer-size: 32768     # traditional-parquet: footer GET size (Phase 2)

      # ── Concurrency / weight ─────────────────────────────────────────────────
      weight: 1
      concurrency: 0              # 0 = derived from weight + total-concurrency
```

### Which fields apply to which path

| Field | `new-reader` | `multirange` aggregate | `multirange` per-range | `traditional-parquet` |
|---|:---:|:---:|:---:|:---:|
| `read-size` | ✓ | ✓ | fallback only | ✓ (Phase 3) |
| `read-size-min` / `read-size-max` | — | — | ✓ | — |
| `read-offset` | ✓ (≥0 only) | ✓ | ✓ | — (computed from stat) |
| `read-offset-random` | ✓ | ✓ | ✓ | ✓ (Phase 3 offsets) |
| `reads-per-object` | — | ✓ | ✓ | ✓ (N Phase 3 GETs) |
| `samples-per-object` | ✓ | — | — | — |
| `read-footer-size` | — | — | — | ✓ (Phase 2) |
