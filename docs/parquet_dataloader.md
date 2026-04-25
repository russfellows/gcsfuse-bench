# Parquet Dataloader Simulation in gcs-bench

**Added in:** v1.3.0 — `read-offset`, `read-offset-random`, `reads-per-object`  
**Updated in:** v1.4.0 — `read-size-min`, `read-size-max`, `samples_per_sec`, `doReadMultiRangePerRange`

**Config examples:**
- [`examples/benchmark-configs/parquet-like-mrd-prepare.yaml`](../examples/benchmark-configs/parquet-like-mrd-prepare.yaml) — write the dataset
- [`examples/benchmark-configs/parquet-like-mrd.yaml`](../examples/benchmark-configs/parquet-like-mrd.yaml) — file-level throughput benchmark
- [`examples/benchmark-configs/parquet-like-mrd-perrange.yaml`](../examples/benchmark-configs/parquet-like-mrd-perrange.yaml) — per-row-group sample benchmark (**recommended for AI/ML**)

**Unit tests:** [`internal/benchmark/parquet_test.go`](../internal/benchmark/parquet_test.go)

---

## Background: What a Parquet file looks like

A Parquet file is a self-describing columnar format. Every file ends with a
metadata footer that contains the schema and an index of every row group and
column chunk stored in the file body. An AI/ML training dataloader reads each
file in two passes:

```
┌───────────────────────────────────────────────────────┐
│  Magic bytes  "PAR1"  (4 B)                           │
├───────────────────────────────────────────────────────┤
│  Row Group 0                                          │
│    Column chunk A  (e.g. 48 MiB)                      │  ← sizes differ
│    Column chunk B  (e.g.  9 MiB)                      │    across columns
│    Column chunk C  (e.g.  2 MiB)                      │    AND row groups
├───────────────────────────────────────────────────────┤
│  Row Group 1                                          │
│    Column chunk A  (e.g. 51 MiB)                      │
│    Column chunk B  (e.g.  8 MiB)                      │
│    Column chunk C  (e.g.  3 MiB)                      │
├───────────────────────────────────────────────────────┤
│  …                                                    │
├───────────────────────────────────────────────────────┤
│  File Footer  (Thrift-encoded FileMetaData)           │
│    · schema: column names, types, encodings           │
│    · per-row-group: byte_offset, total_byte_size      │
│    · per-column-chunk: file_offset, compressed_size,  │
│        uncompressed_size, encoding, compression_codec │
├───────────────────────────────────────────────────────┤
│  Footer length  (4 B little-endian int32)             │
│  Magic bytes  "PAR1"  (4 B)                           │
└───────────────────────────────────────────────────────┘
```

### Key facts about real Parquet files

- **Column chunk sizes are NOT all the same.** They vary:
  - Across columns in the same row group — a high-cardinality string column is many
    times larger than a boolean column.
  - Across row groups for the same column — compression ratio changes with data
    distribution (dictionary encoding, RLE, Snappy/Zstd all interact).
  - Across files — same schema, different data content → different compression.
- **Footer size** is typically 4–64 KiB for production-scale datasets. 32 KiB is
  a conservative upper bound used by most readers (pyarrow, Arrow C++, Spark).
- **Read sequence:**
  1. Read the last 8 bytes to find the footer length.
  2. Seek to `objectSize - footerLength - 8` and read the footer.
  3. Parse the footer to get row-group byte offsets.
  4. For each training batch, select N row groups and issue N parallel range reads.

---

## What the benchmark simulates

The benchmark objects are **opaque byte blobs** — not real Parquet. The content
is pseudo-random (Xoshiro256++) and has no Parquet structure. What the benchmark
faithfully models is the **I/O access pattern**:

| Real Parquet dataloader operation | Benchmark equivalent |
|---|---|
| Read last 8 B + footer bytes | `read-offset: -32768` (static tail read, 32 KiB) |
| Parse footer → get row-group offsets | implicit — represented by `read-offset-random: true` |
| Read N column chunks from one file | `reads-per-object: N` (N concurrent `Add()` calls on one MRD stream) |
| Column chunk sizes vary across files | `read-size-min` / `read-size-max` (uniform random per range) |
| Objects range from 32 MiB to 512 MiB | `object-size-min: 33554432`, `object-size-max: 536870912` |

The GCS MultiRangeDownloader (MRD) bidi-gRPC stream keeps a single bidirectional
connection open and dispatches all N range reads over it — exactly as a Parquet
reader batches row-group reads in one server round-trip.

---

## Why bytes/s and latency alone are insufficient for AI/ML

Aggregate throughput answers "how fast is the storage?" — useful for vendor
comparisons, not for training pipeline sizing. AI/ML training consumes data in
**samples** (row groups) and builds mini-batches. The metric that determines
whether your dataloader can keep GPUs busy is:

> **Samples/second** — how many independent row groups the storage layer delivers
> per second, regardless of which Parquet file they came from.

Consider a workload with 100-row-group files where each training step reads 8
row groups from a file:

| Metric | What it tells you |
|---|---|
| Throughput (bytes/s) | Storage bandwidth — necessary but not sufficient |
| Ops/sec (file ops) | How many files were opened per second |
| **Samples/sec** | How many row groups were actually delivered per second |
| Per-range P99 latency | Worst-case stall per sample — determines tail latency felt by the GPU |

`gcs-bench` reports all four when the per-range path is active.

---

## Worked example: 100 Parquet objects, 64 row groups each, read 5 at random

This section shows exactly what happens when you run a realistic Parquet
dataloader benchmark — 100 objects, each with 64 row groups, reading 5 at
random per training step.

### The config

```yaml
tracks:
  - name: parquet-footer
    read-type: multirange
    read-offset: -32768        # last 32 KiB = Parquet footer
    read-size: 32768
    reads-per-object: 1
    weight: 1

  - name: parquet-rowgroup-samples
    read-type: multirange
    read-offset-random: true   # random position within object
    read-size-min: 8388608     # 8 MiB  (tune to your actual row-group size)
    read-size-max: 16777216    # 16 MiB
    reads-per-object: 5        # ← read 5 row groups per object visit
    object-count: 100
    weight: 20
```

### What the engine does for each iteration

For each goroutine iteration on `parquet-rowgroup-samples`,
`doReadMultiRangePerRange` is called once:

```
op start  (one "object visit")
│
├── getOrCreateMRD("object-042.parquet")     ← reuse cached bidi-gRPC stream
│
├── for i in 0..5:
│     offset_i = uniform random in [0, objectSize − readSize_i]  ← independent
│     size_i   = uniform random in [8 MiB, 16 MiB]
│     mrd.Add(ttfbWriter_i, offset_i, size_i, callback_i)        ← non-blocking
│
├── wait for 5 callbacks (all 5 ranges in flight over one stream)
│     callback fires when range i completes → records latency, bytes
│     totalRanges += 1 per successful range
│
└── op complete: 1 file op counted, 5 samples counted
```

With 100 objects and 5 row groups each, one full pass delivers:
- **100 file-level ops** — each op visits one Parquet file
- **500 samples** — the actual data units consumed by the training job

### Example output

Assuming ~12 MiB average row groups and ~2.8 GiB/s with 32 goroutines:

```
--- Track: parquet-footer (read) ---

  Threads:          2 goroutines
  Throughput:       12.4 MiB/s
  Ops/sec:          398.0   (1,194 total, 0 errors)
  Avg object size:  32.0 KiB

--- Track: parquet-rowgroup-samples (read) ---

  Threads:          30 goroutines
  Throughput:       2.81 GiB/s
  Total data:       120.3 GiB
  Ops/sec:          22.4   (6,720 total, 0 errors)     ← Parquet FILE ops/sec
  Avg object size:  60.1 MiB  (≈ 5 × 12 MiB average)
  Throughput check: 2.81 GiB/s  ✓
  Samples/sec:      112.1  (33,600 total row-group reads)   ← KEY METRIC
  Avg sample size:  12.0 MiB  (bytes per row-group)

  Read latency (end-to-end, per row group):
    P50      214 ms
    P90      380 ms
    P99      710 ms
    Mean     238 ms
```

### Why Ops/sec alone would mislead here

| Config | Ops/sec | Samples/sec | Observation |
|---|---|---|---|
| 100 objects, **5** row groups read | 22 | 112 | baseline |
| 100 objects, **10** row groups read | 11 | 112 | same training throughput, half the ops/sec |
| 100 NPZ objects, **1** sample each | 112 | 112 | single-sample format: ops == samples |

Doubling `reads-per-object` from 5 → 10 halves `Ops/sec` (each op takes twice
as long) while `Samples/sec` remains constant — the training pipeline sees
identical throughput. Reporting `Ops/sec` only would make the 10-row-group
config look 50% slower than the 5-row-group config, which is wrong.

This equivalence also highlights the relationship to simpler formats:

- **NPZ, single-array HDF5, raw binary** — 1 object = 1 sample; `Ops/sec == Samples/sec`
- **Parquet, Arrow IPC, multi-sample HDF5** — 1 object = N samples; `Samples/sec = Ops/sec × reads-per-object`

`Samples/sec` is the unifying metric that works correctly across all formats.

---

## Configuration fields

All Parquet-simulation fields belong to `BenchmarkTrack` in
`cfg/benchmark_config.go`. They require `read-type: multirange`.

### `read-offset` (`int64`, default 0)

Byte offset at which to start each read.

- `>= 0`: absolute position from the start of the object.
- `< 0`: position relative to the end of the object. `start = objectSize + read_offset`.
  Example: `read-offset: -32768` always reads the last 32 KiB regardless of file
  size. This is how Parquet footer reads work.

> Negative values require `read-type: multirange`. The non-MRD `ByteRange` field
> uses `uint64` and cannot represent negative positions. The engine returns a
> counted error with a clear message if misused.

### `read-offset-random` (`bool`, default false)

When `true`, each range read picks a uniformly random offset within:

```
offset ∈ [0, objectSize − effectiveReadSize]
```

where `effectiveReadSize = max(read-size, read-size-max)`. The larger bound is
used so that every randomly-drawn offset leaves room for even the largest
possible range.

`reads-per-object: N` with `read-offset-random: true` issues N ranges at N
independent offsets — exactly how a dataloader selects N different row groups in
one file.

### `reads-per-object` (`int`, default 1)

Number of concurrent range reads issued per MRD operation. All share the same
cached bidi-gRPC stream.

```yaml
reads-per-object: 8   # 8 row groups per Parquet file per op
```

### `read-size-min` (`int64`, default 0) — **activates per-range path**

When `> 0`, **switches from `doReadMultiRange` to `doReadMultiRangePerRange`**.
Each range draws an independent size uniformly from `[read-size-min, read-size-max]`.

```yaml
read-size-min: 8388608    # 8 MiB minimum (small/compressed column chunk)
read-size-max: 33554432   # 32 MiB maximum (large float32 tensor column)
```

Set `read-size-min == read-size-max` for a fixed range size with per-range
latency tracking enabled. When `read-size-min <= 0`, `read-size` is used and
the aggregate path runs.

### `read-size-max` (`int64`, default 0)

Upper bound of the uniform random range size when `read-size-min > 0`. The safe
offset window automatically uses `read-size-max` so no range ever exceeds the
object boundary.

---

## The two execution paths

### `doReadMultiRange` — aggregate op latency (default)

Activated when `read-type: multirange` and `read-size-min <= 0`.

All N ranges run inside one op call. One data point is recorded in the
total-latency histogram, covering the window from the first `mrd.Add()` to the
last callback. No per-range breakdown; no Samples/sec metric.

**Use when:** you want raw file-level throughput or all row groups are the same
size and bytes/s is the only metric you need.

---

### `doReadMultiRangePerRange` — per-sample latency (recommended for AI/ML)

Activated when `read-type: multirange` **and** `read-size-min > 0`.

```
op start
│
├── getOrCreateMRD(objectName)    ← cached bidi-gRPC stream
│
├── for i in range(reads-per-object):
│     offset_i = computeReadOffset(rng)         ← random or static
│     length_i = computeReadSize(rng)           ← uniform [min, max] per range
│     start_i  = time.Now()                     ← independent per-range clock
│     tw_i     = TTFBWriter(start_i, 256 KiB)   ← per-range TTFB capture
│     mrd.Add(tw_i, offset_i, length_i, callback_i)
│
├── for each callback result:
│     record total_latency_i in histogram   ← 1 data point per row group
│     record ttfb_i in histogram            ← 1 data point per row group
│     totalBytes   += bytesRead_i
│     successfulRanges++
│
├── ts.totalRanges.Add(successfulRanges)   ← drives Samples/sec metric
└── ts.totalBytes.Add(totalBytes)
```

**Key properties:**

1. **N histogram data points per op.** With `reads-per-object: 8`, each call
   contributes 8 entries to the latency histogram — one per row group. P99
   latency reflects the tail at the row-group level, which is the GPU stall
   distribution that matters for training.

2. **Random size per range.** Each `mrd.Add()` draws an independent size from
   `[read-size-min, read-size-max]`. This models variable column-chunk sizes
   without requiring per-range size lists.

3. **Samples/sec metric.** `TotalSamples` counts every successfully delivered
   row group. `SamplesPerSec = TotalSamples / elapsed`. This is the headline
   AI/ML training throughput metric.

4. **Shared MRD connection.** All N ranges in one op still use the same cached
   bidi-gRPC stream — no extra connections per range.

5. **TTFB is tracked but secondary.** Per-range TTFB (time to first 256 KiB) is
   recorded in the histogram and printed in output. It is a useful low-level
   diagnostic for connection ramp-up behaviour. For training pipeline sizing,
   **total row-group latency** (P50/P99) and **Samples/sec** are the
   metrics that matter.

#### Activation decision tree

```
read-type: multirange?
└─ yes
   ├─ read-size-min > 0?
   │   ├─ yes → doReadMultiRangePerRange
   │   │         per-range latency · random sizes · Samples/sec
   │   └─ no  → doReadMultiRange
   │             aggregate op latency · bytes/s only
   └─ no → (non-MRD path; negative read-offset will error)
```

---

## Example configs

### Step 1 — Write the dataset

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd-prepare.yaml
```

Writes 10,000 objects under `parquet-dataset/` with lognormal-distributed sizes:

| Parameter | Value |
|---|---|
| Object count | 10,000 |
| Size distribution | lognormal |
| Mean object size | 128 MiB |
| Std-dev | 64 MiB |
| Floor | 32 MiB |
| Ceiling | 512 MiB |
| Concurrency | 16 writers |

### Step 2A — File-level throughput benchmark

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd.yaml
```

Two tracks weighted 1:8 (footer:rowgroup), 64 goroutines. Uses `doReadMultiRange`.
Reports bytes/s and file-level ops/s. No per-range breakdown.

| Track | Path | `read-offset` | `read-size` | `reads-per-object` |
|---|---|---|---|---|
| `parquet-footer` | `doReadMultiRange` | `-32768` | 32768 | 1 |
| `parquet-rowgroup` | `doReadMultiRange` | random | 16 MiB | 4 |

### Step 2B — Per-row-group sample benchmark ✅ recommended for AI/ML

```bash
gcs-bench bench --config examples/benchmark-configs/parquet-like-mrd-perrange.yaml
```

Two tracks (footer 3% + row-group 97%), 32 goroutines. Row-group track uses
`doReadMultiRangePerRange` (activated by `read-size-min > 0`).

| Track | Path | `read-size-min` | `read-size-max` | `reads-per-object` |
|---|---|---|---|---|
| `parquet-footer` | `doReadMultiRange` | — | — | 1 |
| `parquet-rowgroup-samples` | `doReadMultiRangePerRange` | 8 MiB | 32 MiB | 8 |

#### Reading the output

```
--- Track: parquet-rowgroup-samples (read) ---

  Threads:          31 goroutines
  Throughput:       2.843 GiB/s
  Total data:       851.0 GiB
  Ops/sec:          14.22  (4,266 total, 0 errors)      ← Parquet file ops
  Avg object size:  204.4 MiB
  Throughput check: 2.843 GiB/s  (ops/s × avg-size; must match Throughput above)
  Samples/sec:      113.76  (34,128 total row-group reads)    ← KEY ML METRIC
  Avg sample size:  18.1 MiB  (bytes per row-group read)

  Read latency (end-to-end, per row group):
    P50      182.35 ms
    P90      310.12 ms
    P99      520.44 ms
    P99.9    841.00 ms
    Max      1,204.00 ms
    Mean     201.44 ms
```

`Samples/sec` answers: *how many mini-batch samples can this storage system
deliver per second to a training job?*

`Ops/sec` answers: *how many Parquet files per second* — a less meaningful metric
when each file holds many row groups.

---

## How negative offsets work

The gRPC wire format uses signed `int64` for `ReadOffset`. Negative values are
resolved server-side in `grpc_reader_multi_range.go`:

```go
func convertToPositiveOffset(offset, objectSize int64) int64 {
    if offset >= 0 { return offset }
    return max(objectSize + offset, 0)
}
```

- `read-offset: -32768` on a 200 MiB object → reads bytes `[209,682,432, 200 MiB]`
- `read-offset: -32768` on a 40 MiB object → reads bytes `[41,910,272, 40 MiB]`

The benchmark engine passes the negative value directly to `mrd.Add()`. No
special calculation is needed on the engine side.

The `fake.fakeMultiRangeDownloader` used in unit tests applies identical logic,
so tests exercise real offset resolution without network calls.

---

## Unit tests

**File:** [`internal/benchmark/parquet_test.go`](../internal/benchmark/parquet_test.go)

```bash
# Parquet tests only
go test ./internal/benchmark/... -run "TestComputeReadOffset|TestNegativeOffset|TestParquet|TestComputeReadSize|TestPerRange" -v

# Full suite
go test ./internal/benchmark/... ./cfg/... -count=1 -timeout 120s
```

### Test inventory

| Test | Layer | What it verifies |
|---|---|---|
| `TestComputeReadOffsetStatic` | `computeReadOffset` | Zero, +1 MiB, −32 KiB, −4 MiB pass through unchanged |
| `TestComputeReadOffsetRandom` | `computeReadOffset` | 10,000 draws each in `[0, maxObjSize − readSize]` |
| `TestComputeReadOffsetRandomClampSmallObject` | `computeReadOffset` | `objSize == readSize` → always 0 |
| `TestComputeReadOffsetRandomUsesReadSizeMaxForWindow` | `computeReadOffset` | Window uses `ReadSizeMax`; no range exceeds object end |
| `TestNegativeOffsetRejectedForNewReader` | `doRead` | Negative offset on non-MRD path → counted error |
| `TestComputeReadSizeFallback` | `computeReadSize` | `ReadSizeMin <= 0` → returns `ReadSize` unchanged |
| `TestComputeReadSizeFixed` | `computeReadSize` | `ReadSizeMin == ReadSizeMax` → always that value |
| `TestComputeReadSizeRandom` | `computeReadSize` | 10,000 draws all in `[ReadSizeMin, ReadSizeMax]` |
| `TestParquetLikePrepare` | engine (prepare) | Lognormal write: N objects, 0 errors |
| `TestParquetLikeFooterRead` | engine (MRD read) | `read-offset: -32768`; avg bytes ≈ readSize |
| `TestParquetLikeRowGroupRead` | engine (MRD read) | `reads-per-object: 4` + random offsets; avg bytes ≈ 4 × readSize |
| `TestParquetLikeTwoTrackEngine` | engine (2-track) | Footer + row-group tracks, 0 errors |
| `TestPerRangeLatencyFixed` | `doReadMultiRangePerRange` | Fixed size (`min==max`): 0 errors, bytes ≈ N × rangeSize |
| `TestPerRangeLatencyRandomSize` | `doReadMultiRangePerRange` | Random sizes: 0 errors, avg bytes in `[N×min, N×max]` |

---

## Output metrics reference

| Metric | YAML field | When present | Description |
|---|---|---|---|
| Throughput | `throughput_bytes_per_sec` | always | Total bytes/s |
| Ops/sec | `ops_per_sec` | always | File-level operations per second |
| Total ops | `total_ops` | always | File operations completed |
| Avg object size | `avg_op_size_bytes` | always | Mean bytes per file op |
| **Samples/sec** | `samples_per_sec` | `read-size-min > 0` | Row-group reads per second — key AI/ML metric |
| **Total samples** | `total_samples` | `read-size-min > 0` | Total row groups delivered |
| **Avg sample size** | `avg_sample_size_bytes` | `read-size-min > 0` | Mean bytes per row group |
| Per-range latency | `TotalLatency` | `read-size-min > 0` | P50/P90/P99/P999 histogram at row-group granularity |
| TTFB | `TTFB` | read tracks | Time to first 256 KiB per range (diagnostic, not primary metric) |

---

## Summary of code changes

### `cfg/benchmark_config.go`

Five fields added to `BenchmarkTrack`:

```go
ReadOffset       int64  `yaml:"read-offset"`
ReadOffsetRandom bool   `yaml:"read-offset-random"`
ReadsPerObject   int    `yaml:"reads-per-object"`
ReadSizeMin      int64  `yaml:"read-size-min"`   // > 0 activates per-range path
ReadSizeMax      int64  `yaml:"read-size-max"`
```

### `internal/benchmark/types.go`

Three fields added to `TrackStats`:

```go
TotalSamples        int64   `yaml:"total_samples,omitempty"`
SamplesPerSec       float64 `yaml:"samples_per_sec,omitempty"`
AvgSampleSizeBytes  float64 `yaml:"avg_sample_size_bytes,omitempty"`
```

### `internal/benchmark/engine.go`

| Symbol | Change |
|---|---|
| `trackState.totalRanges` | New `atomic.Int64`; incremented per successful range in per-range path |
| `computeReadOffset(rng, track)` | Uses `max(ReadSize, ReadSizeMax)` for safe-window when random sizes active |
| `computeReadSize(rng, track)` | New helper; uniform draw from `[ReadSizeMin, ReadSizeMax]` |
| `doRead(ctx, ts, rng, objectName)` | Dispatches to `doReadMultiRangePerRange` when `ReadSizeMin > 0` |
| `doReadMultiRangePerRange(...)` | New function; per-range latency + random sizes + `totalRanges` counter |
| warmup reset | `ts.totalRanges.Store(0)` added to counter reset block |
| summary building | `SamplesPerSec`, `TotalSamples`, `AvgSampleSizeBytes` computed from `totalRanges` |

### `internal/benchmark/exporter.go`

- TSV: three new columns — `total_samples`, `samples_per_sec`, `avg_sample_size_bytes`
- Human-readable: `Samples/sec` and `Avg sample size` lines printed when `TotalSamples > 0`

### Files added or created

| File | Purpose |
|---|---|
| `examples/benchmark-configs/parquet-like-mrd-perrange.yaml` | Per-row-group sample benchmark (new) |
| `examples/benchmark-configs/parquet-like-mrd.yaml` | Aggregate file-level benchmark |
| `examples/benchmark-configs/parquet-like-mrd-prepare.yaml` | Dataset preparation |
| `internal/benchmark/parquet_test.go` | 14 unit tests for all Parquet-simulation functionality |
| `docs/parquet_dataloader.md` | This document |

