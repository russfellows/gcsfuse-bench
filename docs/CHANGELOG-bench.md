# gcs-bench Changelog

This file tracks changes to the **gcs-bench benchmark tool** added on top of the
upstream [GoogleCloudPlatform/gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse)
library. Upstream changes are not recorded here — see the upstream repository's
own history for those.

The version string embedded in the binary is `gcsfuse-v3-snap.<upstream-sha>+bench-<BENCH_VERSION>`.
Use `./gcs-bench --version` to confirm.

---

## v1.3.0 — Synthetic Parquet objects, real-footer traditional-parquet reader, DLRM and UNet3D benchmark configs, multi-prefix automation scripts

### New features

- **`write-format: parquet`** — write tracks now produce synthetic Apache Parquet
  objects with a genuine Thrift CompactProtocol `FileMetaData` footer. The
  on-disk layout is:

  ```
  [PAR1 4B][row-group 0][row-group 1]...[row-group N-1][padding][FileMetaData][4-byte LE metaLen][PAR1 4B]
  ```

  Row group *i* starts at byte offset `4 + i × row-group-size`. The `FileMetaData`
  encodes the exact `data_page_offset` and `total_compressed_size` for each row
  group, making the footer parseable by any conformant Parquet reader (including
  `pyarrow`, `pandas`, `spark`).  The data-page bytes are random (not valid
  encoded data pages) — only the footers are spec-compliant.

  New config fields on `op-type: write` tracks:
  - `row-group-count` — number of row groups (default: `reads-per-object`, then 1)
  - `row-group-size` — bytes per row group (default: `read-size`, then 65 536)

- **`read-type: traditional-parquet`** — simulates a non-MRD Parquet reader.
  Per operation it issues three sequential phases:
  1. `StatObject` → learn the exact object size.
  2. Byte-range GET of the last `read-footer-size` bytes → decode `FileMetaData`
     with a hand-rolled Thrift CompactProtocol parser (no external deps).
  3. `reads-per-object` concurrent `NewReaderWithReadHandle` range GETs, one per
     randomly-selected row group at its real `data_page_offset`.

  Objects without a valid `PAR1` footer magic are rejected immediately with a
  counted error (no silent fallback).  Prepare objects with `write-format: parquet`.

  New config field on `read-type: traditional-parquet` tracks:
  - `read-footer-size` — bytes to read for the footer GET (default: 32 768 = 32 KiB)

- **DLRM embedding-table benchmark configs** — three new example configs under
  `examples/benchmark-configs/`:
  - `dlrm-prepare.yaml` — write 5 000 lognormal-distributed Parquet objects
    (64 MiB – 1 GiB, mean ~256 MiB) representing DLRM v2 embedding tables
    (~1.25 TiB total), 16 row groups × 64 KiB each.
  - `dlrm-compare-traditional.yaml` — `traditional-parquet` reader: stat + 32 KiB
    footer GET + 16 parallel 64 KiB row-group GETs per op.
  - `dlrm-compare-mrd.yaml` — MRD reader on the same objects for direct comparison.

  Automation scripts under `examples/scripts/`:
  - `run-dlrm-prepare.sh` — parameterized per-host prepare (WID, EPOCH, CONCURRENCY).
  - `run-dlrm-compare.sh` — single-prefix MRD vs traditional comparison.
  - `run-dlrm-compare-all.sh` — all-prefix comparison (all 4 `host-<WID>/` prefixes in
    parallel) with separate concurrency knobs for each reader type:
    - `CONCURRENCY` (default 256) — MRD goroutines total across all workers.
    - `TRAD_CONCURRENCY` (default `CONCURRENCY/4 = 64`) — traditional goroutines total.
      Traditional ops each open ~18 independent bidi-gRPC streams, so far fewer goroutines
      are needed to stay below per-prefix GCS throttle thresholds.

- **UNet3D random-read benchmark configs** — new example configs and scripts for
  MLPerf Storage UNet3D-like workloads (full-object GETs, ~6.9 MiB lognormal objects):
  - `examples/benchmark-configs/unet3d-rapid-prepare.yaml` — write 100 352 objects per
    host into `unet3d/host-<WID>/` (width=28, depth=2, 128 files/dir, ~678 GiB/host).
  - `examples/benchmark-configs/unet3d-rapid.yaml` — RAPID full-object GET workload;
    `total-concurrency: 64`, 60 s warmup, `rapid-mode: on`.
  - `examples/scripts/run-unet3d-prepare.sh`, `run-unet3d-read.sh`,
    `run-unet3d-read-all.sh`, `unet3d-rapid-playbook.sh` — parameterized automation.

- **Additional automation scripts**:
  - `run-prepare.sh` — generic parameterized prepare wrapper.
  - `run-read.sh` / `run-read-all.sh` — generic single-host and all-host read wrappers.
  - `parse_results.py` — Python script to aggregate and display TSV result files from
    multi-host runs, printing per-host and combined throughput/latency summaries.

### Bug fixes

- **`exporter.go` throughput-check calculation** — the `Throughput check` line in
  human-readable bench output now uses `successful-ops/s × avg-size` instead of
  `ops/s × avg-size`.  The old formula overstated throughput when error rate was
  non-zero (because `OpsPerSec` counts all attempts while `AvgOpSizeBytes` is derived
  from successful bytes only).  For error-free runs the result is identical to before.

### No breaking changes

All existing benchmark configs and result files are unaffected. The new
`write-format`, `row-group-count`, `row-group-size`, and `read-footer-size` fields
default to the previous behaviour (raw random bytes, no Parquet footer) when absent.
The `traditional-parquet` read type is new and does not alter `new-reader` or
`multirange` tracks.

---

## v1.2.3 — Cleanup / delete subcommand, prepare retry tracking, prepare data reporting, RAPID write performance

### New features

- **`gcs-bench cleanup` / `gcs-bench delete` subcommand** — deletes all objects
  under a GCS prefix using a streaming producer/consumer pipeline. The LIST
  goroutine pages through GCS and feeds names into a bounded channel (5,000-name
  buffer); a pool of delete workers (default 64) consumes from the channel
  simultaneously. Deletes begin as soon as the first LIST page arrives.

  Key properties:
  - **Constant memory** — channel buffer is capped at 5,000 names regardless of
    total object count; a 100-billion-object prefix uses the same memory as one
    with 1,000 objects.
  - **Natural backpressure** — the lister blocks when the channel is full, so it
    can never get more than 5 pages ahead of the workers.
  - **`--dry-run` mode** — lists and counts objects without issuing any DELETEs.
  - **`gcs-bench delete` alias** — either name works identically:
    ```bash
    gcs-bench cleanup --bucket my-bucket --object-prefix resnet50/
    gcs-bench delete  --bucket my-bucket --object-prefix resnet50/
    ```
  - Progress printed every 5 seconds:
    ```
    [cleanup] elapsed=10s  listed=36000  deleted=33400  3640/s  queue=2600  errs=0
    [cleanup] COMPLETE — deleted 50176/50176 objects  elapsed=17s  avg=2951/s  errs=0
    ```
  - See [docs/bench-user-guide.md §14](bench-user-guide.md#14-cleanup--delete--removing-benchmark-objects)
    for full documentation.

- **Prepare-mode retry tracking** — transient write failures are retried up to 5
  times with exponential backoff (500 ms → 8 s). Retries are counted and shown
  on the progress line and in the final summary:
  ```
  [prepare] track="resnet50"  COMPLETE — objects created: 50176/50176  data written: 9.88 GiB  elapsed=3m12s  avg=261/s  retries=3  errs=0
  ```

- **Prepare total-bytes reporting** — the prepare completion line and result YAML
  now always include the total amount of data written (`data written: X GiB/MiB`),
  visible without any `-v` flag.

### No breaking changes

All existing benchmark configs and result files remain compatible. The new
`cleanup`/`delete` subcommand is additive. The `Retries` and `TotalBytes` fields
added to `TrackStats` are zero for benchmark (non-prepare) runs and zero-valued
in existing YAML files if absent.

### Performance improvements (RAPID/Zonal storage)

Root-cause analysis of write latency for RAPID/Zonal buckets revealed two
independent bottlenecks.  Both are fixed in this release.

- **Zero-copy writes for small objects** (`internal/storage/bucket_handle.go`) —
  When writing to a zonal bucket, the gRPC writer previously allocated a **16 MiB
  buffer per object** regardless of actual object size (the library's default
  `ChunkSize`). For the typical lognormal workload (64 KB–1 MB, median ~128 KB)
  this meant 16 MiB of heap per goroutine with no data in it — at 64 goroutines
  that is 1 GiB of live heap from write buffers alone, causing heavy GC pressure
  and latency spikes.

  Fix: `wc.ChunkSize = 0` is set for all zonal writes.  This enables
  `forceOneShot` mode in the gRPC writer: the 16 MiB buffer is never allocated,
  and the object data flows directly into a single `BidiWriteObject` gRPC message
  without an intermediate copy.

- **gRPC connection pool for concurrent writes** (`cmd/benchmark.go`,
  `cmd/cleanup.go`) — `GrpcConnPoolSize` was never set, so all goroutines shared
  a single gRPC connection (one HTTP/2 channel).  Under high write concurrency
  every `BidiWriteObject` stream competed for the same connection's congestion
  window.

  Fix: `GrpcConnPoolSize = 4` is set when `rapid-mode` is `on` or `auto`.  Writes
  are spread across four independent TCP connections, quadrupling the effective
  bandwidth-delay product available for concurrent uploads.

---

## v1.2.2 — Live performance stats, verbosity cleanup, accurate OS memory annotations

### Improvements

- **Live real-time performance stats** (`internal/benchmark/engine.go`) — Every
  10-second progress line now shows the full picture with no flags required:
  ```
  [bench] track="resnet50-read"  elapsed=30s  remaining=4m30s  ops=34812  1164/s  28.3 MiB/s  p50=2.1ms  p99=18.4ms  errs=0
  ```
  Previously the line showed only `interval-ops` and `GiB/s`; latency and
  per-second rates were hidden behind `-vv`.

- **Verbosity levels restructured** — Output is now tiered by usefulness:
  - *(no flags)* — progress lines always: `ops`, `ops/s`, `MiB/s`, `p50`/`p99` ms, `errs`
  - `-v` (INFO) — adds RAPID detection, DirectPath verification, phase-transition
    messages, write-pool pipeline health stats per tick
  - `-vv` (DEBUG) — adds Go heap/GC cycles, per-interval CPU percentages, process
    RSS, OS page-cache and anon-page deltas (`[os-mem]` lines)
  - `-vvv` (TRACE) — every individual GCS call (unchanged)

- **Throughput units changed from GiB/s to MiB/s** in progress lines — better
  scaled for typical GCS object sizes (KiB–MiB range).

### Bug fixes / Clarifications

- **OS memory section relabeled** (`internal/benchmark/exporter.go`) — The
  "System memory" block in results output previously carried annotations that
  implied Linux page-cache hits could be serving GCS object data, which is
  incorrect.  GCS reads travel network → socket buffer → Go heap (anon pages)
  and never enter the file-backed page cache.  The section is now headed:
  > *OS memory (Linux page cache — local disk/file activity only; GCS data does not enter page cache)*
  with accurate per-field annotations: anon-page growth = Go heap expansion;
  `pgpgout` = normal OS memory reclaim; `pgpgin` = local disk reads by kernel.

- **Updated `docs/bench-user-guide.md` section 12** — Verbosity table and
  sample output blocks updated to match new level breakdown; added "Sample
  default output (no flags)" block alongside existing `-v`/`-vvv` samples.

---

## v1.2.1 — `--bucket` CLI flag, examples directory, thread-curve sweep script

### New features

- **`--bucket <name>` flag** (`cmd/benchmark.go`) — The bucket name can now be
  overridden on the command line without editing the YAML config file.  This
  makes it trivial to run the same config against different buckets
  (e.g. RAPID vs standard) in back-to-back comparisons:
  ```bash
  ./gcs-bench bench --config myconfig.yaml --bucket my-rapid-bucket  --rapid-mode on
  ./gcs-bench bench --config myconfig.yaml --bucket my-normal-bucket --rapid-mode off
  ```
  The YAML `bucket:` field is still used when `--bucket` is not passed.

- **`examples/README.md`** — New top-level README for the examples directory.
  Covers all benchmark configs and scripts with runnable commands for each
  workload, including RAPID vs standard comparison examples and multi-host
  distributed setup.

- **`examples/benchmark-configs/resnet50.yaml`** — ResNet50-like image
  classification benchmark: 614,400 objects, lognormal sizes (mean ≈ 224 KiB),
  full-object reads, 64 goroutines.

- **`examples/benchmark-configs/resnet50-prepare.yaml`** — Matching prepare
  config to populate the ResNet50 object corpus (~134 GiB per host).

- **`examples/benchmark-configs/rapid-mrd-8k-example.yaml`** — Reproduces the
  Danny Jones RAPID 8 KiB MRD reference benchmark (96 goroutines, `read-type:
  multirange`, `read-size: 8192`).  Includes documented reference numbers
  (P50/P90/P99 latency, ops/sec).

- **`examples/scripts/thread-curve.sh`** — Concurrency sweep helper.  Runs
  `gcs-bench bench` at a configurable list of thread counts (`--sweep`),
  captures per-level TSV results, and prints a consolidated latency/throughput
  table.  Supports `--bucket` and `--rapid-mode` overrides so a single YAML
  config can be swept across multiple bucket types in one command.

### Bug fixes

- **Makefile `UPSTREAM_SHA`** — The UPSTREAM_SHA calculation previously used
  `origin/master` as the reference point.  Once all bench commits were merged
  back to `origin/master`, `git merge-base HEAD origin/master` returned HEAD
  itself (wrong).  Now uses `upstream/master` (the `GoogleCloudPlatform/gcsfuse`
  remote) directly, which always identifies the actual upstream snapshot
  regardless of local branch state.

- **README.md upstream snapshot** updated from `582a2201` (2026-03-27) to
  `4b7892bc` (2026-04-01) to reflect the upstream base after PR #8 merged the
  latest GoogleCloudPlatform/gcsfuse commits.

### Version / tag notes

The git tag for this release is `bench-v1.2.1`.  Note that `v1.2.0` and
`v1.2.1` already exist as upstream gcsfuse tags; the `bench-v*` namespace is
used for all gcs-bench tool releases to avoid collisions.

---

## v1.2 — MultiRangeDownloader (MRD) read path

Integrates GCS's bidi-gRPC `MultiRangeDownloader` API as a second read strategy,
selectable per track via the new `read-type` config field.

### New features

- **`read-type: multirange`** — New track-level configuration field (default:
  `new-reader`). When set to `multirange`, reads use the GCS
  `NewMultiRangeDownloader` bidi-gRPC API instead of the standard
  `NewReader` path. MRD is only available on RAPID/zonal buckets with
  `rapid-mode: auto` or `rapid-mode: on`.

- **LRU connection cache** — MRD connections are cached per object key in a
  2048-entry LRU (`internal/cache/lru`). Repeated reads against the same objects
  reuse the open bidi-gRPC stream rather than creating a new connection each time.

- **Singleflight deduplication** — Concurrent goroutines racing to obtain an
  MRD connection for the same object key are collapsed to a single
  `NewMultiRangeDownloader` call via `golang.org/x/sync/singleflight`. All
  waiters share the result. This eliminates connection storms on cache misses.

- **Push-based drain via `io.Discard`** — The MRD API pushes data to the caller's
  `io.Writer`. The engine uses `io.Discard` as the drain writer (no allocation,
  no memory copy). Data is "received" for correctness but discarded immediately,
  consistent with the `new-reader` path.

- **Instrumented `MultiRangeDownloader`** — `instrumented_bucket.go` now wraps
  the MRD with the same per-op metrics as the standard reader path: `totalOps`,
  `totalBytes`, HDR histograms (TTFB + total latency), error counting, and
  TRACE-level logging.

- **Shared `TTFBWriter`** — A single `benchmark.TTFBWriter` type (new file
  `internal/benchmark/ttfb_writer.go`) is used by both the `new-reader` and
  `multirange` paths. Fires a TTFB callback once ≥ 256 KiB is received (or on
  `Finalize()` for sub-threshold objects).

- **New example configs** — Two ready-to-use MRD configs added to
  `examples/benchmark-configs/`:
  - `unet3d-like-mrd.yaml` — full-object MRD reads, 32 goroutines
  - `unet3d-like-mrd-ranged.yaml` — 8 KiB range MRD reads, 96 goroutines

### Source changes

| File | Change |
|------|--------|
| `cfg/benchmark_config.go` | Added `ReadType string \`yaml:"read-type"\`` to `BenchmarkTrack` |
| `internal/benchmark/ttfb_writer.go` | **New file** — shared `TTFBWriter` struct |
| `internal/benchmark/engine.go` | Added `mrdCache`, `mrdGroup`, `getOrCreateMRD()`, `doReadMultiRange()`, dispatch in `doRead()` |
| `internal/storage/instrumented_bucket.go` | `NewMultiRangeDownloader` now returns an instrumented wrapper; new `instrumentedMultiRangeDownloader` struct |
| `examples/benchmark-configs/unet3d-like-mrd.yaml` | **New file** — full-object MRD example |
| `examples/benchmark-configs/unet3d-like-mrd-ranged.yaml` | **New file** — range MRD example |

---

## v1.1 — `/proc` memory monitoring

Adds per-tick RSS and page-cache tracking to the live progress output,
making it easy to observe memory growth and kernel page-cache activity
during a benchmark run.

### New features

- **RSS and page-cache metrics** — Each 10-second progress tick now includes
  a `[memory]` line alongside the throughput tick:

  ```
  [bench] track="unet3d-read"  interval-ops=15690  interval-throughput=10.62 GiB/s  total-ops=15690
  [memory] rss=1423 MiB  page-cache=8192 MiB  pgpgin-delta=131072 pages/s
  ```

- **Start/end RSS in result files** — `bench.yaml` and `bench.txt` include
  `start_rss_kib` and `end_rss_kib` fields for the measurement phase.

- **`/proc`-based implementation** — Reads `/proc/self/status` (RSS),
  `/proc/meminfo` (Cached + Buffers), and `/proc/vmstat` (`pgpgin`) directly.
  No external dependencies.

### Source changes

| File | Change |
|------|--------|
| `internal/benchmark/procstats.go` | **New file** — `/proc` reader functions |
| `internal/benchmark/types.go` | Added `StartRSSKiB`, `EndRSSKiB` to result structs |
| `internal/benchmark/engine.go` | Capture RSS at phase start/end; emit `[memory]` ticks |
| `internal/benchmark/exporter.go` | Write RSS fields to YAML and `.txt` output |

---

## v1.0 — Initial gcs-bench tool

The initial standalone GCS I/O benchmark tool, built as an overlay on the
upstream `gcsfuse` v3 storage client library.

### Features at initial release

- Direct GCS reads, writes, stats, and list operations — no FUSE mount required
- HDR histogram latency recording (TTFB + end-to-end / TTLB) — never averaged
- RAPID/zonal bucket support via bidi-gRPC (`--rapid-mode auto|on|off`)
- Warmup phase with continuous goroutines; stats reset at the measurement boundary
- Distributed multi-host mode (`--worker-id` / `--num-workers` / `--start-at`)
- `merge-results` subcommand — statistically correct HDR merge across workers
- `plot-hgrm` subcommand — built-in SVG frequency-distribution renderer
- Write pool (`ChanPool`) — pre-fills random data before measurement; zero consumer stall
- lognormal size distribution for writes (`size-spec: type: lognormal`)
- Directory-tree object naming (`directory-structure` config block)
- Self-contained result directories: `bench.txt`, `bench.yaml`, `bench.tsv`,
  per-track `.hgrm` files, `config.yaml` copy, `console.log` capture
