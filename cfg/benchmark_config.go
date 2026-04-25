// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains hand-authored benchmark config types for the gcsfuse-bench
// fork. It is intentionally NOT generated from params.yaml.

package cfg

import "time"

// BenchmarkConfig holds all configuration for the standalone benchmark engine.
// It is intended to be embedded in cfg.Config under the "benchmark" YAML key.
type BenchmarkConfig struct {
	// Enabled turns the benchmark instrumentation layer on. When false, the
	// instrumented bucket wrapper is a no-op pass-through.
	Enabled bool `yaml:"enabled"`

	// Bucket is the GCS bucket name to benchmark. Can also be supplied as a
	// positional argument on the command line; the CLI argument takes precedence.
	Bucket string `yaml:"bucket"`

	// Mode controls the engine's execution strategy.
	//   "benchmark" (default): time-bounded I/O loop collecting latency histograms.
	//   "prepare": iterates through all object paths exactly once, writing each
	//              object. No warmup, no time limit. Use this to populate a bucket
	//              before running a benchmark. Only tracks with op-type: write are
	//              executed; all others are skipped.
	Mode string `yaml:"mode"`

	// Duration controls how long the benchmark measurement phase runs.
	Duration time.Duration `yaml:"duration"`

	// WarmupDuration controls how long the engine warms up before capturing
	// statistics. I/O during warm-up is discarded from histograms.
	WarmupDuration time.Duration `yaml:"warmup-duration"`

	// TotalConcurrency is the total number of goroutines issuing I/O. Each
	// BenchmarkTrack receives (Weight / total_weight) * TotalConcurrency goroutines.
	TotalConcurrency int `yaml:"total-concurrency"`

	// ObjectPrefix is prepended to all object names when issuing GCS I/O.
	// Example: "bench/train/". Leave empty to operate from the bucket root.
	ObjectPrefix string `yaml:"object-prefix"`

	// OutputPath is the directory where result files (YAML summary, TSV) are
	// written. Defaults to the current working directory when empty.
	OutputPath string `yaml:"output-path"`

	// OutputFormat selects result serialisation. Supported values: "yaml", "tsv", "both".
	OutputFormat string `yaml:"output-format"`

	// Tracks defines one or more I/O workload tracks that run concurrently.
	// Each track gets its own histogram set and summary statistics.
	Tracks []BenchmarkTrack `yaml:"tracks"`

	// Histograms controls HDR-histogram precision and range.
	Histograms HistogramConfig `yaml:"histograms"`

	// WorkerID identifies this instance within a distributed multi-host run
	// (0-based, default 0). Used to:
	//   • Stripe 'prepare' mode writes so each host handles a unique shard.
	//   • Tag the output YAML so 'gcs-bench merge-results' can label per-worker rows.
	// Has no effect when NumWorkers <= 1.
	WorkerID int `yaml:"worker-id"`

	// NumWorkers is the total number of hosts participating in a distributed run
	// (default 1 = single-host). When > 1, prepare mode assigns object i to the
	// worker where (i % NumWorkers) == WorkerID, so each host writes a disjoint
	// shard of the full object space.
	NumWorkers int `yaml:"num-workers"`

	// StartAt is an optional Unix epoch timestamp (seconds). When non-zero the
	// benchmark sleeps until this time before beginning warm-up, enabling a
	// synchronized measurement window across all distributed workers.
	// Zero means start immediately (default).
	StartAt int64 `yaml:"start-at"`

	// RapidMode controls GCS RAPID/zonal storage detection and client selection.
	// RAPID buckets require the bidi-streaming gRPC protocol for both reads and
	// writes; using HTTP/2 against a RAPID bucket results in 100% I/O errors.
	//
	//   "auto" (default): call GetStorageLayout at startup to detect whether the
	//          bucket is a RAPID (zonal) bucket and switch to bidi-gRPC automatically.
	//          Requires storage.buckets.get permission (included in Storage Object Admin).
	//   "on":  force bidi-gRPC RAPID mode unconditionally; skips GetStorageLayout.
	//          Use when auto-detection is unavailable or the bucket is known to be RAPID.
	//   "off": disable detection; use HTTP/2 regardless of bucket type.
	//          Use for non-RAPID buckets when you want to avoid the one-time detection call.
	RapidMode string `yaml:"rapid-mode"`
}

// BenchmarkTrack describes a single I/O workload component.
type BenchmarkTrack struct {
	// Name is a human-readable label used in output (e.g. "random-read-4MB").
	Name string `yaml:"name"`

	// Weight is a relative integer weight used to assign goroutines when
	// TotalConcurrency is distributed across tracks.
	Weight int `yaml:"weight"`

	// OpType selects the I/O operation.
	// Supported in benchmark mode: "read", "write", "stat", "list".
	// Supported in prepare mode:   "write" only (other types are skipped).
	OpType string `yaml:"op-type"`

	// ObjectSizeMin is the minimum object size in bytes for this track.
	ObjectSizeMin int64 `yaml:"object-size-min"`

	// ObjectSizeMax is the maximum object size in bytes for this track.
	// When equal to ObjectSizeMin, all objects have a fixed size.
	ObjectSizeMax int64 `yaml:"object-size-max"`

	// AccessPattern selects how objects are chosen within a track.
	// Supported: "sequential", "random".
	AccessPattern string `yaml:"access-pattern"`

	// ReadType selects the GCS read implementation for read tracks.
	// "new-reader" (default): calls NewReaderWithReadHandle for every read.
	// "multirange": uses MultiRangeDownloader with a singleflight-deduped LRU
	// cache of bidi-gRPC connections, amortizing connection-setup cost across
	// repeated reads of the same object.
	// "traditional-parquet": simulates how a traditional object-storage client
	// reads a Parquet file without negative-offset or MRD support:
	//   1. HEAD/stat the object to discover its size.
	//   2. Byte-range GET of the last read-footer-size bytes (footer).
	//   3. reads-per-object parallel byte-range GETs for random row groups.
	// Total latency = stat + footer-GET + max(row-group GETs).
	// Credits reads-per-object samples per op, enabling direct Samples/sec
	// comparison against read-type: multirange with read-size-min > 0.
	ReadType string `yaml:"read-type"`

	// ReadSize is the number of bytes requested per individual read call.
	// Relevant when the object-size >> read granularity (e.g. streaming reads).
	// Zero means read the entire object in one call.
	ReadSize int64 `yaml:"read-size"`

	// Concurrency overrides goroutine count for this specific track.
	// When zero, goroutines are derived from Weight and TotalConcurrency.
	Concurrency int `yaml:"concurrency"`

	// ObjectCount is the number of distinct objects available in this track.
	// The benchmark pre-creates (or lists) this many objects before running.
	// Ignored when DirectoryStructure is set (count is derived from the tree).
	ObjectCount int `yaml:"object-count"`

	// DirectoryStructure defines a nested directory tree of objects for this
	// track. When set, object paths are generated from the tree (Width^Depth
	// leaf directories × FilesPerDir objects each) and ObjectCount is ignored.
	DirectoryStructure *DirectoryStructureConfig `yaml:"directory-structure"`

	// SizeSpec controls per-object size distribution for write operations.
	// When set, it overrides ObjectSizeMin / ObjectSizeMax.
	SizeSpec *SizeSpecConfig `yaml:"size-spec"`

	// ReadOffset is the byte offset at which to start each read.
	//
	//   >= 0: read starting at this byte position (0 = beginning of object).
	//   < 0:  read starting this many bytes before the end of the object.
	//         Example: -32768 reads the last 32 KiB (Parquet footer pattern).
	//         Negative offsets require read-type: multirange; the MRD bidi-gRPC
	//         stream resolves them to a positive position using the object size.
	//         Using a negative offset with read-type: new-reader returns an error.
	//
	// Ignored when read-offset-random is true.
	ReadOffset int64 `yaml:"read-offset"`

	// ReadOffsetRandom, when true, picks a uniformly random byte offset for each
	// read within the safe window [0, object_size - read_size].
	//
	// The assumed object size range is taken from object-size-min / object-size-max
	// (or size-spec min/max when set).  When the window is zero or negative
	// (object smaller than read-size), offset 0 is used.
	//
	// When true, read-offset is ignored.
	// Supported on both new-reader and multirange read types.
	ReadOffsetRandom bool `yaml:"read-offset-random"`

	// ReadsPerObject is the number of independent range reads issued per operation
	// when read-type is multirange.  All reads share the cached bidi-gRPC
	// connection and are dispatched concurrently; the operation is complete when
	// the last read callback fires.
	//
	// Use this to simulate reading multiple row groups (or column chunks) from a
	// single Parquet file per training step.  When read-offset-random is true,
	// each of the N reads picks a different random offset, approximating random
	// batch access across column chunks in a file.
	//
	// Default: 1.  Ignored for read-type: new-reader.
	ReadsPerObject int `yaml:"reads-per-object"`

	// ReadSizeMin and ReadSizeMax define a per-range random read size when
	// read-type is multirange.  When ReadSizeMin > 0, the engine switches to the
	// per-range latency tracking path (doReadMultiRangePerRange):
	//
	//   - Each range draws an independent size uniformly from
	//     [ReadSizeMin, ReadSizeMax].
	//   - Each range records its own TTFB and total-latency histogram entry.
	//     The histograms therefore show the per-range distribution; with
	//     ReadsPerObject: N there are N data points per operation call.
	//
	// When ReadSizeMin == ReadSizeMax > 0: fixed per-range size with per-range
	// latency tracking (no size randomness, but each range gets its own entry).
	//
	// When ReadSizeMin <= 0: falls back to ReadSize on the aggregate path
	// (existing doReadMultiRange behaviour — one histogram entry per operation).
	//
	// Note: when read-offset-random is true, the safe-offset window uses
	// ReadSizeMax (or ReadSize when ReadSizeMax is 0) so that every drawn offset
	// leaves room for the largest possible range.
	ReadSizeMin int64 `yaml:"read-size-min"`
	ReadSizeMax int64 `yaml:"read-size-max"`

	// SamplesPerObject is the number of training samples credited per completed
	// operation on any non-per-range read path.  When > 0, each op adds this
	// value to the totalRanges counter so that SamplesPerSec is computed and
	// printed in the summary — even when read-type is not multirange.
	//
	// Use this when a single read delivers multiple samples:
	//   - Full-object GET of a Parquet file with N row groups → set to N
	//   - read-type: traditional-parquet → leave at 0 (uses reads-per-object)
	//
	// When read-size-min > 0 (doReadMultiRangePerRange path), this field is
	// ignored; the per-range counter increments by 1 per delivered range.
	SamplesPerObject int `yaml:"samples-per-object"`

	// ReadFooterSize is the number of bytes read as the "footer" step in the
	// traditional-parquet read path.  The footer is read as a byte-range GET
	// ending at the last byte of the object (i.e. [objectSize-ReadFooterSize,
	// objectSize]).  Defaults to 32768 (32 KiB) if zero when read-type is
	// traditional-parquet.
	ReadFooterSize int64 `yaml:"read-footer-size"`
}

// DirectoryStructureConfig describes a nested object tree for a track.
// Total objects = Width^Depth × FilesPerDir.
type DirectoryStructureConfig struct {
	// Width is the number of subdirectories at each level of the tree.
	Width int `yaml:"width"`

	// Depth is the number of directory levels (edges from root to leaf).
	// Total leaf directories = Width^Depth.
	Depth int `yaml:"depth"`

	// FilesPerDir is the number of objects placed in each leaf directory.
	FilesPerDir int `yaml:"files-per-dir"`

	// DirPattern is a printf format string for directory segment names.
	// Receives the zero-based directory index. Default: "dir-%04d".
	DirPattern string `yaml:"dir-pattern"`

	// FilePattern is a printf format string for object names within a leaf dir.
	// Receives the zero-based file index. Default: "obj-%06d".
	FilePattern string `yaml:"file-pattern"`
}

// SizeSpecConfig describes how object sizes are distributed in a track.
// Used by write operations; read operations observe the sizes already in GCS.
type SizeSpecConfig struct {
	// Type selects the distribution. Supported: "fixed", "uniform", "lognormal".
	// Default: "uniform".
	Type string `yaml:"type"`

	// Min is the minimum object size in bytes.
	// For uniform: lower bound of the range.
	// For lognormal: clamps samples below this value.
	Min int64 `yaml:"min"`

	// Max is the maximum object size in bytes.
	// For uniform: upper bound of the range.
	// For lognormal: clamps samples above this value.
	Max int64 `yaml:"max"`

	// Mean is the real-space mean object size in bytes (lognormal only).
	// The underlying normal parameters (μ, σ) are computed from Mean + StdDev.
	Mean float64 `yaml:"mean"`

	// StdDev is the real-space standard deviation in bytes (lognormal only).
	StdDev float64 `yaml:"std-dev"`
}

// HistogramConfig controls HDR-histogram precision.
type HistogramConfig struct {
	// MinValueMicros is the minimum latency value tracked (default: 1 µs).
	MinValueMicros int64 `yaml:"min-value-micros"`

	// MaxValueMicros is the maximum latency tracked (default: 60_000_000 = 60s).
	MaxValueMicros int64 `yaml:"max-value-micros"`

	// SignificantDigits controls HDR precision (default: 3 → 0.1% error).
	SignificantDigits int `yaml:"significant-digits"`
}

// DefaultHistogramConfig returns sensible defaults for histogram configuration.
func DefaultHistogramConfig() HistogramConfig {
	return HistogramConfig{
		MinValueMicros:    1,
		MaxValueMicros:    60_000_000,
		SignificantDigits: 3,
	}
}

// DefaultBenchmarkConfig returns a zero-value BenchmarkConfig with sensible
// defaults filled in. Callers should override fields from YAML/flags as needed.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		Duration:         30 * time.Second,
		WarmupDuration:   5 * time.Second,
		TotalConcurrency: 8,
		OutputFormat:     "yaml",
		Histograms:       DefaultHistogramConfig(),
	}
}
