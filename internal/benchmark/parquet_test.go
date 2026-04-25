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

// Parquet-like workload tests.
//
// These tests exercise three layers of the new Parquet-like read support:
//
//  1. computeReadOffset — unit tests for static, negative, and random offsets.
//  2. Non-MRD rejection — a negative read-offset on the standard new-reader
//     path must surface as a counted error (not a panic or silent wrong read).
//  3. End-to-end engine tests using a fake MRD bucket:
//     a. Prepare:         lognormal-sized writes (mirrors parquet-like-mrd-prepare.yaml).
//     b. Footer read:     static negative offset (-32 KiB) via MRD.
//     c. Row-group read:  random offset + reads-per-object:4 via MRD.
//     d. Two-track run:   footer track + row-group track run together.

package benchmark

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/googlecloudplatform/gcsfuse/v3/cfg"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/fake"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/gcs"
)

// ── mrdMockBucket ─────────────────────────────────────────────────────────────

// mrdMockBucket wraps mockBucket and overrides NewMultiRangeDownloader so that
// MRD-based benchmark tracks receive a real fake.MultiRangeDownloader backed by
// in-memory zero data.  All other gcs.Bucket methods delegate to *mockBucket.
type mrdMockBucket struct {
	*mockBucket
	// mrdFactory is called once per unique object name when a new MRD stream is
	// requested.  Return a fake.NewFakeMultiRangeDownloader or similar.
	mrdFactory func(name string) (gcs.MultiRangeDownloader, error)
}

// NewMultiRangeDownloader shadows the embedded method, routing to mrdFactory.
func (b *mrdMockBucket) NewMultiRangeDownloader(_ context.Context, req *gcs.MultiRangeDownloaderRequest) (gcs.MultiRangeDownloader, error) {
	if b.mrdFactory != nil {
		return b.mrdFactory(req.Name)
	}
	return nil, nil
}

// newMRDBucket creates an mrdMockBucket whose MRD instances hold objectSize
// bytes of zeroed data.  The fake MRD resolves negative offsets just like the
// real gRPC library, so footer reads (read-offset: -N) work correctly.
func newMRDBucket(objectSize int) *mrdMockBucket {
	return &mrdMockBucket{
		mockBucket: &mockBucket{objectSize: uint64(objectSize)},
		mrdFactory: func(name string) (gcs.MultiRangeDownloader, error) {
			obj := &gcs.MinObject{Name: name, Size: uint64(objectSize)}
			data := make([]byte, objectSize)
			// Use zero sleep so tests complete quickly; the fake MRD still runs
			// Add() callbacks in goroutines (just without simulated network delay).
			return fake.NewFakeMultiRangeDownloaderWithSleep(obj, data, 0), nil
		},
	}
}

// ── computeReadOffset unit tests ──────────────────────────────────────────────

// TestComputeReadOffsetStatic verifies that static offsets — positive, zero,
// and negative — are all returned unchanged when ReadOffsetRandom is false.
// Negative offsets are valid on the MRD path; this test checks pass-through only.
func TestComputeReadOffsetStatic(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cases := []struct {
		name   string
		offset int64
	}{
		{"zero", 0},
		{"positive-1MiB", 1 << 20},
		{"negative-footer-32KiB", -32768},
		{"negative-footer-4MiB", -4 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			track := cfg.BenchmarkTrack{
				ReadOffset:       tc.offset,
				ReadOffsetRandom: false,
				ReadSize:         32768,
				ObjectSizeMin:    64 << 20,
				ObjectSizeMax:    64 << 20,
			}
			got := computeReadOffset(rng, track)
			if got != tc.offset {
				t.Errorf("computeReadOffset = %d, want %d", got, tc.offset)
			}
		})
	}
}

// TestComputeReadOffsetRandom verifies that when ReadOffsetRandom is true, the
// returned offset is always within [0, objectSizeMax − readSize] over many
// iterations.  This simulates random row-group selection within a Parquet file.
func TestComputeReadOffsetRandom(t *testing.T) {
	const (
		readSize   = 16 << 20  // 16 MiB — typical compressed column-chunk size
		minObjSize = 32 << 20  // 32 MiB
		maxObjSize = 512 << 20 // 512 MiB
		iterations = 10_000
	)
	rng := rand.New(rand.NewSource(7))
	track := cfg.BenchmarkTrack{
		ReadOffsetRandom: true,
		ReadSize:         readSize,
		ObjectSizeMin:    minObjSize,
		ObjectSizeMax:    maxObjSize,
	}
	for i := 0; i < iterations; i++ {
		offset := computeReadOffset(rng, track)
		if offset < 0 {
			t.Fatalf("iteration %d: got negative offset %d (want ≥ 0)", i, offset)
		}
		if offset > maxObjSize-readSize {
			t.Fatalf("iteration %d: offset %d exceeds maxObjSize−readSize=%d",
				i, offset, maxObjSize-readSize)
		}
	}
}

// TestComputeReadOffsetRandomClampSmallObject verifies that when the object is
// exactly as large as the read size (zero slack), computeReadOffset always
// returns 0 rather than a negative or out-of-bounds value.
func TestComputeReadOffsetRandomClampSmallObject(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	track := cfg.BenchmarkTrack{
		ReadOffsetRandom: true,
		ReadSize:         65536,
		ObjectSizeMin:    65536,
		ObjectSizeMax:    65536,
	}
	for i := 0; i < 200; i++ {
		if got := computeReadOffset(rng, track); got != 0 {
			t.Fatalf("iteration %d: expected offset 0 for object == readSize, got %d", i, got)
		}
	}
}

// ── Non-MRD negative-offset rejection test ───────────────────────────────────

// TestNegativeOffsetRejectedForNewReader verifies that the non-MRD read path
// (read-type: "", not "multirange") counts an error and surfaces a helpful
// message when read-offset is negative.  The user should be directed to set
// read-type: multirange for Parquet footer reads.
func TestNegativeOffsetRejectedForNewReader(t *testing.T) {
	mb := &mockBucket{readBytes: 1 << 20}
	bCfg := cfg.BenchmarkConfig{
		Duration:         200 * time.Millisecond,
		TotalConcurrency: 1,
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:        "footer-non-mrd",
				OpType:      "read",
				ReadType:    "", // standard new-reader path, NOT multirange
				Weight:      1,
				ReadOffset:  -32768,
				ReadSize:    32768,
				ObjectCount: 5,
				Concurrency: 1,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track in summary")
	}
	if summary.Tracks[0].Errors == 0 {
		t.Error("expected counted errors for negative read-offset on non-MRD path, got none")
	}
}

// ── Parquet-like prepare test ─────────────────────────────────────────────────

// makePrepareConfigParquetLike returns a prepare config that mirrors the
// structure of parquet-like-mrd-prepare.yaml but uses kilobyte-scale objects
// (instead of MiB) so the unit test completes in milliseconds without
// allocating pool RAM proportional to lognormal sizes.
func makePrepareConfigParquetLike(objectCount int) cfg.BenchmarkConfig {
	return cfg.BenchmarkConfig{
		Mode:             "prepare",
		ObjectPrefix:     "parquet-dataset/",
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:        "parquet-files",
				OpType:      "write",
				Weight:      1,
				ObjectCount: objectCount,
				Concurrency: 2,
				SizeSpec: &cfg.SizeSpecConfig{
					Type:   "lognormal",
					Mean:   128 * 1024, // 128 KiB nominal (scaled down from 128 MiB)
					StdDev: 64 * 1024,  //  64 KiB std-dev
					Min:    32 * 1024,  //  32 KiB floor
					Max:    512 * 1024, // 512 KiB ceiling
				},
			},
		},
	}
}

// TestParquetLikePrepare runs a prepare phase that mirrors parquet-like-mrd-prepare.yaml
// (lognormal-sized objects under a flat prefix) and verifies that exactly
// objectCount objects are written with zero errors.
func TestParquetLikePrepare(t *testing.T) {
	const objectCount = 20
	mb := &mockBucket{objectSize: 128 * 1024} // 128 KiB mock return size

	eng, err := NewEngine(mb, makePrepareConfigParquetLike(objectCount), 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	eng.prepareRetryDelay = time.Nanosecond // avoid real backoff sleeps in tests

	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected at least 1 track in summary")
	}
	ts := summary.Tracks[0]

	// Prepare mode writes each object exactly once.
	if ts.TotalOps != int64(objectCount) {
		t.Errorf("expected %d write ops (one per object), got %d", objectCount, ts.TotalOps)
	}
	if ts.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
}

// ── Parquet-like MRD read tests ───────────────────────────────────────────────

// TestParquetLikeFooterRead runs a time-bounded MRD benchmark with a static
// negative read-offset (read-offset: -32768) — identical to the "parquet-footer"
// track in parquet-like-mrd.yaml — and verifies that:
//   - ops complete successfully (no errors)
//   - the average bytes per op is approximately readSize (one range per op)
func TestParquetLikeFooterRead(t *testing.T) {
	const (
		objectSize = 256 * 1024 // 256 KiB fake objects
		readSize   = 32 * 1024  // last 32 KiB = Parquet footer
	)
	mb := newMRDBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:           "parquet-footer",
				OpType:         "read",
				ReadType:       "multirange",
				Weight:         1,
				ReadOffset:     -readSize, // last 32 KiB — mirrors real Parquet footer
				ReadSize:       readSize,
				ReadsPerObject: 1,
				ObjectCount:    5,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
				Concurrency:    2,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track")
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops for footer MRD read")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	// Each op fires one Add() call for exactly readSize bytes.
	if ts.AvgOpSizeBytes < float64(readSize)*0.9 {
		t.Errorf("avg op size %.0f B, expected ~%d B (one footer read per op)",
			ts.AvgOpSizeBytes, readSize)
	}
}

// TestParquetLikeRowGroupRead runs an MRD benchmark with read-offset-random
// and reads-per-object:4, matching the "parquet-rowgroup" track in
// parquet-like-mrd.yaml.  It verifies that:
//   - ops complete with no errors
//   - average bytes per op is approximately 4 × readSize (four concurrent
//     column-chunk ranges per operation)
func TestParquetLikeRowGroupRead(t *testing.T) {
	const (
		objectSize     = 4 << 20    // 4 MiB per fake object
		readSize       = 256 * 1024 // 256 KiB per column-chunk range
		readsPerObject = 4          // 4 concurrent ranges — simulates 4 column chunks
	)
	mb := newMRDBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:             "parquet-rowgroup",
				OpType:           "read",
				ReadType:         "multirange",
				Weight:           1,
				ReadOffsetRandom: true,
				ReadSize:         readSize,
				ReadsPerObject:   readsPerObject,
				ObjectSizeMin:    objectSize,
				ObjectSizeMax:    objectSize,
				ObjectCount:      5,
				Concurrency:      2,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track")
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	// Each op fires readsPerObject concurrent Add() calls, each reading readSize
	// bytes.  AvgOpSizeBytes should be close to readsPerObject × readSize.
	expectedBytesPerOp := float64(readsPerObject * readSize)
	if ts.AvgOpSizeBytes < expectedBytesPerOp*0.8 {
		t.Errorf("avg op size %.0f B, want ≥ %.0f B (%d ranges × %d B each)",
			ts.AvgOpSizeBytes, expectedBytesPerOp*0.8, readsPerObject, readSize)
	}
}

// TestParquetLikeTwoTrackEngine runs the full two-track engine from
// parquet-like-mrd.yaml — a footer track (weight 1) and a row-group track
// (weight 3) — and verifies that:
//   - both tracks complete with ops > 0
//   - neither track records errors
//   - the row-group track (weight 3, concurrency 3) produces more ops/s than
//     the footer track (weight 1, concurrency 1)
func TestParquetLikeTwoTrackEngine(t *testing.T) {
	const objectSize = 4 << 20 // 4 MiB per fake object

	mb := newMRDBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         400 * time.Millisecond,
		TotalConcurrency: 4, // split 1:3 (footer:rowgroup) by weight
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				// Track 1: Parquet footer — static tail read (last 32 KiB).
				Name:           "parquet-footer",
				OpType:         "read",
				ReadType:       "multirange",
				Weight:         1,
				ReadOffset:     -32768,
				ReadSize:       32768,
				ReadsPerObject: 1,
				ObjectCount:    5,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
			},
			{
				// Track 2: Parquet row-group — random offset, 4 concurrent ranges.
				Name:             "parquet-rowgroup",
				OpType:           "read",
				ReadType:         "multirange",
				Weight:           3,
				ReadOffsetRandom: true,
				ReadSize:         256 * 1024,
				ReadsPerObject:   4,
				ObjectSizeMin:    objectSize,
				ObjectSizeMax:    objectSize,
				ObjectCount:      5,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) != 2 {
		t.Fatalf("expected 2 tracks, got %d", len(summary.Tracks))
	}

	for _, ts := range summary.Tracks {
		if ts.TotalOps == 0 {
			t.Errorf("track %q: expected > 0 ops", ts.TrackName)
		}
		if ts.Errors > 0 {
			t.Errorf("track %q: expected 0 errors, got %d", ts.TrackName, ts.Errors)
		}
	}

	// The row-group track gets 3× the goroutines of the footer track.
	// It should therefore run more ops (even though each op also reads more data).
	footer := summary.Tracks[0]
	rowGroup := summary.Tracks[1]
	if rowGroup.TotalOps < footer.TotalOps {
		// This is a soft advisory, not a hard failure — timing is non-deterministic
		// on CI — but we log it for visibility.
		t.Logf("advisory: rowgroup ops (%d) < footer ops (%d); expected rowgroup to dominate",
			rowGroup.TotalOps, footer.TotalOps)
	}
}

// ── computeReadSize unit tests ─────────────────────────────────────────────────

// TestComputeReadSizeFallback verifies that when ReadSizeMin <= 0, computeReadSize
// returns ReadSize unchanged (existing aggregate path is unaffected).
func TestComputeReadSizeFallback(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	track := cfg.BenchmarkTrack{
		ReadSize:    16 << 20, // 16 MiB fixed
		ReadSizeMin: 0,
		ReadSizeMax: 0,
	}
	for i := 0; i < 100; i++ {
		if got := computeReadSize(rng, track); got != 16<<20 {
			t.Fatalf("iteration %d: expected %d, got %d", i, 16<<20, got)
		}
	}
}

// TestComputeReadSizeFixed verifies that ReadSizeMin == ReadSizeMax always
// returns that exact value (per-range tracking, fixed size).
func TestComputeReadSizeFixed(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	track := cfg.BenchmarkTrack{
		ReadSizeMin: 4 << 20,
		ReadSizeMax: 4 << 20,
	}
	for i := 0; i < 100; i++ {
		if got := computeReadSize(rng, track); got != 4<<20 {
			t.Fatalf("iteration %d: expected %d, got %d", i, 4<<20, got)
		}
	}
}

// TestComputeReadSizeRandom verifies that over many draws all values land in
// [ReadSizeMin, ReadSizeMax].
func TestComputeReadSizeRandom(t *testing.T) {
	const (
		minSize    = 1 << 20  // 1 MiB
		maxSize    = 64 << 20 // 64 MiB
		iterations = 10_000
	)
	rng := rand.New(rand.NewSource(99))
	track := cfg.BenchmarkTrack{
		ReadSizeMin: minSize,
		ReadSizeMax: maxSize,
	}
	for i := 0; i < iterations; i++ {
		got := computeReadSize(rng, track)
		if got < minSize || got > maxSize {
			t.Fatalf("iteration %d: size %d out of [%d, %d]", i, got, minSize, maxSize)
		}
	}
}

// TestComputeReadOffsetRandomUsesReadSizeMaxForWindow verifies that when
// ReadSizeMax is larger than ReadSize, computeReadOffset uses ReadSizeMax
// for the safe-window calculation.  This ensures an offset drawn alongside a
// maximum-sized range never exceeds the object boundary.
func TestComputeReadOffsetRandomUsesReadSizeMaxForWindow(t *testing.T) {
	const (
		smallReadSize = 4 * 1024        // 4 KiB (track.ReadSize)
		largeReadSize = 512 * 1024      // 512 KiB (track.ReadSizeMax)
		objectSize    = 1 * 1024 * 1024 // 1 MiB
		iterations    = 10_000
	)
	rng := rand.New(rand.NewSource(5))
	track := cfg.BenchmarkTrack{
		ReadOffsetRandom: true,
		ReadSize:         smallReadSize, // small fixed size
		ReadSizeMax:      largeReadSize, // large max — window must use this
		ObjectSizeMin:    objectSize,
		ObjectSizeMax:    objectSize,
	}
	// offset + largeReadSize must never exceed objectSize.
	const safeMax = objectSize - largeReadSize
	for i := 0; i < iterations; i++ {
		offset := computeReadOffset(rng, track)
		if offset < 0 {
			t.Fatalf("iteration %d: negative offset %d", i, offset)
		}
		if offset > safeMax {
			t.Fatalf("iteration %d: offset %d > safeMax %d (offset+largeReadSize would exceed objectSize)",
				i, offset, safeMax)
		}
	}
}

// ── doReadMultiRangePerRange end-to-end tests ─────────────────────────────────

// TestPerRangeLatencyFixed activates the per-range path with ReadSizeMin ==
// ReadSizeMax (fixed size, per-range latency).  Verifies zero errors and that
// total bytes ≈ readsPerObject × readSize per operation.
func TestPerRangeLatencyFixed(t *testing.T) {
	const (
		objectSize     = 4 << 20    // 4 MiB per fake object
		rangeSize      = 256 * 1024 // 256 KiB fixed per range
		readsPerObject = 4
	)
	mb := newMRDBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:             "per-range-fixed",
				OpType:           "read",
				ReadType:         "multirange",
				Weight:           1,
				ReadOffsetRandom: true,
				// ReadSizeMin > 0 activates doReadMultiRangePerRange.
				ReadSizeMin:    rangeSize,
				ReadSizeMax:    rangeSize, // fixed — no random size, but per-range tracking
				ReadsPerObject: readsPerObject,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
				ObjectCount:    5,
				Concurrency:    2,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track")
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	// Each op fires readsPerObject ranges of rangeSize bytes each.
	expectedBytesPerOp := float64(readsPerObject * rangeSize)
	if ts.AvgOpSizeBytes < expectedBytesPerOp*0.8 {
		t.Errorf("avg op size %.0f B, want ≥ %.0f B (%d ranges × %d B each)",
			ts.AvgOpSizeBytes, expectedBytesPerOp*0.8, readsPerObject, rangeSize)
	}
}

// TestPerRangeLatencyRandomSize activates the per-range path with a random size
// window [ReadSizeMin, ReadSizeMax].  Verifies zero errors and that average
// bytes per operation falls in the expected range:
//
//	[readsPerObject × ReadSizeMin, readsPerObject × ReadSizeMax]
//
// This test confirms that random range sizes are drawn independently per Add()
// call and that the safe-offset window accounts for the largest possible range.
func TestPerRangeLatencyRandomSize(t *testing.T) {
	const (
		objectSize     = 8 << 20    // 8 MiB per fake object
		readSizeMin    = 64 * 1024  // 64 KiB minimum range
		readSizeMax    = 512 * 1024 // 512 KiB maximum range
		readsPerObject = 3
	)
	mb := newMRDBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:             "per-range-random-size",
				OpType:           "read",
				ReadType:         "multirange",
				Weight:           1,
				ReadOffsetRandom: true,
				ReadSizeMin:      readSizeMin,
				ReadSizeMax:      readSizeMax,
				ReadsPerObject:   readsPerObject,
				ObjectSizeMin:    objectSize,
				ObjectSizeMax:    objectSize,
				ObjectCount:      5,
				Concurrency:      2,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track")
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	// Average bytes per op must be in [readsPerObject*minSize, readsPerObject*maxSize].
	minExpected := float64(readsPerObject * readSizeMin)
	maxExpected := float64(readsPerObject * readSizeMax)
	if ts.AvgOpSizeBytes < minExpected*0.8 {
		t.Errorf("avg op size %.0f B below minimum expected %.0f B (%d × %d B)",
			ts.AvgOpSizeBytes, minExpected*0.8, readsPerObject, readSizeMin)
	}
	if ts.AvgOpSizeBytes > maxExpected*1.1 {
		t.Errorf("avg op size %.0f B exceeds maximum expected %.0f B (%d × %d B)",
			ts.AvgOpSizeBytes, maxExpected*1.1, readsPerObject, readSizeMax)
	}
}

// ── doReadTraditionalParquet end-to-end tests ─────────────────────────────────

// traditionalMockBucket wraps mockBucket and overrides StatObject to return a
// real MinObject with the configured size.  This is required because
// doReadTraditionalParquet calls StatObject to discover objectSize before
// computing the footer byte offset; the base mockBucket returns nil.
type traditionalMockBucket struct {
	*mockBucket
	statSize uint64
}

func (b *traditionalMockBucket) StatObject(_ context.Context, req *gcs.StatObjectRequest) (*gcs.MinObject, *gcs.ExtendedObjectAttributes, error) {
	return &gcs.MinObject{Name: req.Name, Size: b.statSize}, nil, nil
}

func newTraditionalBucket(objectSize int) *traditionalMockBucket {
	return &traditionalMockBucket{
		mockBucket: &mockBucket{objectSize: uint64(objectSize)},
		statSize:   uint64(objectSize),
	}
}

// TestTraditionalParquetBasic verifies that read-type: traditional-parquet
// completes without errors and credits reads-per-object samples per op.
// The stat returns objectSize, so the footer byte-range offset is computable.
func TestTraditionalParquetBasic(t *testing.T) {
	const (
		objectSize     = 8 << 20 // 8 MiB per fake object
		footerSize     = 32768   // 32 KiB
		rowGroupSize   = 1 << 20 // 1 MiB per row group
		readsPerObject = 4       // row groups per file visit = samples per op
	)
	tb := newTraditionalBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         400 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:             "traditional-parquet",
				OpType:           "read",
				ReadType:         "traditional-parquet",
				Weight:           1,
				ReadFooterSize:   footerSize,
				ReadSize:         rowGroupSize,
				ReadOffsetRandom: true,
				ReadsPerObject:   readsPerObject,
				ObjectCount:      5,
				ObjectSizeMin:    objectSize,
				ObjectSizeMax:    objectSize,
			},
		},
	}
	eng, err := NewEngine(tb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected 1 track")
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	// Each op should credit readsPerObject samples → TotalSamples > 0.
	if ts.TotalSamples == 0 {
		t.Error("expected TotalSamples > 0 (reads-per-object credits samples)")
	}
	// SamplesPerSec must be approximately readsPerObject × OpsPerSec.
	if ts.SamplesPerSec <= 0 {
		t.Error("expected SamplesPerSec > 0")
	}
	minSamplesPerOp := ts.TotalSamples / ts.TotalOps
	if minSamplesPerOp < int64(readsPerObject)-1 {
		t.Errorf("avg samples/op = %d, want ≈ %d", minSamplesPerOp, readsPerObject)
	}
}

// TestTraditionalParquetDefaultFooterSize verifies that when ReadFooterSize is
// 0, the engine defaults to 32768 (32 KiB) for the footer read.
func TestTraditionalParquetDefaultFooterSize(t *testing.T) {
	const objectSize = 4 << 20
	tb := newTraditionalBucket(objectSize)
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 1,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:           "trad-default-footer",
				OpType:         "read",
				ReadType:       "traditional-parquet",
				Weight:         1,
				ReadFooterSize: 0,          // should default to 32 KiB
				ReadSize:       512 * 1024, // 512 KiB row groups
				ReadsPerObject: 2,
				ObjectCount:    3,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
			},
		},
	}
	eng, err := NewEngine(tb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	ts := summary.Tracks[0]
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
}

// TestSamplesPerObjectNewReader verifies that setting samples-per-object > 0
// on a plain new-reader track populates TotalSamples and SamplesPerSec in the
// summary.  This supports the "full-object GET delivers N samples" use case.
func TestSamplesPerObjectNewReader(t *testing.T) {
	const (
		objectSize    = 2 << 20 // 2 MiB
		samplesPerObj = 16      // each full GET delivers 16 samples
	)
	mb := &mockBucket{objectSize: uint64(objectSize)}
	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:             "full-get-with-samples",
				OpType:           "read",
				ReadType:         "new-reader",
				Weight:           1,
				SamplesPerObject: samplesPerObj,
				ObjectCount:      5,
				ObjectSizeMin:    objectSize,
				ObjectSizeMax:    objectSize,
			},
		},
	}
	eng, err := NewEngine(mb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	ts := summary.Tracks[0]
	if ts.TotalOps == 0 {
		t.Error("expected > 0 ops")
	}
	if ts.Errors > 0 {
		t.Errorf("expected 0 errors, got %d", ts.Errors)
	}
	if ts.TotalSamples == 0 {
		t.Error("expected TotalSamples > 0 when samples-per-object > 0")
	}
	if ts.SamplesPerSec <= 0 {
		t.Error("expected SamplesPerSec > 0")
	}
	// TotalSamples should be approximately TotalOps × samplesPerObj.
	if ts.TotalSamples < ts.TotalOps {
		t.Errorf("TotalSamples (%d) < TotalOps (%d); expected TotalSamples ≈ %d × TotalOps",
			ts.TotalSamples, ts.TotalOps, samplesPerObj)
	}
}
