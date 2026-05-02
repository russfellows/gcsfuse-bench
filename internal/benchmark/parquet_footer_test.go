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

// Unit tests for the Parquet footer encoder and decoder (parquet.go).
//
// Coverage:
//
//  1. BuildParquetFooter + ParseParquetFooter round-trips across various
//     (objectSize, rgCount, rgSize) combinations.
//  2. Exact row-group offset and size values from the encoder.
//  3. Error paths in BuildParquetFooter and ParseParquetFooter.
//  4. Integration: doWriteParquet (via Engine in prepare mode) produces objects
//     whose footer ParseParquetFooter can decode, with correct offsets.
//  5. Integration: doReadTraditionalParquet navigates to actual row-group offsets
//     when given objects with real Parquet footers.

package benchmark

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	storagev2 "cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v3/cfg"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/gcs"
)

// ── Test helpers ─────────────────────────────────────────────────────────────

// buildParquetObjectData builds a complete synthetic Parquet object in memory:
//
//	[PAR1 4B] [zero fill] [Thrift FileMetaData] [4-byte LE metaLen] [PAR1 4B]
//
// The zero fill is fine for testing — only the footer bytes matter for
// ParseParquetFooter.  The row group region is intentionally left zeroed because
// doReadTraditionalParquet only validates the byte range and doesn't decode data.
func buildParquetObjectData(objectSize int64, rgCount int, rgSize int64) []byte {
	footerTail, _, err := BuildParquetFooter(objectSize, rgCount, rgSize)
	if err != nil {
		panic("buildParquetObjectData: " + err.Error())
	}
	data := make([]byte, objectSize)
	copy(data[0:4], parquetMagic[:])
	copy(data[objectSize-int64(len(footerTail)):], footerTail)
	return data
}

// sliceReader is an io.ReadCloser that serves bytes from a fixed slice.
// It implements gcs.StorageReader (Read + Close + ReadHandle).
type sliceReader struct {
	data []byte
	pos  int
}

func (r *sliceReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}
func (r *sliceReader) Close() error                     { return nil }
func (r *sliceReader) ReadHandle() storagev2.ReadHandle { return nil }

// parquetMockBucket is a test bucket that serves a pre-built Parquet object
// from memory.  It is used for traditional-parquet engine tests where the footer
// bytes must contain a valid Thrift FileMetaData (not zero bytes).
//
// StatObject returns a MinObject whose Size matches len(objectData).
// NewReaderWithReadHandle slices objectData according to the requested range.
// All other methods delegate to the embedded *mockBucket.
type parquetMockBucket struct {
	*mockBucket
	objectData []byte
}

// newParquetBucket constructs a parquetMockBucket from pre-built Parquet bytes.
func newParquetBucket(objectData []byte) *parquetMockBucket {
	return &parquetMockBucket{
		mockBucket: &mockBucket{objectSize: uint64(len(objectData))},
		objectData: objectData,
	}
}

func (b *parquetMockBucket) StatObject(_ context.Context, req *gcs.StatObjectRequest) (*gcs.MinObject, *gcs.ExtendedObjectAttributes, error) {
	return &gcs.MinObject{Name: req.Name, Size: uint64(len(b.objectData))}, nil, nil
}

func (b *parquetMockBucket) NewReaderWithReadHandle(_ context.Context, req *gcs.ReadObjectRequest) (gcs.StorageReader, error) {
	start, end := 0, len(b.objectData)
	if req.Range != nil {
		start = int(req.Range.Start)
		end = int(req.Range.Limit)
		if start > len(b.objectData) {
			start = len(b.objectData)
		}
		if end > len(b.objectData) {
			end = len(b.objectData)
		}
	}
	return &sliceReader{data: b.objectData[start:end]}, nil
}

// capturingMockBucket is a test bucket whose CreateObject reads and stores the
// full body from req.Contents so tests can inspect the written bytes.
type capturingMockBucket struct {
	*mockBucket
	mu      sync.Mutex
	objects map[string][]byte
}

func newCapturingBucket() *capturingMockBucket {
	return &capturingMockBucket{
		mockBucket: &mockBucket{},
		objects:    make(map[string][]byte),
	}
}

func (b *capturingMockBucket) CreateObject(_ context.Context, req *gcs.CreateObjectRequest) (*gcs.Object, error) {
	data, err := io.ReadAll(req.Contents)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.objects[req.Name] = data
	b.mu.Unlock()
	return &gcs.Object{Name: req.Name, Size: uint64(len(data))}, nil
}

// ── BuildParquetFooter + ParseParquetFooter round-trip tests ─────────────────

// TestBuildParseRoundTrip verifies that BuildParquetFooter produces a footer
// that ParseParquetFooter can decode, and that the decoded offsets and sizes
// exactly match what was requested for a variety of (objectSize, rgCount, rgSize)
// combinations that span the realistic range of AI/ML workloads.
func TestBuildParseRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		objectSize int64
		rgCount    int
		rgSize     int64
	}{
		{"tiny-1group-4KiB-rg", 256 * 1024, 1, 4096},
		{"small-4groups-64KiB-rg", 4 << 20, 4, 65536},
		{"medium-16groups-64KiB-rg", 256 << 20, 16, 65536},
		{"large-32groups-128KiB-rg", 1 << 30, 32, 128 * 1024},
		{"single-group-exact-size", 1 << 20, 1, 65536},
		{"many-groups-small", 512 << 20, 64, 32 * 1024},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			footerTail, buildGroups, err := BuildParquetFooter(tc.objectSize, tc.rgCount, tc.rgSize)
			if err != nil {
				t.Fatalf("BuildParquetFooter: %v", err)
			}
			if len(footerTail) < 8 {
				t.Fatalf("footer tail too short: %d bytes", len(footerTail))
			}
			// Trailing bytes of the object are in footerTail.
			// Construct the full object to exercise ParseParquetFooter with the
			// tail bytes only (as the engine does when it reads read-footer-size
			// bytes from the end of the object).
			parsedGroups, err := ParseParquetFooter(footerTail, tc.objectSize)
			if err != nil {
				t.Fatalf("ParseParquetFooter: %v", err)
			}
			if len(parsedGroups) != tc.rgCount {
				t.Fatalf("got %d row groups, want %d", len(parsedGroups), tc.rgCount)
			}
			for i := 0; i < tc.rgCount; i++ {
				wantOffset := int64(4+i)*tc.rgSize + 4
				// Correct formula: offset_i = 4 + i * rgSize
				wantOffset = 4 + int64(i)*tc.rgSize

				if parsedGroups[i].Offset != wantOffset {
					t.Errorf("group[%d].Offset = %d, want %d", i, parsedGroups[i].Offset, wantOffset)
				}
				if parsedGroups[i].Size != tc.rgSize {
					t.Errorf("group[%d].Size = %d, want %d", i, parsedGroups[i].Size, tc.rgSize)
				}
				// Consistency check: build and parse must agree.
				if buildGroups[i].Offset != parsedGroups[i].Offset {
					t.Errorf("group[%d]: build offset %d ≠ parse offset %d",
						i, buildGroups[i].Offset, parsedGroups[i].Offset)
				}
				if buildGroups[i].Size != parsedGroups[i].Size {
					t.Errorf("group[%d]: build size %d ≠ parse size %d",
						i, buildGroups[i].Size, parsedGroups[i].Size)
				}
			}
		})
	}
}

// TestBuildParquetFooterSingleGroupOffset verifies the exact offset of the single
// row group when rgCount=1: it must be 4 (immediately after the leading PAR1 magic).
func TestBuildParquetFooterSingleGroupOffset(t *testing.T) {
	const (
		objectSize = 1 << 20 // 1 MiB
		rgSize     = 65536   // 64 KiB
	)
	footerTail, groups, err := BuildParquetFooter(objectSize, 1, rgSize)
	if err != nil {
		t.Fatalf("BuildParquetFooter: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(groups))
	}
	if groups[0].Offset != 4 {
		t.Errorf("single row group offset = %d, want 4 (immediately after PAR1 magic)", groups[0].Offset)
	}
	if groups[0].Size != rgSize {
		t.Errorf("single row group size = %d, want %d", groups[0].Size, rgSize)
	}
	// Confirm ParseParquetFooter agrees.
	parsed, err := ParseParquetFooter(footerTail, objectSize)
	if err != nil {
		t.Fatalf("ParseParquetFooter: %v", err)
	}
	if parsed[0].Offset != 4 {
		t.Errorf("parsed offset = %d, want 4", parsed[0].Offset)
	}
}

// TestBuildParquetFooterGroupOffsets verifies that consecutive row groups are
// packed at exactly 4 + i*rgSize for i=0…rgCount-1 (no gaps, no overlap).
func TestBuildParquetFooterGroupOffsets(t *testing.T) {
	const (
		objectSize = 512 << 20 // 512 MiB
		rgCount    = 16
		rgSize     = 64 * 1024 // 64 KiB
	)
	_, groups, err := BuildParquetFooter(objectSize, rgCount, rgSize)
	if err != nil {
		t.Fatalf("BuildParquetFooter: %v", err)
	}
	if len(groups) != rgCount {
		t.Fatalf("got %d groups, want %d", len(groups), rgCount)
	}
	for i, g := range groups {
		want := int64(4 + i*rgSize)
		if g.Offset != want {
			t.Errorf("group[%d].Offset = %d, want %d", i, g.Offset, want)
		}
		if g.Size != rgSize {
			t.Errorf("group[%d].Size = %d, want %d", i, g.Size, rgSize)
		}
		// Each row group ends exactly where the next begins.
		if i > 0 && groups[i].Offset != groups[i-1].Offset+groups[i-1].Size {
			t.Errorf("gap or overlap between group[%d] and group[%d]", i-1, i)
		}
	}
}

// TestBuildParquetFooterTooSmall verifies that BuildParquetFooter returns a
// non-nil error (rather than panicking or producing a corrupt footer) when the
// requested object size is too small to contain the leading PAR1 + row groups +
// Thrift metadata + trailing metaLen + PAR1.
func TestBuildParquetFooterTooSmall(t *testing.T) {
	cases := []struct {
		name       string
		objectSize int64
		rgCount    int
		rgSize     int64
	}{
		{"zero-object", 0, 1, 4096},
		{"exactly-8-bytes", 8, 1, 4096},                   // 4-byte PAR1 × 2 = 8; no room for data or meta
		{"row-groups-overflow", 1024, 1, 512 * 1024},      // rgSize > objectSize
		{"many-groups-overflow", 1 << 20, 100, 64 * 1024}, // 100×64KiB > 1MiB
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := BuildParquetFooter(tc.objectSize, tc.rgCount, tc.rgSize)
			if err == nil {
				t.Errorf("expected error for objectSize=%d rgCount=%d rgSize=%d, got nil",
					tc.objectSize, tc.rgCount, tc.rgSize)
			}
		})
	}
}

// ── ParseParquetFooter error-path tests ──────────────────────────────────────

// TestParseParquetFooterBadMagic verifies that ParseParquetFooter returns
// ErrNotParquet when the last 4 bytes of footerBytes are not "PAR1".
func TestParseParquetFooterBadMagic(t *testing.T) {
	cases := []struct {
		name  string
		bytes []byte
	}{
		{"all-zeros", make([]byte, 64)},
		{"wrong-magic", append(make([]byte, 60), 'X', 'Y', 'Z', '1')},
		{"partial-magic", append(make([]byte, 60), 'P', 'A', 'R', '2')},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseParquetFooter(tc.bytes, int64(len(tc.bytes)))
			if !errors.Is(err, ErrNotParquet) {
				t.Errorf("expected ErrNotParquet, got %v", err)
			}
		})
	}
}

// TestParseParquetFooterTooShort verifies that ParseParquetFooter returns a
// non-nil error (not a panic) when fewer than 8 bytes are supplied — which is
// too short to contain the trailing [4-byte metaLen][PAR1] tail.
func TestParseParquetFooterTooShort(t *testing.T) {
	for size := 0; size < 8; size++ {
		_, err := ParseParquetFooter(make([]byte, size), 1<<20)
		if err == nil {
			t.Errorf("size=%d: expected error for too-short buffer, got nil", size)
		}
	}
}

// TestParseParquetFooterMetadataLargerThanBuffer verifies that ParseParquetFooter
// returns a descriptive error when the 4-byte metaLen field claims that the
// metadata blob extends beyond the supplied buffer.  This can happen when
// read-footer-size is set smaller than the actual Thrift metadata.
func TestParseParquetFooterMetadataLargerThanBuffer(t *testing.T) {
	// Build a valid 8-byte tail with PAR1 magic but a metaLen of 9999 bytes —
	// far larger than the 8-byte buffer.
	buf := make([]byte, 8)
	buf[0] = 0x0F // metaLen LE: 0x00002710 = 9999
	buf[1] = 0x27
	buf[2] = 0x00
	buf[3] = 0x00
	buf[4] = 'P'
	buf[5] = 'A'
	buf[6] = 'R'
	buf[7] = '1'

	_, err := ParseParquetFooter(buf, 1<<20)
	if err == nil {
		t.Fatal("expected error when metaLen > available buffer, got nil")
	}
	if errors.Is(err, ErrNotParquet) {
		t.Errorf("error should NOT be ErrNotParquet for a valid PAR1 magic + bad metaLen; got %v", err)
	}
}

// TestParseParquetFooterFullObject verifies that ParseParquetFooter works
// correctly when passed the complete object bytes (not just the footer tail),
// as would happen if a caller reads the entire small object and then calls
// ParseParquetFooter on it.
func TestParseParquetFooterFullObject(t *testing.T) {
	const (
		objectSize = 2 << 20 // 2 MiB
		rgCount    = 4
		rgSize     = 65536
	)
	data := buildParquetObjectData(objectSize, rgCount, rgSize)

	groups, err := ParseParquetFooter(data, objectSize)
	if err != nil {
		t.Fatalf("ParseParquetFooter on full object: %v", err)
	}
	if len(groups) != rgCount {
		t.Fatalf("got %d row groups, want %d", len(groups), rgCount)
	}
	for i, g := range groups {
		wantOffset := int64(4 + i*rgSize)
		if g.Offset != wantOffset {
			t.Errorf("group[%d].Offset = %d, want %d", i, g.Offset, wantOffset)
		}
	}
}

// ── doWriteParquet integration test ──────────────────────────────────────────

// TestDoWriteParquetProducesValidFooter runs the engine in prepare mode with
// write-format: parquet and verifies that:
//
//   - Every written object has a valid PAR1 magic trailer.
//   - ParseParquetFooter successfully decodes the FileMetaData.
//   - Row-group count matches RowGroupCount from the config.
//   - Row-group offsets are exactly 4 + i*RowGroupSize.
//   - Row-group sizes match RowGroupSize from the config.
//   - Object length matches ObjectSizeMin (fixed-size objects).
func TestDoWriteParquetProducesValidFooter(t *testing.T) {
	const (
		// 512 KiB: row-group region is 4×64 KiB = 256 KiB; remaining ~256 KiB
		// comfortably holds the ~250-byte Thrift metadata + 8-byte trailer.
		objectSize = 512 * 1024
		rgCount    = 4
		rgSize     = 65536 // 64 KiB per row group
	)
	cb := newCapturingBucket()
	bCfg := cfg.BenchmarkConfig{
		Mode:             "prepare",
		ObjectPrefix:     "parquet-test/",
		TotalConcurrency: 2,
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:          "parquet-write",
				OpType:        "write",
				Weight:        1,
				ObjectCount:   4,
				Concurrency:   2,
				WriteFormat:   "parquet",
				RowGroupCount: rgCount,
				RowGroupSize:  rgSize,
				ObjectSizeMin: objectSize,
				ObjectSizeMax: objectSize,
			},
		},
	}
	eng, err := NewEngine(cb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	eng.prepareRetryDelay = time.Nanosecond

	summary, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if len(summary.Tracks) == 0 {
		t.Fatal("expected at least 1 track in summary")
	}
	if summary.Tracks[0].Errors > 0 {
		t.Fatalf("expected 0 write errors, got %d", summary.Tracks[0].Errors)
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if len(cb.objects) == 0 {
		t.Fatal("no objects captured by bucket — engine did not write anything")
	}

	for name, data := range cb.objects {
		if int64(len(data)) != objectSize {
			t.Errorf("object %q: length %d, want %d", name, len(data), objectSize)
		}
		// Verify leading PAR1 magic.
		if data[0] != 'P' || data[1] != 'A' || data[2] != 'R' || data[3] != '1' {
			t.Errorf("object %q: missing leading PAR1 magic", name)
		}

		groups, parseErr := ParseParquetFooter(data, int64(len(data)))
		if parseErr != nil {
			t.Errorf("object %q: ParseParquetFooter: %v", name, parseErr)
			continue
		}
		if len(groups) != rgCount {
			t.Errorf("object %q: %d row groups, want %d", name, len(groups), rgCount)
			continue
		}
		for i, g := range groups {
			wantOffset := int64(4 + i*rgSize)
			if g.Offset != wantOffset {
				t.Errorf("object %q group[%d]: offset=%d, want %d", name, i, g.Offset, wantOffset)
			}
			if g.Size != int64(rgSize) {
				t.Errorf("object %q group[%d]: size=%d, want %d", name, i, g.Size, rgSize)
			}
		}
	}
}

// TestDoWriteParquetDefaultRgCount verifies that when RowGroupCount is 0,
// doWriteParquet falls back to ReadsPerObject (and ultimately to 1 if that is
// also zero).
func TestDoWriteParquetDefaultRgCount(t *testing.T) {
	const (
		// 512 KiB: fits 4×64 KiB row groups + Thrift metadata overhead.
		objectSize     = 512 * 1024
		readsPerObject = 4 // RowGroupCount is 0 → should default to this
		rgSize         = 65536
	)
	cb := newCapturingBucket()
	bCfg := cfg.BenchmarkConfig{
		Mode:             "prepare",
		ObjectPrefix:     "parquet-default-count/",
		TotalConcurrency: 1,
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:           "parquet-default",
				OpType:         "write",
				Weight:         1,
				ObjectCount:    2,
				Concurrency:    1,
				WriteFormat:    "parquet",
				RowGroupCount:  0, // zero → defaults to ReadsPerObject
				ReadsPerObject: readsPerObject,
				RowGroupSize:   rgSize,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
			},
		},
	}
	eng, err := NewEngine(cb, bCfg, 0, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	eng.prepareRetryDelay = time.Nanosecond

	_, err = eng.Run(context.Background())
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if len(cb.objects) == 0 {
		t.Fatal("no objects captured — engine wrote nothing (all writes may have failed)")
	}
	for name, data := range cb.objects {
		groups, parseErr := ParseParquetFooter(data, int64(len(data)))
		if parseErr != nil {
			t.Errorf("object %q: ParseParquetFooter: %v", name, parseErr)
			continue
		}
		if len(groups) != readsPerObject {
			t.Errorf("object %q: %d row groups, want %d (defaulted from ReadsPerObject)",
				name, len(groups), readsPerObject)
		}
	}
}

// ── doReadTraditionalParquet integration tests with real Parquet objects ──────

// TestTraditionalParquetWithRealFooter is the canonical end-to-end test for the
// traditional-parquet read path.  It uses a parquetMockBucket that serves real
// Parquet bytes so that ParseParquetFooter can decode the FileMetaData and supply
// actual row-group offsets to the range GETs.
//
// This test validates the complete flow introduced in v1.3.0:
//
//	stat → footer GET (decode FileMetaData) → N parallel row-group range GETs
func TestTraditionalParquetWithRealFooter(t *testing.T) {
	const (
		objectSize     = 8 << 20 // 8 MiB
		footerSize     = 32768   // 32 KiB — must contain the full Thrift metadata
		rgCount        = 4       // matches readsPerObject below
		rgSize         = 1 << 20 // 1 MiB per row group
		readsPerObject = rgCount
	)
	data := buildParquetObjectData(objectSize, rgCount, rgSize)
	pb := newParquetBucket(data)

	bCfg := cfg.BenchmarkConfig{
		Duration:         400 * time.Millisecond,
		TotalConcurrency: 2,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:           "traditional-parquet",
				OpType:         "read",
				ReadType:       "traditional-parquet",
				Weight:         1,
				ReadFooterSize: footerSize,
				ReadSize:       rgSize, // irrelevant — row-group size comes from parsed footer
				ReadsPerObject: readsPerObject,
				ObjectCount:    5,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
			},
		},
	}
	eng, err := NewEngine(pb, bCfg, 0, nil)
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
	// Each op should credit readsPerObject samples.
	if ts.TotalSamples == 0 {
		t.Error("expected TotalSamples > 0")
	}
	minSamplesPerOp := ts.TotalSamples / ts.TotalOps
	if minSamplesPerOp < int64(readsPerObject)-1 {
		t.Errorf("avg samples/op = %d, want ≈ %d", minSamplesPerOp, readsPerObject)
	}
}

// TestTraditionalParquetDefaultFooterSizeWithRealFooter mirrors
// TestTraditionalParquetDefaultFooterSize but uses a parquetMockBucket so
// ParseParquetFooter succeeds.  When ReadFooterSize is 0 the engine should
// default to 32768 bytes (32 KiB) for the footer GET.
func TestTraditionalParquetDefaultFooterSizeWithRealFooter(t *testing.T) {
	const (
		objectSize     = 4 << 20 // 4 MiB
		rgCount        = 2
		rgSize         = 512 * 1024 // 512 KiB
		readsPerObject = rgCount
	)
	data := buildParquetObjectData(objectSize, rgCount, rgSize)
	pb := newParquetBucket(data)

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
				ReadFooterSize: 0, // should default to 32 KiB
				ReadSize:       rgSize,
				ReadsPerObject: readsPerObject,
				ObjectCount:    3,
				ObjectSizeMin:  objectSize,
				ObjectSizeMax:  objectSize,
			},
		},
	}
	eng, err := NewEngine(pb, bCfg, 0, nil)
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

// TestTraditionalParquetInvalidFooterReturnsError verifies that
// doReadTraditionalParquet returns an error (rather than silently producing
// wrong offsets) when the object bytes have no valid PAR1 trailer.  This is the
// "no fallback" policy introduced in v1.3.0: objects without real Parquet footers
// are rejected immediately so misconfigured workloads fail loudly.
func TestTraditionalParquetInvalidFooterReturnsError(t *testing.T) {
	const objectSize = 4 << 20 // 4 MiB — but bytes are all zeros (no PAR1)
	// newTraditionalBucket returns a mockBucket whose NewReaderWithReadHandle
	// produces zero bytes.  Those zero bytes have no PAR1 magic → ErrNotParquet.
	tb := newTraditionalBucket(objectSize)

	bCfg := cfg.BenchmarkConfig{
		Duration:         300 * time.Millisecond,
		TotalConcurrency: 1,
		OutputFormat:     "yaml",
		Histograms:       cfg.DefaultHistogramConfig(),
		Tracks: []cfg.BenchmarkTrack{
			{
				Name:           "trad-bad-footer",
				OpType:         "read",
				ReadType:       "traditional-parquet",
				Weight:         1,
				ReadFooterSize: 32768,
				ReadSize:       512 * 1024,
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
	// Every op should fail with ParseParquetFooter: ErrNotParquet.
	if ts.Errors == 0 {
		t.Error("expected errors when footer has no PAR1 magic, got none")
	}
}
