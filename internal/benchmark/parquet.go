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

// Minimal Parquet footer encoder and decoder using Thrift CompactProtocol.
//
// Objects written with write-format: parquet have this layout:
//
//	[PAR1 magic — 4 bytes]
//	[row-group data region — rgCount × rgSize bytes of Xoshiro256++ random data]
//	[padding — random bytes to fill remaining space before metadata]
//	[Thrift CompactProtocol FileMetaData — variable size]
//	[4-byte LE uint32 — byte length of the FileMetaData blob]
//	[PAR1 magic — 4 bytes]
//
// This matches the Apache Parquet on-disk format exactly.  The FileMetaData
// encodes the real data_page_offset of every row group, so any conforming
// Parquet footer reader can navigate to row groups by byte offset.
//
// Only the footer is valid Parquet; row-group data pages are raw random bytes
// without Parquet page headers.  This is intentional: we benchmark I/O latency,
// not data decoding.

package benchmark

import (
	"encoding/binary"
	"fmt"
)

// ── Parquet / Thrift constants ──────────────────────────────────────────────

// parquetMagic is the 4-byte PAR1 marker that opens and closes every Parquet file.
var parquetMagic = [4]byte{'P', 'A', 'R', '1'}

const (
	// Thrift CompactProtocol field type tags.
	thriftStop         byte = 0x00
	thriftBooleanTrue  byte = 0x01
	thriftBooleanFalse byte = 0x02
	thriftByte         byte = 0x03
	thriftI16          byte = 0x04
	thriftI32          byte = 0x05
	thriftI64          byte = 0x06
	thriftDouble       byte = 0x07
	thriftBinary       byte = 0x08
	thriftList         byte = 0x09
	thriftSet          byte = 0x0A
	thriftMap          byte = 0x0B
	thriftStruct       byte = 0x0C

	// Parquet enum values (from the Parquet Thrift IDL).
	parquetVersion2     = 2 // FileMetaData.version
	parquetByteArray    = 6 // Type::BYTE_ARRAY
	parquetOptional     = 1 // FieldRepetitionType::OPTIONAL
	parquetPlain        = 0 // Encoding::PLAIN
	parquetUncompressed = 0 // CompressionCodec::UNCOMPRESSED
)

// ── RowGroupInfo ────────────────────────────────────────────────────────────

// RowGroupInfo holds the byte offset and data size of one row group within an
// object, as stored in ColumnMetaData.data_page_offset and
// ColumnMetaData.total_compressed_size.
type RowGroupInfo struct {
	// Offset is the byte offset from the start of the object where row-group
	// data begins (ColumnMetaData.data_page_offset).
	Offset int64
	// Size is the total byte length of the row-group data
	// (ColumnMetaData.total_compressed_size).
	Size int64
}

// ── Thrift CompactProtocol encoder helpers ──────────────────────────────────

// appendVarint appends v as a little-endian base-128 varint.
func appendVarint(b []byte, v uint64) []byte {
	for v > 0x7F {
		b = append(b, byte(v&0x7F)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// appendZigI32 encodes v as a ZigZag-encoded int32 varint.
func appendZigI32(b []byte, v int32) []byte {
	return appendVarint(b, uint64((v<<1)^(v>>31)))
}

// appendZigI64 encodes v as a ZigZag-encoded int64 varint.
func appendZigI64(b []byte, v int64) []byte {
	return appendVarint(b, uint64((v<<1)^(v>>63)))
}

// appendString appends a Thrift binary value: varint(len) followed by raw bytes.
func appendString(b []byte, s string) []byte {
	b = appendVarint(b, uint64(len(s)))
	return append(b, s...)
}

// appendFieldHeader appends a Thrift CompactProtocol field header.
// prevID is updated to curID on return.  typ is the Thrift type tag.
// Uses the short 1-byte form when the delta fits in 4 bits (1–15),
// otherwise falls back to the long form (type byte + zigzag field ID varint).
func appendFieldHeader(b []byte, prevID *int, curID int, typ byte) []byte {
	delta := curID - *prevID
	*prevID = curID
	if delta > 0 && delta <= 15 {
		return append(b, byte(delta<<4)|typ)
	}
	// Long form: low-nibble type byte, then zigzag-encoded field ID.
	b = append(b, typ)
	return appendZigI32(b, int32(curID))
}

// appendListHeader appends a Thrift CompactProtocol list/set header.
// Uses the compact 1-byte form for sizes 0–14; long form for sizes ≥ 15.
func appendListHeader(b []byte, size int, elemType byte) []byte {
	if size < 15 {
		return append(b, byte(size<<4)|elemType)
	}
	b = append(b, 0xF0|elemType)
	return appendVarint(b, uint64(size))
}

// ── Thrift encoder: Parquet FileMetaData ───────────────────────────────────

// encodeFileMetaData builds the Thrift CompactProtocol representation of a
// minimal Parquet FileMetaData for the given row groups.
//
// Schema contains two elements: a root message group (name "schema",
// num_children 1) and one BYTE_ARRAY column (name "embeddings").
// Each row group contains one column chunk whose data_page_offset and
// total_compressed_size are taken from the corresponding RowGroupInfo.
func encodeFileMetaData(groups []RowGroupInfo, rgSize int64) []byte {
	buf := make([]byte, 0, len(groups)*200+512)
	pf := 0 // previous field ID (delta-encoded)

	// Field 1: version = 2
	buf = appendFieldHeader(buf, &pf, 1, thriftI32)
	buf = appendZigI32(buf, parquetVersion2)

	// Field 2: schema (list<SchemaElement> — 2 elements: root + column)
	buf = appendFieldHeader(buf, &pf, 2, thriftList)
	buf = appendListHeader(buf, 2, thriftStruct)

	// SchemaElement[0]: root message group — name="schema", num_children=1
	{
		sf := 0
		buf = appendFieldHeader(buf, &sf, 4, thriftBinary) // name
		buf = appendString(buf, "schema")
		buf = appendFieldHeader(buf, &sf, 5, thriftI32) // num_children
		buf = appendZigI32(buf, 1)
		buf = append(buf, thriftStop)
	}

	// SchemaElement[1]: column — type=BYTE_ARRAY, repetition=OPTIONAL, name="embeddings"
	{
		sf := 0
		buf = appendFieldHeader(buf, &sf, 1, thriftI32) // type = BYTE_ARRAY (6)
		buf = appendZigI32(buf, parquetByteArray)
		buf = appendFieldHeader(buf, &sf, 3, thriftI32) // repetition_type = OPTIONAL (1)
		buf = appendZigI32(buf, parquetOptional)
		buf = appendFieldHeader(buf, &sf, 4, thriftBinary) // name
		buf = appendString(buf, "embeddings")
		buf = append(buf, thriftStop)
	}

	// Field 3: num_rows — one row per row group (data is random bytes)
	buf = appendFieldHeader(buf, &pf, 3, thriftI64)
	buf = appendZigI64(buf, int64(len(groups)))

	// Field 4: row_groups (list<RowGroup>)
	buf = appendFieldHeader(buf, &pf, 4, thriftList)
	buf = appendListHeader(buf, len(groups), thriftStruct)
	for _, rg := range groups {
		buf = encodeRowGroup(buf, rg, rgSize)
	}

	buf = append(buf, thriftStop) // stop FileMetaData
	return buf
}

// encodeRowGroup appends one RowGroup struct.
func encodeRowGroup(buf []byte, rg RowGroupInfo, rgSize int64) []byte {
	pf := 0

	// Field 1: columns (list<ColumnChunk> — one column per row group)
	buf = appendFieldHeader(buf, &pf, 1, thriftList)
	buf = appendListHeader(buf, 1, thriftStruct)
	buf = encodeColumnChunk(buf, rg.Offset, rgSize)

	// Field 2: total_byte_size
	buf = appendFieldHeader(buf, &pf, 2, thriftI64)
	buf = appendZigI64(buf, rgSize)

	// Field 3: num_rows = 1
	buf = appendFieldHeader(buf, &pf, 3, thriftI64)
	buf = appendZigI64(buf, 1)

	// Field 5: file_offset (delta 2 from field 3)
	buf = appendFieldHeader(buf, &pf, 5, thriftI64)
	buf = appendZigI64(buf, rg.Offset)

	buf = append(buf, thriftStop) // stop RowGroup
	return buf
}

// encodeColumnChunk appends one ColumnChunk struct with an embedded ColumnMetaData.
func encodeColumnChunk(buf []byte, offset, size int64) []byte {
	pf := 0

	// Field 2: file_offset (delta 2 from 0)
	buf = appendFieldHeader(buf, &pf, 2, thriftI64)
	buf = appendZigI64(buf, offset)

	// Field 3: meta_data (ColumnMetaData struct)
	buf = appendFieldHeader(buf, &pf, 3, thriftStruct)
	buf = encodeColumnMetaData(buf, offset, size)

	buf = append(buf, thriftStop) // stop ColumnChunk
	return buf
}

// encodeColumnMetaData appends a ColumnMetaData struct.
// Fields written (in order): type, encodings, path_in_schema, codec,
// num_values, total_uncompressed_size, total_compressed_size, data_page_offset.
func encodeColumnMetaData(buf []byte, offset, size int64) []byte {
	pf := 0

	// Field 1: type = BYTE_ARRAY (6)
	buf = appendFieldHeader(buf, &pf, 1, thriftI32)
	buf = appendZigI32(buf, parquetByteArray)

	// Field 2: encodings = [PLAIN]
	buf = appendFieldHeader(buf, &pf, 2, thriftList)
	buf = appendListHeader(buf, 1, thriftI32)
	buf = appendZigI32(buf, parquetPlain)

	// Field 3: path_in_schema = ["embeddings"]
	buf = appendFieldHeader(buf, &pf, 3, thriftList)
	buf = appendListHeader(buf, 1, thriftBinary)
	buf = appendString(buf, "embeddings")

	// Field 4: codec = UNCOMPRESSED (0)
	buf = appendFieldHeader(buf, &pf, 4, thriftI32)
	buf = appendZigI32(buf, parquetUncompressed)

	// Field 5: num_values = 1 (one row in this row group)
	buf = appendFieldHeader(buf, &pf, 5, thriftI64)
	buf = appendZigI64(buf, 1)

	// Field 6: total_uncompressed_size
	buf = appendFieldHeader(buf, &pf, 6, thriftI64)
	buf = appendZigI64(buf, size)

	// Field 7: total_compressed_size
	buf = appendFieldHeader(buf, &pf, 7, thriftI64)
	buf = appendZigI64(buf, size)

	// Field 9: data_page_offset (delta 2 from field 7)
	buf = appendFieldHeader(buf, &pf, 9, thriftI64)
	buf = appendZigI64(buf, offset)

	buf = append(buf, thriftStop) // stop ColumnMetaData
	return buf
}

// ── BuildParquetFooter ──────────────────────────────────────────────────────

// BuildParquetFooter returns the footer tail bytes and the row group layout for
// an object of objectSize bytes with rgCount row groups of rgSize bytes each.
//
// Row groups are packed consecutively after the leading PAR1 magic:
//
//	offset 4 + i*rgSize  →  row group i  (i = 0 … rgCount-1)
//
// The caller writes the complete object as:
//
//	[PAR1 4B][random fill: objectSize - 4 - len(footerTail)][footerTail]
//
// where footerTail = [Thrift FileMetaData][4-byte LE metaLen][PAR1 4B].
// The final byte of footerTail is at byte offset objectSize-1.
func BuildParquetFooter(objectSize int64, rgCount int, rgSize int64) (footerTail []byte, groups []RowGroupInfo, err error) {
	if rgCount <= 0 {
		rgCount = 1
	}
	if rgSize <= 0 {
		rgSize = 65536 // 64 KiB default
	}

	// Place row groups consecutively after the leading PAR1 magic.
	groups = make([]RowGroupInfo, rgCount)
	for i := range groups {
		groups[i] = RowGroupInfo{
			Offset: 4 + int64(i)*rgSize,
			Size:   rgSize,
		}
	}
	lastRGEnd := groups[rgCount-1].Offset + rgSize

	// Encode FileMetaData with the real row-group offsets.
	meta := encodeFileMetaData(groups, rgSize)

	// Validate: row-group region + metadata + 8-byte trailer must fit.
	// The +1 ensures at least one byte of random padding between the row-group
	// data region and the metadata blob.
	needed := lastRGEnd + int64(len(meta)) + 8 + 1
	if needed > objectSize {
		return nil, nil, fmt.Errorf(
			"object too small: need %d bytes for %d×%d-byte row groups + %d-byte metadata, have %d",
			needed, rgCount, rgSize, len(meta), objectSize)
	}

	// Build footerTail: [meta][4-byte LE metaLen][PAR1]
	tail := make([]byte, 0, len(meta)+8)
	tail = append(tail, meta...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(meta)))
	tail = append(tail, lenBuf[:]...)
	tail = append(tail, parquetMagic[:]...)

	return tail, groups, nil
}

// ── Thrift CompactProtocol decoder helpers ──────────────────────────────────

// readVarint reads a little-endian base-128 varint from data[pos:].
// Returns (value, newPos, ok); ok is false on truncation or overflow.
func readVarint(data []byte, pos int) (uint64, int, bool) {
	var v uint64
	var shift uint
	for pos < len(data) {
		b := data[pos]
		pos++
		v |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return v, pos, true
		}
		shift += 7
		if shift >= 64 {
			return 0, pos, false // overflow
		}
	}
	return 0, pos, false // truncated
}

// zigzagI64 decodes a ZigZag-encoded uint64 to int64.
func zigzagI64(v uint64) int64 { return int64((v >> 1) ^ -(v & 1)) }

// zigzagI32 decodes a ZigZag-encoded uint64 to int32.
func zigzagI32(v uint64) int32 { return int32((v >> 1) ^ -(v & 1)) }

// readFieldHeader reads one Thrift CompactProtocol field header from data[pos:].
// Returns (fieldID, typeTag, newPos, isStop, ok).
// isStop is true when the stop byte (0x00) is read; typeTag and fieldID are zero.
// ok is false when data is truncated.
func readFieldHeader(data []byte, pos, prevFieldID int) (fieldID int, typ byte, newPos int, isStop bool, ok bool) {
	if pos >= len(data) {
		return 0, 0, pos, false, false
	}
	b := data[pos]
	pos++
	if b == 0x00 {
		return 0, 0, pos, true, true
	}
	typ = b & 0x0F
	delta := int(b >> 4)
	if delta != 0 {
		// Short form: 4-bit non-zero delta.
		return prevFieldID + delta, typ, pos, false, true
	}
	// Long form: zigzag-encoded field ID follows.
	v, newPos, ok2 := readVarint(data, pos)
	if !ok2 {
		return 0, 0, newPos, false, false
	}
	return int(zigzagI32(v)), typ, newPos, false, true
}

// skipValue advances pos past one Thrift CompactProtocol value of the given type.
func skipValue(data []byte, pos int, typ byte) (int, error) {
	switch typ {
	case thriftBooleanTrue, thriftBooleanFalse:
		return pos, nil
	case thriftByte:
		if pos >= len(data) {
			return pos, fmt.Errorf("truncated byte value at pos %d", pos)
		}
		return pos + 1, nil
	case thriftI16, thriftI32, thriftI64:
		_, newPos, ok := readVarint(data, pos)
		if !ok {
			return newPos, fmt.Errorf("truncated varint at pos %d", pos)
		}
		return newPos, nil
	case thriftDouble:
		if pos+8 > len(data) {
			return pos, fmt.Errorf("truncated double at pos %d", pos)
		}
		return pos + 8, nil
	case thriftBinary:
		v, newPos, ok := readVarint(data, pos)
		if !ok {
			return newPos, fmt.Errorf("truncated string length at pos %d", pos)
		}
		end := newPos + int(v)
		if end > len(data) {
			return newPos, fmt.Errorf("truncated string body at pos %d (need %d bytes)", pos, v)
		}
		return end, nil
	case thriftList, thriftSet:
		return skipListOrSet(data, pos)
	case thriftMap:
		return skipMap(data, pos)
	case thriftStruct:
		return skipStruct(data, pos)
	default:
		return pos, fmt.Errorf("unknown Thrift type 0x%02x at pos %d", typ, pos)
	}
}

// skipStruct skips all fields of a struct until the stop byte.
func skipStruct(data []byte, pos int) (int, error) {
	prevField := 0
	for {
		fieldID, typ, newPos, stop, ok := readFieldHeader(data, pos, prevField)
		if !ok {
			return newPos, fmt.Errorf("truncated struct field header at pos %d", pos)
		}
		pos = newPos
		if stop {
			return pos, nil
		}
		prevField = fieldID
		var err error
		pos, err = skipValue(data, pos, typ)
		if err != nil {
			return pos, fmt.Errorf("field %d: %w", fieldID, err)
		}
	}
}

// skipListOrSet skips a Thrift list or set.
func skipListOrSet(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return pos, fmt.Errorf("truncated list header at pos %d", pos)
	}
	b := data[pos]
	pos++
	elemType := b & 0x0F
	var size int
	if b>>4 == 0x0F {
		v, newPos, ok := readVarint(data, pos)
		if !ok {
			return newPos, fmt.Errorf("truncated list size varint at pos %d", pos)
		}
		pos = newPos
		size = int(v)
	} else {
		size = int(b >> 4)
	}
	for i := 0; i < size; i++ {
		var err error
		pos, err = skipValue(data, pos, elemType)
		if err != nil {
			return pos, fmt.Errorf("list element %d: %w", i, err)
		}
	}
	return pos, nil
}

// skipMap skips a Thrift map.
func skipMap(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return pos, fmt.Errorf("truncated map header at pos %d", pos)
	}
	v, newPos, ok := readVarint(data, pos)
	if !ok {
		return newPos, fmt.Errorf("truncated map count varint at pos %d", pos)
	}
	pos = newPos
	count := int(v)
	if count == 0 {
		return pos, nil
	}
	if pos >= len(data) {
		return pos, fmt.Errorf("truncated map type byte at pos %d", pos)
	}
	types := data[pos]
	pos++
	keyType := (types >> 4) & 0x0F
	valType := types & 0x0F
	for i := 0; i < count; i++ {
		var err error
		if pos, err = skipValue(data, pos, keyType); err != nil {
			return pos, fmt.Errorf("map key %d: %w", i, err)
		}
		if pos, err = skipValue(data, pos, valType); err != nil {
			return pos, fmt.Errorf("map val %d: %w", i, err)
		}
	}
	return pos, nil
}

// ── Parquet FileMetaData decoder ────────────────────────────────────────────

// decodeFileMetaData parses a Thrift CompactProtocol FileMetaData blob and
// returns a RowGroupInfo for each row group.  Only data_page_offset and
// total_compressed_size are extracted; all other fields are skipped.
func decodeFileMetaData(data []byte) ([]RowGroupInfo, error) {
	pos := 0
	prevField := 0
	var groups []RowGroupInfo

	for {
		fieldID, typ, newPos, stop, ok := readFieldHeader(data, pos, prevField)
		if !ok {
			return nil, fmt.Errorf("truncated FileMetaData field header at pos %d", pos)
		}
		pos = newPos
		if stop {
			break
		}
		prevField = fieldID

		switch fieldID {
		case 4: // row_groups: list<RowGroup>
			if typ != thriftList {
				return nil, fmt.Errorf("FileMetaData field 4: expected list (0x09), got 0x%02x", typ)
			}
			var err error
			groups, pos, err = decodeRowGroupList(data, pos)
			if err != nil {
				return nil, fmt.Errorf("row_groups: %w", err)
			}
		default:
			var err error
			pos, err = skipValue(data, pos, typ)
			if err != nil {
				return nil, fmt.Errorf("skip FileMetaData field %d: %w", fieldID, err)
			}
		}
	}
	return groups, nil
}

// decodeRowGroupList reads the list header and decodes each RowGroup struct.
func decodeRowGroupList(data []byte, pos int) ([]RowGroupInfo, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("truncated row_groups list header at pos %d", pos)
	}
	b := data[pos]
	pos++
	elemType := b & 0x0F
	if elemType != thriftStruct {
		return nil, pos, fmt.Errorf("row_groups list elem type 0x%02x, expected struct (0x0c)", elemType)
	}
	var size int
	if b>>4 == 0x0F {
		v, newPos, ok := readVarint(data, pos)
		if !ok {
			return nil, newPos, fmt.Errorf("truncated row_groups list size varint at pos %d", pos)
		}
		pos = newPos
		size = int(v)
	} else {
		size = int(b >> 4)
	}

	groups := make([]RowGroupInfo, 0, size)
	for i := 0; i < size; i++ {
		rg, newPos, err := decodeRowGroup(data, pos)
		if err != nil {
			return nil, newPos, fmt.Errorf("row_group[%d]: %w", i, err)
		}
		pos = newPos
		groups = append(groups, rg)
	}
	return groups, pos, nil
}

// decodeRowGroup parses one RowGroup struct.  The RowGroupInfo is populated
// from the first ColumnChunk's ColumnMetaData.
func decodeRowGroup(data []byte, pos int) (RowGroupInfo, int, error) {
	var rg RowGroupInfo
	prevField := 0
	for {
		fieldID, typ, newPos, stop, ok := readFieldHeader(data, pos, prevField)
		if !ok {
			return rg, newPos, fmt.Errorf("truncated RowGroup field header at pos %d", pos)
		}
		pos = newPos
		if stop {
			break
		}
		prevField = fieldID

		switch fieldID {
		case 1: // columns: list<ColumnChunk>
			if typ != thriftList {
				return rg, pos, fmt.Errorf("RowGroup.columns expected list, got 0x%02x", typ)
			}
			var err error
			rg, pos, err = decodeColumnChunkList(data, pos)
			if err != nil {
				return rg, pos, fmt.Errorf("columns: %w", err)
			}
		default:
			var err error
			pos, err = skipValue(data, pos, typ)
			if err != nil {
				return rg, pos, fmt.Errorf("skip RowGroup field %d: %w", fieldID, err)
			}
		}
	}
	return rg, pos, nil
}

// decodeColumnChunkList reads the list header and decodes each ColumnChunk.
// Returns the RowGroupInfo from the first column chunk (we write exactly one).
func decodeColumnChunkList(data []byte, pos int) (RowGroupInfo, int, error) {
	var rg RowGroupInfo
	if pos >= len(data) {
		return rg, pos, fmt.Errorf("truncated columns list header at pos %d", pos)
	}
	b := data[pos]
	pos++
	elemType := b & 0x0F
	if elemType != thriftStruct {
		return rg, pos, fmt.Errorf("columns list elem type 0x%02x, expected struct (0x0c)", elemType)
	}
	var size int
	if b>>4 == 0x0F {
		v, newPos, ok := readVarint(data, pos)
		if !ok {
			return rg, newPos, fmt.Errorf("truncated columns list size varint at pos %d", pos)
		}
		pos = newPos
		size = int(v)
	} else {
		size = int(b >> 4)
	}

	for i := 0; i < size; i++ {
		chunk, newPos, err := decodeColumnChunk(data, pos)
		if err != nil {
			return rg, newPos, fmt.Errorf("column_chunk[%d]: %w", i, err)
		}
		pos = newPos
		if i == 0 {
			rg = chunk // only the first column chunk contributes to RowGroupInfo
		}
	}
	return rg, pos, nil
}

// decodeColumnChunk parses one ColumnChunk struct.  Returns a RowGroupInfo
// populated from the embedded ColumnMetaData (data_page_offset + total_compressed_size).
func decodeColumnChunk(data []byte, pos int) (RowGroupInfo, int, error) {
	var rg RowGroupInfo
	prevField := 0
	for {
		fieldID, typ, newPos, stop, ok := readFieldHeader(data, pos, prevField)
		if !ok {
			return rg, newPos, fmt.Errorf("truncated ColumnChunk field header at pos %d", pos)
		}
		pos = newPos
		if stop {
			break
		}
		prevField = fieldID

		switch fieldID {
		case 3: // meta_data: ColumnMetaData struct
			if typ != thriftStruct {
				return rg, pos, fmt.Errorf("ColumnChunk.meta_data expected struct, got 0x%02x", typ)
			}
			var err error
			rg, pos, err = decodeColumnMetaData(data, pos)
			if err != nil {
				return rg, pos, fmt.Errorf("meta_data: %w", err)
			}
		default:
			var err error
			pos, err = skipValue(data, pos, typ)
			if err != nil {
				return rg, pos, fmt.Errorf("skip ColumnChunk field %d: %w", fieldID, err)
			}
		}
	}
	return rg, pos, nil
}

// decodeColumnMetaData parses a ColumnMetaData struct and extracts
// data_page_offset (field 9) and total_compressed_size (field 7).
func decodeColumnMetaData(data []byte, pos int) (RowGroupInfo, int, error) {
	var rg RowGroupInfo
	prevField := 0
	for {
		fieldID, typ, newPos, stop, ok := readFieldHeader(data, pos, prevField)
		if !ok {
			return rg, newPos, fmt.Errorf("truncated ColumnMetaData field header at pos %d", pos)
		}
		pos = newPos
		if stop {
			break
		}
		prevField = fieldID

		switch fieldID {
		case 7: // total_compressed_size: i64
			v, newPos2, ok2 := readVarint(data, pos)
			if !ok2 {
				return rg, newPos2, fmt.Errorf("truncated total_compressed_size varint at pos %d", pos)
			}
			pos = newPos2
			rg.Size = zigzagI64(v)
		case 9: // data_page_offset: i64
			v, newPos2, ok2 := readVarint(data, pos)
			if !ok2 {
				return rg, newPos2, fmt.Errorf("truncated data_page_offset varint at pos %d", pos)
			}
			pos = newPos2
			rg.Offset = zigzagI64(v)
		default:
			var err error
			pos, err = skipValue(data, pos, typ)
			if err != nil {
				return rg, pos, fmt.Errorf("skip ColumnMetaData field %d: %w", fieldID, err)
			}
		}
	}
	return rg, pos, nil
}

// ── ParseParquetFooter ──────────────────────────────────────────────────────

// ErrNotParquet is returned by ParseParquetFooter when the footer bytes do not
// end with the PAR1 magic number — indicating the object was not written with
// write-format: parquet.
var ErrNotParquet = fmt.Errorf("missing PAR1 magic: not a Parquet object")

// ParseParquetFooter extracts the RowGroupInfo slice from a byte slice that
// spans the tail of a Parquet object.
//
// footerBytes must contain at least the last 8 bytes of the object (4-byte LE
// metadata length + trailing PAR1 magic).  In practice the caller reads
// read-footer-size bytes from the end of the object; as long as that value
// exceeds the metadata size by ≥ 8 bytes the parse will succeed.
//
// objectSize is used only in error messages.
func ParseParquetFooter(footerBytes []byte, objectSize int64) ([]RowGroupInfo, error) {
	n := len(footerBytes)
	if n < 8 {
		return nil, fmt.Errorf("footer buffer too short (%d bytes, need ≥ 8)", n)
	}

	// Validate trailing PAR1 magic.
	if footerBytes[n-4] != 'P' || footerBytes[n-3] != 'A' ||
		footerBytes[n-2] != 'R' || footerBytes[n-1] != '1' {
		return nil, ErrNotParquet
	}

	// 4-byte LE uint32 metadata length immediately before the trailing PAR1.
	metaLen := int(binary.LittleEndian.Uint32(footerBytes[n-8 : n-4]))
	if metaLen <= 0 {
		return nil, fmt.Errorf("invalid metadata length %d (object size %d)", metaLen, objectSize)
	}
	if metaLen > n-8 {
		return nil, fmt.Errorf(
			"metadata length %d exceeds available footer buffer (%d bytes before length field); "+
				"increase read-footer-size to at least %d bytes",
			metaLen, n-8, metaLen+8)
	}

	// Thrift blob occupies footerBytes[n-8-metaLen : n-8].
	thriftStart := n - 8 - metaLen
	return decodeFileMetaData(footerBytes[thriftStart : n-8])
}
