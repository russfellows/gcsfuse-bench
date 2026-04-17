# Write Performance Investigation — RAPID/Zonal Storage

> **Note**: This document supersedes the earlier draft analysis (below the separator). The earlier
> analysis incorrectly assumed HTTP/2 JSON resumable upload. The user confirmed they are testing
> **Zonal (RAPID) storage with append-only writes**, which uses a completely different code path.

---

## Object Sizes Being Tested

- Range: 64 KB – 1 MB
- Distribution: lognormal, mean ~226 KB, **median ~128 KB**
- All objects are well below the default 16 MiB chunk size

---

## Actual Code Path for RAPID/Zonal Writes

The write chain is:

```
doWrite (engine.go)
  → e.bucket.CreateObject(ctx, req)           // always, no special-casing
    → bucket_handle.go: obj.NewWriter(ctx)
        wc.Append = bh.BucketType().Zonal      // true for RAPID
        wc.FinalizeOnClose = true              // finalize on Close
        wc.ChunkSize = <not set> → 16 MiB default
      → storage.Writer.openWriter()
        → grpcStorageClient.OpenWriter()       // Append=true FORCES gRPC path
          → BidiWriteObject (gRPC bidi-stream) per object
```

**The HTTP/2 JSON API and resumable upload protocol are not used for RAPID storage.**
When `wc.Append = true`, the storage library enforces the gRPC path. The `DisableKeepAlives: true`
HTTP/2 transport setting is **completely irrelevant** for RAPID writes.

---

## gRPC Writer Internals (What Actually Happens for 128 KB Objects)

### Chunk and message sizing

Constants (from `grpc_writer.go` + protobuf):

| Constant | Value | Source |
|---|---|---|
| `ChunkSize` (default) | **16 MiB** | `googleapi.DefaultUploadChunkSize` — never overridden in `bucket_handle.go` |
| `MAX_WRITE_CHUNK_BYTES` | **2 MiB** | `ServiceConstants_MAX_WRITE_CHUNK_BYTES` in `storage.pb.go` |
| `MinUploadChunkSize` | **256 KiB** | `googleapi.MinUploadChunkSize` — floor for `gRPCChunkSize()` |

From `OpenWriter`:
```go
chunkSize := gRPCChunkSize(params.chunkSize)   // gRPCChunkSize(16 MiB) = 16 MiB
writeQuantum := maxPerMessageWriteSize          // = 2 MiB
if writeQuantum > chunkSize {
    writeQuantum = chunkSize
}
// → writeQuantum = min(2 MiB, 16 MiB) = 2 MiB
forceOneShot := (params.chunkSize <= 0)         // = false (16 MiB > 0)
```

### Per-object write execution for a 128 KB object

1. `OpenWriter` spawns **a background goroutine** per write (via `go w.gatherFirstBuffer()`).
2. `gatherFirstBuffer()` reads from `writesChan`. Since `forceOneShot=false` and
   `128 KB < 16 MiB (chunkSize)`, it takes the buffered path:
   ```go
   w.buf = make([]byte, 0, w.chunkSize)  // ← 16 MiB allocation for 128 KB of data
   copy(w.buf[origLen:], v.p)            // copies 128 KB into the 16 MiB buffer
   ```
3. `Close()` is called → `w.forceOneShot = true` is set → proceeds to `writeLoop`.
4. `writeLoop` opens a `BidiWriteObject` bidi-stream and sends:
   - **1 gRPC message** (128 KB < 2 MiB writeQuantum) with `finish_write=true` + `finalize=true`
5. Server persists the object durably and responds.
6. Total round trips: **effectively 1 RTT** (stream HEADERS + DATA can be pipelined in one HTTP/2 round trip).

### gRPC connection pool

`GrpcConnPoolSize` is **never set** in `cmd/benchmark.go` → defaults to 0.

In `transport/grpc/dial.go`:
```go
if poolSize == 0 || poolSize == 1 {
    // Fast path for common case for a connection pool with a single connection.
    conn, err := dial(ctx, false, o)
    return &singleConnPool{conn}, nil
}
```

**Result: 1 gRPC connection (HTTP/2 channel) shared across ALL goroutines.**

All concurrent `BidiWriteObject` streams are multiplexed over this single connection.

---

## Root Causes of Write Latency/Throughput Being Worse Than Reads

### Issue 1: 16 MiB Buffer Allocated Per Write — Even for 128 KB Objects (PRIMARY)

The gRPC writer allocates `make([]byte, 0, w.chunkSize)` = **16 MiB** for every single object,
regardless of object size. The Google storage library documentation even warns about this:

> *"If you upload small objects (< 16MiB), you should set ChunkSize to a value slightly larger than
> your typical object size to avoid the overhead of buffering."*

With N concurrent goroutines: **N × 16 MiB** of live heap from write buffers alone.

- 16 goroutines: 256 MiB
- 32 goroutines: 512 MiB  
- 64 goroutines: **1 GiB**

This causes heavy GC pressure. Every goroutine stall during GC directly adds to write latency.
Reads have no equivalent allocation — the read path receives data into a pre-sized output buffer.

**Fix**: Set `wc.ChunkSize = 0` in `bucket_handle.go`'s `CreateObject` for the zonal path (or
unconditionally). With `ChunkSize = 0`:
- `forceOneShot = true` immediately (no buffer allocated)
- `gatherFirstBuffer()` returns on first write command without allocating a 16 MiB buffer
- Object data passes through without extra copy

### Issue 2: Single gRPC Connection for All Concurrent Streams

`GrpcConnPoolSize = 0` → 1 gRPC connection shared by all goroutines.

Under high write concurrency (e.g., 32–64 goroutines), all BidiWriteObject streams share a single
HTTP/2 connection. Each 128 KB write is small, but the TCP send buffer becomes the bottleneck:

- HTTP/2 connection-level flow control window limits total in-flight bytes
- Head-of-line blocking: a slow ACK from one stream can delay window updates for others
- Single TCP connection's bandwidth-delay product (BDP) caps aggregate throughput regardless of
  how many goroutines are writing

Reads with bidi-streams (`BidiReadObject`) face the same constraint, but the server sends data to
the client — the server-side sending is pipelined efficiently. For writes, the **client** must push
data to the server, which is bottlenecked by this single connection's congestion window.

**Fix**: Set `GrpcConnPoolSize = 4` (or `runtime.NumCPU()`) when creating the storage handle for
RAPID mode in `cmd/benchmark.go`.

### Issue 3: Server-Side Write Durability (Fundamental Lower Bound)

RAPID/Zonal is designed for **durable, append-committed writes**. Before the server ACKs a write,
it must persist the data to at least one replica. For 128 KB objects, this server-side commit
latency is the dominant factor — typically 1–5 ms for RAPID storage.

Reads, by contrast, can be served from in-memory cache or read ahead buffers with sub-ms latency.
**No client-side optimization can eliminate this asymmetry.** It's the physical floor.

### Issue 4: Per-Write Goroutine + Channel Overhead (Minor)

Each `CreateObject` call spawns a background goroutine and communicates via `writesChan`
(buffered channel, capacity 1). For small objects:

- The caller's `io.Copy(wc, bytes.NewReader(data))` sends one Write command through the channel
- The Close() sends a Close command through the channel
- The goroutine context-switches twice (Write recv, Close recv)

At high concurrency, goroutine scheduling overhead adds microseconds of latency per write. For
128 KB objects where the total write takes only a few milliseconds, this fraction is noticeable.

### Issue 5: `BidiWriteObject` vs `BidiReadObject` Per-Request Metadata Overhead (Minor)

Writes send more metadata than reads:
- `WriteObjectSpec` (object name, content type, checksums, `Appendable: true`)
- CRC32C checksum computation over the data payload
- `finish_write=true` + `finalize=true` flags

Reads just send `{bucket, object, read_offset, read_length}`. The metadata overhead is a few KB
at most, but at 128 KB object sizes it represents a non-trivial percentage of total payload.

---

## What the Earlier Draft Analysis Got Wrong

The prior analysis in this file (below) was written before confirming the RAPID/Zonal code path.
Key errors:

| Claim | Status | Reality |
|---|---|---|
| Uses HTTP/2 JSON API + resumable upload | ❌ WRONG | RAPID uses gRPC `BidiWriteObject` |
| 2–3 HTTP round trips per object | ❌ WRONG | ~1 gRPC RTT per object |
| `DisableKeepAlives: true` causes new TCP per request | ❌ WRONG | Irrelevant for gRPC path |
| Assumes `io.Pipe` (unseekable) | ❌ WRONG | gRPC path doesn't use `io.Pipe`; HTTP path uses `bytes.NewReader` |
| 16 MiB buffer per writer | ✅ CORRECT | Applies to gRPC path too |
| Single writer per goroutine | ✅ CORRECT | |

---

## Summary: Ordered by Impact

1. **16 MiB buffer per write** (GC pressure) — fix by setting `wc.ChunkSize = 0` for zonal writes
2. **Single gRPC connection** under high concurrency — fix by setting `GrpcConnPoolSize > 1`
3. **Server-side write durability** — fundamental, cannot be optimized away
4. **Goroutine spawn + channel overhead per write** — minor, inherent to the library design
5. **BidiWriteObject metadata overhead** — minor

---

## Original Draft Analysis (Preserved for Reference — Contains Errors for RAPID Path)

> The following was the original analysis before RAPID/Zonal was confirmed. It describes the
> **HTTP/2 JSON API resumable upload path**, which applies when `rapid-mode = off`.
> It does NOT apply to RAPID/Zonal storage.

---

Now I have everything I need to give a thorough investigation report. Let me compile the findings:

Which writer is used: CreateObject → bucket_handle.go → obj.NewWriter(ctx) → uses HTTP/2 JSON API with resumable upload via googleapi.ChunkSize(16 * 1024 * 1024) (default 16 MiB). No override is set in doWrite.

The core problem: For 256 KiB objects, the default 16 MiB ChunkSize means every write goes through the resumable upload protocol, which requires:

Round trip 1: POST to initiate the resumable upload (returns upload URI)
Round trip 2: PUT with the object data to the upload URI
Round trip 3 (for GCS): PUT to finalize
That's 2-3 HTTP round trips per 256 KiB object instead of 1. For reads, it's 1 round trip per object. This explains why latency is higher and throughput is lower for writes vs reads. If ChunkSize were set to 0 or higher than the object size, GCS would use a single-request multipart upload instead, cutting the overhead dramatically.

The HTTP/2 transport has DisableKeepAlives: true, which forces a new TCP connection for every write operation. With MaxConnsPerHost: 0 allowing unlimited connections, each resumable upload's initiation and data phases likely use separate connections, compounding the latency penalty compared to reads where multiplexing might help reuse connections more efficiently. I'm realizing that DisableKeepAlives: true affects both reads and writes, but the distinction matters for HTTP/2 — it might close connections after each request rather than being a no-op like I initially thought, so I need to reconsider how this actually impacts performance between the two operations.

The real bottleneck is clearer now: 256 KiB writes trigger resumable uploads with two HTTP round trips (initiation plus data transfer), while reads only need one round trip, which explains why writes are consistently slower. There's also just a single GCS writer object managing the connections. ) per goroutine per write. The benchmark creates N goroutines (concurrency setting), each calling doWrite → CreateObject → NewWriter. There is no pipelining of multiple write streams per goroutine. So yes, it's effectively 1 writer per goroutine at a time.

Could it be "more"? In the HTTP/2 model, you could pipeline multiple requests over the same connection, but the current implementation doesn't do that. The benchmark's concurrency controls the number of parallel independent writes.

Let me now summarize all findings clearly.

Root Cause Summary:

Wrong upload protocol for 256 KiB objects — The biggest issue. CreateObject → bucket_handle.go → obj.NewWriter(ctx) with ChunkSize left at the library default of 16 MiB. For objects smaller than ChunkSize, the library uses the resumable upload protocol (requires an initiation round trip before data can be sent) instead of the faster single-request multipart upload. Google's own documentation warns against this for small objects but doesn't emphasize that the protocol choice itself is affected. Looking at the library source, objects smaller than the configured chunk size get sent in a single request, while larger ones are split across multiple requests—but the key issue is that with the default 16 MiB threshold, my 256 KiB objects are triggering the resumable path unnecessarily.

Concurrency model — The benchmark spawns N goroutines, each performing independent writes sequentially. There's no pipelining of multiple write streams per goroutine, so it's effectively one writer per goroutine at a time.

I'm realizing the protocol choice is the critical bottleneck here. Setting ChunkSize to something just above 256 KiB would force the library to use the faster single-request upload for these small objects, eliminating the unnecessary initiation round trip. request" for objects smaller than ChunkSize. Let me look at what "single request" means here. Is it a simple PUT/POST, or is it still the 2-step resumable upload where the initiation happens first but the single data payload completes it?

Let me look at the googleapi internals more carefully. Actually the key is: when ChunkSize > 0 and the object fits in one chunk, it still uses the resumable upload protocol (sends the initiation POST first, then the PUT with all data). The advantage over ChunkSize=0 is retry capability. When ChunkSize=0, it uses a multipart upload (single POST with metadata + data, no initiation request).

So the distinction is clearer now: with ChunkSize=0, I get a single multipart POST combining metadata and data in one HTTP request, whereas ChunkSize>0 always goes through the resumable protocol with an initiation request followed by the data upload, even for tiny objects that fit in one chunk. So ChunkSize=0 forces a single HTTP request for the upload, while the default 16MiB setting might still use a resumable protocol with an initiation round trip even for small objects. I'm trying to trace through the actual googleapi internals to confirm whether that resumable initiation happens for 256 KiB objects with the current settings, but the key insight is understanding how many HTTP calls are actually being made in each scenario.

The current code doesn't override ChunkSize, so it defaults to 16MiB, which means for 256 KiB objects the data fits in one chunk and gets buffered then flushed in a single request. But that 16MiB buffer is allocated per writer regardless of object size—so with 64 concurrent goroutines, I'm looking at 1 GiB of memory just for write buffers. If I set ChunkSize to something like 512 KiB (just above the object size), I could reduce each writer's buffer from 16MiB down to 512 KiB while still uploading in one request.

Alternatively, ChunkSize=0 would eliminate buffering entirely and use multipart upload, but that sacrifices retry capability. I keep going back and forth on whether the default ChunkSize actually triggers one or two HTTP round trips for small objects. Let me trace through the actual upload implementation to see what's really happening—specifically, I need to check if the resumable upload protocol in the JSON API adds an extra POST before the actual data send.

Looking at the google-api-go-client source, when I use Media() with googleapi.ChunkSize(N) where N is greater than zero, the behavior depends on whether the media body is seekable and its size is known. For seekable streams smaller than the chunk size, it might optimize to a direct upload, but the writer implementation uses io.Pipe(), which isn't seekable, so it always initiates a resumable upload even for small objects like 256 KiB files.

With ChunkSize set to zero, the code skips chunking entirely and uses a single multipart POST request instead, cutting the HTTP round trips down to one. That's the core issue driving the latency.

Beyond that, there's another performance killer: the HTTP/2 transport in gcsfuse has DisableKeepAlives: true, which means TCP connections aren't reused between requests. Each write operation triggers a fresh TCP and TLS handshake just for the initiation POST, adding 10-20ms of overhead before any data even gets sent.

Though actually, I'm realizing DisableKeepAlives might not even matter with HTTP/2 since HTTP/2 inherently uses persistent connections through multiplexing—the setting primarily affects HTTP/1.1 keep-alives, so it could be a no-op in practice.

The real constraint here is that each goroutine makes sequential CreateObject calls with no HTTP/2 request pipelining—there's no way to have multiple in-flight writes from a single goroutine, so the only path to parallelism is increasing the number of goroutines.

Now I'm seeing the critical issue: the benchmark is using obj.NewWriter(ctx) with the default 16 MiB chunk size, but since the content comes from an unseekable pipe with unknown length, the library falls back to resumable uploads that require 2 round trips per object instead of 1. For reads it's just a single GET. The fix would be to set the chunk size to 0 to trigger multipart upload instead.