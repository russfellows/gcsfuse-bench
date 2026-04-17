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

package benchmark

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/gcs"
)

// cleanupProgressInterval controls how often the cleanup phase prints a
// live progress line showing objects deleted and deletion rate.
const cleanupProgressInterval = 5 * time.Second

// cleanupListPageSize is the number of objects requested per LIST page.
// GCS returns at most 1000 per page; using the maximum minimises round trips.
const cleanupListPageSize = 1000

// cleanupChannelPages is the number of LIST pages the producer is allowed
// to get ahead of the delete workers. The work channel buffer is:
//
//	cleanupChannelPages × cleanupListPageSize
//
// This caps peak in-flight name memory at ~5000 strings regardless of total
// object count — a 100-billion-object bucket uses no more heap than a 1000-
// object bucket. The producer blocks on a full channel, providing natural
// backpressure when deletes are slower than listing.
const cleanupChannelPages = 5

// CleanupOptions controls the behaviour of RunCleanup.
type CleanupOptions struct {
	// Prefix is the object prefix to enumerate and delete. All objects whose
	// names start with this string are deleted.
	Prefix string

	// Concurrency is the number of concurrent DeleteObject goroutines.
	// Defaults to 64 when zero.
	Concurrency int

	// DryRun prints what would be listed without issuing any DELETE calls.
	DryRun bool
}

// CleanupResult summarises a completed cleanup run.
type CleanupResult struct {
	Listed   int64
	Deleted  int64
	Errors   int64
	Duration time.Duration
}

// RunCleanup deletes every object under opts.Prefix using a producer/consumer
// pipeline: a single lister goroutine pages through LIST responses and feeds
// object names into a bounded channel; a pool of delete workers consumes from
// that channel simultaneously. The channel provides backpressure so the lister
// stays at most cleanupChannelPages pages ahead of the workers — peak memory
// is O(channel buffer), not O(total object count).
//
// Progress is printed to out every 5 seconds. Errors are counted but do not
// abort the run — the caller should inspect CleanupResult.Errors.
func RunCleanup(ctx context.Context, bucket gcs.Bucket, bucketName string, opts CleanupOptions, out io.Writer) (CleanupResult, error) {
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 64
	}

	displayPath := fmt.Sprintf("gs://%s/%s", bucketName, opts.Prefix)
	chanBuf := cleanupChannelPages * cleanupListPageSize

	if opts.DryRun {
		_, _ = fmt.Fprintf(out, "\n[cleanup] DRY RUN — listing objects under %s (no deletes will be issued)\n\n", displayPath)
	} else {
		_, _ = fmt.Fprintf(out, "\n[cleanup] Starting producer/consumer cleanup of %s\n", displayPath)
		_, _ = fmt.Fprintf(out, "[cleanup] delete-workers=%d  list-page=%d  channel-buffer=%d (max ~%d names in memory)\n\n",
			concurrency, cleanupListPageSize, chanBuf, chanBuf)
	}

	// work carries object names from the lister to the delete workers.
	// It is closed by the lister when all pages have been sent.
	work := make(chan string, chanBuf)

	var listed, deleted, delErrs atomic.Int64
	var listErr atomic.Value // stores error from lister goroutine

	start := time.Now()

	// ── Producer: paginated LIST ─────────────────────────────────────────────
	// Runs in its own goroutine. Sends every matching object name into work,
	// then closes work. If it encounters a LIST error it stores the error and
	// closes work so consumers drain and exit cleanly.
	go func() {
		defer close(work)
		token := ""
		for {
			if ctx.Err() != nil {
				return
			}
			listing, err := bucket.ListObjects(ctx, &gcs.ListObjectsRequest{
				Prefix:            opts.Prefix,
				MaxResults:        cleanupListPageSize,
				ContinuationToken: token,
			})
			if err != nil {
				listErr.Store(fmt.Errorf("LIST %s: %w", displayPath, err))
				return
			}
			for _, obj := range listing.MinObjects {
				listed.Add(1)
				select {
				case work <- obj.Name:
				case <-ctx.Done():
					return
				}
			}
			if listing.ContinuationToken == "" {
				return // all pages exhausted
			}
			token = listing.ContinuationToken
		}
	}()

	// ── Progress reporter ────────────────────────────────────────────────────
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go func() {
		ticker := time.NewTicker(cleanupProgressInterval)
		defer ticker.Stop()
		var lastDeleted int64
		lastAt := time.Now()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				d := deleted.Load()
				l := listed.Load()
				e := delErrs.Load()
				elapsed := time.Since(start)
				intervalSecs := now.Sub(lastAt).Seconds()
				rate := float64(d-lastDeleted) / intervalSecs
				lastDeleted = d
				lastAt = now
				if opts.DryRun {
					_, _ = fmt.Fprintf(out, "[cleanup] elapsed=%s  listed=%d  (dry-run, no deletes)\n",
						elapsed.Round(time.Second), l)
				} else {
					// Total is not known until the lister finishes, so show
					// listed and deleted counts side-by-side without a percentage
					// until listing completes.
					_, _ = fmt.Fprintf(out, "[cleanup] elapsed=%s  listed=%d  deleted=%d  %.0f/s  queue=%d  errs=%d\n",
						elapsed.Round(time.Second), l, d, rate, l-d, e)
				}
			case <-progressCtx.Done():
				return
			}
		}
	}()

	// ── Consumers: concurrent delete workers ────────────────────────────────
	// Each worker reads names from work until the channel is closed (meaning
	// the lister has sent every object). Workers do nothing in dry-run mode
	// (names are still consumed so the channel doesn't block the lister).
	var wg sync.WaitGroup
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range work {
				if ctx.Err() != nil {
					return
				}
				if opts.DryRun {
					continue // lister already counted; just drain the channel
				}
				err := bucket.DeleteObject(ctx, &gcs.DeleteObjectRequest{Name: name})
				if err != nil {
					n := delErrs.Add(1)
					if n <= 5 {
						_, _ = fmt.Fprintf(out, "[cleanup] delete error on %q: %v\n", name, err)
					}
				} else {
					deleted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	progressCancel()

	// Check for a lister error (e.g. permission denied mid-listing).
	if v := listErr.Load(); v != nil {
		return CleanupResult{}, v.(error)
	}
	if ctx.Err() != nil {
		return CleanupResult{}, ctx.Err()
	}

	elapsed := time.Since(start)
	l := listed.Load()
	d := deleted.Load()
	e := delErrs.Load()

	if opts.DryRun {
		_, _ = fmt.Fprintf(out, "[cleanup] DRY RUN complete — %d objects would be deleted  elapsed=%s\n",
			l, elapsed.Round(time.Second))
		return CleanupResult{Listed: l, Duration: elapsed}, nil
	}

	_, _ = fmt.Fprintf(out, "[cleanup] COMPLETE — deleted %d/%d objects  elapsed=%s  avg=%.0f/s  errs=%d\n",
		d, l, elapsed.Round(time.Second), float64(d)/elapsed.Seconds(), e)

	return CleanupResult{
		Listed:   l,
		Deleted:  d,
		Errors:   e,
		Duration: elapsed,
	}, nil
}
