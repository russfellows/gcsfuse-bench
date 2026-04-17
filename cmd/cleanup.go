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

package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/googlecloudplatform/gcsfuse/v3/cfg"
	"github.com/googlecloudplatform/gcsfuse/v3/common"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/benchmark"
	internalstorage "github.com/googlecloudplatform/gcsfuse/v3/internal/storage"
	storageutil "github.com/googlecloudplatform/gcsfuse/v3/internal/storage/storageutil"
	"github.com/spf13/cobra"
)

// ExecuteCleanupCmd is the standalone entry point for the gcs-bench cleanup
// subcommand. It enumerates all objects under a GCS prefix and deletes them
// concurrently, with live progress reporting.
var ExecuteCleanupCmd = func() {
	rootCmd := newCleanupRootCmd()
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("cleanup command failed: %v", err)
	}
}

// newCleanupRootCmd builds the Cobra command for the cleanup subcommand.
func newCleanupRootCmd() *cobra.Command {
	var (
		bucket         string
		objectPrefix   string
		concurrency    int
		dryRun         bool
		keyFile        string
		customEndpoint string
		rapidMode      string
	)

	cmd := &cobra.Command{
		Use:          "gcs-bench cleanup --bucket BUCKET --object-prefix PREFIX [flags]",
		Aliases:      []string{"delete"},
		Short:        "Delete all objects under a GCS prefix",
		Long:         cleanupLongDescription,
		Version:      common.GetVersion(),
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if bucket == "" {
				return fmt.Errorf("--bucket is required")
			}
			if objectPrefix == "" {
				return fmt.Errorf("--object-prefix is required (deleting an entire bucket is not supported via this command)")
			}

			out := io.MultiWriter(os.Stdout, os.Stderr)
			_ = out // use stdout directly; cleanup output is modest

			ctx := context.Background()

			// --- Resolve rapid-mode ---
			effectiveRapidMode := rapidMode
			if effectiveRapidMode == "" {
				effectiveRapidMode = "off" // cleanup doesn't need RAPID detection
			}
			if effectiveRapidMode != "auto" && effectiveRapidMode != "on" && effectiveRapidMode != "off" {
				return fmt.Errorf("invalid --rapid-mode %q: must be auto, on, or off", effectiveRapidMode)
			}

			// --- Create GCS storage client ---
			userAgent := fmt.Sprintf("gcsfuse-bench/%s", common.GetVersion())
			storageClientConfig := storageutil.StorageClientConfig{
				ClientProtocol:  cfg.HTTP2,
				MaxConnsPerHost: 0,
				UserAgent:       userAgent,
				KeyFile:         keyFile,
				CustomEndpoint:  customEndpoint,
			}
			switch effectiveRapidMode {
			case "on":
				storageClientConfig.ForceZonal = true
				storageClientConfig.GrpcConnPoolSize = 4
			case "auto":
				storageClientConfig.EnableHNS = true
				storageClientConfig.GrpcConnPoolSize = 4
			}

			sh, err := internalstorage.NewStorageHandle(ctx, storageClientConfig, "")
			if err != nil {
				return fmt.Errorf("NewStorageHandle: %w", err)
			}

			finalizeForRapid := effectiveRapidMode != "off"
			bh, err := sh.BucketHandle(ctx, bucket, "", finalizeForRapid)
			if err != nil {
				return fmt.Errorf("BucketHandle(%q): %w", bucket, err)
			}

			// --- Run cleanup ---
			opts := benchmark.CleanupOptions{
				Prefix:      objectPrefix,
				Concurrency: concurrency,
				DryRun:      dryRun,
			}
			result, err := benchmark.RunCleanup(ctx, bh, bucket, opts, os.Stdout)
			if err != nil {
				return fmt.Errorf("cleanup: %w", err)
			}

			if !dryRun && result.Errors > 0 {
				fmt.Fprintf(os.Stdout, "\nWarning: %d/%d objects could not be deleted. Re-run to retry.\n",
					result.Errors, result.Listed)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&bucket, "bucket", "", "GCS bucket name (required)")
	cmd.Flags().StringVar(&objectPrefix, "object-prefix", "", "Object prefix to delete (required; e.g. resnet50/ or resnet50/host-0/)")
	cmd.Flags().IntVar(&concurrency, "concurrency", 64, "Number of concurrent DELETE goroutines")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "List objects that would be deleted without actually deleting them")
	cmd.Flags().StringVar(&keyFile, "key-file", "", "Path to service account key JSON (default: ADC)")
	cmd.Flags().StringVar(&customEndpoint, "endpoint", "", "Custom GCS endpoint (for testing proxies)")
	cmd.Flags().StringVar(&rapidMode, "rapid-mode", "off", "RAPID/zonal bucket handling: auto (detect), on (force bidi-gRPC), off (HTTP/2 only, default for cleanup)")

	_ = cmd.MarkFlagRequired("bucket")
	_ = cmd.MarkFlagRequired("object-prefix")

	return cmd
}

const cleanupLongDescription = `gcs-bench cleanup (alias: delete) deletes all objects under a given GCS prefix.

A producer goroutine pages through the GCS object listing and feeds object names
into a bounded channel. A pool of delete workers reads from that channel and
issues concurrent DELETE requests. The channel size is capped at 5,000 names so
peak memory stays constant regardless of how many objects exist — a bucket with
100 billion objects uses the same memory as one with 1,000. Deletes begin as
soon as the first page of names arrives; there is no need to enumerate everything
before deletion starts.

Live progress is printed every 5 seconds showing objects listed, deleted,
deletion rate, queue depth, and error count.

This subcommand is intended for cleaning up objects written by 'gcs-bench bench'
in prepare mode so that the prepare phase can be re-run with fresh data.

Examples:
  # Delete all objects under resnet50/ in my-bucket (64 parallel deletes):
  gcs-bench cleanup --bucket my-bucket --object-prefix resnet50/
  gcs-bench delete  --bucket my-bucket --object-prefix resnet50/

  # Preview what would be deleted without actually deleting:
  gcs-bench cleanup --bucket my-bucket --object-prefix resnet50/ --dry-run

  # Use a specific service-account key and higher concurrency:
  gcs-bench cleanup --bucket my-bucket --object-prefix resnet50/host-0/ \
    --concurrency 128 --key-file /path/to/key.json`
