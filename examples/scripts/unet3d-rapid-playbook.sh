#!/bin/bash
# =============================================================================
# UNet3D RAPID Storage Benchmark Playbook
# =============================================================================
#
# MANUAL STEP-BY-STEP GUIDE — do NOT run this script directly.
# Copy and paste each command block as instructed.
#
# Prerequisites:
#   • gcs-bench binary in ~/  (home directory on each VM)
#   • This tarball extracted on each VM as ~/Tests/
#     giving: ~/Tests/examples/benchmark-configs/unet3d-rapid-*.yaml
#             ~/Tests/examples/scripts/run-unet3d-*.sh
#   • Bucket sig65-rapid1 accessible from all 4 VMs
#
# All commands below assume you have first cd’d into the scripts directory:
#   ssh rfellows@sig65-central-vmN
#   cd ~/Tests/examples/scripts
#
# The scripts are self-locating — they find gcs-bench in $HOME and find
# the yaml configs via their own path (../benchmark-configs/).  You do NOT
# need to copy anything to $HOME or change any paths.
#
# Hosts:
#   vm1 = sig65-central-vm1  → WID=0
#   vm2 = sig65-central-vm2  → WID=1
#   vm3 = sig65-central-vm3  → WID=2
#   vm4 = sig65-central-vm4  → WID=3
# =============================================================================


# =============================================================================
# STEP 0: cd into the scripts directory (do this on EVERY VM before anything)
# =============================================================================
#
# --- ON EACH VM ---
cd ~/Tests/examples/scripts


# =============================================================================
# STEP 1: Single-host prepare (vm1 only)
# =============================================================================
# Writes 100,352 objects (~678 GiB) to unet3d/host-0/.
# Expected time: ~20-25 minutes at 128 goroutines.
# Write throughput and latency will be reported in the results.
#
# --- ON vm1 ONLY ---

EPOCH=$(date -d '+2 minutes' +%s)
echo "Start epoch: $EPOCH"
bash run-unet3d-prepare.sh 0 $EPOCH

# Wait for completion before proceeding to Step 2.


# =============================================================================
# STEP 2: Cleanup — remove objects from Step 1
# =============================================================================
# Deletes unet3d/host-0/ so Step 3 can write a clean coordinated corpus.
#
# --- ON vm1 ---

~/gcs-bench cleanup --bucket sig65-rapid1 --object-prefix unet3d/host-0/ --concurrency 256


# =============================================================================
# STEP 3: Coordinated 4-host prepare (all 4 vms simultaneously)
# =============================================================================
# Each host writes its own 100,352 objects under unet3d/host-N/.
# Total across cluster: 401,408 objects, ~2,712 GiB.
# All 4 hosts start at the same EPOCH — write throughput is measured per host.
#
# --- FIRST: Generate a shared EPOCH (run on vm1, copy the value to all hosts) ---

EPOCH=$(date -d '+5 minutes' +%s)
echo "======================================"
echo "  COPY THIS EPOCH TO ALL 4 VMs: $EPOCH"
echo "======================================"

# --- ON vm1 ---
bash run-unet3d-prepare.sh 0 $EPOCH

# --- ON vm2 (paste the same EPOCH from above) ---
# bash run-unet3d-prepare.sh 1 $EPOCH

# --- ON vm3 ---
# bash run-unet3d-prepare.sh 2 $EPOCH

# --- ON vm4 ---
# bash run-unet3d-prepare.sh 3 $EPOCH

# Wait for ALL 4 hosts to complete before proceeding to read tests.
# Results for each host: ~/results/unet3d-worker-N-prepare/


# =============================================================================
# STEP 4: Single-host GET thread sweep (vm1 only)
# =============================================================================
# Reads from unet3d/host-0/ with increasing concurrency to find peak bandwidth.
# Suggested thread counts: 32, 64, 128, 256
#   32 goroutines  → ~3-4 GiB/s expected (warmup-limited / under-loaded)
#   64 goroutines  → ~7-10 GiB/s expected (near-linear scaling)
#   128 goroutines → ~10-14 GiB/s expected (near saturation)
#   256 goroutines → plateau or saturation point
#
# Wait ~5 minutes between runs (bidi-gRPC state clears between fresh processes).
# Results: ~/results/unet3d-worker-0-read/
#
# --- ON vm1 ONLY ---

# --- 4a: 32 goroutines ---
EPOCH=$(date -d '+2 minutes' +%s)
bash run-unet3d-read.sh 0 $EPOCH 32

# --- 4b: 64 goroutines (config default) ---
EPOCH=$(date -d '+2 minutes' +%s)
bash run-unet3d-read.sh 0 $EPOCH 64

# --- 4c: 128 goroutines ---
EPOCH=$(date -d '+2 minutes' +%s)
bash run-unet3d-read.sh 0 $EPOCH 128

# --- 4d: 256 goroutines ---
EPOCH=$(date -d '+2 minutes' +%s)
bash run-unet3d-read.sh 0 $EPOCH 256


# =============================================================================
# STEP 5: 4-host cross-namespace GET thread sweep (all 4 vms simultaneously)
# =============================================================================
# Each host runs 4 parallel readers (one per prefix unet3d/host-{0,1,2,3}/).
# CONCURRENCY is the TOTAL per host, split evenly across 4 prefixes.
#
#   256T  total (64/prefix)  → resnet50 baseline equivalent, expect clean
#   512T  total (128/prefix) → 2× scaling point, small error uptick expected
#   1024T total (256/prefix) → saturation region, higher errors expected
#
# --- FIRST: Generate EPOCH (run on vm1, copy to all hosts) ---
EPOCH=$(date -d '+5 minutes' +%s)
echo "======================================"
echo "  COPY THIS EPOCH TO ALL 4 VMs: $EPOCH"
echo "======================================"

# Then on each VM run the appropriate WID command below.
# Results: ~/results/unet3d-worker-N-read-all/

# ---- 5a: 256 total goroutines (64 per prefix) ----

# --- ON vm1 ---
bash run-unet3d-read-all.sh 0 $EPOCH 256

# --- ON vm2 ---
# bash run-unet3d-read-all.sh 1 $EPOCH 256

# --- ON vm3 ---
# bash run-unet3d-read-all.sh 2 $EPOCH 256

# --- ON vm4 ---
# bash run-unet3d-read-all.sh 3 $EPOCH 256

# Wait for all hosts to finish, then run 5b.

# ---- 5b: 512 total goroutines (128 per prefix) ----
EPOCH=$(date -d '+5 minutes' +%s)
echo "======================================"
echo "  COPY THIS EPOCH TO ALL 4 VMs: $EPOCH"
echo "======================================"

# --- ON vm1 ---
bash run-unet3d-read-all.sh 0 $EPOCH 512

# --- ON vm2 ---
# bash run-unet3d-read-all.sh 1 $EPOCH 512

# --- ON vm3 ---
# bash run-unet3d-read-all.sh 2 $EPOCH 512

# --- ON vm4 ---
# bash run-unet3d-read-all.sh 3 $EPOCH 512

# Wait for all hosts to finish, then run 5c.

# ---- 5c: 1024 total goroutines (256 per prefix) — saturation test ----
EPOCH=$(date -d '+5 minutes' +%s)
echo "======================================"
echo "  COPY THIS EPOCH TO ALL 4 VMs: $EPOCH"
echo "======================================"

# --- ON vm1 ---
bash run-unet3d-read-all.sh 0 $EPOCH 1024

# --- ON vm2 ---
# bash run-unet3d-read-all.sh 1 $EPOCH 1024

# --- ON vm3 ---
# bash run-unet3d-read-all.sh 2 $EPOCH 1024

# --- ON vm4 ---
# bash run-unet3d-read-all.sh 3 $EPOCH 1024


# =============================================================================
# RESULTS SUMMARY — where to find output files
# =============================================================================
#
# Step 1 (1-host prepare):
#   ~/results/unet3d-worker-0-prepare/bench-*/bench.txt
#
# Step 3 (4-host prepare, per host):
#   ~/results/unet3d-worker-N-prepare/bench-*/bench.txt  (on each vm)
#
# Step 4 (1-host read sweep):
#   ~/results/unet3d-worker-0-read/bench-*/bench.txt     (one dir per run)
#
# Step 5 (4-host read-all sweep):
#   ~/results/unet3d-worker-N-read-all/host-M/bench-*/bench.txt
#   ~/results/unet3d-worker-N-read-all-host-M.log        (interleaved console)
# =============================================================================
