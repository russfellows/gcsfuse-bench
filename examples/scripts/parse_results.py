#!/usr/bin/env python3
"""
parse_results.py — Parse GCS benchmark results from VM*-results directories.

Produces three TSV files:
  results_raw.tsv             — one row per bench.yaml (atomic gcs-bench invocation)
  results_read_all_per_vm.tsv — read-all runs aggregated across 4 prefixes per VM
  results_read_all_cluster.tsv— read-all runs aggregated across all VMs (cluster totals)

Usage:
  python3 parse_results.py [RESULTS_ROOT]
  Default RESULTS_ROOT = directory containing this script.
"""

import os
import re
import sys
import yaml
from pathlib import Path
from collections import defaultdict


def parse_duration(s):
    """Parse Go duration string like '4m59.998s' or '1h2m3s' to seconds."""
    total = 0.0
    m = re.search(r'(\d+)h', s)
    if m:
        total += int(m.group(1)) * 3600
    m = re.search(r'(\d+)m', s)
    if m:
        total += int(m.group(1)) * 60
    m = re.search(r'([\d.]+)s', s)
    if m:
        total += float(m.group(1))
    return total


def us_to_ms(us):
    return us / 1000.0 if us else 0.0


def bytes_to_gib(b):
    return b / (1024 ** 3)


def bps_to_gibps(b):
    return b / (1024 ** 3)


def fmt(v):
    """Format a value for TSV: floats to 4dp, everything else as str."""
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v) if v is not None else ""


def parse_bench_yaml(path):
    with open(path) as f:
        data = yaml.safe_load(f)

    duration_s = parse_duration(data.get('measurement_duration', '0s'))
    start_time = data.get('start_time', '')

    track = data['tracks'][0]
    runtime = data.get('runtime', {})
    pipeline = data.get('pipeline', {})

    lat = track.get('totallatency', {})
    ttfb = track.get('ttfb', {})

    # Handle older bench.yaml format that omits totalbytes and retries
    total_ops = track.get('totalops', 0)
    avg_obj_bytes = track.get('avgopsizebytes', 0)
    total_bytes = track.get('totalbytes') or (total_ops * avg_obj_bytes)

    return {
        'start_time': start_time,
        'duration_s': round(duration_s, 2),
        'trackname': track.get('trackname', ''),
        'op_type': track.get('op_type', ''),
        'goroutines': track.get('goroutines', 0),
        'total_ops': total_ops,
        'errors': track.get('errors', 0),
        'retries': track.get('retries', 0),
        'total_bytes_gib': bytes_to_gib(total_bytes),
        'throughput_gibps': bps_to_gibps(track.get('throughputbytespersec', 0)),
        'ops_per_sec': track.get('opspersec', 0),
        'avg_obj_size_mib': track.get('avgopsizebytes', 0) / (1024 ** 2),
        'lat_p50_ms': us_to_ms(lat.get('p50_us', 0)),
        'lat_p90_ms': us_to_ms(lat.get('p90_us', 0)),
        'lat_p95_ms': us_to_ms(lat.get('p95_us', 0)),
        'lat_p99_ms': us_to_ms(lat.get('p99_us', 0)),
        'lat_p999_ms': us_to_ms(lat.get('p999_us', 0)),
        'lat_mean_ms': us_to_ms(lat.get('mean_us', 0)),
        # TTFB is only meaningful for reads (writes report 0)
        'ttfb_p50_ms': us_to_ms(ttfb.get('p50_us', 0)),
        'ttfb_p90_ms': us_to_ms(ttfb.get('p90_us', 0)),
        'ttfb_p95_ms': us_to_ms(ttfb.get('p95_us', 0)),
        'ttfb_p99_ms': us_to_ms(ttfb.get('p99_us', 0)),
        'ttfb_mean_ms': us_to_ms(ttfb.get('mean_us', 0)),
        'cpu_process_pct': round(
            runtime.get('process_user_cpu_pct', 0) + runtime.get('process_sys_cpu_pct', 0), 2),
        'cpu_system_pct': round(runtime.get('system_cpu_percent', 0), 2),
        'peak_rss_gib': bytes_to_gib(runtime.get('peak_rss_kib', 0) * 1024),
        # Write-pool pipeline (writes only; reads will have zeros)
        'producer_rate_gibps': pipeline.get('producer_rate_gib_ps', 0),
        'consumer_stall_goroutine_s': pipeline.get('consumer_stall_sec', 0),
    }


def parse_path(bench_dir):
    """
    Parse path components to determine vm, workload, test_type, worker_id, host_prefix.
    bench_dir: Path object ending in bench-YYYYMMDD-HHMMSS
    """
    parts = bench_dir.parts

    # Find VM number
    vm = None
    for p in parts:
        m = re.match(r'VM(\d+)-results', p)
        if m:
            vm = int(m.group(1))
            break

    bench_name = parts[-1]  # bench-YYYYMMDD-HHMMSS
    timestamp = bench_name.replace('bench-', '')

    # Determine whether this is inside a host-N subdirectory (read-all layout)
    parent = parts[-2]
    host_prefix = None
    if re.match(r'host-\d+', parent):
        host_prefix = int(parent.split('-')[1])
        test_dir = parts[-3]
    else:
        test_dir = parent

    # Parse workload and test type from test_dir
    # Patterns:
    #   resnet50: worker-N | worker-N-read | worker-N-read-all
    #   unet3d:   unet3d-worker-N-prepare | unet3d-worker-N-read | unet3d-worker-N-read-all
    if test_dir.startswith('unet3d-'):
        workload = 'unet3d'
        remainder = test_dir[len('unet3d-'):]
    else:
        workload = 'resnet50'
        remainder = test_dir

    m = re.match(r'worker-(\d+)(.*)', remainder)
    if m:
        worker_id = int(m.group(1))
        suffix = m.group(2)  # '', '-prepare', '-read', '-read-all'
    else:
        worker_id = -1
        suffix = ''

    if suffix in ('', '-prepare'):
        test_type = 'prepare'
    elif suffix == '-read':
        test_type = 'single-read'
    elif suffix == '-read-all':
        test_type = 'read-all'
    else:
        test_type = suffix.lstrip('-') or 'unknown'

    return {
        'vm': vm,
        'worker_id': worker_id,
        'workload': workload,
        'test_type': test_type,
        'host_prefix': host_prefix,
        'timestamp': timestamp,
    }


def weighted_avg(recs, field, weight_field='total_ops'):
    total_weight = sum(r[weight_field] for r in recs)
    if total_weight == 0:
        return 0.0
    return sum(r[field] * r[weight_field] for r in recs) / total_weight


def collect_all_bench_dirs(root):
    for bench_yaml in sorted(root.rglob('bench.yaml')):
        yield bench_yaml.parent


def write_tsv(path, fields, records):
    with open(path, 'w') as f:
        f.write('\t'.join(fields) + '\n')
        for r in records:
            f.write('\t'.join(fmt(r.get(k)) for k in fields) + '\n')
    print(f"  Wrote {len(records)} rows → {path}")


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    output_dir = root

    all_records = []
    errors = []

    for bench_dir in collect_all_bench_dirs(root):
        yaml_path = bench_dir / 'bench.yaml'
        if not yaml_path.exists():
            continue
        try:
            path_info = parse_path(bench_dir)
            metrics = parse_bench_yaml(yaml_path)
            all_records.append({**path_info, **metrics})
        except Exception as e:
            errors.append(f"{bench_dir}: {e}")

    if errors:
        print("PARSE WARNINGS:")
        for e in errors:
            print(f"  {e}")

    print(f"\nParsed {len(all_records)} bench.yaml files total\n")

    # -------------------------------------------------------------------------
    # Output 1: Raw per-run TSV — one row per bench.yaml
    # -------------------------------------------------------------------------
    raw_fields = [
        'timestamp', 'vm', 'worker_id', 'workload', 'test_type', 'host_prefix',
        'op_type', 'goroutines',
        'throughput_gibps', 'total_bytes_gib', 'ops_per_sec', 'avg_obj_size_mib',
        'errors', 'retries', 'duration_s',
        'lat_p50_ms', 'lat_p90_ms', 'lat_p95_ms', 'lat_p99_ms', 'lat_p999_ms', 'lat_mean_ms',
        'ttfb_p50_ms', 'ttfb_p90_ms', 'ttfb_p95_ms', 'ttfb_p99_ms', 'ttfb_mean_ms',
        'cpu_process_pct', 'cpu_system_pct', 'peak_rss_gib',
        'producer_rate_gibps', 'consumer_stall_goroutine_s',
        'start_time', 'trackname',
    ]
    write_tsv(output_dir / 'results_raw.tsv', raw_fields, sorted(all_records, key=lambda r: (r['timestamp'], r['vm'], r['workload'], r['test_type'], r.get('host_prefix', -1))))

    # -------------------------------------------------------------------------
    # Output 2: Read-all per VM — aggregate 4 prefix sub-processes per (vm, workload, timestamp)
    # -------------------------------------------------------------------------
    readall_groups = defaultdict(list)
    for r in all_records:
        if r['test_type'] == 'read-all':
            key = (r['vm'], r['workload'], r['timestamp'])
            readall_groups[key].append(r)

    per_vm_records = []
    for (vm, workload, timestamp), recs in sorted(readall_groups.items()):
        goroutines_per_prefix = recs[0]['goroutines']
        per_vm_records.append({
            'timestamp': timestamp,
            'vm': vm,
            'workload': workload,
            'test_type': 'read-all',
            'op_type': 'read',
            'num_prefixes': len(recs),
            'goroutines_per_prefix': goroutines_per_prefix,
            'goroutines_total': sum(r['goroutines'] for r in recs),
            'throughput_gibps': sum(r['throughput_gibps'] for r in recs),
            'total_bytes_gib': sum(r['total_bytes_gib'] for r in recs),
            'ops_per_sec': sum(r['ops_per_sec'] for r in recs),
            'avg_obj_size_mib': weighted_avg(recs, 'avg_obj_size_mib'),
            'errors': sum(r['errors'] for r in recs),
            'retries': sum(r['retries'] for r in recs),
            'duration_s': max(r['duration_s'] for r in recs),
            'lat_p50_ms': weighted_avg(recs, 'lat_p50_ms'),
            'lat_p90_ms': weighted_avg(recs, 'lat_p90_ms'),
            'lat_p95_ms': weighted_avg(recs, 'lat_p95_ms'),
            'lat_p99_ms': weighted_avg(recs, 'lat_p99_ms'),
            'lat_mean_ms': weighted_avg(recs, 'lat_mean_ms'),
            'ttfb_p50_ms': weighted_avg(recs, 'ttfb_p50_ms'),
            'ttfb_p90_ms': weighted_avg(recs, 'ttfb_p90_ms'),
            'ttfb_p95_ms': weighted_avg(recs, 'ttfb_p95_ms'),
            'ttfb_p99_ms': weighted_avg(recs, 'ttfb_p99_ms'),
            'ttfb_mean_ms': weighted_avg(recs, 'ttfb_mean_ms'),
            'cpu_process_pct': weighted_avg(recs, 'cpu_process_pct'),
            'cpu_system_pct': weighted_avg(recs, 'cpu_system_pct'),
        })

    per_vm_fields = [
        'timestamp', 'vm', 'workload', 'test_type', 'op_type',
        'num_prefixes', 'goroutines_per_prefix', 'goroutines_total',
        'throughput_gibps', 'total_bytes_gib', 'ops_per_sec', 'avg_obj_size_mib',
        'errors', 'retries', 'duration_s',
        'lat_p50_ms', 'lat_p90_ms', 'lat_p95_ms', 'lat_p99_ms', 'lat_mean_ms',
        'ttfb_p50_ms', 'ttfb_p90_ms', 'ttfb_p95_ms', 'ttfb_p99_ms', 'ttfb_mean_ms',
        'cpu_process_pct', 'cpu_system_pct',
    ]
    write_tsv(output_dir / 'results_read_all_per_vm.tsv', per_vm_fields, per_vm_records)

    # -------------------------------------------------------------------------
    # Output 3: Cluster totals for read-all — sum per-VM rows for same (workload, timestamp)
    # -------------------------------------------------------------------------
    cluster_groups = defaultdict(list)
    for r in per_vm_records:
        key = (r['workload'], r['timestamp'])
        cluster_groups[key].append(r)

    cluster_records = []
    for (workload, timestamp), recs in sorted(cluster_groups.items()):
        n = len(recs)
        cluster_records.append({
            'timestamp': timestamp,
            'workload': workload,
            'num_vms': n,
            'goroutines_per_prefix': recs[0]['goroutines_per_prefix'],
            'goroutines_per_vm': recs[0]['goroutines_total'],
            'goroutines_cluster': sum(r['goroutines_total'] for r in recs),
            'throughput_gibps_per_vm': sum(r['throughput_gibps'] for r in recs) / n,
            'throughput_gibps_cluster': sum(r['throughput_gibps'] for r in recs),
            'ops_per_sec_per_vm': sum(r['ops_per_sec'] for r in recs) / n,
            'ops_per_sec_cluster': sum(r['ops_per_sec'] for r in recs),
            'avg_obj_size_mib': weighted_avg(recs, 'avg_obj_size_mib', 'ops_per_sec'),
            'total_bytes_gib': sum(r['total_bytes_gib'] for r in recs),
            'errors': sum(r['errors'] for r in recs),
            'retries': sum(r['retries'] for r in recs),
            'duration_s': max(r['duration_s'] for r in recs),
            'lat_p50_ms': weighted_avg(recs, 'lat_p50_ms', 'ops_per_sec'),
            'lat_p90_ms': weighted_avg(recs, 'lat_p90_ms', 'ops_per_sec'),
            'lat_p95_ms': weighted_avg(recs, 'lat_p95_ms', 'ops_per_sec'),
            'lat_p99_ms': weighted_avg(recs, 'lat_p99_ms', 'ops_per_sec'),
            'lat_mean_ms': weighted_avg(recs, 'lat_mean_ms', 'ops_per_sec'),
            'ttfb_p50_ms': weighted_avg(recs, 'ttfb_p50_ms', 'ops_per_sec'),
            'ttfb_p95_ms': weighted_avg(recs, 'ttfb_p95_ms', 'ops_per_sec'),
            'ttfb_p99_ms': weighted_avg(recs, 'ttfb_p99_ms', 'ops_per_sec'),
            'ttfb_mean_ms': weighted_avg(recs, 'ttfb_mean_ms', 'ops_per_sec'),
        })

    cluster_fields = [
        'timestamp', 'workload', 'num_vms',
        'goroutines_per_prefix', 'goroutines_per_vm', 'goroutines_cluster',
        'throughput_gibps_per_vm', 'throughput_gibps_cluster',
        'ops_per_sec_per_vm', 'ops_per_sec_cluster',
        'avg_obj_size_mib', 'total_bytes_gib', 'errors', 'retries', 'duration_s',
        'lat_p50_ms', 'lat_p90_ms', 'lat_p95_ms', 'lat_p99_ms', 'lat_mean_ms',
        'ttfb_p50_ms', 'ttfb_p95_ms', 'ttfb_p99_ms', 'ttfb_mean_ms',
    ]
    write_tsv(output_dir / 'results_read_all_cluster.tsv', cluster_fields, cluster_records)

    # -------------------------------------------------------------------------
    # Output 4: Prepare (write) runs summary
    # -------------------------------------------------------------------------
    prepare_records = [r for r in all_records if r['test_type'] == 'prepare']
    prepare_fields = [
        'timestamp', 'vm', 'worker_id', 'workload', 'op_type', 'goroutines',
        'throughput_gibps', 'total_bytes_gib', 'ops_per_sec', 'avg_obj_size_mib',
        'errors', 'retries', 'duration_s',
        'lat_p50_ms', 'lat_p90_ms', 'lat_p95_ms', 'lat_p99_ms', 'lat_mean_ms',
        'cpu_process_pct', 'cpu_system_pct',
        'producer_rate_gibps', 'consumer_stall_goroutine_s',
        'start_time',
    ]
    write_tsv(output_dir / 'results_prepare.tsv', prepare_fields,
              sorted(prepare_records, key=lambda r: (r['timestamp'], r['vm'])))

    # -------------------------------------------------------------------------
    # Output 5: Single-host read runs summary
    # -------------------------------------------------------------------------
    single_read_records = [r for r in all_records if r['test_type'] == 'single-read']
    write_tsv(output_dir / 'results_single_read.tsv', raw_fields,
              sorted(single_read_records, key=lambda r: (r['timestamp'], r['vm'])))

    # -------------------------------------------------------------------------
    # Summary to stdout
    # -------------------------------------------------------------------------
    print("\n=== Test Run Summary ===")
    by_workload_type = defaultdict(lambda: defaultdict(list))
    for r in all_records:
        by_workload_type[r['workload']][r['test_type']].append(r)

    for workload in sorted(by_workload_type):
        print(f"\n  Workload: {workload}")
        for test_type in sorted(by_workload_type[workload]):
            recs = by_workload_type[workload][test_type]
            timestamps = sorted(set(r['timestamp'] for r in recs))
            vms = sorted(set(r['vm'] for r in recs))
            goroutines = sorted(set(r['goroutines'] for r in recs))
            print(f"    {test_type:15s}: {len(timestamps)} timestamp(s), VMs={vms}, goroutines/proc={goroutines}")
            for ts in timestamps:
                ts_recs = [r for r in recs if r['timestamp'] == ts]
                avg_tput = sum(r['throughput_gibps'] for r in ts_recs) / len(ts_recs)
                total_tput = sum(r['throughput_gibps'] for r in ts_recs)
                print(f"      {ts}  n={len(ts_recs):2d}  avg={avg_tput:.2f} GiB/s/proc  total={total_tput:.2f} GiB/s")


if __name__ == '__main__':
    main()
