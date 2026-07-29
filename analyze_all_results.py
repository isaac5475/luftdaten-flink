#!/usr/bin/env python3
"""
Analyze all benchmark results from a run_all_queries.sh execution.

Reads metadata files and latency logs to produce a comprehensive
thesis-ready report with proper timing validation.
"""

import json
import csv
import sys
from pathlib import Path
from collections import defaultdict
import statistics
from datetime import datetime

def read_metadata(run_dir):
    """Read JSON metadata from a benchmark run."""
    metadata_files = list(Path(run_dir).glob("*/run_metadata/*.json"))
    if not metadata_files:
        return {}

    metadata_by_query = {}
    for meta_file in sorted(metadata_files):
        with open(meta_file) as f:
            meta = json.load(f)
            # Use parent directory as query name
            query_name = meta_file.parent.parent.name
            metadata_by_query[query_name] = meta

    return metadata_by_query

def read_latencies(latency_file, sample_size=100000):
    """Read latency measurements from latency_output.log."""
    latencies = []
    with open(latency_file) as f:
        for i, line in enumerate(f):
            if i >= sample_size:
                break
            parts = line.strip().split(',')
            if len(parts) >= 3:
                try:
                    latency = float(parts[2])
                    if 0 < latency < 10000:  # 0-10 seconds, filter outliers
                        latencies.append(latency)
                except (ValueError, IndexError):
                    pass
    return latencies

def analyze_run_directory(run_dir):
    """Analyze a complete benchmark run directory."""
    run_path = Path(run_dir)

    if not run_path.exists():
        print(f"ERROR: Directory not found: {run_dir}")
        return None

    # Read metadata
    metadata = read_metadata(run_dir)

    # Read latencies for each query
    results = {}
    for query_dir in sorted(run_path.iterdir()):
        if not query_dir.is_dir() or query_dir.name.startswith("run_metadata"):
            continue

        query_name = query_dir.name
        latency_file = query_dir / "latency_output.log"

        if not latency_file.exists():
            continue

        latencies = read_latencies(latency_file)
        if not latencies:
            continue

        meta = metadata.get(query_name, {})

        results[query_name] = {
            "metadata": meta,
            "latencies": {
                "count": len(latencies),
                "min_ms": min(latencies),
                "max_ms": max(latencies),
                "mean_ms": statistics.mean(latencies),
                "median_ms": statistics.median(latencies),
                "p95_ms": statistics.quantiles(latencies, n=20)[18] if len(latencies) > 20 else max(latencies),
                "p99_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) > 100 else max(latencies),
                "stdev_ms": statistics.stdev(latencies) if len(latencies) > 1 else 0,
            }
        }

    return results

def print_report(run_dir, results):
    """Print a thesis-ready report."""
    if not results:
        print(f"No results found in {run_dir}")
        return

    run_path = Path(run_dir)
    run_name = run_path.name

    print("\n" + "=" * 80)
    print(f"BENCHMARK ANALYSIS REPORT: {run_name}")
    print("=" * 80)
    print()

    # Timing validation
    print("TIMING VALIDATION")
    print("-" * 80)
    timing_valid = True
    for query_name in sorted(results.keys()):
        meta = results[query_name].get("metadata", {})
        if not meta:
            print(f"  {query_name:45} | No metadata")
            continue

        actual_duration = meta.get("actual_duration_seconds", 0)
        requested_duration = meta.get("requested_duration_seconds", 0)
        variance_pct = meta.get("timing_variance_percent", 0)

        status = "✓" if abs(variance_pct) <= 5 else "⚠"
        if abs(variance_pct) > 5:
            timing_valid = False

        print(f"  {query_name:45} | {actual_duration:3d}s (req: {requested_duration:3d}s, var: {variance_pct:+5.1f}%) {status}")

    print()
    if timing_valid:
        print("✓ All runs within ±5% timing tolerance")
    else:
        print("⚠ Some runs exceeded timing tolerance — investigate delays")

    # Latency summary
    print()
    print("LATENCY STATISTICS (milliseconds)")
    print("-" * 80)
    print(f"{'Query':<40} | {'Samples':>8} | {'Min':>8} | {'Med':>8} | {'Mean':>8} | {'Max':>8} | {'σ':>8}")
    print("-" * 80)

    for query_name in sorted(results.keys()):
        lat = results[query_name].get("latencies", {})
        print(f"{query_name:<40} | {lat.get('count', 0):8d} | "
              f"{lat.get('min_ms', 0):8.2f} | {lat.get('median_ms', 0):8.2f} | "
              f"{lat.get('mean_ms', 0):8.2f} | {lat.get('max_ms', 0):8.2f} | "
              f"{lat.get('stdev_ms', 0):8.2f}")

    # Percentile summary
    print()
    print("PERCENTILE LATENCIES (milliseconds)")
    print("-" * 80)
    print(f"{'Query':<40} | {'p95':>8} | {'p99':>8}")
    print("-" * 80)
    for query_name in sorted(results.keys()):
        lat = results[query_name].get("latencies", {})
        print(f"{query_name:<40} | {lat.get('p95_ms', 0):8.2f} | {lat.get('p99_ms', 0):8.2f}")

    print()
    print("=" * 80)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_all_results.py <benchmark_results_dir>")
        sys.exit(1)

    run_dir = sys.argv[1]
    results = analyze_run_directory(run_dir)
    print_report(run_dir, results)

if __name__ == "__main__":
    main()
