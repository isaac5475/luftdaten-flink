#!/usr/bin/env python3
"""
Plot latency time series from rtracker latency_output.log.

Input format per line:
  237,sensor=141 window=[1577836800000,1577836860000) avg=4.280,1780849502772,1780849503009
  ^                                                            ^                ^
  latency (ms)                                                 event_ts         ingestion_ts
"""

import re
import sys
import matplotlib.pyplot as plt


def parse_line(line: str):
    """
    Returns (latency_ms, ingestion_ts_ms) or None if line can't be parsed.
    """
    line = line.strip()
    if not line:
        return None
    parts = line.split(",")
    if len(parts) < 3:
        return None
    try:
        latency_ms = int(parts[0])
        ingestion_ts = int(parts[-1])
        return latency_ms, ingestion_ts
    except ValueError:
        return None


def load(path: str):
    latencies = []
    timestamps = []
    with open(path) as f:
        for line in f:
            result = parse_line(line)
            if result:
                latency_ms, ingestion_ts = result
                latencies.append(latency_ms)
                timestamps.append(ingestion_ts)
    return timestamps, latencies


def plot(timestamps, latencies, p50, p90, output_path: str = None):
    # Normalise timestamps so x-axis starts at 0 seconds
    t0 = timestamps[0]
    x = [(t - t0) / 1000.0 for t in timestamps]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x, latencies, linewidth=0.8, alpha=0.8, label="latency")

    # Add horizontal lines for p50 and p90
    ax.axhline(y=p50, linestyle="--", color="orange", linewidth=1.5, label=f"p50 ({p50} ms)")
    ax.axhline(y=p90, linestyle="--", color="red", linewidth=1.5, label=f"p90 ({p90} ms)")

    ax.set_xlabel("Time since first tuple (s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("End-to-end latency over time")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Saved to {output_path}")
    else:
        plt.show()


if __name__ == "__main__":
    log_file = sys.argv[1] if len(sys.argv) > 1 else "latency_output.log"
    out_file = sys.argv[2] if len(sys.argv) > 2 else None

    timestamps, latencies = load(log_file)
    if not latencies:
        print("No data parsed — check the log file path and format.")
        sys.exit(1)

    # Calculate percentiles
    sorted_latencies = sorted(latencies)
    n = len(sorted_latencies)
    p50_idx = int(n * 0.50)
    p90_idx = int(n * 0.90)
    p50 = sorted_latencies[p50_idx]
    p90 = sorted_latencies[p90_idx]

    print(f"Parsed {len(latencies)} tuples")
    print(f"  min latency : {min(latencies)} ms")
    print(f"  max latency : {max(latencies)} ms")
    print(f"  avg latency : {sum(latencies)/len(latencies):.1f} ms")
    print(f"  p50 latency : {p50} ms")
    print(f"  p90 latency : {p90} ms")

    plot(timestamps, latencies, p50, p90, out_file)