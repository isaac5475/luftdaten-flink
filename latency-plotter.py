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
import math
import argparse
import bisect
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


def percentile_from_sorted(sorted_vals, q: float):
    """Return the q percentile (0..1) from a sorted list using linear interpolation."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def plot(timestamps, latencies, sorted_latencies, p50, p90, output_path: str = None, fat: bool = False, with_percentile: bool = False):
    # Normalise timestamps so x-axis starts at 0 seconds
    t0 = timestamps[0]
    x = [(t - t0) / 1000.0 for t in timestamps]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x, latencies, linewidth=0.8, alpha=0.8, label="latency")

    if with_percentile:
        # Add horizontal lines for p50 and p90
        ax.axhline(y=p50, linestyle="--", color="orange", linewidth=1.5, label=f"p50 ({int(p50)} ms)")
        ax.axhline(y=p90, linestyle="--", color="red", linewidth=1.5, label=f"p90 ({int(p90)} ms)")

        # Add a secondary y-axis that maps latency value to percentile (empirical CDF)
        def value_to_percentile(v):
            # proportion of values <= v
            if not sorted_latencies:
                return 0.0
            cnt = bisect.bisect_right(sorted_latencies, v)
            return 100.0 * cnt / len(sorted_latencies)

        ax2 = ax.twinx()
        # Use the same tick positions as left axis but show percentiles on the right
        yticks = ax.get_yticks()
        yticks_pct = [value_to_percentile(y) for y in yticks]
        ax2.set_ylim(ax.get_ylim())
        ax2.set_yticks(yticks)
        ax2.set_yticklabels([f"{p:.0f}%" for p in yticks_pct])
        ax2.set_ylabel("Percentile")

    ax.set_xlabel("Time since first tuple (s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("End-to-end latency over time")
    ax.legend()
    ax.grid(True, alpha=0.3)

    if fat:
        # Add a text box with the summary stats (same as printed to console)
        count = len(latencies)
        mn = min(latencies)
        mx = max(latencies)
        avg = sum(latencies) / count if count else 0.0
        info_lines = [
            f"Parsed {count} tuples",
            f"min latency : {mn} ms",
            f"max latency : {mx} ms",
            f"avg latency : {avg:.1f} ms",
            f"p50 latency : {int(p50)} ms",
            f"p90 latency : {int(p90)} ms",
        ]
        textbox = "\n".join(info_lines)
        # place inside axes (axes coords) upper left
        ax.text(0.01, 0.99, textbox, transform=ax.transAxes, fontsize=9,
                verticalalignment='top', bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray'))

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Saved to {output_path}")
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot latency time series from rtracker latency_output.log")
    parser.add_argument("logfile", nargs="?", default="latency_output.log", help="path to latency log file")
    parser.add_argument("-o", "--output", dest="output", help="output image path (if omitted shows interactive window)")
    parser.add_argument("--summary", action="store_true", help="embed full textual summary on the plot (same as console output)")
    parser.add_argument("--with-percentile", action="store_true", help="show percentile axis/lines on the plot")
    args = parser.parse_args()

    timestamps, latencies = load(args.logfile)
    if not latencies:
        print("No data parsed — check the log file path and format.")
        sys.exit(1)

    # Calculate percentiles using interpolation
    sorted_latencies = sorted(latencies)
    p50 = percentile_from_sorted(sorted_latencies, 0.5)
    p90 = percentile_from_sorted(sorted_latencies, 0.9)

    # Print summary to console (same format as before)
    print(f"Parsed {len(latencies)} tuples")
    print(f"  min latency : {min(latencies)} ms")
    print(f"  max latency : {max(latencies)} ms")
    print(f"  avg latency : {sum(latencies)/len(latencies):.1f} ms")
    print(f"  p50 latency : {int(p50)} ms")
    print(f"  p90 latency : {int(p90)} ms")

    plot(timestamps, latencies, sorted_latencies, p50, p90, output_path=args.output, fat=args.summary, with_percentile=args.with_percentile)
