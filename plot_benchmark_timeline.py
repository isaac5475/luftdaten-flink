#!/usr/bin/env python3
"""
Plot parallelism, resource utilization, backpressure, and bucketed latency
for one benchmark run directory (e.g. benchmark_results/<run>/<Q_label>/),
all aligned on one shared "seconds since run start" time axis.

Inputs (auto-discovered inside run_dir, latest timestamped file of each kind
is used):
  parallelism_*.csv    timestamp,job_id,vertex_id,vertex_name,parallelism,status
  resources_*.csv      timestamp,namespace,pod,cpu,memory
  backpressure_*.csv   timestamp,job_id,vertex_id,vertex_name,busy_ms_per_sec,
                       backpressured_ms_per_sec,idle_ms_per_sec  (optional --
                       older runs won't have this yet; its panel is skipped)
  <timestamp>.csv      timeline events (kafka_lag/flink_event) -- used only to
                       mark JobStatusChanged->RUNNING (rescale/restart) points
  latency_output.log   raw rtracker latency lines (see latency-plotter.py)

Latency is bucketed to --bucket-seconds (default: matches the ~5s infra poll
interval) since plotting millions of raw per-tuple points against a handful
of 5s-cadence infra samples is both unreadable and slow. For full-resolution
single-run latency inspection, use latency-plotter.py instead.
"""

import argparse
import glob
import os
import sys

import matplotlib.pyplot as plt
import pandas as pd


def latest(pattern):
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def parse_cpu_millicores(value):
    value = str(value).strip()
    if not value or value == "NA":
        return float("nan")
    if value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000.0


def parse_mem_mi(value):
    value = str(value).strip()
    if not value or value == "NA":
        return float("nan")
    if value.endswith("Gi"):
        return float(value[:-2]) * 1024.0
    if value.endswith("Mi"):
        return float(value[:-2])
    if value.endswith("Ki"):
        return float(value[:-2]) / 1024.0
    # bare byte count
    return float(value) / (1024.0 * 1024.0)


def truncate(name, width=40):
    name = str(name)
    return name if len(name) <= width else name[: width - 1] + "…"


def load_parallelism(path):
    df = pd.read_csv(path, dtype=str)
    df = df[df["vertex_id"] != "NA"].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True)
    df["parallelism"] = pd.to_numeric(df["parallelism"], errors="coerce")
    return df


def load_resources(path, pod_prefixes):
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df = df[df["pod"].astype(str).str.startswith(tuple(pod_prefixes))].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True)
    df["cpu_m"] = df["cpu"].map(parse_cpu_millicores)
    df["mem_mi"] = df["memory"].map(parse_mem_mi)
    agg = df.groupby("ts", as_index=False)[["cpu_m", "mem_mi"]].sum()
    return agg


def load_backpressure(path):
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df = df[df["vertex_id"] != "NA"].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True)
    for col in ("busy_ms_per_sec", "backpressured_ms_per_sec", "idle_ms_per_sec"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["backpressure_pct"] = df["backpressured_ms_per_sec"] / 10.0
    return df


def load_restart_events(path):
    if path is None:
        return []
    df = pd.read_csv(path, dtype=str)
    if "detail" not in df.columns:
        return []
    mask = df["detail"].astype(str).str.contains("to RUNNING", na=False)
    return list(pd.to_datetime(df.loc[mask, "timestamp"], utc=True))


def load_latency(path, bucket_seconds):
    ing_ms = []
    lat_ms = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            try:
                lat = int(parts[0])
                ing = int(parts[-1])
            except ValueError:
                continue
            lat_ms.append(lat)
            ing_ms.append(ing)

    if not lat_ms:
        return pd.DataFrame(columns=["ts", "mean", "p95"])

    df = pd.DataFrame({"ing_ms": ing_ms, "lat_ms": lat_ms})
    df["ts"] = pd.to_datetime(df["ing_ms"], unit="ms", utc=True)
    bucket = f"{bucket_seconds}s"
    grouped = df.set_index("ts").resample(bucket)["lat_ms"]
    out = grouped.agg(mean="mean", p95=lambda s: s.quantile(0.95)).dropna(how="all").reset_index()
    return out


def find_timeline_path(run_dir):
    excluded_prefixes = ("parallelism_", "resources_", "backpressure_")
    candidates = [
        p
        for p in glob.glob(os.path.join(run_dir, "*.csv"))
        if not os.path.basename(p).startswith(excluded_prefixes)
    ]
    return sorted(candidates)[-1] if candidates else None


def main():
    parser = argparse.ArgumentParser(
        description="Plot parallelism, resource utilization, backpressure, and "
        "bucketed latency for one benchmark run directory, aligned on one time axis."
    )
    parser.add_argument("run_dir", help="e.g. benchmark_results/<run>/<Q_label>/")
    parser.add_argument("-o", "--output", required=True, help="output image path")
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=5,
        help="latency bucket width in seconds, should match the infra poll interval (default: 5)",
    )
    parser.add_argument(
        "--pod-prefix",
        default="luftdaten-job,nebula-worker,nebula-cli",
        help="comma-separated list of pod-name prefixes; pods matching ANY of them are "
        "included in the resource panel, summed together (default covers both the Flink "
        "job pods -- jobmanager/taskmanager share the 'luftdaten-job' prefix and change "
        "name on every rescale/restart -- and the NebulaStream worker/cli pods, so the "
        "same invocation works for either system's run dir without extra flags)",
    )
    args = parser.parse_args()

    run_dir = args.run_dir
    parallelism_path = latest(os.path.join(run_dir, "parallelism_*.csv"))
    resources_path = latest(os.path.join(run_dir, "resources_*.csv"))
    backpressure_path = latest(os.path.join(run_dir, "backpressure_*.csv"))
    timeline_path = find_timeline_path(run_dir)
    latency_path = os.path.join(run_dir, "latency_output.log")

    if not os.path.exists(latency_path):
        sys.exit(f"error: no latency_output.log found in {run_dir}")
    if parallelism_path is None:
        print(
            "warning: no parallelism_*.csv found in this run dir -- skipping that panel",
            file=sys.stderr,
        )
    if backpressure_path is None:
        print(
            "warning: no backpressure_*.csv found in this run dir (older run, "
            "predates backpressure logging) -- skipping that panel",
            file=sys.stderr,
        )

    pod_prefixes = [p.strip() for p in args.pod_prefix.split(",") if p.strip()]
    par = load_parallelism(parallelism_path) if parallelism_path else pd.DataFrame()
    res = load_resources(resources_path, pod_prefixes)
    bp = load_backpressure(backpressure_path)
    restarts = load_restart_events(timeline_path)
    lat = load_latency(latency_path, args.bucket_seconds)
    if lat.empty:
        sys.exit(f"error: no parseable lines in {latency_path}")

    t0_candidates = [lat["ts"].min()]
    if not par.empty:
        t0_candidates.append(par["ts"].min())
    if not res.empty:
        t0_candidates.append(res["ts"].min())
    if not bp.empty:
        t0_candidates.append(bp["ts"].min())
    t0 = min(t0_candidates)

    def secs(ts_series):
        return (ts_series - t0).dt.total_seconds()

    restart_secs = [(r - t0).total_seconds() for r in restarts]

    show_par_panel = not par.empty
    show_bp_panel = not bp.empty
    n_panels = 2 + show_par_panel + show_bp_panel
    fig, axes = plt.subplots(n_panels, 1, figsize=(14, 3.2 * n_panels), sharex=True)

    def mark_restarts(ax):
        for i, x in enumerate(restart_secs):
            ax.axvline(
                x,
                color="red",
                linestyle="--",
                linewidth=1,
                alpha=0.5,
                label="rescale/restart (RUNNING)" if i == 0 else None,
            )

    idx = 0

    # Panel: parallelism per vertex (optional)
    if show_par_panel:
        ax = axes[idx]
        idx += 1
        for vertex_id, group in par.groupby("vertex_id"):
            name = truncate(group["vertex_name"].iloc[0])
            ax.step(secs(group["ts"]), group["parallelism"], where="post", label=name)
        mark_restarts(ax)
        ax.set_ylabel("Parallelism")
        ax.set_title("Parallelism per vertex")
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, alpha=0.3)

    # Panel: resource utilization (CPU left axis, memory right axis via twinx)
    ax = axes[idx]
    idx += 1
    if not res.empty:
        ax.plot(secs(res["ts"]), res["cpu_m"], color="tab:blue", label="CPU (millicores)")
        ax.set_ylabel("CPU (millicores)", color="tab:blue")
        ax.tick_params(axis="y", labelcolor="tab:blue")
        ax2 = ax.twinx()
        ax2.plot(secs(res["ts"]), res["mem_mi"], color="tab:green", label="Memory (Mi)")
        ax2.set_ylabel("Memory (Mi)", color="tab:green")
        ax2.tick_params(axis="y", labelcolor="tab:green")
    else:
        ax.text(0.5, 0.5, "no resources_*.csv data", transform=ax.transAxes, ha="center")
    mark_restarts(ax)
    prefix_label = "', '".join(pod_prefixes)
    ax.set_title(f"Resource utilization (pods matching '{prefix_label}*', summed)")
    ax.grid(True, alpha=0.3)

    # Panel: backpressure per vertex (optional)
    if show_bp_panel:
        ax = axes[idx]
        idx += 1
        for vertex_id, group in bp.groupby("vertex_id"):
            name = truncate(group["vertex_name"].iloc[0])
            ax.plot(secs(group["ts"]), group["backpressure_pct"], label=name)
        mark_restarts(ax)
        ax.set_ylabel("Backpressure (%)")
        ax.set_ylim(-5, 105)
        ax.set_title("Backpressure per vertex")
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, alpha=0.3)

    # Panel: latency, bucketed to bucket_seconds
    ax = axes[idx]
    idx += 1
    ax.plot(secs(lat["ts"]), lat["mean"], label=f"mean latency ({args.bucket_seconds}s buckets)")
    ax.plot(
        secs(lat["ts"]),
        lat["p95"],
        linestyle="--",
        alpha=0.7,
        label=f"p95 latency ({args.bucket_seconds}s buckets)",
    )
    mark_restarts(ax)
    ax.set_ylabel("Latency (ms)")
    ax.set_title("End-to-end latency (bucketed)")
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Time since run start (s)")
    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
