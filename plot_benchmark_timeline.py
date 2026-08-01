#!/usr/bin/env python3
"""
One figure per benchmark run directory: everything that happened, on one shared
time axis, so a latency curve can be read against the load that caused it.

    python3 plot_benchmark_timeline.py benchmark_results/<run>/<Q_label>/ -o out.png

Panels, top to bottom:
  1. input rate      - records/s produced into Kafka (10s-smoothed, from the
                       per-partition log_end_offset in the timeline CSV), with
                       the configured mean rate as a reference line and the
                       generator's idle stretches shaded.
  2. backlog         - the Flink source's own pendingRecords where available,
                       otherwise broker-reported consumer lag (which is a
                       checkpoint-commit sawtooth, not a true backlog - stated
                       in the panel title when that is what is being shown).
  3. parallelism     - applied parallelism per vertex (step plot).
  4. busy/backpressure - per vertex, % of wall time. This is the panel that
                       explains the latency curve: a vertex at 100% busy is
                       compute-bound, a vertex at high backpressure is waiting
                       on something downstream.
  5. CPU             - millicores for the Flink pods, against the TaskManager
                       request.
  6. memory          - Mi for the same pods (a separate panel, not a second
                       y-axis on the CPU panel: two scales on one panel is the
                       single most misread chart form there is).
  7. latency         - p50 and p95 per bucket. The line BREAKS across gaps in
                       the data instead of interpolating over them, because the
                       load generator is silent for ~40% of every period and a
                       straight line across that hole reads as steady traffic.

Deliberate choices worth knowing:
  * Latency rows are sorted by receive timestamp before plotting. rtracker's 16
    worker threads write 3-7% of lines out of order (inversions up to ~4s), so
    an unsorted plot zigzags backwards.
  * All series are drawn from a fixed, colorblind-checked categorical order; the
    same vertex keeps the same colour across panels.
  * Rescale/restart markers come from the Kubernetes event stream in the
    timeline CSV.
"""

import argparse
import bisect
import csv
import glob
import gzip
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# Categorical palette, fixed order (validated colorblind-safe as a set on the
# adjacent pairlist). Series identity follows the entity, never its rank in a
# filtered subset, so a vertex keeps its colour in every panel.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
INK = "#0b0b0b"
INK_MUTED = "#52514e"
GRID = "#d8d7d2"
IDLE_BAND = "#e8e7e3"


# ------------------------------------------------------------------- discovery


def newest(pattern):
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def find_timeline_path(run_dir):
    excluded = ("parallelism_", "resources_", "backpressure_", "throughput_", "checkpoints_")
    candidates = [
        p
        for p in glob.glob(os.path.join(run_dir, "*.csv"))
        if not os.path.basename(p).startswith(excluded)
    ]
    return sorted(candidates)[-1] if candidates else None


def find_latency_path(run_dir):
    for name in ("latency_output.log", "latency_output.log.gz"):
        path = os.path.join(run_dir, name)
        if os.path.exists(path):
            return path
    return None


def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, "r", errors="replace")


# ------------------------------------------------------------------- unit parse


def parse_cpu_millicores(value):
    value = str(value).strip()
    if not value or value == "NA":
        return float("nan")
    if value.endswith("m"):
        return float(value[:-1])
    try:
        return float(value) * 1000.0
    except ValueError:
        return float("nan")


def parse_mem_mi(value):
    value = str(value).strip()
    if not value or value == "NA":
        return float("nan")
    for suffix, factor in (("Gi", 1024.0), ("Mi", 1.0), ("Ki", 1 / 1024.0)):
        if value.endswith(suffix):
            try:
                return float(value[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    try:
        return float(value) / (1024.0 * 1024.0)
    except ValueError:
        return float("nan")


def truncate(name, width=44):
    name = str(name)
    return name if len(name) <= width else name[: width - 1] + "…"


# ------------------------------------------------------------------ CSV loaders


def load_input_curve(timeline_path, window_s=10):
    """(rate_df, lag_df) from the timeline CSV's kafka_lag rows.

    Only polls that reported EVERY partition are used: the lag poller shells out
    to a JVM `kafka-consumer-groups.sh` per poll and loses 8-66% of them, and a
    partial poll sums a subset of partitions, which reads as the topic shrinking
    and then jumping.
    """
    if not timeline_path or not os.path.exists(timeline_path):
        return pd.DataFrame(), pd.DataFrame()
    per_ts_end, per_ts_lag = {}, {}
    with open(timeline_path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("event_type") != "kafka_lag":
                continue
            part = row.get("partition")
            if not part or not str(part).isdigit():
                continue
            ts = row.get("timestamp")
            try:
                end = int(row["log_end_offset"])
                lag = int(row["lag"])
            except (KeyError, TypeError, ValueError):
                continue
            per_ts_end.setdefault(ts, {})[part] = end
            per_ts_lag.setdefault(ts, {})[part] = lag
    if not per_ts_end:
        return pd.DataFrame(), pd.DataFrame()

    full = max(len(v) for v in per_ts_end.values())
    rows = [
        (ts, sum(v.values()), sum(per_ts_lag.get(ts, {}).values()))
        for ts, v in per_ts_end.items()
        if len(v) == full
    ]
    df = pd.DataFrame(rows, columns=["timestamp", "end_offset", "lag"])
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df = df.sort_values("ts").reset_index(drop=True)
    df["end_offset"] = df["end_offset"].cummax()  # high watermark cannot decrease

    # Smooth onto a 1s grid so the curve does not depend on poll jitter.
    # .mean() before .interpolate(): the samples land on fractional seconds
    # (…:24.687Z), and Resampler.interpolate() upsamples with asfreq, which only
    # reads values sitting exactly on a bin boundary — every bin would be NaN.
    grid = df.set_index("ts")["end_offset"].resample("1s").mean().interpolate("linear")
    rate = grid.diff(window_s) / float(window_s)
    rate_df = rate.dropna().reset_index()
    rate_df.columns = ["ts", "rate"]
    rate_df = rate_df[rate_df["rate"] >= 0]
    return rate_df, df[["ts", "lag"]]


def load_parallelism(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df = df[df["vertex_id"] != "NA"].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["parallelism"] = pd.to_numeric(df["parallelism"], errors="coerce")
    return df


def load_resources(path, pod_prefixes):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df = df[df["pod"].astype(str).str.startswith(tuple(pod_prefixes))].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["cpu_m"] = df["cpu"].map(parse_cpu_millicores)
    df["mem_mi"] = df["memory"].map(parse_mem_mi)
    return df.groupby("ts", as_index=False)[["cpu_m", "mem_mi"]].sum()


def load_backpressure(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df = df[df["vertex_id"] != "NA"].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    for col in ("busy_ms_per_sec", "backpressured_ms_per_sec", "idle_ms_per_sec"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["busy_pct"] = df.get("busy_ms_per_sec", pd.Series(dtype=float)) / 10.0
    df["backpressure_pct"] = df.get("backpressured_ms_per_sec", pd.Series(dtype=float)) / 10.0
    return df


def load_pending(path):
    """pendingRecords per vertex from throughput_*.csv (newer runs only)."""
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    if "pending_records" not in df.columns:
        return pd.DataFrame()
    df = df[(df["vertex_id"] != "NA") & (df["pending_records"] != "NA")].copy()
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["pending"] = pd.to_numeric(df["pending_records"], errors="coerce")
    return df.dropna(subset=["pending"])


def load_restart_events(path):
    if not path or not os.path.exists(path):
        return []
    df = pd.read_csv(path, dtype=str)
    if "detail" not in df.columns:
        return []
    mask = df["detail"].astype(str).str.contains("to RUNNING", na=False)
    return list(pd.to_datetime(df.loc[mask, "timestamp"], utc=True, format="ISO8601"))


def load_latency(path, bucket_seconds):
    """Bucketed p50/p95. Latency is field 0; the receive timestamp is the LAST
    field. The payload in between contains commas by construction, so a fixed
    positive index would parse a payload fragment (which is exactly the bug that
    made analyze_all_results.py report nothing)."""
    recv, lat = [], []
    with open_maybe_gzip(path) as fh:
        for line in fh:
            if "\x00" in line:
                continue
            parts = line.rstrip("\n").split(",")
            if len(parts) < 3:
                continue
            try:
                lat.append(float(parts[0]))
                recv.append(int(parts[-1]))
            except ValueError:
                continue
    if not lat:
        return pd.DataFrame(columns=["ts", "p50", "p95"])
    df = pd.DataFrame({"recv_ms": recv, "lat_ms": lat}).sort_values("recv_ms")
    df["ts"] = pd.to_datetime(df["recv_ms"], unit="ms", utc=True)
    grouped = df.set_index("ts").resample(f"{bucket_seconds}s")["lat_ms"]
    out = grouped.agg(p50=lambda s: s.quantile(0.50), p95=lambda s: s.quantile(0.95))
    # Keep the empty buckets as NaN: they are where the generator was silent, and
    # the plot must show a hole there rather than a line across it.
    return out.reset_index()


# --------------------------------------------------------------------- plotting


def idle_spans(rate_df, design_mean, t0, threshold=0.2):
    """Contiguous stretches where production is below `threshold` x the mean."""
    if rate_df.empty or not design_mean:
        return []
    secs = (rate_df["ts"] - t0).dt.total_seconds().to_numpy()
    idle = (rate_df["rate"] < threshold * design_mean).to_numpy()
    spans, start = [], None
    for i, flag in enumerate(idle):
        if flag and start is None:
            start = secs[i]
        elif not flag and start is not None:
            if secs[i] - start >= 10:
                spans.append((start, secs[i]))
            start = None
    if start is not None and secs[-1] - start >= 10:
        spans.append((start, secs[-1]))
    return spans


def style_axis(ax, ylabel, title):
    ax.set_ylabel(ylabel, color=INK_MUTED, fontsize=9)
    ax.set_title(title, color=INK, fontsize=10, loc="left", pad=6)
    ax.grid(True, color=GRID, linewidth=0.6, alpha=0.9)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=INK_MUTED, labelsize=8)


def main():
    ap = argparse.ArgumentParser(
        description="Plot one benchmark run directory as an aligned multi-panel timeline."
    )
    ap.add_argument("run_dir", help="e.g. benchmark_results/<run>/<Q_label>/")
    ap.add_argument("-o", "--output", required=True, help="output image path")
    ap.add_argument("--bucket-seconds", type=int, default=5, help="latency bucket width (default 5)")
    ap.add_argument(
        "--design-mean-rate",
        type=float,
        default=None,
        help="configured mean input rate (data_rate/period, records/s). Read from "
        "run_metadata when present; pass explicitly for older runs.",
    )
    ap.add_argument(
        "--pod-prefix",
        default="luftdaten-job,nebula-worker,nebula-cli",
        help="comma-separated pod-name prefixes summed into the resource panels",
    )
    ap.add_argument("--title", default=None, help="figure title (default: directory name)")
    args = ap.parse_args()

    run_dir = args.run_dir
    latency_path = find_latency_path(run_dir)
    if not latency_path:
        sys.exit(f"error: no latency_output.log(.gz) found in {run_dir}")

    timeline_path = find_timeline_path(run_dir)
    par = load_parallelism(newest(os.path.join(run_dir, "parallelism_*.csv")))
    res = load_resources(
        newest(os.path.join(run_dir, "resources_*.csv")),
        [p.strip() for p in args.pod_prefix.split(",") if p.strip()],
    )
    bp = load_backpressure(newest(os.path.join(run_dir, "backpressure_*.csv")))
    pending = load_pending(newest(os.path.join(run_dir, "throughput_*.csv")))
    restarts = load_restart_events(timeline_path)
    rate_df, lag_df = load_input_curve(timeline_path)
    lat = load_latency(latency_path, args.bucket_seconds)
    if lat.empty:
        sys.exit(f"error: no parseable latency lines in {latency_path}")

    design_mean = args.design_mean_rate
    meta_files = sorted(glob.glob(os.path.join(run_dir, "run_metadata", "*.json")))
    if design_mean is None and meta_files:
        import json

        try:
            with open(meta_files[-1]) as fh:
                dg = (json.load(fh) or {}).get("datagen", {})
            if dg.get("data_rate_per_period") and dg.get("period_ms"):
                design_mean = dg["data_rate_per_period"] / (dg["period_ms"] / 1000.0)
        except (json.JSONDecodeError, OSError, TypeError, KeyError):
            pass
    if design_mean is None and not rate_df.empty:
        design_mean = float(rate_df["rate"].mean())

    # Time origin: the earliest thing we know about, so no panel starts off-frame.
    t0_candidates = [lat["ts"].min()]
    for frame, col in ((par, "ts"), (res, "ts"), (bp, "ts"), (rate_df, "ts"), (pending, "ts")):
        if not frame.empty:
            t0_candidates.append(frame[col].min())
    t0 = min(t0_candidates)

    def secs(series):
        return (series - t0).dt.total_seconds()

    restart_secs = [(r - t0).total_seconds() for r in restarts]
    bands = idle_spans(rate_df, design_mean, t0)

    panels = [
        ("input", not rate_df.empty),
        ("backlog", not (pending.empty and lag_df.empty)),
        ("parallelism", not par.empty),
        ("backpressure", not bp.empty),
        ("cpu", not res.empty),
        ("memory", not res.empty),
        ("latency", True),
    ]
    active = [name for name, present in panels if present]
    fig, axes = plt.subplots(
        len(active), 1, figsize=(13, 2.5 * len(active)), sharex=True, facecolor="white"
    )
    if len(active) == 1:
        axes = [axes]
    ax_by = dict(zip(active, axes))

    def decorate(ax, first=False):
        for i, (a, b) in enumerate(bands):
            ax.axvspan(
                a, b, color=IDLE_BAND, zorder=0,
                label="generator idle" if (first and i == 0) else None,
            )
        for i, x in enumerate(restart_secs):
            ax.axvline(
                x, color=INK_MUTED, linestyle=":", linewidth=1.1, alpha=0.8, zorder=1,
                label="job reached RUNNING (start / rescale)" if (first and i == 0) else None,
            )

    # 1 — input rate
    if "input" in ax_by:
        ax = ax_by["input"]
        decorate(ax, first=True)
        ax.plot(secs(rate_df["ts"]), rate_df["rate"], color=SERIES[0], linewidth=1.8,
                label="produced into Kafka (10s mean)")
        if design_mean:
            ax.axhline(design_mean, color=INK_MUTED, linestyle="--", linewidth=1.1,
                       label=f"configured mean {design_mean:,.0f} rec/s")
        style_axis(ax, "records / s", "Input rate — what load the system actually received")
        ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    # 2 — backlog
    if "backlog" in ax_by:
        ax = ax_by["backlog"]
        decorate(ax)
        if not pending.empty:
            for i, (vertex, group) in enumerate(pending.groupby("vertex_name")):
                ax.plot(secs(group["ts"]), group["pending"], color=SERIES[i % len(SERIES)],
                        linewidth=1.8, label=f"pendingRecords — {truncate(vertex, 30)}")
            title = "Backlog — source pendingRecords (true backlog)"
        else:
            ax.plot(secs(lag_df["ts"]), lag_df["lag"], color=SERIES[0], linewidth=1.8,
                    label="consumer-group lag (committed)")
            title = ("Backlog — broker-reported consumer lag. NOTE: a sawtooth, not a true "
                     "backlog: Flink commits only at each 30s checkpoint")
        style_axis(ax, "records", title)
        ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    # 3 — parallelism
    if "parallelism" in ax_by:
        ax = ax_by["parallelism"]
        decorate(ax)
        for i, (_, group) in enumerate(par.groupby("vertex_id")):
            ax.step(secs(group["ts"]), group["parallelism"], where="post",
                    color=SERIES[i % len(SERIES)], linewidth=1.8,
                    label=truncate(group["vertex_name"].iloc[0]))
        style_axis(ax, "parallelism", "Applied parallelism per vertex")
        ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    # 4 — busy / backpressure
    if "backpressure" in ax_by:
        ax = ax_by["backpressure"]
        decorate(ax)
        for i, (_, group) in enumerate(bp.groupby("vertex_id")):
            colour = SERIES[i % len(SERIES)]
            name = truncate(group["vertex_name"].iloc[0], 34)
            ax.plot(secs(group["ts"]), group["backpressure_pct"], color=colour, linewidth=1.8,
                    label=f"backpressured — {name}")
            ax.plot(secs(group["ts"]), group["busy_pct"], color=colour, linewidth=1.4,
                    linestyle="--", alpha=0.85, label=f"busy — {name}")
        ax.set_ylim(-4, 104)
        style_axis(ax, "% of wall time", "Busy (dashed) and backpressured (solid) time per vertex")
        ax.legend(fontsize=7, loc="upper right", frameon=False, ncol=2, labelcolor=INK_MUTED)

    # 5 — CPU
    if "cpu" in ax_by:
        ax = ax_by["cpu"]
        decorate(ax)
        ax.plot(secs(res["ts"]), res["cpu_m"], color=SERIES[0], linewidth=1.8, label="Flink pods, summed")
        ax.axhline(4000, color=INK_MUTED, linestyle="--", linewidth=1.1, label="TaskManager request (4000m)")
        style_axis(ax, "millicores", "CPU — metrics-server refreshes ~every 50s, so this is a ~50s average")
        ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    # 6 — memory (its own panel: a second y-axis on the CPU panel would be a
    #     dual-scale chart, the most consistently misread form there is)
    if "memory" in ax_by:
        ax = ax_by["memory"]
        decorate(ax)
        ax.plot(secs(res["ts"]), res["mem_mi"], color=SERIES[2], linewidth=1.8, label="Flink pods, summed")
        ax.axhline(4096 + 1024, color=INK_MUTED, linestyle="--", linewidth=1.1,
                   label="JM+TM request (5120 Mi)")
        style_axis(ax, "Mi", "Memory")
        ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    # 7 — latency
    ax = ax_by["latency"]
    decorate(ax)
    x = secs(lat["ts"])
    ax.plot(x, lat["p50"], color=SERIES[0], linewidth=1.8, label=f"p50 ({args.bucket_seconds}s buckets)")
    ax.plot(x, lat["p95"], color=SERIES[1], linewidth=1.6, linestyle="--", label=f"p95 ({args.bucket_seconds}s buckets)")
    style_axis(ax, "latency (ms)", "End-to-end latency — gaps are real: no records arrived there")
    ax.legend(fontsize=8, loc="upper right", frameon=False, labelcolor=INK_MUTED)

    axes[-1].set_xlabel("time since start of run (s)", color=INK_MUTED, fontsize=9)
    title = args.title or os.path.basename(run_dir.rstrip("/"))
    fig.suptitle(title, color=INK, fontsize=12, x=0.01, ha="left", y=0.997)
    fig.tight_layout(rect=(0, 0, 1, 0.985))
    fig.savefig(args.output, dpi=150, facecolor="white")
    print(f"Saved to {args.output}")
    if bands:
        total_idle = sum(b - a for a, b in bands)
        print(f"  shaded {len(bands)} idle stretch(es), {total_idle:.0f}s total with no input")


if __name__ == "__main__":
    main()
