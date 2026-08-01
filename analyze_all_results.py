#!/usr/bin/env python3
"""
Analyse one or more benchmark run directories produced by run_all_queries.sh.

    python3 analyze_all_results.py benchmark_results/<run_dir> [<run_dir> ...]
    python3 analyze_all_results.py --compare benchmark_results/<on> benchmark_results/<off>

What it reports per query
-------------------------
  * data COVERAGE  - the interval actually covered by latency samples, how many
    seconds inside it are populated, and the largest gap. A run whose window is
    660s but whose data spans 479s with a 120s hole in the middle is not a
    "660s run", and every percentile below is qualified by that.
  * latency        - min / p50 / p90 / p95 / p99 / max / mean / stdev over the
    WHOLE file, and the same split by input phase (burst / baseline / idle /
    drain) derived from the measured Kafka production curve, because aggregate
    percentiles over a duty-cycled workload mix a cold start, two bursts of
    different intensity and an idle tail into one meaningless number.
  * load           - records produced (from Kafka log_end_offset in the timeline
    CSV), records emitted by the sink (lines in the latency log), and the ratio.
    A stateless query emitting 55% more records than a comparable run is the
    signature of the source replaying the topic after a restart.
  * elasticity     - rescales, cumulative time the job was NOT RUNNING, applied
    parallelism per vertex, and whether the sink was pinned at 1.
  * resources      - TaskManager CPU/memory, with the metrics-server sampling
    resolution stated instead of implied.
  * integrity      - NUL-byte corruption, missing artifacts, lag-poll loss rate,
    out-of-order ingestion timestamps.

Why the previous version reported nothing
-----------------------------------------
It parsed `float(parts[2])` as the latency. rtracker writes
`<latency_ms>,<payload...>,<datagen_emit_ms>,<rtracker_recv_ms>` and the payload
itself contains commas ("Category: MODERATE, AQI: 56, P1: 14.96, ..."), so
parts[2] is a payload fragment (ValueError) or a 13-digit timestamp (rejected by
its own `0 < latency < 10000` filter). Measured: 0 of 29,636,790 lines across all
five queries produced a usable value, so every query was skipped and the report
printed "No results found". It also only ever read the first 100,000 lines - the
first ~4 seconds of a 479-second run, i.e. the cold-start ramp.

Latency is field 0. The payload is everything between field 0 and the last two
fields. Never index a positive fixed position into these lines.
"""

import argparse
import csv
import glob
import gzip
import json
import math
import os
import statistics
import sys
from collections import defaultdict

NUL = b"\x00"
GAP_THRESHOLD_S = 2.0


# --------------------------------------------------------------------------- IO


def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, "r", errors="replace")


def find_latency_log(query_dir):
    for name in ("latency_output.log", "latency_output.log.gz"):
        path = os.path.join(query_dir, name)
        if os.path.exists(path):
            return path
    return None


def newest(pattern):
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def find_timeline_csv(query_dir):
    excluded = ("resources_", "parallelism_", "backpressure_", "throughput_", "checkpoints_")
    candidates = [
        p
        for p in glob.glob(os.path.join(query_dir, "*.csv"))
        if not os.path.basename(p).startswith(excluded)
    ]
    return sorted(candidates)[-1] if candidates else None


def check_nul_corruption(path, probe_bytes=4 * 1024 * 1024):
    """Detect the sparse-file corruption produced by two rtracker pods sharing a
    log: a huge run of NUL bytes in the middle of the file. Probes the middle of
    the file rather than reading gigabytes."""
    if path.endswith(".gz"):
        return None
    size = os.path.getsize(path)
    if size < probe_bytes:
        return None
    with open(path, "rb") as fh:
        fh.seek(size // 2)
        chunk = fh.read(probe_bytes)
    nul_share = chunk.count(NUL) / max(1, len(chunk))
    if nul_share > 0.5:
        return nul_share
    return None


# ---------------------------------------------------------------- latency stats


def load_latency(path):
    """Stream the whole file. Returns (recv_ms, latency_ms) lists plus counters."""
    recv, lat = [], []
    bad = 0
    with open_maybe_gzip(path) as fh:
        for line in fh:
            if "\x00" in line:
                bad += 1
                continue
            parts = line.rstrip("\n").split(",")
            if len(parts) < 3:
                bad += 1
                continue
            try:
                lat.append(float(parts[0]))
                recv.append(int(parts[-1]))
            except ValueError:
                bad += 1
    return recv, lat, bad


def percentile(sorted_values, q):
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = q * (len(sorted_values) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] * (hi - pos) + sorted_values[hi] * (pos - lo)


def describe(values):
    if not values:
        return None
    s = sorted(values)
    return {
        "n": len(s),
        "min": s[0],
        "p50": percentile(s, 0.50),
        "p90": percentile(s, 0.90),
        "p95": percentile(s, 0.95),
        "p99": percentile(s, 0.99),
        "max": s[-1],
        "mean": statistics.fmean(s),
        "stdev": statistics.pstdev(s) if len(s) > 1 else 0.0,
    }


def coverage(recv_ms):
    """Covered interval, populated seconds and gaps. recv_ms need not be sorted:
    rtracker's 16 worker threads write slightly out of order (measured 3-7% of
    lines, inversions up to ~4s), which is also why plots must sort first."""
    if not recv_ms:
        return None
    lo, hi = min(recv_ms), max(recv_ms)
    seconds = {ms // 1000 for ms in recv_ms}
    span = (hi - lo) / 1000.0
    populated = len(seconds)
    ordered = sorted(seconds)
    gaps = []
    for a, b in zip(ordered, ordered[1:]):
        if b - a > GAP_THRESHOLD_S:
            gaps.append((a, b - a))
    inversions = sum(1 for a, b in zip(recv_ms, recv_ms[1:]) if b < a)
    return {
        "first_ms": lo,
        "last_ms": hi,
        "span_s": span,
        "populated_s": populated,
        "empty_s": max(0.0, span - populated),
        "gaps": sorted(gaps, key=lambda g: -g[1])[:5],
        "out_of_order_frac": inversions / max(1, len(recv_ms) - 1),
    }


# ------------------------------------------------------------- input rate curve


def load_input_curve(timeline_csv):
    """Reconstruct produced-records-over-time from the per-partition
    log_end_offset column (the broker high watermark, which is unaffected by the
    consumer-commit sawtooth that makes the `lag` column jump in ~4.8M steps).

    Returns (samples, na_rate) with samples = [(epoch_s, total_offset, total_lag)].
    """
    if not timeline_csv or not os.path.exists(timeline_csv):
        return [], None
    per_ts_end = defaultdict(dict)
    per_ts_lag = defaultdict(dict)
    lag_rows = na_rows = 0
    with open(timeline_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("event_type") != "kafka_lag":
                continue
            lag_rows += 1
            part = row.get("partition")
            if not part or not part.isdigit():
                na_rows += 1
                continue
            ts = parse_iso_epoch(row.get("timestamp"))
            if ts is None:
                continue
            try:
                per_ts_end[ts][part] = int(row["log_end_offset"])
                per_ts_lag[ts][part] = int(row["lag"])
            except (ValueError, TypeError, KeyError):
                na_rows += 1
    # Only keep polls that reported EVERY partition. A poll that timed out
    # halfway (the 3s timeout was shorter than a JVM `kafka-consumer-groups.sh
    # --describe`, so 8-66% of polls were lost or partial) yields a sum over a
    # subset of partitions, which reads as the topic shrinking and then jumping
    # — and an adjacent-difference rate of hundreds of thousands per second that
    # is pure sampling artifact.
    if not per_ts_end:
        return [], (na_rows / lag_rows) if lag_rows else None
    full = max(len(v) for v in per_ts_end.values())
    samples = [
        (ts, sum(per_ts_end[ts].values()), sum(per_ts_lag.get(ts, {}).values()))
        for ts in sorted(per_ts_end)
        if len(per_ts_end[ts]) == full
    ]
    na_rate = (na_rows / lag_rows) if lag_rows else None
    return samples, na_rate


def parse_iso_epoch(text):
    """'2026-07-24T21:57:04.580Z' -> epoch seconds (float). Hand-rolled to avoid
    a pandas dependency in what is otherwise a stdlib script."""
    if not text:
        return None
    import datetime

    try:
        cleaned = text.strip().replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(cleaned).timestamp()
    except ValueError:
        return None


def rate_series(samples, window_s=10):
    """Production rate on a 1-second grid, smoothed over `window_s`.

    Computing a rate from adjacent raw samples is not usable here: the lag
    poller loses 8-66% of its samples (a JVM `kafka-consumer-groups.sh` per
    poll), so consecutive rows can be 0.1s or 125s apart and adjacent-difference
    rates come out anywhere from 0 to millions per second. Interpolating the
    (monotonic) high-watermark onto a fixed grid first makes the curve
    comparable across runs and immune to the sampling jitter.

    Returns [(epoch_s, rate_per_s)].
    """
    clean = []
    last_off = None
    for t, off, _ in samples:
        if last_off is not None and off < last_off:
            off = last_off  # high watermark cannot decrease; guard against a partial poll
        last_off = off
        if clean and t - clean[-1][0] < 0.5:
            clean[-1] = (t, off)  # collapse duplicate/near-duplicate timestamps
            continue
        clean.append((t, off))
    if len(clean) < 2:
        return []

    t0, t1 = clean[0][0], clean[-1][0]
    grid = []
    i = 0
    t = t0
    while t <= t1:
        while i + 1 < len(clean) and clean[i + 1][0] < t:
            i += 1
        (ta, oa) = clean[i]
        (tb, ob) = clean[min(i + 1, len(clean) - 1)]
        if tb > ta:
            frac = min(1.0, max(0.0, (t - ta) / (tb - ta)))
            grid.append((t, oa + frac * (ob - oa)))
        else:
            grid.append((t, oa))
        t += 1.0

    out = []
    for j in range(len(grid)):
        k = max(0, j - window_s)
        dt = grid[j][0] - grid[k][0]
        if dt <= 0:
            continue
        out.append((grid[j][0], (grid[j][1] - grid[k][1]) / dt))
    return out


def classify_phases(samples, design_mean_rate, window_s=10):
    """Label each second of the run by its smoothed production rate:
        burst    >= 1.5x the design mean (data_rate / period)
        baseline 0.2x .. 1.5x
        idle     < 0.2x
    The generator's schedule makes this a real distinction rather than a
    heuristic: it delivers a per-period record quota as ~30s at 3x the mean,
    ~60s at 2x, ~90s at 1x, then produces nothing until the period ends.
    """
    series = rate_series(samples, window_s=window_s)
    if not design_mean_rate or not series:
        return []
    phases = []
    for (t, rate), (t_next, _) in zip(series, series[1:] + [(series[-1][0] + 1.0, 0.0)]):
        if rate >= 1.5 * design_mean_rate:
            label = "burst"
        elif rate >= 0.2 * design_mean_rate:
            label = "baseline"
        else:
            label = "idle"
        phases.append((t, t_next, label, rate))
    return phases


def latency_by_phase(recv_ms, lat_ms, phases):
    buckets = defaultdict(list)
    if not phases:
        return {}
    starts = [p[0] for p in phases]
    import bisect

    for ms, latency in zip(recv_ms, lat_ms):
        t = ms / 1000.0
        i = bisect.bisect_right(starts, t) - 1
        if i < 0:
            continue
        t0, t1, label, _ = phases[i]
        if t > t1:
            label = "drain"
        buckets[label].append(latency)
    return {k: describe(v) for k, v in buckets.items()}


# ------------------------------------------------------------------ other CSVs


def load_parallelism(path):
    """Applied parallelism per vertex over time + restart accounting.

    A restart shows up two ways: the job_id changes, or the poller writes NA
    rows because the REST endpoint is unreachable while the JobManager pod is
    being recreated. The NA stretches are what measure job downtime.
    """
    if not path or not os.path.exists(path):
        return None
    per_vertex = defaultdict(list)
    job_ids = []
    na_stretch, na_stretches = 0, []
    rows = 0
    interval = 5.0
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            rows += 1
            if row.get("vertex_id") in (None, "", "NA"):
                na_stretch += 1
                continue
            if na_stretch:
                na_stretches.append(na_stretch)
                na_stretch = 0
            jid = row.get("job_id")
            if jid and (not job_ids or job_ids[-1] != jid):
                job_ids.append(jid)
            try:
                par = int(row["parallelism"])
            except (ValueError, TypeError, KeyError):
                continue
            per_vertex[row.get("vertex_name", "?")].append((row["timestamp"], par))
    if na_stretch:
        na_stretches.append(na_stretch)
    changes = {}
    for name, series in per_vertex.items():
        seq = []
        for ts, par in series:
            if not seq or seq[-1][1] != par:
                seq.append((ts, par))
        changes[name] = seq
    downtime = [n * interval for n in na_stretches if n >= 2]
    return {
        "rows": rows,
        "vertices": changes,
        "distinct_job_ids": len(job_ids),
        "unreachable_windows_s": downtime,
        "downtime_total_s": sum(downtime),
        "sink_pinned_at_1": any(
            name.startswith("Sink") and all(p == 1 for _, p in seq) for name, seq in changes.items()
        ),
    }


def parse_cpu(value):
    value = str(value).strip()
    if not value or value == "NA":
        return None
    return float(value[:-1]) if value.endswith("m") else float(value) * 1000.0


def parse_mem(value):
    value = str(value).strip()
    if not value or value == "NA":
        return None
    for suffix, factor in (("Gi", 1024.0), ("Mi", 1.0), ("Ki", 1 / 1024.0)):
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * factor
    try:
        return float(value) / (1024.0 * 1024.0)
    except ValueError:
        return None


def load_resources(path, prefix="luftdaten-job"):
    if not path or not os.path.exists(path):
        return None
    cpu, mem, distinct = [], [], set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            pod = str(row.get("pod", ""))
            if not pod.startswith(prefix):
                continue
            c, m = parse_cpu(row.get("cpu")), parse_mem(row.get("memory"))
            if c is not None:
                cpu.append(c)
                distinct.add(c)
            if m is not None:
                mem.append(m)
    if not cpu:
        return None
    return {
        "cpu_mean_m": statistics.fmean(cpu),
        "cpu_peak_m": max(cpu),
        "mem_mean_mi": statistics.fmean(mem) if mem else None,
        "mem_peak_mi": max(mem) if mem else None,
        "samples": len(cpu),
        "distinct_cpu_values": len(distinct),
    }


def load_backpressure(path):
    if not path or not os.path.exists(path):
        return None
    per_vertex = defaultdict(lambda: {"busy": [], "bp": []})
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            name = row.get("vertex_name")
            if not name or name == "NA":
                continue
            for key, col in (("busy", "busy_ms_per_sec"), ("bp", "backpressured_ms_per_sec")):
                try:
                    per_vertex[name][key].append(float(row[col]))
                except (ValueError, TypeError, KeyError):
                    pass
    out = {}
    for name, d in per_vertex.items():
        if d["busy"] or d["bp"]:
            out[name] = {
                "busy_mean_pct": statistics.fmean(d["busy"]) / 10.0 if d["busy"] else None,
                "busy_max_pct": max(d["busy"]) / 10.0 if d["busy"] else None,
                "backpressured_mean_pct": statistics.fmean(d["bp"]) / 10.0 if d["bp"] else None,
                "backpressured_max_pct": max(d["bp"]) / 10.0 if d["bp"] else None,
            }
    return out or None


def load_metadata(query_dir):
    files = sorted(glob.glob(os.path.join(query_dir, "run_metadata", "*.json")))
    if not files:
        return {}
    try:
        with open(files[-1]) as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        return {"_metadata_error": str(exc)}


def scaling_events(timeline_csv):
    """ScalingReport / ScalingLimited / JobStatusChanged lines from the k8s event
    stream, with the autoscaler's own reported input rate where present."""
    if not timeline_csv or not os.path.exists(timeline_csv):
        return []
    out = []
    with open(timeline_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("event_type") != "flink_event":
                continue
            detail = row.get("detail") or ""
            if any(k in detail for k in ("ScalingReport", "ScalingLimited", "Scaling")):
                out.append((row.get("timestamp"), detail.strip()))
    return out


# ------------------------------------------------------------------- reporting


def analyse_query(query_dir):
    latency_path = find_latency_log(query_dir)
    result = {
        "dir": query_dir,
        "name": os.path.basename(query_dir.rstrip("/")),
        "metadata": load_metadata(query_dir),
        "issues": [],
    }
    if not latency_path:
        result["issues"].append("no latency_output.log")
        return result

    nul_share = check_nul_corruption(latency_path)
    if nul_share:
        result["issues"].append(
            f"CORRUPT: ~{nul_share * 100:.0f}% NUL bytes mid-file (two rtracker pods shared the log)"
        )

    recv, lat, bad = load_latency(latency_path)
    result["file_size_mb"] = os.path.getsize(latency_path) / 1e6
    result["unparseable_lines"] = bad
    if not lat:
        result["issues"].append("no parseable latency lines")
        return result

    result["latency"] = describe(lat)
    result["coverage"] = coverage(recv)
    result["sink_records"] = len(lat)

    timeline = find_timeline_csv(query_dir)
    samples, na_rate = load_input_curve(timeline)
    result["lag_poll_na_rate"] = na_rate
    if na_rate is not None and na_rate > 0.05:
        result["issues"].append(f"{na_rate * 100:.0f}% of Kafka lag polls returned nothing")

    meta = result["metadata"]
    datagen = meta.get("datagen", {}) if isinstance(meta, dict) else {}
    data_rate = datagen.get("data_rate_per_period")
    period_ms = datagen.get("period_ms")
    design_mean = (data_rate / (period_ms / 1000.0)) if (data_rate and period_ms) else None

    if samples:
        produced = max(s[1] for s in samples)
        result["records_produced"] = produced
        result["max_reported_lag"] = max(s[2] for s in samples)
        if not design_mean:
            # No metadata (older run): infer the per-period quota from the curve.
            # The generator stops dead once the quota is met, so the first
            # plateau value is data_rate.
            design_mean = infer_design_mean(samples)
            result["design_mean_inferred"] = True
        result["design_mean_rate"] = design_mean
        phases = classify_phases(samples, design_mean)
        result["phase_seconds"] = summarise_phase_seconds(phases)
        result["latency_by_phase"] = latency_by_phase(recv, lat, phases)
        # Peak = highest 10s-smoothed rate, not the highest adjacent-sample
        # difference (which is dominated by lost polls).
        result["peak_rate"] = max((p[3] for p in phases), default=None)
        if design_mean:
            result["peak_over_design_mean"] = result["peak_rate"] / design_mean
        expected = datagen.get("expected_records")
        if expected:
            result["record_shortfall_pct"] = 100.0 * (expected - produced) / expected

    result["parallelism"] = load_parallelism(newest(os.path.join(query_dir, "parallelism_*.csv")))
    result["resources"] = load_resources(newest(os.path.join(query_dir, "resources_*.csv")))
    result["backpressure"] = load_backpressure(newest(os.path.join(query_dir, "backpressure_*.csv")))
    result["scaling_events"] = scaling_events(timeline)
    if result["backpressure"] is None:
        result["issues"].append("no backpressure_*.csv (run predates in-system metric collection)")
    return result


def infer_design_mean(samples):
    """Fallback for runs with no run_metadata: the mean production rate over the
    whole covered interval. This is NOT the generator's configured
    data_rate/period (that would need the period, which is only in the
    metadata) — it is the achieved mean over the window, which is lower than the
    configured mean by exactly the idle fraction. Phase thresholds relative to
    it still separate burst from baseline from idle; the absolute number is
    labelled as inferred wherever it is printed."""
    if len(samples) < 2:
        return None
    t0, t1 = samples[0][0], samples[-1][0]
    produced = max(s[1] for s in samples)
    return produced / (t1 - t0) if t1 > t0 else None


def summarise_phase_seconds(phases):
    out = defaultdict(float)
    for t0, t1, label, _ in phases:
        out[label] += t1 - t0
    return dict(out)


def fmt(value, spec=".1f", dash="-"):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return dash
    return format(value, spec)


def print_run(run_dir, queries):
    print()
    print("=" * 108)
    print(f"RUN: {os.path.basename(run_dir.rstrip('/'))}")
    print("=" * 108)

    first_meta = next((q["metadata"] for q in queries if q.get("metadata")), {})
    if first_meta:
        dg = first_meta.get("datagen", {})
        print(
            f"  autoscaler={first_meta.get('autoscaler_enabled')}  "
            f"entry_parallelism={first_meta.get('configured_parallelism')}  "
            f"data_rate={dg.get('data_rate_per_period')}/period  "
            f"period={dg.get('period_ms')}ms  duration={dg.get('duration_seconds')}s  "
            f"git={str(first_meta.get('git_commit'))[:8]}"
        )
        if dg.get("dataset_wrapped"):
            print("  !! dataset WRAPPED during this run: windowed queries drop everything after the wrap")
    else:
        print("  (no run_metadata — load configuration is not recorded in this run; see tidy_benchmark_results.py)")

    print()
    print("DATA COVERAGE")
    print(f"  {'query':<40} {'MB':>7} {'records':>11} {'span_s':>8} {'popul_s':>8} {'empty_s':>8} {'largest gap':>12}")
    for q in queries:
        cov = q.get("coverage")
        if not cov:
            print(f"  {q['name'][:40]:<40} {'':>7} {'':>11}  (no data)")
            continue
        gap = f"{cov['gaps'][0][1]:.0f}s" if cov["gaps"] else "-"
        print(
            f"  {q['name'][:40]:<40} {fmt(q.get('file_size_mb'), '7.0f')} "
            f"{q.get('sink_records', 0):11d} {fmt(cov['span_s'], '8.1f')} "
            f"{cov['populated_s']:8d} {fmt(cov['empty_s'], '8.0f')} {gap:>12}"
        )

    print()
    print("LATENCY over the whole file (ms)")
    print(f"  {'query':<40} {'p50':>10} {'p90':>10} {'p95':>10} {'p99':>10} {'max':>12} {'mean':>10}")
    for q in queries:
        lat = q.get("latency")
        if not lat:
            continue
        print(
            f"  {q['name'][:40]:<40} {fmt(lat['p50'], '10.1f')} {fmt(lat['p90'], '10.1f')} "
            f"{fmt(lat['p95'], '10.1f')} {fmt(lat['p99'], '10.1f')} {fmt(lat['max'], '12.1f')} "
            f"{fmt(lat['mean'], '10.1f')}"
        )

    if any(q.get("latency_by_phase") for q in queries):
        print()
        print("LATENCY by input phase (ms, p50 / p95) — burst = >=1.5x design mean rate, idle = <0.2x")
        print(f"  {'query':<40} {'burst':>18} {'baseline':>18} {'idle':>18} {'drain':>18}")
        for q in queries:
            by = q.get("latency_by_phase") or {}
            cells = []
            for phase in ("burst", "baseline", "idle", "drain"):
                d = by.get(phase)
                cells.append(f"{d['p50']:.0f}/{d['p95']:.0f}" if d else "-")
            print(f"  {q['name'][:40]:<40} " + " ".join(f"{c:>18}" for c in cells))

    print()
    print("INPUT LOAD (from Kafka log_end_offset — unaffected by the commit sawtooth)")
    print(f"  {'query':<40} {'produced':>12} {'design mean/s':>14} {'peak/s':>10} {'peak/mean':>10} {'burst s':>8} {'idle s':>8}")
    for q in queries:
        if "records_produced" not in q:
            continue
        ph = q.get("phase_seconds", {})
        print(
            f"  {q['name'][:40]:<40} {q['records_produced']:12d} "
            f"{fmt(q.get('design_mean_rate'), '14.0f')} {fmt(q.get('peak_rate'), '10.0f')} "
            f"{fmt(q.get('peak_over_design_mean'), '10.2f')} "
            f"{fmt(ph.get('burst'), '8.0f')} {fmt(ph.get('idle'), '8.0f')}"
        )

    print()
    print("ELASTICITY / RESOURCES")
    print(f"  {'query':<40} {'job ids':>8} {'downtime_s':>11} {'TM cpu mean/peak m':>20} {'sink p=1':>9}")
    for q in queries:
        par, res = q.get("parallelism"), q.get("resources")
        if not par and not res:
            continue
        cpu = f"{res['cpu_mean_m']:.0f}/{res['cpu_peak_m']:.0f}" if res else "-"
        print(
            f"  {q['name'][:40]:<40} {(par or {}).get('distinct_job_ids', '-'):>8} "
            f"{fmt((par or {}).get('downtime_total_s'), '11.0f')} {cpu:>20} "
            f"{str((par or {}).get('sink_pinned_at_1', '-')):>9}"
        )
    if any(q.get("resources") for q in queries):
        res = next(q["resources"] for q in queries if q.get("resources"))
        print(
            f"  note: {res['samples']} CPU samples but only {res['distinct_cpu_values']} distinct values — "
            "metrics-server refreshes roughly every 50s, so 'peak' is a ~50s average."
        )

    if any(q.get("backpressure") for q in queries):
        print()
        print("BACKPRESSURE / BUSY TIME per vertex (%, mean / max)")
        for q in queries:
            bp = q.get("backpressure")
            if not bp:
                continue
            print(f"  {q['name'][:60]}")
            for vertex, d in bp.items():
                print(
                    f"    {vertex[:56]:<56} busy {fmt(d['busy_mean_pct'], '5.1f')}/{fmt(d['busy_max_pct'], '5.1f')}  "
                    f"backpressured {fmt(d['backpressured_mean_pct'], '5.1f')}/{fmt(d['backpressured_max_pct'], '5.1f')}"
                )

    problems = [(q["name"], q["issues"]) for q in queries if q.get("issues")]
    if problems:
        print()
        print("INTEGRITY WARNINGS")
        for name, issues in problems:
            for issue in issues:
                print(f"  [{name[:38]:<38}] {issue}")


def print_comparison(runs):
    print()
    print("=" * 108)
    print("COMPARISON")
    print("=" * 108)
    print(f"  {'query':<36} " + " ".join(f"{os.path.basename(r[0].rstrip('/'))[:28]:>30}" for r in runs))
    names = []
    for _, queries in runs:
        for q in queries:
            if q["name"] not in names:
                names.append(q["name"])
    for metric, spec, getter in (
        ("p50 latency (ms)", "30.1f", lambda q: (q.get("latency") or {}).get("p50")),
        ("p95 latency (ms)", "30.1f", lambda q: (q.get("latency") or {}).get("p95")),
        ("max latency (ms)", "30.1f", lambda q: (q.get("latency") or {}).get("max")),
        ("sink records", "30d", lambda q: q.get("sink_records")),
        ("records produced", "30d", lambda q: q.get("records_produced")),
        ("job downtime (s)", "30.0f", lambda q: (q.get("parallelism") or {}).get("downtime_total_s")),
        ("distinct job ids", "30d", lambda q: (q.get("parallelism") or {}).get("distinct_job_ids")),
    ):
        print(f"\n  {metric}")
        for name in names:
            cells = []
            for _, queries in runs:
                q = next((x for x in queries if x["name"] == name), None)
                value = getter(q) if q else None
                cells.append(format(value, spec) if value is not None else f"{'-':>30}")
            print(f"    {name[:34]:<34} " + " ".join(cells))
    print()
    print("  Reading this table: a stateless query that emitted substantially MORE sink records than the")
    print("  same query in another run reprocessed part of the topic. With upgradeMode: stateless and")
    print("  OffsetsInitializer.earliest() every autoscaler rescale restarted the source from offset 0, so")
    print("  those runs' latency and lag figures describe overlapping replays, not burst behaviour.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dirs", nargs="+", help="benchmark_results/<run_dir> (one or more)")
    ap.add_argument("--compare", action="store_true", help="also print a side-by-side comparison table")
    ap.add_argument("--json", help="write the full analysis to this JSON file")
    args = ap.parse_args()

    runs = []
    for run_dir in args.run_dirs:
        if not os.path.isdir(run_dir):
            print(f"ERROR: not a directory: {run_dir}", file=sys.stderr)
            continue
        query_dirs = sorted(
            p
            for p in glob.glob(os.path.join(run_dir, "*"))
            # not os.path.islink: tidy_benchmark_results.py leaves a symlink at
            # every old query-directory name so existing paths keep working;
            # following them would count each query twice.
            if os.path.isdir(p)
            and not os.path.islink(p)
            and os.path.basename(p) not in ("run_metadata", "config_snapshot")
        )
        queries = [analyse_query(q) for q in query_dirs]
        runs.append((run_dir, queries))
        print_run(run_dir, queries)

    if args.compare and len(runs) > 1:
        print_comparison(runs)

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(
                [{"run": r, "queries": q} for r, q in runs], fh, indent=2, default=str
            )
        print(f"\nFull analysis written to {args.json}")

    return 0 if runs else 1


if __name__ == "__main__":
    sys.exit(main())
