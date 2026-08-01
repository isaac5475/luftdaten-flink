#!/usr/bin/env python3
"""
Make the stored benchmark archive uniform, and make every run directory state
what it actually measured rather than what its name claims.

    python3 tidy_benchmark_results.py                 # dry run: report only
    python3 tidy_benchmark_results.py --apply          # perform the fixes
    python3 tidy_benchmark_results.py --apply --rename-runs   # also rename run dirs
                                                              # (leaves a symlink
                                                              #  at the old name)

Why this exists
---------------
The archive grew across several harness revisions and is not self-consistent:

  * `benchmark_results_metadata_validated_quarter_7m6_600s_20260729_195846` is
    named for a quarter-rate run but every one of its queries produced
    30,265,012 records - the HALF rate, 2x its label - with the autoscaler off.
    The load configuration is recoverable only by reconstructing Kafka offsets,
    because no run directory contains a copy of DatagenConfig/FlinkDeployment.
  * `..._half_..._kafka_8p/Q4_sliding_spike/latency_output.log` is 47 MB of Q4
    data, then 1.234 GB of NUL bytes, then 56 MB of the PREVIOUS query's
    records - two rtracker pods sharing one log file through a rolling restart.
    Every statistic computed from it mixes two queries.
  * Query directories use two different naming schemes (`Q1_aqi_stateless` vs
    `Q1AQIHazardLevelStatelessFilterSPS30`), so no script can join a run from
    July 24 to its counterpart from July 29 without a hand-written alias table.
  * `benchmark_timeline/autoscaler_flink/` holds CSV files with a `.png`
    extension and a PNG with no extension at all, so any glob-based tool either
    misses them or chokes on them.
  * The `benchmark_timeline/` root holds logger output that leaked from runs
    whose result directories were never created, in an older CSV schema, mixed
    naive-local and UTC timestamps - and it shares its naming scheme with real
    results, so a glob picks it up as if it were data.

What it does (with --apply)
---------------------------
  1. writes MEASURED.md into every run directory: records actually produced,
     autoscaler state inferred from the event stream, per-query data coverage,
     and any mismatch against the directory name;
  2. quarantines corrupt latency logs as *.CORRUPT and records why;
  3. normalises query directory names to the canonical entry-class names,
     leaving a symlink at the old name;
  4. fixes the mislabelled file extensions in the two autoscaler_flink dirs;
  5. moves leaked/old-schema logger output out of benchmark_timeline/ into
     benchmark_timeline/archive_invalid/ with a README;
  6. gzips latency logs above --gzip-threshold-mb (they compress ~8x, and every
     analysis script in this repo reads .gz transparently);
  7. writes MANIFEST.sha256 per run directory - the one artifact that would have
     caught the NUL-byte corruption on its own;
  8. writes benchmark_results/INDEX.md, one row per run, sorted by date.

Nothing is deleted. Renames leave a symlink behind.
"""

import argparse
import csv
import datetime
import glob
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys

# Short-name -> canonical entry-class name. The canonical names are the ones
# run_all_queries.sh produces today.
QUERY_ALIASES = {
    "Q1_aqi_stateless": "Q1AQIHazardLevelStatelessFilterSPS30",
    "Q2_coarse_particle_stateless": "Q2CoarseParticleDominanceFilterSPS30",
    "Q3_tumbling_avg": "Q3TumblingWindowMapSPS30",
    "Q4_sliding_spike": "Q4SlidingWindowFilterSPS30",
    "Q5_cross_spectrum": "Q5SlidingWindowExtendedAverageFilter",
}

TIMELINE_EXCLUDE = ("resources_", "parallelism_", "backpressure_", "throughput_", "checkpoints_", "datagen_")


class Actions:
    def __init__(self, apply_changes):
        self.apply = apply_changes
        self.log = []

    def do(self, description, fn):
        self.log.append(description)
        if self.apply:
            fn()

    def note(self, description):
        self.log.append(description)


# ------------------------------------------------------------------ inspection


def find_latency_log(query_dir):
    for name in ("latency_output.log", "latency_output.log.gz"):
        path = os.path.join(query_dir, name)
        if os.path.exists(path):
            return path
    return None


def find_timeline_csv(query_dir):
    candidates = [
        p
        for p in glob.glob(os.path.join(query_dir, "*.csv"))
        if not os.path.basename(p).startswith(TIMELINE_EXCLUDE)
    ]
    return sorted(candidates)[-1] if candidates else None


def nul_share(path, probe=4 * 1024 * 1024):
    if path.endswith(".gz"):
        return 0.0
    size = os.path.getsize(path)
    if size < probe:
        return 0.0
    with open(path, "rb") as fh:
        fh.seek(size // 2)
        chunk = fh.read(probe)
    return chunk.count(b"\x00") / max(1, len(chunk))


def latency_span(path):
    """(first_ms, last_ms, approx_lines) using only the head and tail."""
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", errors="replace") as fh:
            first = fh.readline()
        if path.endswith(".gz"):
            with gzip.open(path, "rt", errors="replace") as fh:
                last = None
                for line in fh:
                    last = line
        else:
            last = subprocess.run(["tail", "-1", path], capture_output=True, text=True).stdout
        f = int(first.strip().split(",")[-1])
        l = int(last.strip().split(",")[-1])
        avg_len = max(1, len(first))
        return f, l, os.path.getsize(path) // avg_len
    except (ValueError, IndexError, OSError):
        return None, None, None


def event_is_live(row_timestamp, detail, max_age_s=180):
    """True if a Kubernetes event actually happened during this run.

    `kubectl get events --watch` replays events that already exist when it
    starts, and Kubernetes keeps events for about an hour — so an
    autoscaler-OFF run recorded right after an autoscaler-ON suite has that
    suite's ScalingReports in its own timeline CSV. The event's real time is the
    first field of the detail string ("2026-07-24T21:57:02Z   ScalingReport
    ..."); the row timestamp is when the watcher read it. If they are far apart,
    it is a replay of somebody else's event.
    """
    match = re.match(r"\"?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", detail.strip())
    if not match or not row_timestamp:
        return True
    try:
        event_at = datetime.datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )
        read_at = datetime.datetime.fromisoformat(row_timestamp.strip().replace("Z", "+00:00"))
    except ValueError:
        return True
    return abs((read_at - event_at).total_seconds()) <= max_age_s


def measured_facts(query_dir):
    """What this query directory can prove about itself, from its own artifacts."""
    facts = {"name": os.path.basename(query_dir.rstrip("/"))}
    latency = find_latency_log(query_dir)
    if latency:
        facts["latency_file"] = os.path.basename(latency)
        facts["size_mb"] = os.path.getsize(latency) / 1e6
        share = nul_share(latency)
        if share > 0.5:
            facts["corrupt_nul_share"] = share
        first, last, lines = latency_span(latency)
        if first:
            facts["span_s"] = (last - first) / 1000.0
            facts["approx_lines"] = lines
            facts["first_utc"] = datetime.datetime.fromtimestamp(
                first / 1000, datetime.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
    timeline = find_timeline_csv(query_dir)
    if timeline:
        produced, scaling, na = 0, 0, 0
        rows = 0
        with open(timeline, newline="") as fh:
            per_ts = {}
            for row in csv.DictReader(fh):
                if row.get("event_type") == "kafka_lag":
                    rows += 1
                    part = row.get("partition")
                    if not part or not str(part).isdigit():
                        na += 1
                        continue
                    try:
                        per_ts.setdefault(row["timestamp"], {})[part] = int(row["log_end_offset"])
                    except (ValueError, KeyError, TypeError):
                        na += 1
                elif row.get("event_type") == "flink_event":
                    detail = row.get("detail") or ""
                    if "Scaling" in detail and event_is_live(row.get("timestamp"), detail):
                        scaling += 1
            if per_ts:
                full = max(len(v) for v in per_ts.values())
                produced = max(
                    (sum(v.values()) for v in per_ts.values() if len(v) == full), default=0
                )
        facts["records_produced"] = produced
        facts["scaling_events"] = scaling
        facts["lag_poll_na_rate"] = (na / rows) if rows else None
    meta_files = sorted(glob.glob(os.path.join(query_dir, "run_metadata", "*.json")))
    if meta_files:
        try:
            with open(meta_files[-1]) as fh:
                facts["metadata"] = json.load(fh)
        except (json.JSONDecodeError, OSError):
            facts["metadata_invalid"] = True
    return facts


def run_facts(run_dir):
    query_dirs = sorted(
        p
        for p in glob.glob(os.path.join(run_dir, "*"))
        if os.path.isdir(p)
        and not os.path.islink(p)  # skip the compatibility symlinks this script creates
        and os.path.basename(p) not in ("run_metadata", "config_snapshot")
    )
    queries = [measured_facts(q) for q in query_dirs]
    produced = [q["records_produced"] for q in queries if q.get("records_produced")]
    scaling = sum(q.get("scaling_events", 0) for q in queries)
    return {
        "dir": run_dir,
        "queries": queries,
        "records_produced_max": max(produced) if produced else None,
        "autoscaler_active": scaling > 0,
        "scaling_events": scaling,
    }


def label_claims(name):
    """Parse what a directory name claims, so it can be checked."""
    claims = {}
    if "no_autoscaler" in name or "autoscaler_off" in name:
        claims["autoscaler"] = False
    elif "autoscaler_on" in name or re.search(r"kafka_\d+p($|_)", name):
        claims["autoscaler"] = None  # "8p" says nothing about the autoscaler
    if "quarter" in name:
        claims["rate_family"] = "quarter"
    elif "half" in name:
        claims["rate_family"] = "half"
    return claims


# ------------------------------------------------------------------- reporting


def write_measured_md(run, actions):
    path = os.path.join(run["dir"], "MEASURED.md")
    name = os.path.basename(run["dir"].rstrip("/"))
    claims = label_claims(name)
    produced = run["records_produced_max"]

    # The generator delivers exactly data_rate records per period and a 600s /
    # 300s-period run is exactly 2 periods, so produced/2 is the effective
    # data_rate for the standard configuration.
    family = None
    if produced:
        if abs(produced - 30_265_012) < 50_000:
            family = "half (data_rate 15,132,506 per period; 2 periods = the whole dataset once)"
        elif abs(produced - 15_132_506) < 50_000:
            family = "quarter (data_rate 7,566,253 per period)"
        else:
            family = f"unrecognised ({produced:,} records produced)"

    mismatches = []
    if claims.get("rate_family") == "quarter" and family and family.startswith("half"):
        mismatches.append(
            f"NAME SAYS QUARTER RATE, DATA SAYS HALF RATE: {produced:,} records produced "
            "(= 2 x 15,132,506). Any table treating this as a quarter-rate data point "
            "doubles the true input rate."
        )
    if claims.get("rate_family") == "half" and family and family.startswith("quarter"):
        mismatches.append(f"name says half rate, data says quarter rate ({produced:,} records)")
    if claims.get("autoscaler") is False and run["autoscaler_active"]:
        mismatches.append("name says the autoscaler was off, but the event stream contains ScalingReports")
    if claims.get("autoscaler") is None and not run["autoscaler_active"] and "kafka_8p" in name:
        mismatches.append(
            "name carries no autoscaler marker; the event stream shows NO scaling activity, "
            "so this run had the autoscaler OFF"
        )

    lines = [
        f"# Measured facts: {name}",
        "",
        "Written by tidy_benchmark_results.py from this directory's own artifacts",
        "(Kafka high-watermark offsets in the timeline CSV, the Kubernetes event",
        "stream, and the latency logs) - not from the directory name.",
        "",
        f"- records produced (max over queries): **{produced:,}**" if produced else "- records produced: unknown",
        f"- effective load configuration: **{family}**" if family else "",
        f"- autoscaler active: **{run['autoscaler_active']}** ({run['scaling_events']} scaling events in the event stream)",
        "",
    ]
    if mismatches:
        lines += ["## Label mismatches", ""] + [f"- **{m}**" for m in mismatches] + [""]
    lines += [
        "## Per-query coverage",
        "",
        "| query | latency file | MB | data span (s) | records produced | lag-poll loss | notes |",
        "|---|---|---|---|---|---|---|",
    ]
    for q in run["queries"]:
        notes = []
        if q.get("corrupt_nul_share"):
            notes.append(f"**CORRUPT: {q['corrupt_nul_share'] * 100:.0f}% NUL bytes**")
        if not q.get("latency_file"):
            notes.append("no latency log")
        na = q.get("lag_poll_na_rate")
        lines.append(
            "| {name} | {f} | {mb} | {span} | {prod} | {na} | {notes} |".format(
                name=q["name"],
                f=q.get("latency_file", "-"),
                mb=f"{q['size_mb']:.0f}" if q.get("size_mb") else "-",
                span=f"{q['span_s']:.0f}" if q.get("span_s") else "-",
                prod=f"{q['records_produced']:,}" if q.get("records_produced") else "-",
                na=f"{na * 100:.0f}%" if na is not None else "-",
                notes=" ".join(notes) or "",
            )
        )
    lines += [
        "",
        "## How to read the data span",
        "",
        "A span of ~479s inside a ~660s window is not truncation: the load",
        "generator's burst schedule packs the non-burst records immediately after",
        "the burst, leaving the last ~40% of every period producing nothing, so the",
        "last record of a 600s run is emitted at about t=480s. Runs made after",
        "DATAGEN_SPREAD_BASELINE=1 was introduced spread the baseline over the whole",
        "period and cover the full duration.",
        "",
    ]
    content = "\n".join(l for l in lines if l is not None) + "\n"
    actions.do(f"write {path}", lambda: open(path, "w").write(content))
    return mismatches


def quarantine_corrupt(run, actions):
    for q in run["queries"]:
        if not q.get("corrupt_nul_share"):
            continue
        qdir = os.path.join(run["dir"], q["name"])
        src = os.path.join(qdir, q["latency_file"])
        dst = src + ".CORRUPT"
        reason = os.path.join(qdir, "CORRUPT_README.md")
        share = q["corrupt_nul_share"]
        text = (
            f"# {q['latency_file']} is corrupt\n\n"
            f"About {share * 100:.0f}% of the middle of this file is NUL bytes, and the data after\n"
            "the hole belongs to the PREVIOUS query.\n\n"
            "Cause: the rtracker Deployment used a RollingUpdate strategy while both the old and\n"
            "new pod mounted the same hostPath log directory. rtracker truncates its output file at\n"
            "startup, so the incoming pod truncated a file the outgoing pod still held an open fd\n"
            "on; the old pod's next write landed at its previous offset, leaving a sparse hole.\n\n"
            "Fixed for future runs by k8s/RTrackerDeployment.yaml (strategy: Recreate, and a\n"
            "pod-local emptyDir instead of the shared hostPath) plus the single-pod barrier in\n"
            "run.sh. This query has to be re-run; do not quote any statistic from this file.\n"
        )
        actions.do(f"quarantine {src} -> {os.path.basename(dst)}", lambda s=src, d=dst: os.rename(s, d))
        actions.do(f"write {reason}", lambda p=reason, t=text: open(p, "w").write(t))


def normalise_query_names(run, actions):
    for q in run["queries"]:
        canonical = QUERY_ALIASES.get(q["name"])
        if not canonical:
            continue
        src = os.path.join(run["dir"], q["name"])
        dst = os.path.join(run["dir"], canonical)
        if os.path.exists(dst):
            actions.note(f"skip rename {src} (target exists)")
            continue

        def rename(s=src, d=dst):
            os.rename(s, d)
            os.symlink(os.path.basename(d), s)  # keep the old path working

        actions.do(f"rename {q['name']} -> {canonical} (+ symlink at the old name)", rename)


def gzip_large_logs(run, actions, threshold_mb):
    for q in run["queries"]:
        f = q.get("latency_file")
        if not f or f.endswith(".gz") or q.get("corrupt_nul_share"):
            continue
        if (q.get("size_mb") or 0) < threshold_mb:
            continue
        src = os.path.join(run["dir"], q["name"], f)
        actions.do(
            f"gzip {src} ({q['size_mb']:.0f} MB)",
            lambda s=src: subprocess.run(["gzip", "-f", s], check=True),
        )


def write_manifest(run, actions):
    path = os.path.join(run["dir"], "MANIFEST.sha256")

    def build():
        with open(path, "w") as out:
            for root, _, files in os.walk(run["dir"]):
                for name in sorted(files):
                    if name == "MANIFEST.sha256":
                        continue
                    full = os.path.join(root, name)
                    if os.path.islink(full):
                        continue
                    h = hashlib.sha256()
                    with open(full, "rb") as fh:
                        for chunk in iter(lambda: fh.read(1 << 20), b""):
                            h.update(chunk)
                    out.write(f"{h.hexdigest()}  {os.path.relpath(full, run['dir'])}\n")

    actions.do(f"write {path} (sha256 of every artifact)", build)


def run_date_stamp(run):
    """A YYYYmmdd_HHMMSS stamp for this run: from the directory name if it has
    one, otherwise from the earliest artifact timestamp inside it (the loggers
    name their files after the run's start)."""
    name = os.path.basename(run["dir"].rstrip("/"))
    match = re.search(r"(20\d{6}_\d{6})", name)
    if match:
        return match.group(1)
    stamps = []
    for path in glob.glob(os.path.join(run["dir"], "*", "*")):
        m = re.search(r"(20\d{6}_\d{6})", os.path.basename(path))
        if m:
            stamps.append(m.group(1))
    return min(stamps) if stamps else None


def suggested_run_name(run):
    """Encode only what was MEASURED: how many records were delivered, whether
    the autoscaler acted, and when.

    Deliberately no period/duration/partition count in the name — those are not
    recoverable from the artifacts of the older runs, and inventing them is how
    the archive ended up with a directory claiming a quarter rate that ran at
    the half rate. The full configuration lives in MEASURED.md and (for runs
    made by the current harness) config_snapshot/.
    """
    produced = run["records_produced_max"]
    stamp = run_date_stamp(run)
    if not produced or not stamp:
        return None
    autoscaler = "autoscaler_on" if run["autoscaler_active"] else "autoscaler_off"
    return f"{produced / 1e6:.2f}Mrec_{autoscaler}_{stamp}"


def rename_run(run, actions):
    new = suggested_run_name(run)
    if not new:
        return
    parent = os.path.dirname(run["dir"].rstrip("/"))
    src = run["dir"].rstrip("/")
    dst = os.path.join(parent, new)
    if os.path.basename(src) == new or os.path.exists(dst):
        return

    def rename():
        os.rename(src, dst)
        os.symlink(os.path.basename(dst), src)

    actions.do(f"rename run {os.path.basename(src)} -> {new} (+ symlink at the old name)", rename)


# ------------------------------------------------- loose files outside run dirs


def tidy_timeline_root(actions):
    root = "benchmark_timeline"
    if not os.path.isdir(root):
        return
    archive = os.path.join(root, "archive_invalid")
    loose = [
        p
        for p in glob.glob(os.path.join(root, "*"))
        if os.path.isfile(p) and not os.path.basename(p).startswith("archive_invalid")
    ]
    if not loose:
        return
    readme = os.path.join(archive, "README.md")
    text = (
        "# Invalid / orphaned logger output\n\n"
        "These files were written by the timeline, resource and parallelism loggers for runs whose\n"
        "result directories were never created (aborted runs, interrupted suites, and loggers that\n"
        "kept running after their run.sh exited). They share the naming scheme of real results, so a\n"
        "`benchmark_timeline/*.csv` glob would pick them up as data. Some also use an older CSV\n"
        "schema and mix naive-local with UTC timestamps, which silently shifts them by hours when\n"
        "loaded with `pd.to_datetime(..., utc=True)`.\n\n"
        "Kept rather than deleted, because a few may be the only trace of an experiment.\n"
    )
    actions.do(f"mkdir {archive}", lambda: os.makedirs(archive, exist_ok=True))
    actions.do(f"write {readme}", lambda: open(readme, "w").write(text))
    for path in loose:
        actions.do(
            f"move {path} -> archive_invalid/",
            lambda p=path: shutil.move(p, os.path.join(archive, os.path.basename(p))),
        )


def fix_extensions(actions):
    """Files whose extension contradicts their content (CSVs named .png, a PNG
    with no extension). Detected by magic bytes, not by name."""
    for directory in ("benchmark_timeline/autoscaler_flink", "plots/autoscaler_flink"):
        if not os.path.isdir(directory):
            continue
        for path in sorted(glob.glob(os.path.join(directory, "*"))):
            if not os.path.isfile(path) or os.path.islink(path):
                continue
            with open(path, "rb") as fh:
                head = fh.read(8)
            is_png = head.startswith(b"\x89PNG")
            base, ext = os.path.splitext(path)
            target = None
            if is_png and ext.lower() != ".png":
                target = base + ".png"
            elif not is_png and ext.lower() == ".png":
                target = base + ".csv"
            if target and not os.path.exists(target):
                actions.do(
                    f"fix extension {os.path.basename(path)} -> {os.path.basename(target)}",
                    lambda s=path, d=target: os.rename(s, d),
                )


def write_index(runs, actions):
    path = os.path.join("benchmark_results", "INDEX.md")
    rows = []
    for run in sorted(runs, key=lambda r: r["dir"]):
        name = os.path.basename(run["dir"].rstrip("/"))
        produced = run["records_produced_max"]
        spans = [q["span_s"] for q in run["queries"] if q.get("span_s")]
        corrupt = sum(1 for q in run["queries"] if q.get("corrupt_nul_share"))
        rows.append(
            "| {name} | {n} | {prod} | {span} | {asc} | {corrupt} |".format(
                name=name,
                n=len(run["queries"]),
                prod=f"{produced:,}" if produced else "-",
                span=f"{min(spans):.0f}-{max(spans):.0f}" if spans else "-",
                asc="ON" if run["autoscaler_active"] else "off",
                corrupt=corrupt or "",
            )
        )
    content = "\n".join(
        [
            "# Benchmark result archive index",
            "",
            "Generated by tidy_benchmark_results.py. Every column is measured from the run's own",
            "artifacts, never from its directory name. See each run's MEASURED.md for detail.",
            "",
            "| run | queries | records produced | data span (s) | autoscaler | corrupt files |",
            "|---|---|---|---|---|---|",
        ]
        + rows
        + [
            "",
            "## Notes",
            "",
            "- **data span** is the interval covered by latency samples. ~479s inside a ~660s window",
            "  is the load generator's own schedule (the last ~40% of every period emitted nothing),",
            "  not truncation - see any MEASURED.md for the arithmetic.",
            "- **autoscaler** is inferred from ScalingReport events in the Kubernetes event stream,",
            "  which is the only autoscaler evidence most of these runs contain.",
            "- Runs with the autoscaler ON and made before the Kafka source switched to",
            "  `committedOffsets` reprocessed the topic from offset 0 on every rescale; their",
            "  latency and lag figures describe overlapping replays. See SUPERVISOR_REPORT.md.",
            "",
        ]
    )
    actions.do(f"write {path}", lambda: open(path, "w").write(content))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="perform the changes (default: dry run)")
    ap.add_argument("--rename-runs", action="store_true", help="also rename run directories to the measured configuration")
    ap.add_argument("--gzip-threshold-mb", type=float, default=200.0, help="gzip latency logs above this size (default 200)")
    ap.add_argument("--root", default="benchmark_results", help="archive root (default benchmark_results)")
    args = ap.parse_args()

    if not os.path.isdir(args.root):
        sys.exit(f"error: {args.root} not found (run from the repo root)")

    run_dirs = sorted(p for p in glob.glob(os.path.join(args.root, "*")) if os.path.isdir(p) and not os.path.islink(p))
    actions = Actions(args.apply)
    all_mismatches = []

    print(f"Scanning {len(run_dirs)} run directories under {args.root}/ ...")
    runs = []
    for run_dir in run_dirs:
        run = run_facts(run_dir)
        runs.append(run)
        mismatches = write_measured_md(run, actions)
        all_mismatches += [(os.path.basename(run_dir), m) for m in mismatches]
        quarantine_corrupt(run, actions)
        normalise_query_names(run, actions)
        gzip_large_logs(run, actions, args.gzip_threshold_mb)

    tidy_timeline_root(actions)
    fix_extensions(actions)

    # Renames before the index, so the index lists the canonical names.
    if args.rename_runs:
        for run in runs:
            new = suggested_run_name(run)
            rename_run(run, actions)
            if new and args.apply:
                run["dir"] = os.path.join(os.path.dirname(run["dir"].rstrip("/")), new)
    write_index(runs, actions)
    if args.apply:
        for run in runs:
            write_manifest(run, actions)  # last: after all renames/gzips
    else:
        actions.note("write MANIFEST.sha256 per run directory (skipped in dry run)")

    print()
    print(f"{'APPLIED' if args.apply else 'WOULD DO'} {len(actions.log)} action(s):")
    for entry in actions.log:
        print(f"  - {entry}")

    if all_mismatches:
        print()
        print("LABEL MISMATCHES FOUND (these are the dangerous ones):")
        for name, m in all_mismatches:
            print(f"  [{name}]")
            print(f"     {m}")

    if not args.apply:
        print()
        print("Dry run. Re-run with --apply (and optionally --rename-runs) to perform these.")


if __name__ == "__main__":
    main()
