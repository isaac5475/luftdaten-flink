#!/usr/bin/env python3
"""
One poll of the Flink REST API -> a row per vertex in each of four CSVs.

Called once per interval by utils/flink_metrics_logging.sh, which owns the
`kubectl port-forward` tunnel. Kept as a real script (rather than an inline
`python3 -c` in the shell) so it can be tested standalone:

    python3 utils/flink_metrics_poll.py --base-url http://localhost:18081 \
        --parallelism /tmp/p.csv --backpressure /tmp/b.csv \
        --throughput /tmp/t.csv --checkpoints /tmp/c.csv

Which endpoint the metrics come from, and why
---------------------------------------------
The obvious endpoint, `/jobs/<jid>/vertices/<vid>/subtasks/metrics?get=…&agg=…`,
returns an EMPTY LIST on this cluster (Flink 1.19 via the Kubernetes Operator) —
verified against a live job: even the no-argument form, which is supposed to list
the available metric ids, returns `[]`. Using it silently wrote NA for every
metric.

What does work is the vertex-level endpoint with SUBTASK-INDEX-PREFIXED names:

    /jobs/<jid>/vertices/<vid>/metrics?get=0.busyTimeMsPerSecond,1.busyTimeMsPerSecond,…
    -> [{"id":"0.busyTimeMsPerSecond","value":"0.0"}, …]

so this script expands each metric name across the vertex's subtasks and
aggregates itself: max across subtasks for the time-share metrics (the slowest
subtask sets the pipeline's pace under a skewed key distribution) and the sum for
the record counters (throughput is additive).

Everything is best-effort: a failure writes an NA row rather than raising, so an
unreachable JobManager (normal during an autoscaler rescale) never stops the
collection. Exit code 3 means "no job reachable", which the caller uses to decide
whether to rebuild its tunnel.
"""

import argparse
import datetime
import json
import os
import sys
import urllib.error
import urllib.request

# Time-share metrics, in ms per second (so 0-1000). Aggregated with max.
TIME_METRICS = ["busyTimeMsPerSecond", "backPressuredTimeMsPerSecond", "idleTimeMsPerSecond"]
# Record-rate metrics. Aggregated with sum.
RATE_METRICS = ["numRecordsInPerSecond", "numRecordsOutPerSecond"]


def get_json(base_url, path, timeout):
    try:
        with urllib.request.urlopen(base_url + path, timeout=timeout) as resp:
            return json.load(resp)
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return None


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clean(name):
    return str(name).replace(",", ";").replace("\n", " ").replace("\r", " ")


def na(value):
    return "NA" if value is None else value


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


PENDING_RETRY_SECONDS = 60


def resolve_pending_metric(base_url, job_id, vid, timeout, cache_path):
    """Find the id of the Kafka source's pendingRecords metric for this vertex.

    Its full name is decorated with the operator scope, and the Kafka source
    registers it LAZILY — it does not exist until the reader has actually fetched
    from its partitions. So it is discovered from the vertex's metric-id list
    rather than guessed, and a NEGATIVE result is only cached for
    PENDING_RETRY_SECONDS: caching "not found" permanently is exactly the bug
    that made the column read NA for a whole run, because the first poll happened
    while the job was still idle. Positive results are cached for good (the id
    list is ~430 entries, so re-fetching it every poll is not free).
    """
    cache = {}
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                cache = json.load(fh)
        except (json.JSONDecodeError, OSError):
            cache = {}

    entry = cache.get(vid)
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if isinstance(entry, dict):
        if entry.get("ids"):
            return entry["ids"]
        if now - entry.get("checked_at", 0) < PENDING_RETRY_SECONDS:
            return None

    ids = get_json(base_url, f"/jobs/{job_id}/vertices/{vid}/metrics", timeout) or []
    matches = [m.get("id") for m in ids if "pendingrecords" in str(m.get("id", "")).lower()]
    cache[vid] = {"ids": matches, "checked_at": now}
    if cache_path:
        try:
            with open(cache_path, "w") as fh:
                json.dump(cache, fh)
        except OSError:
            pass
    return matches or None


def fetch_vertex_metrics(base_url, job_id, vid, parallelism, timeout, pending_ids):
    """Returns (time_metric_max, rate_metric_sum, pending_sum)."""
    subtasks = range(max(1, int(parallelism or 1)))
    names = [f"{i}.{m}" for m in TIME_METRICS + RATE_METRICS for i in subtasks]
    if pending_ids:
        names += list(pending_ids)

    values = {}
    # Chunked: a wide job would otherwise build a URL long enough to be refused.
    chunk = 60
    for start in range(0, len(names), chunk):
        part = names[start : start + chunk]
        data = get_json(
            base_url,
            f"/jobs/{job_id}/vertices/{vid}/metrics?get=" + ",".join(part),
            timeout,
        )
        for entry in data or []:
            values[entry.get("id")] = to_float(entry.get("value"))

    time_max, rate_sum = {}, {}
    for metric in TIME_METRICS:
        found = [values[f"{i}.{metric}"] for i in subtasks if values.get(f"{i}.{metric}") is not None]
        time_max[metric] = max(found) if found else None
    for metric in RATE_METRICS:
        found = [values[f"{i}.{metric}"] for i in subtasks if values.get(f"{i}.{metric}") is not None]
        rate_sum[metric] = sum(found) if found else None

    pending = None
    if pending_ids:
        found = [values[i] for i in pending_ids if values.get(i) is not None]
        pending = sum(found) if found else None
    return time_max, rate_sum, pending


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--parallelism", required=True)
    ap.add_argument("--backpressure", required=True)
    ap.add_argument("--throughput", required=True)
    ap.add_argument("--checkpoints", required=True)
    ap.add_argument("--timeout", type=float, default=3.0)
    args = ap.parse_args()

    ts = now_iso()
    par_rows, bp_rows, tp_rows, cp_rows = [], [], [], []
    cache_path = os.path.join(os.path.dirname(os.path.abspath(args.parallelism)), ".pending_metric_ids.json")

    jobs = get_json(args.base_url, "/jobs", args.timeout)
    job_id = ""
    if jobs:
        running = [j["id"] for j in jobs.get("jobs", []) if j.get("status") == "RUNNING"]
        if running:
            job_id = running[0]
        elif jobs.get("jobs"):
            job_id = jobs["jobs"][0]["id"]

    if not job_id:
        par_rows.append(f"{ts},NA,NA,NA,NA,NA")
        bp_rows.append(f"{ts},NA,NA,NA,NA,NA,NA")
        tp_rows.append(f"{ts},NA,NA,NA,NA,NA,NA")
        cp_rows.append(f"{ts},NA,NA,NA,NA,NA,NA")
        write(args, par_rows, bp_rows, tp_rows, cp_rows)
        return 3

    detail = get_json(args.base_url, f"/jobs/{job_id}", args.timeout)
    vertices = (detail or {}).get("vertices", []) or []

    if not vertices:
        par_rows.append(f"{ts},{job_id},NA,NA,NA,NA")
        bp_rows.append(f"{ts},{job_id},NA,NA,NA,NA,NA")
        tp_rows.append(f"{ts},{job_id},NA,NA,NA,NA,NA")

    for v in vertices:
        vid = v.get("id", "NA")
        vname = clean(v.get("name", "NA"))
        parallelism = v.get("parallelism")
        par_rows.append(f"{ts},{job_id},{vid},{vname},{na(parallelism)},{na(v.get('status'))}")

        pending_ids = resolve_pending_metric(args.base_url, job_id, vid, args.timeout, cache_path)
        time_max, rate_sum, pending = fetch_vertex_metrics(
            args.base_url, job_id, vid, parallelism, args.timeout, pending_ids
        )

        bp_rows.append(
            f"{ts},{job_id},{vid},{vname},"
            f"{na(time_max.get('busyTimeMsPerSecond'))},"
            f"{na(time_max.get('backPressuredTimeMsPerSecond'))},"
            f"{na(time_max.get('idleTimeMsPerSecond'))}"
        )
        tp_rows.append(
            f"{ts},{job_id},{vid},{vname},"
            f"{na(rate_sum.get('numRecordsInPerSecond'))},"
            f"{na(rate_sum.get('numRecordsOutPerSecond'))},"
            f"{na(pending)}"
        )

    cps = get_json(args.base_url, f"/jobs/{job_id}/checkpoints", args.timeout) or {}
    counts = cps.get("counts", {}) or {}
    latest = ((cps.get("latest") or {}).get("completed")) or {}
    cp_rows.append(
        f"{ts},{job_id},{na(counts.get('completed'))},{na(counts.get('failed'))},"
        f"{na(counts.get('in_progress'))},{na(latest.get('duration'))},"
        f"{na(latest.get('state_size'))}"
    )

    write(args, par_rows, bp_rows, tp_rows, cp_rows)
    return 0


def write(args, par_rows, bp_rows, tp_rows, cp_rows):
    for path, rows in (
        (args.parallelism, par_rows),
        (args.backpressure, bp_rows),
        (args.throughput, tp_rows),
        (args.checkpoints, cp_rows),
    ):
        if not rows:
            continue
        with open(path, "a") as fh:
            fh.write("\n".join(rows) + "\n")


if __name__ == "__main__":
    sys.exit(main())
