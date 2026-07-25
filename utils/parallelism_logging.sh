#!/bin/bash
# utils/parallelism_logging.sh
#
# Periodically polls the Flink REST API (GET /jobs/<jobid>) for each job
# vertex's current parallelism and status, writing to its own CSV. Correlate
# with benchmark_timeline/*.csv (ScalingReport/ScalingLimited events + Kafka
# lag) to see the actually-applied parallelism alongside the autoscaler's
# decisions and requests.
#
# Design note: uses a FRESH, short-lived `kubectl port-forward` per poll,
# rather than one long-lived tunnel. The JobManager pod is recreated on
# every autoscaler-triggered rescale (Suspended -> Submit -> new pod name),
# which kills any pre-existing port-forward pointed at the old pod and does
# not reconnect on its own. A per-iteration tunnel self-heals automatically
# (it re-resolves to whatever pod currently backs the Service) with no
# explicit "has JobManager changed" detection needed, and — importantly —
# it cannot outlive the polling loop even if the loop itself is killed with
# SIGKILL, avoiding the class of long-lived-orphan-process bug already hit
# more than once this week with `kubectl get events --watch`.
#
# Usage (from run.sh):
#   source ./utils/parallelism_logging.sh
#   start_parallelism_logging   # sets PARALLELISM_OUTPUT, PARALLELISM_LOG_PID
#   ...
#   stop_parallelism_logging
#
# Optional variables (set before calling start_parallelism_logging):
#   PARALLELISM_POLL_INTERVAL   seconds between polls (default 5)
#   PARALLELISM_EXEC_TIMEOUT    seconds before a single curl is given up on (default 3)
#   PARALLELISM_REST_SERVICE    Flink REST Service name (default "luftdaten-job-rest")
#   PARALLELISM_LOCAL_PORT      local port for the short-lived tunnel (default 18081)
#
# Requires: python3 (stdutils json only, no extra packages) for parsing the
# REST response — already used elsewhere in this project (latency-plotter.py).

start_parallelism_logging() {
    local poll_interval="${PARALLELISM_POLL_INTERVAL:-5}"
    local exec_timeout="${PARALLELISM_EXEC_TIMEOUT:-3}"
    local rest_service="${PARALLELISM_REST_SERVICE:-luftdaten-job-rest}"
    local local_port="${PARALLELISM_LOCAL_PORT:-18081}"

    mkdir -p benchmark_timeline
    PARALLELISM_OUTPUT="benchmark_timeline/parallelism_$(date +%Y%m%d_%H%M%S).csv"
    echo "timestamp,job_id,vertex_id,vertex_name,parallelism,status" > "$PARALLELISM_OUTPUT"

    echo "Starting parallelism logging -> $PARALLELISM_OUTPUT"

    (
        # set -e is inherited from the parent script into this subshell.
        # Without disabling it here, the first failed curl/port-forward
        # (e.g. right after a rescale, before the new JobManager pod is
        # ready) would silently kill this entire loop — same class of bug
        # already fixed in utils/timeline_logging.sh.
        set +e
        while true; do
            TS=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ')

            kubectl port-forward "svc/${rest_service}" "${local_port}:8081" >/dev/null 2>&1 &
            PF_PID=$!
            sleep 1.5   # give the tunnel a moment to establish

            JOBS_JSON=$(timeout -k 1 "$exec_timeout" curl -s "http://localhost:${local_port}/jobs" 2>/dev/null)

            JOB_ID=$(echo "$JOBS_JSON" | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get("jobs", [])
    running = [j["id"] for j in jobs if j.get("status") == "RUNNING"]
    print(running[0] if running else (jobs[0]["id"] if jobs else ""))
except Exception:
    print("")
' 2>/dev/null)

            if [ -n "$JOB_ID" ]; then
                JOB_DETAIL=$(timeout -k 1 "$exec_timeout" curl -s "http://localhost:${local_port}/jobs/${JOB_ID}" 2>/dev/null)
            else
                JOB_DETAIL=""
            fi

            # Tear the tunnel down before sleeping — nothing from this
            # iteration survives into the next one, or past a kill of the
            # outer loop.
            kill -9 "$PF_PID" 2>/dev/null
            wait "$PF_PID" 2>/dev/null

            if [ -z "$JOB_ID" ] || [ -z "$JOB_DETAIL" ]; then
                echo "${TS},NA,NA,NA,NA,NA" >> "$PARALLELISM_OUTPUT"
            else
                echo "$JOB_DETAIL" | python3 -c '
import sys, json
ts = sys.argv[1]
job_id = sys.argv[2]
try:
    data = json.load(sys.stdin)
    for v in data.get("vertices", []):
        vid = v.get("id", "NA")
        name = str(v.get("name", "NA")).replace(",", ";").replace("\n", " ")
        parallelism = v.get("parallelism", "NA")
        status = v.get("status", "NA")
        print(f"{ts},{job_id},{vid},{name},{parallelism},{status}")
except Exception:
    pass
' "$TS" "$JOB_ID" >> "$PARALLELISM_OUTPUT" 2>/dev/null
            fi

            sleep "$poll_interval"
        done
    ) &
    PARALLELISM_LOG_PID=$!

    echo "Parallelism logging PID: $PARALLELISM_LOG_PID"
}

stop_parallelism_logging() {
    local rest_service="${PARALLELISM_REST_SERVICE:-luftdaten-job-rest}"
    local local_port="${PARALLELISM_LOCAL_PORT:-18081}"

    echo "Stopping parallelism logging..."
    kill "$PARALLELISM_LOG_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$PARALLELISM_LOG_PID" 2>/dev/null || true

    # Belt-and-suspenders: if the loop was killed mid-iteration, its
    # port-forward child (bounded to at most ~1.5-3s of extra lifetime)
    # might still be exiting — make sure nothing with this exact port
    # lingers.
    pkill -9 -f "kubectl port-forward svc/${rest_service} ${local_port}:8081" 2>/dev/null || true

    echo "Parallelism data saved: $PARALLELISM_OUTPUT"
}
