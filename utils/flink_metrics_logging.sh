#!/bin/bash
# utils/flink_metrics_logging.sh
#
# Consolidated in-system metrics collector for the Flink job under test.
# One poll loop, one port-forward tunnel, four CSVs:
#
#   parallelism_<ts>.csv   timestamp,job_id,vertex_id,vertex_name,parallelism,status
#   backpressure_<ts>.csv  timestamp,job_id,vertex_id,vertex_name,busy_ms_per_sec,
#                          backpressured_ms_per_sec,idle_ms_per_sec
#   throughput_<ts>.csv    timestamp,job_id,vertex_id,vertex_name,records_in_per_sec,
#                          records_out_per_sec
#   checkpoints_<ts>.csv   timestamp,job_id,completed,failed,in_progress,
#                          latest_duration_ms,latest_size_bytes
#
# WHY this supersedes utils/parallelism_logging.sh:
#   * plot_benchmark_timeline.py has always expected a backpressure_*.csv panel,
#     but nothing in the harness ever produced one — so every stored run
#     silently skipped it. Busy/backpressured/idle time is what actually
#     explains the latency curves (and is the signal the Flink autoscaler
#     itself scales on), so it is collected here alongside parallelism.
#   * The old script started a FRESH `kubectl port-forward` on every poll and
#     slept 1.5s waiting for it — a ~30% duty cycle of process churn at a 5s
#     interval, doubled if a second poller ran. Here ONE tunnel is kept alive
#     and rebuilt only when a poll fails to reach the job. That still handles
#     the original concern (the JobManager pod is recreated on every autoscaler
#     rescale, killing any tunnel bound to the old pod) — reactively rather
#     than pre-emptively. The tunnel PID is tracked, killed on stop, and
#     backed by a pkill safety net, so it cannot outlive the run.
#
# Usage (from run.sh):
#   source ./utils/flink_metrics_logging.sh
#   start_flink_metrics_logging
#   ...
#   stop_flink_metrics_logging
#
# Optional variables (set before calling start_flink_metrics_logging):
#   FLINK_METRICS_POLL_INTERVAL  seconds between polls (default 5)
#   FLINK_METRICS_EXEC_TIMEOUT   seconds before a single REST call is given up on (default 3)
#   FLINK_METRICS_REST_SERVICE   Flink REST Service name (default "luftdaten-job-rest")
#   FLINK_METRICS_LOCAL_PORT     local port for the tunnel (default 18081)
#   BENCHMARK_TIMELINE_DIR       output directory (default "benchmark_timeline")

start_flink_metrics_logging() {
    local poll_interval="${FLINK_METRICS_POLL_INTERVAL:-5}"
    local exec_timeout="${FLINK_METRICS_EXEC_TIMEOUT:-3}"
    local rest_service="${FLINK_METRICS_REST_SERVICE:-luftdaten-job-rest}"
    local local_port="${FLINK_METRICS_LOCAL_PORT:-18081}"
    local outdir="${BENCHMARK_TIMELINE_DIR:-benchmark_timeline}"
    local poller="${SCRIPT_DIR:-.}/utils/flink_metrics_poll.py"
    local ts
    ts="$(date +%Y%m%d_%H%M%S)"

    mkdir -p "$outdir"
    PARALLELISM_OUTPUT="$outdir/parallelism_${ts}.csv"
    BACKPRESSURE_OUTPUT="$outdir/backpressure_${ts}.csv"
    THROUGHPUT_OUTPUT="$outdir/throughput_${ts}.csv"
    CHECKPOINTS_OUTPUT="$outdir/checkpoints_${ts}.csv"

    echo "timestamp,job_id,vertex_id,vertex_name,parallelism,status" > "$PARALLELISM_OUTPUT"
    echo "timestamp,job_id,vertex_id,vertex_name,busy_ms_per_sec,backpressured_ms_per_sec,idle_ms_per_sec" > "$BACKPRESSURE_OUTPUT"
    echo "timestamp,job_id,vertex_id,vertex_name,records_in_per_sec,records_out_per_sec,pending_records" > "$THROUGHPUT_OUTPUT"
    echo "timestamp,job_id,completed,failed,in_progress,latest_duration_ms,latest_size_bytes" > "$CHECKPOINTS_OUTPUT"

    echo "Starting Flink metrics logging -> parallelism/backpressure/throughput/checkpoints (${ts})"

    (
        # set -e is inherited into this subshell; without disabling it the first
        # failed poll (normal right after a rescale, while the new JobManager
        # comes up) would kill the whole loop.
        set +e

        PF_PID=""
        drop_tunnel() {
            if [ -n "$PF_PID" ]; then
                kill -9 "$PF_PID" 2>/dev/null
                wait "$PF_PID" 2>/dev/null
                PF_PID=""
            fi
        }
        trap 'drop_tunnel' EXIT
        trap 'drop_tunnel; exit 0' TERM INT

        while true; do
            if [ -z "$PF_PID" ]; then
                kubectl port-forward "svc/${rest_service}" "${local_port}:8081" >/dev/null 2>&1 &
                PF_PID=$!
                sleep 1.5
            fi

            timeout -k 2 $((exec_timeout * 8)) python3 "$poller" \
                --base-url "http://localhost:${local_port}" \
                --parallelism "$PARALLELISM_OUTPUT" \
                --backpressure "$BACKPRESSURE_OUTPUT" \
                --throughput "$THROUGHPUT_OUTPUT" \
                --checkpoints "$CHECKPOINTS_OUTPUT" \
                --timeout "$exec_timeout" >/dev/null 2>&1
            RC=$?

            # rc 3 = REST reachable but no job (or tunnel dead). Either way the
            # cheapest correct response is to rebuild the tunnel so the next
            # poll re-resolves the Service to the current JobManager pod.
            if [ "$RC" -ne 0 ]; then
                drop_tunnel
            fi

            sleep "$poll_interval"
        done
    ) &
    FLINK_METRICS_LOG_PID=$!

    echo "Flink metrics logging PID: $FLINK_METRICS_LOG_PID"
}

stop_flink_metrics_logging() {
    local rest_service="${FLINK_METRICS_REST_SERVICE:-luftdaten-job-rest}"
    local local_port="${FLINK_METRICS_LOCAL_PORT:-18081}"

    echo "Stopping Flink metrics logging..."
    kill "$FLINK_METRICS_LOG_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$FLINK_METRICS_LOG_PID" 2>/dev/null || true

    # Safety net: the subshell's EXIT trap normally reaps the tunnel, but a
    # SIGKILL skips traps, so make sure nothing on this port survives.
    pkill -9 -f "kubectl port-forward svc/${rest_service} ${local_port}:8081" 2>/dev/null || true

    echo "Flink metrics saved:"
    echo "  $PARALLELISM_OUTPUT"
    echo "  $BACKPRESSURE_OUTPUT"
    echo "  $THROUGHPUT_OUTPUT"
    echo "  $CHECKPOINTS_OUTPUT"
}
