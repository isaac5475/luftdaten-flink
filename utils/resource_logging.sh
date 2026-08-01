#!/bin/bash
# utils/resource_logging.sh
#
# Provides start_resource_logging / stop_resource_logging for periodically
# snapshotting `kubectl top pods` (CPU/memory) across the namespaces relevant
# to the benchmark (default: Flink + datagen + rtracker, kafka: the broker).
# Written to its own CSV — separate from the kafka_lag/flink_event timeline,
# since the schema (pod, cpu, memory) doesn't fit that one.
#
# Requires metrics-server to be enabled in the cluster:
#   minikube addons enable metrics-server
# Without it, kubectl top returns an error and every row will be logged as NA
# (visible immediately in the output, not silently missing).
#
# Usage (from run.sh):
#   source ./utils/resource_logging.sh
#   start_resource_logging   # sets RESOURCE_OUTPUT, RESOURCE_LOG_PID
#   ...
#   stop_resource_logging    # kills the polling loop
#
# Optional variables (set before calling start_resource_logging):
#   RESOURCE_POLL_INTERVAL   seconds between snapshots (default 5)
#   RESOURCE_EXEC_TIMEOUT    seconds before `kubectl top` is given up on (default 3)
#   RESOURCE_NAMESPACES      space-separated namespaces to poll (default "default kafka")

start_resource_logging() {
    local poll_interval="${RESOURCE_POLL_INTERVAL:-5}"
    local exec_timeout="${RESOURCE_EXEC_TIMEOUT:-3}"
    local namespaces="${RESOURCE_NAMESPACES:-default kafka}"

    # See the note in utils/timeline_logging.sh: honour BENCHMARK_TIMELINE_DIR
    # so a --run-dir run keeps all of its artifacts together.
    local outdir="${BENCHMARK_TIMELINE_DIR:-benchmark_timeline}"
    mkdir -p "$outdir"
    RESOURCE_OUTPUT="$outdir/resources_$(date +%Y%m%d_%H%M%S).csv"
    echo "timestamp,namespace,pod,cpu,memory" > "$RESOURCE_OUTPUT"

    echo "Starting resource usage logging -> $RESOURCE_OUTPUT"

    (
        set +e
        while true; do
            TS=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ')

            for ns in $namespaces; do
                RAW=$(timeout -k 2 "$exec_timeout" kubectl top pods -n "$ns" --no-headers 2>/dev/null)

                if [ -z "$RAW" ]; then
                    echo "${TS},${ns},NA,NA,NA" >> "$RESOURCE_OUTPUT"
                else
                    echo "$RAW" | awk -v ts="$TS" -v ns="$ns" '
                        NF >= 3 {
                            printf "%s,%s,%s,%s,%s\n", ts, ns, $1, $2, $3
                        }
                    ' >> "$RESOURCE_OUTPUT"
                fi
            done

            sleep "$poll_interval"
        done
    ) &
    RESOURCE_LOG_PID=$!

    echo "Resource logging PID: $RESOURCE_LOG_PID"
}

stop_resource_logging() {
    echo "Stopping resource usage logging..."
    kill "$RESOURCE_LOG_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$RESOURCE_LOG_PID" 2>/dev/null || true
    echo "Resource usage saved: $RESOURCE_OUTPUT"
}
