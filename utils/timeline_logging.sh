#!/bin/bash
# utils/timeline_logging.sh
#
# Provides start_timeline_logging / stop_timeline_logging for capturing a
# combined benchmark timeline: Kafka consumer-group lag (per partition) and
# live Kubernetes events for the FlinkDeployment, both written with matching
# UTC timestamps into one CSV so they can be correlated after the run.
#
# Usage (from run.sh):
#   source ./utils/timeline_logging.sh
#   start_timeline_logging   # sets TIMELINE_OUTPUT, KAFKA_LOG_PID,
#                             # KUBECTL_EVENTS_PID, FLINK_LOG_PID
#   ...
#   stop_timeline_logging    # kills all three, closes fd 3
#
# Required variables (set these before calling start_timeline_logging,
# typically in .env or at the top of run.sh):
#   KAFKA_GROUP           consumer group id to poll (e.g. "luftdaten-benchmark")
#   KAFKA_BROKER_POD       broker pod name (e.g. "my-cluster-dual-role-0")
#   KAFKA_NAMESPACE        namespace the broker pod lives in (e.g. "kafka")
#   KAFKA_POLL_INTERVAL    seconds between lag polls (e.g. 2)
#   KAFKA_EXEC_TIMEOUT     seconds before `kubectl exec` is given up on (e.g. 3)

start_timeline_logging() {
    mkdir -p benchmark_timeline
    TIMELINE_OUTPUT="benchmark_timeline/$(date +%Y%m%d_%H%M%S).csv"
    echo "timestamp,event_type,partition,current_offset,log_end_offset,lag,detail" > "$TIMELINE_OUTPUT"

    echo "Starting benchmark timeline logging -> $TIMELINE_OUTPUT"

    # --- Kafka per-partition lag polling ---
    (
        # set -e is inherited from the parent script into this subshell.
        # Without disabling it here, the very first failed `kubectl exec`
        # (e.g. because the consumer group hasn't been created yet right
        # after Flink starts) would silently kill this entire loop before
        # it ever writes a row.
        set +e
        while true; do
            TS=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ')

            # -k 2 forces SIGKILL 2s after SIGTERM if kubectl doesn't exit
            # on its own — without it, `timeout` can hang past its own
            # deadline on an unresponsive kubectl.
            RAW=$(timeout -k 2 "$KAFKA_EXEC_TIMEOUT" kubectl exec "$KAFKA_BROKER_POD" -n "$KAFKA_NAMESPACE" -- \
                bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
                --describe --group "$KAFKA_GROUP" 2>/dev/null)

            if [ -z "$RAW" ]; then
                echo "${TS},kafka_lag,NA,NA,NA,NA," >> "$TIMELINE_OUTPUT"
            else
                # $3 ~ /^[0-9]+$/ guards against the table header row
                # ("PARTITION,CURRENT-OFFSET,...") or an error message
                # ("consumer group command failed") being parsed as if it
                # were a real data row — only lines where the 3rd field is
                # actually a numeric partition index are kept.
                echo "$RAW" | awk -v ts="$TS" '
                    $3 ~ /^[0-9]+$/ {
                        printf "%s,kafka_lag,%s,%s,%s,%s,\n", ts, $3, $4, $5, $6
                    }
                ' >> "$TIMELINE_OUTPUT"
            fi

            sleep "$KAFKA_POLL_INTERVAL"
        done
    ) &
    KAFKA_LOG_PID=$!

    # --- Flink events stream ---
    # `cmd1 | cmd2 &` only lets $! capture the PID of cmd2 (the last stage) —
    # cmd1 (kubectl get events --watch) would keep running as an untracked
    # orphan even after cmd2 is killed. Process substitution lets us capture
    # both PIDs explicitly so both can be terminated on stop.
    exec 3< <(kubectl get events --watch --field-selector involvedObject.kind=FlinkDeployment \
        -o custom-columns=TIME:.lastTimestamp,REASON:.reason,MESSAGE:.message --no-headers)
    KUBECTL_EVENTS_PID=$!

    (
        set +e
        while read -r line <&3; do
            TS=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ')
            ESCAPED_LINE=$(echo "$line" | sed 's/"/""/g')
            echo "${TS},flink_event,,,,,\"${ESCAPED_LINE}\"" >> "$TIMELINE_OUTPUT"
        done
    ) &
    FLINK_LOG_PID=$!

    echo "Timeline logging PIDs: kafka=$KAFKA_LOG_PID kubectl_events=$KUBECTL_EVENTS_PID flink_reader=$FLINK_LOG_PID"
}

stop_timeline_logging() {
    echo "Stopping timeline logging..."
    # Terminate all three tracked processes: the kafka-lag polling loop, the
    # raw `kubectl get events --watch` process, and the while-read consumer
    # of it.
    kill "$KAFKA_LOG_PID" "$KUBECTL_EVENTS_PID" "$FLINK_LOG_PID" 2>/dev/null || true

    # Give them a moment to exit cleanly, then force-kill anything still
    # alive — avoids the script hanging indefinitely if a kubectl process
    # ignores SIGTERM.
    sleep 2
    kill -9 "$KAFKA_LOG_PID" "$KUBECTL_EVENTS_PID" "$FLINK_LOG_PID" 2>/dev/null || true

    exec 3<&- 2>/dev/null || true  # close the process-substitution fd
    echo "Timeline saved: $TIMELINE_OUTPUT"
}
