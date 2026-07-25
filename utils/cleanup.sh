# utils/cleanup.sh
cleanup_on_exit() {
    echo "Cleaning up monitoring processes..."
    kill "$KAFKA_LOG_PID" "$KUBECTL_EVENTS_PID" "$FLINK_LOG_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$KAFKA_LOG_PID" "$KUBECTL_EVENTS_PID" "$FLINK_LOG_PID" 2>/dev/null || true
}
