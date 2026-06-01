#!/usr/bin/env sh
set -eu

: "${JOB_MANAGER_HOST:=jobmanager}"
: "${JOB_MANAGER_REST_PORT:=8081}"
: "${JOB_JAR:=/opt/jars/luftdaten-flink-0.1.jar}"
: "${JOB_CLASS:=com.yourname.luftdaten.DailyTemperatureBME280StreamJobDataGen}"
: "${SOURCE_PORT:=9000}"
: "${SINK_HOST:=sink}"
: "${SINK_PORT:=9001}"

wait_for_port() {
  host="$1"
  port="$2"
  label="$3"

  echo "Waiting for ${label} at ${host}:${port}..."
  until nc -z "$host" "$port" >/dev/null 2>&1; do
    sleep 2
  done
}

echo "Waiting for Flink JobManager at http://${JOB_MANAGER_HOST}:${JOB_MANAGER_REST_PORT}/overview..."
until curl -fsS "http://${JOB_MANAGER_HOST}:${JOB_MANAGER_REST_PORT}/overview" >/dev/null 2>&1; do
  sleep 2
done

wait_for_port "$SOURCE_HOST" "$SOURCE_PORT" "source socket"
wait_for_port "$SINK_HOST" "$SINK_PORT" "sink socket"

echo "Submitting ${JOB_CLASS} from ${JOB_JAR}"
exec /opt/flink/bin/flink run \
  -m "${JOB_MANAGER_HOST}:${JOB_MANAGER_REST_PORT}" \
  -c "${JOB_CLASS}" \
  "${JOB_JAR}" \
  "${SOURCE_HOST}" "${SOURCE_PORT}" "${SINK_HOST}" "${SINK_PORT}"
