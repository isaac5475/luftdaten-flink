#!/bin/bash
set -e

# Refuse to start if another instance of this script is already running.
#
# Using flock instead of a PID file: a PID-file lock (checking `kill -0` on
# a stored PID) is fragile on a long-uptime shared server — PIDs get
# recycled, so a stale lockfile can point at PID N right as some unrelated
# process reuses that same number, making the check succeed even though
# run.sh isn't actually running (this happened repeatedly in practice).
# flock ties the lock to an open file descriptor at the kernel level: it's
# released automatically the instant the holding process exits, for any
# reason, with no PID comparison and no stale-lock cleanup required.
LOCKFILE="/tmp/luftdaten-run.lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "ERROR: another instance of run.sh is already running."
    echo "Check for it with: ps aux | grep run.sh"
    exit 1
fi
# No trap needed to release the lock — closing fd 200 (which happens
# automatically when the script exits, for any reason) releases it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source "$SCRIPT_DIR/utils/progress_bar.sh"
source "$SCRIPT_DIR/utils/timeline_logging.sh"
source "$SCRIPT_DIR/utils/resource_logging.sh"
source "$SCRIPT_DIR/utils/flink_metrics_logging.sh"

# ---------------------------------------------------------------------------
# Arguments
#
# --skip-build / -s   skip rebuilding and reloading the datagen/rtracker/
#                     luftdaten-flink images. Useful when only
#                     spec.job.entryClass changed (e.g. when called repeatedly
#                     from run_all_queries.sh) and the jar/image themselves are
#                     already up to date on this Minikube.
# --skip-infra        skip the one-time cluster prerequisites (minikube,
#                     metrics-server, cert-manager, Flink operator, Strimzi,
#                     Kafka CR, python venv). run_all_queries.sh sets this for
#                     every query after the first, where they are invariants —
#                     re-checking them cost ~15-25s per query for nothing.
# --run-dir DIR       write this run's artifacts straight into DIR instead of
#                     the shared benchmark_timeline/ + plots/ directories.
#                     Replaces the old `find -newer` result shuffling in
#                     run_all_queries.sh, which silently depended on file
#                     mtimes and could pick up (or miss) files.
# --plot-pattern-out FILE
#                     Capture the datagen burst pattern as it is actually
#                     produced (via patternTest_timeseries.py against the
#                     `datagen` Service's TCP broadcast port, which every
#                     record is fanned out to independently of the Kafka
#                     publish path — connecting a client to it cannot affect
#                     delivery) and save it to FILE. Optional: the load
#                     pattern is identical across every query in a suite, so
#                     run_all_queries.sh only passes this for the first one.
# ---------------------------------------------------------------------------
SKIP_BUILD=false
SKIP_INFRA=false
RUN_DIR=""
PLOT_PATTERN_OUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build|-s)     SKIP_BUILD=true; shift ;;
        --skip-infra)        SKIP_INFRA=true; shift ;;
        --run-dir)           RUN_DIR="$2"; shift 2 ;;
        --plot-pattern-out)  PLOT_PATTERN_OUT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; echo "Usage: ./run.sh [--skip-build] [--skip-infra] [--run-dir DIR] [--plot-pattern-out FILE]"; exit 1 ;;
    esac
done

source ./.env

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

if [ -n "$RUN_DIR" ]; then
    mkdir -p "$RUN_DIR"
    BENCHMARK_TIMELINE_DIR="$RUN_DIR"
    PLOTS_DIR="$RUN_DIR"
    LATENCY_DIR="$RUN_DIR"
else
    BENCHMARK_TIMELINE_DIR="benchmark_timeline"
    PLOTS_DIR="plots"
    LATENCY_DIR="latency-logs"
    mkdir -p "$BENCHMARK_TIMELINE_DIR" "$PLOTS_DIR" "$LATENCY_DIR"
fi
export BENCHMARK_TIMELINE_DIR

# Config used by utils/timeline_logging.sh
KAFKA_GROUP="luftdaten-benchmark"
KAFKA_TOPIC="bid"
KAFKA_BROKER_POD="my-cluster-dual-role-0"
KAFKA_NAMESPACE="kafka"
KAFKA_POLL_INTERVAL=${KAFKA_POLL_INTERVAL:-5}
# Was 3s, which is SHORTER than a `kafka-consumer-groups.sh --describe` (it
# starts a JVM inside the broker pod on every poll). Measured consequence in the
# archived runs: 8-66% of lag samples came back empty and were written as NA,
# with gaps up to 125s — and the polls failed preferentially while the broker was
# busy, i.e. exactly during the bursts, so peak lag was systematically
# under-sampled. 20s is comfortably above the observed describe latency.
KAFKA_EXEC_TIMEOUT=${KAFKA_EXEC_TIMEOUT:-20}

# Config used by utils/flink_metrics_logging.sh — the Flink Kubernetes Operator
# creates a "<deployment-name>-rest" Service automatically.
FLINK_METRICS_REST_SERVICE="luftdaten-job-rest"
FLINK_METRICS_POLL_INTERVAL=${FLINK_METRICS_POLL_INTERVAL:-5}
FLINK_METRICS_EXEC_TIMEOUT=${FLINK_METRICS_EXEC_TIMEOUT:-3}

DATAGEN_CONFIG_YAML="k8s/DatagenConfig.yaml"
DATAGEN_JOB_YAML="k8s/DatagenJob.yaml"
FLINK_DEPLOYMENT_YAML="k8s/FlinkDeployment.yaml"

# ---------------------------------------------------------------------------
# Helpers
#
# `bc` is NOT installed on this host — the previous version of this script
# piped the timing-variance calculation through it, which silently produced an
# empty value and therefore an INVALID metadata JSON (`"...": ,`). All
# arithmetic here goes through awk, which is always present.
# ---------------------------------------------------------------------------
fdiv() { awk -v a="$1" -v b="$2" -v s="${3:-2}" 'BEGIN{ if (b+0==0) print "null"; else printf "%.*f", s, a/b }'; }
now_epoch_ms() { date +%s%3N; }
iso_now() { date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"; }

# Sum of the topic's latest offsets across all partitions = total records ever
# produced into it (the topic is recreated empty for every run, so this is the
# authoritative count of what the load generator actually delivered).
topic_end_offsets() {
    timeout -k 2 15 kubectl exec "$KAFKA_BROKER_POD" -n "$KAFKA_NAMESPACE" -- \
        bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
        | awk -F':' '{s+=$3} END{print s+0}'
}

# Total consumer lag across partitions for the benchmark's consumer group.
consumer_lag() {
    timeout -k 2 15 kubectl exec "$KAFKA_BROKER_POD" -n "$KAFKA_NAMESPACE" -- \
        bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --describe --group "$KAFKA_GROUP" 2>/dev/null \
        | awk '$3 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {s+=$6} END{print (NR?s+0:"NA")}'
}

# ---------------------------------------------------------------------------
# Read the datagen schedule out of the ConfigMap so the harness can align its
# measurement window with the load it is actually about to generate, instead of
# a hardcoded SLEEP_SECONDS that has to be kept in sync by hand.
# ---------------------------------------------------------------------------
dg() { awk -v key="$1" '
    /config\.kafka\.yaml:/ {inblock=1; next}
    /^  [a-z._]+\.yaml:/  {if (inblock) exit}
    inblock && $1 == key":" {gsub(/#.*/,"",$2); print $2+0; exit}
' "$DATAGEN_CONFIG_YAML"; }

DATAGEN_DURATION=$(dg duration)
DATAGEN_PERIOD_MS=$(dg period)
DATAGEN_DATA_RATE=$(dg data_rate)
DATAGEN_FRACTION=$(awk '/config\.kafka\.yaml:/{b=1} b && /fraction:/{gsub(/#.*/,"",$3); print $3+0; exit}' "$DATAGEN_CONFIG_YAML")
DATAGEN_DISTRIBUTION=$(awk '/config\.kafka\.yaml:/{b=1} b && /distribution:/{gsub(/#.*/,"",$3); print $3+0; exit}' "$DATAGEN_CONFIG_YAML")

: "${DATAGEN_DURATION:=600}"
: "${DATAGEN_PERIOD_MS:=300000}"
: "${DATAGEN_DATA_RATE:=0}"

# Drain margin: how long to keep observing after the generator stops, so the
# backlog-drain tail (the most interesting part of a bursty run) is captured.
DRAIN_MARGIN=${DRAIN_MARGIN:-90}

# The measurement window now starts when the FIRST record hits Kafka, not when
# the datagen Job object is created. SLEEP_SECONDS is still honoured if set, so
# existing invocations keep working, but the default is derived.
OBSERVE_SECONDS=${SLEEP_SECONDS:-$((DATAGEN_DURATION + DRAIN_MARGIN))}

echo "============================================================"
echo "Datagen schedule (from $DATAGEN_CONFIG_YAML):"
echo "  data_rate     : $DATAGEN_DATA_RATE records per period"
echo "  period        : $DATAGEN_PERIOD_MS ms"
echo "  duration      : ${DATAGEN_DURATION}s"
echo "  fraction      : $DATAGEN_FRACTION (share of records inside the burst)"
echo "  distribution  : $DATAGEN_DISTRIBUTION (share of the period the burst occupies)"
echo "  observe window: ${OBSERVE_SECONDS}s from first record (drain margin ${DRAIN_MARGIN}s)"
echo "============================================================"

# --- Preflight 1: dataset wrap ------------------------------------------------
# The generator walks the CSV circularly (idx = (idx+1) % dataset.size()), and
# event time comes from the dataset's own timestamp column. If it wraps, event
# time jumps ~2 years BACKWARDS mid-run; with forMonotonousTimestamps /
# forBoundedOutOfOrderness watermarks every record after the wrap is late and
# every windowed query (Q3/Q4/Q5) silently drops it and stops emitting. So the
# number of records emitted must stay below the dataset row count.
DATASET_ROWS=${DATASET_ROWS:-30265013}   # /home/docker/dataset/data.csv on the minikube node
EXPECTED_RECORDS=$(awk -v r="$DATAGEN_DATA_RATE" -v d="$DATAGEN_DURATION" -v p="$DATAGEN_PERIOD_MS" \
    'BEGIN{ printf "%d", r * (d * 1000.0 / p) }')
echo "Preflight: expected records this run = $EXPECTED_RECORDS (dataset has $DATASET_ROWS rows)"
if [ "$EXPECTED_RECORDS" -gt "$DATASET_ROWS" ]; then
    echo "  !! WARNING: the generator will WRAP the dataset ($EXPECTED_RECORDS > $DATASET_ROWS)."
    echo "  !! Event time jumps backwards on wrap -> windowed queries (Q3/Q4/Q5) will drop"
    echo "  !! everything after that point and their latency logs will end early."
    echo "  !! Lower data_rate/duration, or switch the jobs to the datagen wall-clock"
    echo "  !! timestamp as the event-time attribute before raising the rate further."
fi

# Expected silent-tail duration, used below to size the pattern-capture read
# timeout and recorded in run metadata. Not printed as a preflight prediction:
# that used to describe what DATAGEN_SPREAD_BASELINE in the YAML *claims* the
# generator will do, decoupled from what the deployed datagen_parallel binary
# actually implements — when the binary and the YAML flag disagreed (as
# happened when the spread-baseline fix was reverted upstream while the YAML
# still referenced it), the message asserted a silent tail had been eliminated
# when it had not.
SPREAD_BASELINE=$(awk '/DATAGEN_SPREAD_BASELINE/{getline; gsub(/[" ]/,"",$2); print $2; exit}' "$DATAGEN_JOB_YAML")
if [ -n "$DATAGEN_FRACTION" ] && [ -n "$DATAGEN_DISTRIBUTION" ]; then
    SILENT_FRACTION=$(awk -v f="$DATAGEN_FRACTION" -v d="$DATAGEN_DISTRIBUTION" \
        'BEGIN{ s = 1 - d - (1 - f); if (s < 0) s = 0; printf "%.3f", s }')
    SILENT_SECONDS=$(awk -v s="$SILENT_FRACTION" -v p="$DATAGEN_PERIOD_MS" 'BEGIN{printf "%.0f", s*p/1000}')
    case "$SPREAD_BASELINE" in
        1|t*|T*|y*|Y*) SILENT_SECONDS=0 ;;
    esac
fi

RUN_START_TIME=$(iso_now)
RUN_START_EPOCH=$(date +%s)
echo "Run started at: $RUN_START_TIME (epoch: $RUN_START_EPOCH)"

# ---------------------------------------------------------------------------
# One-time cluster prerequisites
# ---------------------------------------------------------------------------
if [ "$SKIP_INFRA" = true ]; then
    echo "Skipping cluster prerequisite checks (--skip-infra)."
else
    echo "Checking Minikube status..."
    if minikube status >/dev/null 2>&1; then
        echo "Minikube already running."
    else
        echo "Starting Minikube"
        minikube start --driver=docker --cpus=12 --memory=12g
    fi

    echo "Checking metrics-server (required for resource usage logging)..."
    if ! minikube addons list 2>/dev/null | grep -q "metrics-server.*enabled"; then
        echo "Enabling metrics-server..."
        minikube addons enable metrics-server
        echo "Waiting a moment for metrics-server to start reporting..."
        sleep 15
    fi

    echo "Checking Flink Kubernetes Operator..."
    if ! kubectl get pods -l app.kubernetes.io/name=flink-kubernetes-operator 2>/dev/null | grep -q Running; then
        echo "Installing Flink Kubernetes Operator..."
        kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.18.2/cert-manager.yaml
        kubectl wait --for=condition=Available --timeout=300s -n cert-manager deployment/cert-manager-webhook

        helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/ 2>/dev/null || true
        helm repo update
        helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
        kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=flink-kubernetes-operator --timeout=180s
    else
        echo "Flink Kubernetes Operator already running."
    fi

    echo "Checking ServiceAccount flink..."
    kubectl get serviceaccount flink >/dev/null 2>&1 || {
        kubectl create serviceaccount flink
        kubectl create clusterrolebinding flink-role-binding \
            --clusterrole=edit --serviceaccount=default:flink
    }

    echo "Checking Kafka (Strimzi)..."
    kubectl get namespace kafka >/dev/null 2>&1 || kubectl create namespace kafka

    if ! kubectl get crd kafkas.kafka.strimzi.io >/dev/null 2>&1; then
        echo "Installing Strimzi operator..."
        kubectl apply --server-side -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
        kubectl wait --for=condition=Available --timeout=300s -n kafka deploy/strimzi-cluster-operator
        rm -rf ~/.kube/cache/discovery/
    else
        echo "Strimzi CRDs already installed."
        kubectl wait --for=condition=Available --timeout=120s -n kafka deploy/strimzi-cluster-operator 2>/dev/null || true
    fi

    kubectl apply -f k8s/kafka.yaml
    echo "Waiting for Kafka cluster to be ready..."
    kubectl wait kafka/my-cluster --for=condition=Ready --timeout=600s -n kafka

    # The python venv is a per-suite invariant too; creating it and running pip
    # for every single query cost ~10-20s each time for no benefit.
    if [ ! -x venv/bin/python3 ] || ! venv/bin/python3 -c "import matplotlib, pandas" 2>/dev/null; then
        echo "Preparing python venv..."
        python3 -m venv venv
        venv/bin/pip install -q -r requirements.txt
    else
        echo "python venv already prepared."
    fi
fi

if [ "$SKIP_BUILD" = true ]; then
    echo "Skipping image build/load (--skip-build)."
else
    echo "Building images"
    DOCKER_BUILDKIT=0 docker build -t luftdaten-flink:local .
    (cd infra/latency-tracker && DOCKER_BUILDKIT=0 docker build -t rtracker:local .)
    (cd infra/datagen_parallel && DOCKER_BUILDKIT=0 docker build -t datagen:local -f docker/Dockerfile .)
    minikube image load datagen:local
    minikube image load rtracker:local
    minikube image load luftdaten-flink:local
fi

# ---------------------------------------------------------------------------
# Per-query preparation.
#
# rtracker restart, Kafka topic reset and consumer-group deletion are mutually
# independent, so they run concurrently instead of end-to-end. (Measured
# individually they are only a few seconds each on this cluster — the point is
# that none of them has to wait for the others.)
# ---------------------------------------------------------------------------
echo "Deploying infrastructure..."
kubectl delete configmap/rtracker-config --ignore-not-found
kubectl delete configmap/datagen-config --ignore-not-found
kubectl apply -f k8s/DatagenConfig.yaml
kubectl apply -f k8s/RTrackerConfig.yaml
# Idempotent; only actually needed on a freshly (re)created cluster, but
# --plot-pattern-out depends on it existing, so it is applied unconditionally
# rather than adding a first-run-only special case.
kubectl apply -f k8s/DatagenService.yaml
kubectl apply -f k8s/RTrackerDeployment.yaml
kubectl apply -f k8s/RTrackerService.yaml

(
    kubectl rollout restart deployment/rtracker
    kubectl rollout status deployment/rtracker
) > /tmp/luftdaten-rtracker-restart.log 2>&1 &
RTRACKER_PID=$!

(
    # Fresh topic per run so no records survive from the previous query.
    # --timeout: the delete blocks on the Strimzi topic-operator finalizer, so
    # without it a wedged entity-operator hangs the whole suite indefinitely.
    kubectl delete kafkatopic "$KAFKA_TOPIC" -n kafka --ignore-not-found --timeout=120s
    kubectl apply -f k8s/topic.yaml
    kubectl wait kafkatopic/"$KAFKA_TOPIC" -n kafka --for=condition=Ready --timeout=120s

    # Fixed consumer group id ("luftdaten-benchmark") survives Flink autoscaler
    # restarts without losing offsets, but that means after recreating the topic
    # above, any old committed offsets may point past the new (empty) log.
    # Delete the group so each run starts clean against the fresh topic.
    kubectl exec "$KAFKA_BROKER_POD" -n "$KAFKA_NAMESPACE" -- \
        bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --delete --group "$KAFKA_GROUP" 2>/dev/null || true
) > /tmp/luftdaten-topic-reset.log 2>&1 &
TOPIC_PID=$!

wait "$RTRACKER_PID" || { echo "ERROR: rtracker restart failed:"; cat /tmp/luftdaten-rtracker-restart.log; exit 1; }
wait "$TOPIC_PID"    || { echo "ERROR: topic reset failed:";      cat /tmp/luftdaten-topic-reset.log;      exit 1; }

# Barrier: exactly one rtracker pod must remain before the sink is allowed to
# connect. The Deployment now uses strategy Recreate, but a terminating pod can
# still briefly back the Service, which would point the (parallelism-1) socket
# sink at a dying pod.
for _ in $(seq 1 30); do
    RTRACKER_PODS=$(kubectl get pod -l app=rtracker --no-headers 2>/dev/null | wc -l)
    [ "$RTRACKER_PODS" = "1" ] && break
    sleep 2
done
echo "rtracker restarted (pods: ${RTRACKER_PODS:-?}) and topic '$KAFKA_TOPIC' recreated empty."

# The autoscaler keeps its decision history in a ConfigMap that outlives the
# FlinkDeployment. Left in place it leaks the previous query's parallelism into
# the next run: in the archived half-dataset suite Q1 started at parallelism 1
# instead of the spec's 2, because the preceding suite had scaled it down.
kubectl delete configmap "autoscaler-luftdaten-job" --ignore-not-found >/dev/null 2>&1 || true

# Measurement-hygiene check: the NebulaStream side of the thesis shares this
# cluster. Its pods were resident (and holding ~870 MiB) during past Flink runs.
CO_TENANTS=$(kubectl get pods -A --no-headers 2>/dev/null | grep -cE "nebula|kafka-tcp-bridge" || true)
if [ "${CO_TENANTS:-0}" -gt 0 ]; then
    echo "NOTE: $CO_TENANTS NebulaStream-side pod(s) are running in this cluster during a Flink measurement."
    echo "      Recorded in run metadata as co_tenant_pods. Use run_all_queries.sh --isolate to scale them to zero."
fi

PREP_DONE_EPOCH=$(date +%s)

# ---------------------------------------------------------------------------
# Deploy the job under test
# ---------------------------------------------------------------------------
ENTRY_CLASS=$(awk '/entryClass:/ {print $2; exit}' "$FLINK_DEPLOYMENT_YAML")
AUTOSCALER_ENABLED=$(awk -F'"' '/job.autoscaler.enabled:/ {print $2; exit}' "$FLINK_DEPLOYMENT_YAML")
JOB_PARALLELISM=$(awk '/^    parallelism:/ {print $2; exit}' "$FLINK_DEPLOYMENT_YAML")
echo "Deploying Flink (entryClass=$ENTRY_CLASS, autoscaler=$AUTOSCALER_ENABLED, parallelism=$JOB_PARALLELISM)..."
kubectl apply -f "$FLINK_DEPLOYMENT_YAML"

echo "Waiting for Flink job to reach RUNNING..."
FLINK_WAIT_TIMEOUT=${FLINK_WAIT_TIMEOUT:-420}
FLINK_WAIT_START=$(date +%s)
while true; do
    STATE=$(kubectl get flinkdeployment luftdaten-job -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
    [ "$STATE" = "RUNNING" ] && break
    ELAPSED=$(( $(date +%s) - FLINK_WAIT_START ))
    if [ "$ELAPSED" -gt "$FLINK_WAIT_TIMEOUT" ]; then
        # The old version looped here forever; a job that never starts would
        # hang the whole suite silently overnight.
        echo "ERROR: Flink job did not reach RUNNING within ${FLINK_WAIT_TIMEOUT}s (last state: ${STATE:-<none>})."
        kubectl get flinkdeployment luftdaten-job -o yaml | tail -40 || true
        kubectl logs deployment/luftdaten-job --tail=50 2>/dev/null || true
        exit 1
    fi
    sleep 2
done
FLINK_READY_EPOCH=$(date +%s)
echo "Flink RUNNING after $((FLINK_READY_EPOCH - PREP_DONE_EPOCH))s."

# Record how many TaskManagers the job actually came up with. An autoscaler
# ConfigMap left behind by a previous suite silently overrode the spec once
# already (spec said parallelism 2, the job started at 1 and spent its whole
# first burst there), which quietly breaks the autoscaler-on/off comparison;
# the ConfigMap is now deleted during prep, and this line makes the result
# visible in the run log. The per-vertex truth is in parallelism_*.csv.
sleep 3
TM_PODS=$(kubectl get pods -l component=taskmanager --no-headers 2>/dev/null | grep -c luftdaten-job || true)
TASK_SLOTS=$(awk -F'"' '/taskmanager.numberOfTaskSlots:/ {print $2; exit}' "$FLINK_DEPLOYMENT_YAML")
echo "TaskManager pods: ${TM_PODS:-0} (spec parallelism ${JOB_PARALLELISM}, ${TASK_SLOTS:-4} slots each)"

# Started now, before datagen, so the timeline captures the baseline (lag=0)
# period as well as the burst.
start_timeline_logging
start_resource_logging
start_flink_metrics_logging

# ---------------------------------------------------------------------------
# Load generation
# ---------------------------------------------------------------------------
echo "Starting datagen job..."
kubectl delete job datagen-run --ignore-not-found
kubectl apply -f "$DATAGEN_JOB_YAML"
DATAGEN_APPLIED_EPOCH=$(date +%s)

echo "Waiting for the datagen pod to run..."
DATAGEN_POD=""
for _ in $(seq 1 120); do
    DATAGEN_POD=$(kubectl get pod -l job-name=datagen-run -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$DATAGEN_POD" ] && [ "$(kubectl get pod "$DATAGEN_POD" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break
    sleep 1
done
[ -z "$DATAGEN_POD" ] && { echo "ERROR: datagen pod never appeared."; exit 1; }
DATAGEN_RUNNING_EPOCH=$(date +%s)
echo "datagen pod $DATAGEN_POD running after $((DATAGEN_RUNNING_EPOCH - DATAGEN_APPLIED_EPOCH))s."

# Record the image now, while the pod still exists.
DATAGEN_IMAGE_ID=$(kubectl get pod "$DATAGEN_POD" -o jsonpath='{.status.containerStatuses[0].imageID}' 2>/dev/null || echo "unknown")

# Stream the generator's log to disk from here on, rather than reading it once at
# the end: the Job has ttlSecondsAfterFinished, so the pod is garbage-collected
# shortly after generation stops — which is well before the observation window
# closes — and a `kubectl logs` at the end therefore captured nothing at all.
# The log carries the realized schedule and the end-of-run record accounting, so
# it is the only artifact that can prove the generator did what it was asked.
DATAGEN_LOG="$BENCHMARK_TIMELINE_DIR/datagen_${TIMESTAMP}.log"
kubectl logs -f "$DATAGEN_POD" --timestamps > "$DATAGEN_LOG" 2>/dev/null &
DATAGEN_LOG_PID=$!

# The generator loads the whole 3.8GB CSV into memory BEFORE it emits anything,
# and its own `duration` timer only starts after that. It also buffers stdout,
# so its "TCP connection ready" line is not visible in `kubectl logs` until it
# exits — the only reliable readiness signal is the topic's first record.
# Anchoring the measurement window here (instead of at Job creation, as before)
# is what makes the observed window match the configured duration.
echo "Waiting for the first record to reach Kafka (CSV load in progress)..."
PRODUCTION_START_EPOCH=""
for _ in $(seq 1 300); do
    OFFS=$(topic_end_offsets)
    if [ -n "$OFFS" ] && [ "$OFFS" -gt 0 ]; then
        PRODUCTION_START_EPOCH=$(date +%s)
        break
    fi
    sleep 2
done
if [ -z "$PRODUCTION_START_EPOCH" ]; then
    echo "ERROR: no records were produced within 600s of the datagen pod starting."
    kubectl logs "$DATAGEN_POD" --tail=40 2>/dev/null || true
    exit 1
fi
DATAGEN_LOAD_SECONDS=$((PRODUCTION_START_EPOCH - DATAGEN_RUNNING_EPOCH))
PRODUCTION_START_TIME=$(iso_now)
echo "First record observed after ${DATAGEN_LOAD_SECONDS}s of CSV loading."
echo "Measurement window: ${OBSERVE_SECONDS}s starting now."

# ---------------------------------------------------------------------------
# Optional: capture the burst pattern as actually produced. Runs in the
# background, concurrently with the observe/drain window below, against the
# datagen Service's TCP broadcast (independent of the Kafka publish path, so
# this cannot perturb the measurement). One period is enough to show a full
# burst/baseline/silent-tail cycle; +30s covers the port-forward's own
# connection setup so the capture doesn't miss the very start of a period.
# ---------------------------------------------------------------------------
PATTERN_PLOT_PID=""
PATTERN_PORTFWD_PID=""
if [ -n "$PLOT_PATTERN_OUT" ]; then
    PATTERN_LOCAL_PORT=${PATTERN_LOCAL_PORT:-19090}
    kubectl port-forward svc/datagen "${PATTERN_LOCAL_PORT}:9090" \
        > /tmp/luftdaten-pattern-portforward.log 2>&1 &
    PATTERN_PORTFWD_PID=$!

    for _ in $(seq 1 15); do
        (exec 3<>"/dev/tcp/127.0.0.1/${PATTERN_LOCAL_PORT}") 2>/dev/null && { exec 3>&-; break; }
        sleep 1
    done

    PATTERN_CAPTURE_SECONDS=$(( DATAGEN_PERIOD_MS / 1000 + 30 ))
    # patternTest_timeseries.py's --timeout is a per-read socket timeout, not a
    # capture budget: it raises (uncaught) the instant no byte arrives within
    # that window. The burst pattern's own silent tail (SILENT_SECONDS, computed
    # above in Preflight 2) routinely exceeds the script's 5s default, which
    # crashes the capture partway through every single time this pattern is used
    # -- so it must always be overridden here, not just when SILENT_SECONDS>0.
    PATTERN_READ_TIMEOUT=$(( ${SILENT_SECONDS:-0} + 30 ))
    [ "$PATTERN_READ_TIMEOUT" -lt 30 ] && PATTERN_READ_TIMEOUT=30
    mkdir -p "$(dirname "$PLOT_PATTERN_OUT")"
    PATTERN_PY=venv/bin/python3; [ -x "$PATTERN_PY" ] || PATTERN_PY=python3
    echo "Capturing burst pattern for ${PATTERN_CAPTURE_SECONDS}s (read timeout ${PATTERN_READ_TIMEOUT}s) -> $PLOT_PATTERN_OUT (background)..."
    "$PATTERN_PY" infra/datagen_parallel/patternTest_timeseries.py \
        --host 127.0.0.1 --port "$PATTERN_LOCAL_PORT" \
        --timeout "$PATTERN_READ_TIMEOUT" \
        --duration "$PATTERN_CAPTURE_SECONDS" \
        --out "$PLOT_PATTERN_OUT" \
        --csv "${PLOT_PATTERN_OUT%.png}.csv" \
        > /tmp/luftdaten-pattern-plot.log 2>&1 &
    PATTERN_PLOT_PID=$!
fi

progress_bar "$OBSERVE_SECONDS"

# ---------------------------------------------------------------------------
# Drain: keep observing until the consumer has caught up, so the post-burst
# recovery tail is in the data instead of being cut off mid-drain.
# ---------------------------------------------------------------------------
# The stop condition is "the pipeline has stopped producing output", measured
# directly from the growth of rtracker's latency log. Two conditions that look
# equivalent are NOT usable here:
#   * consumer-group lag == 0 — it returns to zero for ~110-120s BETWEEN the two
#     bursts (the generator's schedule is silent for the last 40% of every
#     period), so a lag-only gate would stop the run halfway through;
#   * `kubectl wait --for=condition=complete job/datagen-run` — the Job only
#     completes at duration + CSV-load, well after the last record is consumed.
# Lag is still recorded, as a cross-check, but the log is what decides.
RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-240}
LOG_STABLE_SECONDS=${LOG_STABLE_SECONDS:-45}
echo "Waiting for output to stop (log stable for ${LOG_STABLE_SECONDS}s, timeout ${DRAIN_TIMEOUT}s)..."
DRAIN_START=$(date +%s)
DRAINED=false
LAST_SIZE=-1
STABLE_SINCE=$(date +%s)
while [ $(( $(date +%s) - DRAIN_START )) -lt "$DRAIN_TIMEOUT" ]; do
    SIZE=$(kubectl exec "$RTRACKER_POD" -- stat -c %s /app/latency-logs/latency_output.log 2>/dev/null || echo "$LAST_SIZE")
    if [ "$SIZE" != "$LAST_SIZE" ]; then
        LAST_SIZE="$SIZE"
        STABLE_SINCE=$(date +%s)
    elif [ $(( $(date +%s) - STABLE_SINCE )) -ge "$LOG_STABLE_SECONDS" ]; then
        DRAINED=true
        break
    fi
    sleep 5
done
DRAIN_SECONDS=$(( $(date +%s) - DRAIN_START ))
if [ "$DRAINED" = true ]; then
    echo "Output stopped; latency log stable at ${LAST_SIZE} bytes after ${DRAIN_SECONDS}s."
else
    echo "WARNING: output was still arriving when the ${DRAIN_TIMEOUT}s drain budget ran out —"
    echo "         this run ends mid-drain and its latency tail is truncated."
fi

TOTAL_PRODUCED=$(topic_end_offsets)
FINAL_LAG=$(consumer_lag)

stop_timeline_logging
stop_resource_logging
stop_flink_metrics_logging

RUN_END_TIME=$(iso_now)
RUN_END_EPOCH=$(date +%s)
RUN_DURATION=$((RUN_END_EPOCH - RUN_START_EPOCH))
MEASURED_WINDOW=$((RUN_END_EPOCH - PRODUCTION_START_EPOCH))
echo "Run ended at: $RUN_END_TIME (epoch: $RUN_END_EPOCH)"
echo "Wall clock total: ${RUN_DURATION}s | measured window: ${MEASURED_WINDOW}s | records produced: ${TOTAL_PRODUCED:-NA}"

# ---------------------------------------------------------------------------
# Collect results
# ---------------------------------------------------------------------------
if [ -n "$PATTERN_PLOT_PID" ]; then
    echo "Waiting for burst pattern capture to finish..."
    wait "$PATTERN_PLOT_PID" 2>/dev/null || echo "  (pattern capture exited non-zero; see /tmp/luftdaten-pattern-plot.log)"
    if [ -s "$PLOT_PATTERN_OUT" ]; then
        echo "  burst pattern plot: $PLOT_PATTERN_OUT"
    else
        echo "  WARNING: no burst pattern plot was produced — check /tmp/luftdaten-pattern-plot.log"
    fi
fi
[ -n "$PATTERN_PORTFWD_PID" ] && kill "$PATTERN_PORTFWD_PID" 2>/dev/null || true

echo "Collecting datagen log..."
kill "$DATAGEN_LOG_PID" 2>/dev/null || true
wait "$DATAGEN_LOG_PID" 2>/dev/null || true
if [ -s "$DATAGEN_LOG" ]; then
    echo "  datagen log: $(wc -l < "$DATAGEN_LOG") lines"
    grep -E "Schedule:|Accounting:|error|Error|LOST" "$DATAGEN_LOG" | tail -5 || true
else
    echo "  WARNING: the datagen log is empty — the generator's own record counts are not available."
fi

echo "Collecting results..."
# Only latency_output.log — the previous version copied the whole directory,
# which also transferred timestamped_tuples.log (~0.95x the size of the latency
# log, so nearly double the bytes) that nothing in this repo ever reads.
kubectl cp "$RTRACKER_POD":/app/latency-logs/latency_output.log "$LATENCY_DIR/latency_output.log"

PY=venv/bin/python3
[ -x "$PY" ] || PY=python3

"$PY" ./latency-plotter.py --summary \
    "$LATENCY_DIR/latency_output.log" \
    -o "$PLOTS_DIR/latency_plot_${TIMESTAMP}.png" || true

echo "Stopping Flink..."
kubectl delete flinkdeployment luftdaten-job --wait=true --timeout=120s || true
kubectl delete job datagen-run --ignore-not-found
# `delete flinkdeployment` returns when the operator drops its finalizer, not
# when the pods are gone. Without this barrier a terminating 4-CPU TaskManager
# overlaps the next query's JobManager, inflating that query's startup time and
# mixing two queries' footprints in the resource CSV.
kubectl wait --for=delete pod -l app=luftdaten-job --timeout=90s 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run metadata — everything needed to interpret or reproduce this run, in one
# file. The old version recorded only start/end/requested-duration, so a stored
# run could not tell you what load it actually received, whether the autoscaler
# was on, or which code produced it.
# ---------------------------------------------------------------------------
METADATA_DIR="${RUN_DIR:-.}/run_metadata"
mkdir -p "$METADATA_DIR"
METADATA_FILE="$METADATA_DIR/run_${TIMESTAMP}.json"

GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY=$(test -n "$(git status --porcelain 2>/dev/null)" && echo true || echo false)
cat > "$METADATA_FILE" << EOF
{
  "timestamp": "$TIMESTAMP",
  "entry_class": "$ENTRY_CLASS",
  "autoscaler_enabled": ${AUTOSCALER_ENABLED:-null},
  "configured_parallelism": ${JOB_PARALLELISM:-null},
  "git_commit": "$GIT_COMMIT",
  "git_dirty": $GIT_DIRTY,
  "datagen_image_id": "$DATAGEN_IMAGE_ID",

  "start_time_utc": "$RUN_START_TIME",
  "end_time_utc": "$RUN_END_TIME",
  "start_epoch": $RUN_START_EPOCH,
  "end_epoch": $RUN_END_EPOCH,
  "wall_clock_seconds": $RUN_DURATION,

  "setup_seconds": $((PREP_DONE_EPOCH - RUN_START_EPOCH)),
  "flink_start_seconds": $((FLINK_READY_EPOCH - PREP_DONE_EPOCH)),
  "datagen_pod_start_seconds": $((DATAGEN_RUNNING_EPOCH - DATAGEN_APPLIED_EPOCH)),
  "datagen_csv_load_seconds": $DATAGEN_LOAD_SECONDS,

  "production_start_utc": "$PRODUCTION_START_TIME",
  "production_start_epoch": $PRODUCTION_START_EPOCH,
  "observe_seconds_requested": $OBSERVE_SECONDS,
  "measured_window_seconds": $MEASURED_WINDOW,
  "drain_seconds": $DRAIN_SECONDS,
  "drained": $DRAINED,
  "final_consumer_lag": ${FINAL_LAG:-null},

  "datagen": {
    "data_rate_per_period": $DATAGEN_DATA_RATE,
    "period_ms": $DATAGEN_PERIOD_MS,
    "duration_seconds": $DATAGEN_DURATION,
    "burst_fraction": ${DATAGEN_FRACTION:-null},
    "burst_distribution": ${DATAGEN_DISTRIBUTION:-null},
    "expected_records": $EXPECTED_RECORDS,
    "records_produced": ${TOTAL_PRODUCED:-null},
    "expected_silent_tail_seconds_per_period": ${SILENT_SECONDS:-null},
    "dataset_rows": $DATASET_ROWS,
    "dataset_wrapped": $( [ "$EXPECTED_RECORDS" -gt "$DATASET_ROWS" ] && echo true || echo false )
  },

  "kafka_topic": "$KAFKA_TOPIC",
  "kafka_group": "$KAFKA_GROUP",
  "kafka_partitions": $(awk '/partitions:/ {print $2; exit}' k8s/topic.yaml),
  "co_tenant_pods": ${CO_TENANTS:-0},
  "kafka_starting_offsets": "${KAFKA_STARTING_OFFSETS:-committed}"
}
EOF
echo "Metadata saved: $METADATA_FILE"

# Loud, explicit reconciliation of intended vs delivered load. Without this a
# run that produced fewer records than configured (flush timeout, QUEUE_FULL
# drops, an activeDeadlineSeconds kill, an OOM) is indistinguishable from a
# clean one in the stored artifacts.
if [ -n "${TOTAL_PRODUCED:-}" ] && [ "$TOTAL_PRODUCED" -gt 0 ]; then
    SHORTFALL_PCT=$(awk -v p="$TOTAL_PRODUCED" -v e="$EXPECTED_RECORDS" 'BEGIN{ if (e>0) printf "%.2f", 100*(e-p)/e; else print "0" }')
    echo "Load delivered: $TOTAL_PRODUCED / $EXPECTED_RECORDS expected records (shortfall ${SHORTFALL_PCT}%)"
    awk -v s="$SHORTFALL_PCT" 'BEGIN{ if (s > 1.0 || s < -1.0) exit 1 }' || \
        echo "  !! WARNING: delivered record count deviates >1% from the configured schedule — treat this run's throughput numbers with care."
else
    echo "  !! WARNING: could not read the produced record count from Kafka."
fi

# Snapshot the exact configuration this run used, so a result directory is
# self-describing and does not depend on the repo still being at that commit.
CONFIG_SNAPSHOT="${RUN_DIR:-.}/config_snapshot"
mkdir -p "$CONFIG_SNAPSHOT"
cp "$FLINK_DEPLOYMENT_YAML" "$DATAGEN_CONFIG_YAML" "$DATAGEN_JOB_YAML" \
   k8s/topic.yaml k8s/RTrackerConfig.yaml .env "$CONFIG_SNAPSHOT/" 2>/dev/null || true

echo "Done."
