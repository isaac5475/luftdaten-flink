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
source "$SCRIPT_DIR/utils/progress_bar.sh"
source "$SCRIPT_DIR/utils/timeline_logging.sh"
source "$SCRIPT_DIR/utils/resource_logging.sh"
source "$SCRIPT_DIR/utils/parallelism_logging.sh"

# --skip-build / -s: skip rebuilding and reloading the datagen/rtracker/
# luftdaten-flink images. Useful when only spec.job.entryClass changed
# (e.g. when called repeatedly from run_all_jobs.sh) and the jar/image
# themselves are already up to date on this Minikube.
SKIP_BUILD=false
for arg in "$@"; do
    case "$arg" in
        --skip-build|-s)
            SKIP_BUILD=true
            ;;
    esac
done

source ./.env
echo "Sleep: $SLEEP_SECONDS"

# Log timing metadata for reproducibility
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
RUN_START_EPOCH=$(date +%s)
echo "Run started at: $RUN_START_TIME (epoch: $RUN_START_EPOCH)"

# Config used by utils/timeline_logging.sh
KAFKA_GROUP="luftdaten-benchmark"
KAFKA_TOPIC="bid"
KAFKA_BROKER_POD="my-cluster-dual-role-0"
KAFKA_NAMESPACE="kafka"
KAFKA_POLL_INTERVAL=2
KAFKA_EXEC_TIMEOUT=3

# Config used by utils/parallelism_logging.sh — the Flink Kubernetes Operator
# creates a "<deployment-name>-rest" Service automatically.
PARALLELISM_REST_SERVICE="luftdaten-job-rest"
PARALLELISM_POLL_INTERVAL=5
PARALLELISM_EXEC_TIMEOUT=3

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
kubectl apply -f k8s/topic.yaml

## echo "Syncing data into Minikube node..."
## tar cf /tmp/data.tar -C /home/murat/BA/datasets .
## minikube cp /tmp/data.tar /tmp/data.tar
## minikube ssh -- "sudo mkdir -p /home/murat/BA/datasets && sudo tar xf /tmp/data.tar -C /home/murat/BA/datasets"
## rm /tmp/data.tar

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

echo "Deploying infrastructure..."
kubectl delete configmap/rtracker-config --ignore-not-found
kubectl delete configmap/datagen-config --ignore-not-found
kubectl apply -f k8s/DatagenConfig.yaml
kubectl apply -f k8s/RTrackerConfig.yaml
kubectl apply -f k8s/RTrackerDeployment.yaml
kubectl apply -f k8s/RTrackerService.yaml
kubectl rollout restart deployment/rtracker
kubectl rollout status deployment/rtracker

echo "Restarting Kafka topic to clear the state of the queues..."
kubectl delete kafkatopic "$KAFKA_TOPIC" -n kafka --ignore-not-found
kubectl apply -f k8s/topic.yaml
kubectl wait kafkatopic/"$KAFKA_TOPIC" -n kafka --for=condition=Ready --timeout=60s

# Fixed consumer group id ("luftdaten-benchmark") survives Flink autoscaler
# restarts without losing offsets, but that means after recreating the topic
# above, any old committed offsets may point past the new (empty) log.
# Delete the group so each run starts clean against the fresh topic.
echo "Clearing stale consumer group offsets..."
kubectl exec "$KAFKA_BROKER_POD" -n "$KAFKA_NAMESPACE" -- \
    bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --delete --group "$KAFKA_GROUP" 2>/dev/null || true

echo "Deploying Flink..."
kubectl apply -f k8s/FlinkDeployment.yaml
echo "Waiting for Flink..."
until [ "$(kubectl get flinkdeployment luftdaten-job \
    -o jsonpath='{.status.jobStatus.state}')" = "RUNNING" ]
do
    sleep 2
done

# Started now, before datagen, so the timeline captures the baseline (lag=0)
# period as well as the burst.
start_timeline_logging
start_resource_logging
start_parallelism_logging

echo "Starting datagen job..."
kubectl delete job datagen-run --ignore-not-found
kubectl apply -f k8s/DatagenJob.yaml

echo "Benchmark started."
progress_bar "$SLEEP_SECONDS"

stop_timeline_logging
stop_resource_logging
stop_parallelism_logging

RUN_END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
RUN_END_EPOCH=$(date +%s)
RUN_DURATION=$((RUN_END_EPOCH - RUN_START_EPOCH))
echo "Run ended at: $RUN_END_TIME (epoch: $RUN_END_EPOCH)"
echo "Actual duration: ${RUN_DURATION}s (requested: ${SLEEP_SECONDS}s)"

echo "Collecting results..."
RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$RTRACKER_POD":/app/latency-logs/. ./latency-logs/
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
python3 ./latency-plotter.py --summary \
    latency-logs/latency_output.log \
    -o "plots/latency_plot_${TIMESTAMP}.png"

echo "Stopping Flink..."
kubectl delete flinkdeployment luftdaten-job
kubectl delete job datagen-run --ignore-not-found

# Write run metadata file for reproducibility and analysis
mkdir -p run_metadata
METADATA_FILE="run_metadata/run_${TIMESTAMP}.json"
cat > "$METADATA_FILE" << EOF
{
  "start_time_utc": "$RUN_START_TIME",
  "end_time_utc": "$RUN_END_TIME",
  "start_epoch": $RUN_START_EPOCH,
  "end_epoch": $RUN_END_EPOCH,
  "actual_duration_seconds": $RUN_DURATION,
  "requested_duration_seconds": $SLEEP_SECONDS,
  "timing_variance_percent": $(echo "scale=2; ($RUN_DURATION - $SLEEP_SECONDS) * 100 / $SLEEP_SECONDS" | bc),
  "kafka_topic": "$KAFKA_TOPIC",
  "kafka_group": "$KAFKA_GROUP"
}
EOF
echo "Metadata saved: $METADATA_FILE"

echo "Done."
