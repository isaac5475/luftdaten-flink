#!/bin/bash
set -e
source ./.env
echo "Sleep: $SLEEP_SECONDS"

echo "Checking Minikube status..."
if minikube status >/dev/null 2>&1; then
    echo "Minikube already running."
else
    echo "Starting Minikube"
    minikube start --driver=docker --cpus=12 --memory=12g
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

echo "Building images"
docker build -t luftdaten-flink:local .
(cd infra/latency-tracker && docker build -t rtracker:local .)
(cd infra/datagen_parallel && docker build -t datagen:local -f docker/Dockerfile .)
minikube image load datagen:local
minikube image load rtracker:local
minikube image load luftdaten-flink:local

echo "Deploying infrastructure..."
kubectl delete configmap/rtracker-config --ignore-not-found
kubectl delete configmap/datagen-config --ignore-not-found
kubectl apply -f k8s/DatagenConfig.yaml
kubectl apply -f k8s/RTrackerConfig.yaml
kubectl apply -f k8s/RTrackerDeployment.yaml
kubectl apply -f k8s/RTrackerService.yaml
kubectl rollout restart deployment/rtracker
kubectl rollout status deployment/rtracker

echo "Clearing previous latency logs..."
RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$RTRACKER_POD" -- sh -c "rm -f /app/latency-logs/*.log"

echo "Deploying Flink..."
kubectl apply -f k8s/FlinkDeployment.yaml
echo "Waiting for Flink..."
until [ "$(kubectl get flinkdeployment luftdaten-job \
    -o jsonpath='{.status.jobStatus.state}')" = "RUNNING" ]
do
    sleep 2
done

echo "Starting datagen job..."
kubectl delete job datagen-run --ignore-not-found
kubectl apply -f k8s/DatagenJob.yaml

echo "Benchmark started."
sleep "$SLEEP_SECONDS"

echo "Stopping Flink..."
RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$RTRACKER_POD":/app/latency-logs/. ./latency-logs/
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
python3 ./latency-plotter.py --summary \
    latency-logs/latency_output.log \
    -o "plots/latency_plot_${TIMESTAMP}.png"
kubectl delete flinkdeployment luftdaten-job
kubectl delete job datagen-run --ignore-not-found

echo "Done."
