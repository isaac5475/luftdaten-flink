#!/bin/bash
set -e

source ./.env

echo "Sleep: $SLEEP_SECONDS"

echo "Checking Minikube status..."
if minikube status >/dev/null 2>&1; then
    echo "Minikube already running."
else
    echo "Starting Minikube"
    minikube start
      echo "Installing Flink Kubernetes Operator..."
        helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
        helm repo update
#        helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
#        kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=flink-kubernetes-operator --timeout=180s
fi

echo "Syncing data into Minikube node..."
tar cf /tmp/data.tar -C /home/murat/BA/datasets .
minikube cp /tmp/data.tar /tmp/data.tar
minikube ssh -- "sudo mkdir -p /home/murat/BA/datasets && sudo tar xf /tmp/data.tar -C /home/murat/BA/datasets"
rm /tmp/data.tar

echo "Building images"
docker build -t luftdaten-flink:local .
(cd infra/latency-tracker && docker build -t rtracker:local .)
(cd infra/datagen_parallel && docker build -t datagen:local -f docker/Dockerfile .)
minikube image load datagen:local
minikube image load rtracker:local
echo "Deploying infrastructure..."

echo "Clearing previous latency logs..."
RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$RTRACKER_POD" -- sh -c "rm -f /app/latency-logs/*.log"

kubectl delete configmap/rtracker-config --ignore-not-found
kubectl delete configmap/datagen-config --ignore-not-found

kubectl apply -f k8s/DatagenConfig.yaml
kubectl apply -f k8s/RTrackerConfig.yaml

kubectl apply -f k8s/DatagenDeployment.yaml
kubectl apply -f k8s/DatagenService.yaml

kubectl apply -f k8s/RTrackerDeployment.yaml
kubectl apply -f k8s/RTrackerService.yaml

kubectl rollout restart deployment/datagen
kubectl rollout restart deployment/rtracker

kubectl rollout status deployment/datagen
kubectl rollout status deployment/rtracker

echo "Deploying Flink..."

kubectl apply -f k8s/FlinkDeployment.yaml

echo "Waiting for Flink..."

until [ "$(kubectl get flinkdeployment luftdaten-job \
    -o jsonpath='{.status.jobStatus.state}')" = "RUNNING" ]
do
    sleep 2
done

echo "Benchmark started."

sleep "$SLEEP_SECONDS"

echo "Stopping Flink..."

RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$RTRACKER_POD":/app/latency-logs/. ./latency-logs/

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
python3 ./latency-plotter.py --summary \
    latency-logs/latency_output.log \
    "plots/latency_plot_${TIMESTAMP}.png"

kubectl delete flinkdeployment luftdaten-job
echo "Done."