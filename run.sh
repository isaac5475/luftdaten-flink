#!/bin/bash
set -e

source ./.env

echo "Sleep: $SLEEP_SECONDS"

echo "Deploying infrastructure..."

kubectl delete configmap/rtracker-config
kubectl delete configmap/datagen-config

kubectl apply -f k8s/DatagenConfig.yaml
kubectl apply -f k8s/RTrackerConfig.yaml

kubectl apply -f k8s/DatagenDeployment.yaml
kubectl apply -f k8s/DatagenService.yaml

kubectl apply -f k8s/RTrackerDeployment.yaml
kubectl apply -f k8s/RTrackerService.yaml

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

sleep "$BENCHMARK_TIME"

echo "Stopping Flink..."

RTRACKER_POD=$(kubectl get pod -l app=rtracker -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$RTRACKER_POD":/app/latency-logs/. ./latency-logs/

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

python3 ./latency-plotter.py \
    latency-logs/latency_output.log \
    "plots/latency_plot_${TIMESTAMP}.png"

kubectl delete flinkdeployment luftdaten-job

echo "Done."