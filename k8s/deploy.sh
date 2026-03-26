#!/usr/bin/env bash
set -euo pipefail

echo "==> Creating namespace..."
kubectl apply -f k8s/namespace.yaml

echo "==> Deploying infrastructure..."
kubectl apply -f k8s/infrastructure/redis.yaml
kubectl apply -f k8s/infrastructure/minio.yaml
kubectl apply -f k8s/infrastructure/kafka.yaml
kubectl apply -f k8s/infrastructure/elasticsearch.yaml

echo "==> Waiting for infra to be ready..."
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/redis
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/minio
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/zookeeper
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/kafka
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/elasticsearch
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/kibana

echo "==> Deploying application..."
kubectl apply -f k8s/app/backend.yaml
kubectl apply -f k8s/app/frontend.yaml
# Spark streaming (deployment) - uncomment when image is built
# kubectl apply -f k8s/app/spark.yaml

echo "==> Done! Services in namespace 'law-chatbot':"
kubectl -n law-chatbot get all
