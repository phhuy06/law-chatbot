#!/usr/bin/env bash
set -euo pipefail

echo "==> Building Docker images for Minikube..."
echo "    (Make sure you ran: eval \$(minikube docker-env))"

docker build -t law-chatbot/backend:latest -f backend/Dockerfile .
docker build -t law-chatbot/frontend:latest -f frontend/Dockerfile ./frontend
docker build -t law-chatbot/spark:latest -f spark/Dockerfile .
docker build -t law-chatbot/kafka:latest -f kafka/Dockerfile .

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

echo "==> Waiting for init jobs to complete..."
kubectl -n law-chatbot wait --for=condition=complete --timeout=120s job/kafka-init
kubectl -n law-chatbot wait --for=condition=complete --timeout=120s job/es-init

echo "==> Deploying application..."
kubectl apply -f k8s/app/backend.yaml
kubectl apply -f k8s/app/frontend.yaml
kubectl apply -f k8s/app/kafka-workers.yaml
kubectl apply -f k8s/app/spark.yaml

echo "==> Waiting for app to be ready..."
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/backend
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/frontend
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/kafka-consumer
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-master
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-worker
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-job

echo ""
echo "==> Done! All services deployed in namespace 'law-chatbot':"
kubectl -n law-chatbot get all
echo ""
echo "==> Access frontend: minikube service frontend -n law-chatbot"
