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
kubectl apply -f k8s/infrastructure/prometheus.yaml
kubectl apply -f k8s/infrastructure/grafana.yaml

echo "==> Creating Kibana dashboards ConfigMap..."
kubectl -n law-chatbot create configmap kibana-dashboards \
  --from-file=kibana/dashboard-export.ndjson \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/infrastructure/kibana-init.yaml

echo "==> Waiting for infra to be ready..."
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/redis
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/minio
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/zookeeper
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/kafka
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/elasticsearch
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/kibana
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/prometheus
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/grafana

echo "==> Waiting for init jobs to complete..."
kubectl -n law-chatbot wait --for=condition=complete --timeout=120s job/kafka-init
kubectl -n law-chatbot wait --for=condition=complete --timeout=120s job/es-init
kubectl -n law-chatbot wait --for=condition=complete --timeout=180s job/kibana-init

echo "==> Deploying application..."
kubectl apply -f k8s/app/backend.yaml
kubectl apply -f k8s/app/frontend.yaml
kubectl apply -f k8s/app/kafka-workers.yaml
kubectl apply -f k8s/app/spark.yaml

echo "==> Waiting for app to be ready..."
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/backend
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/frontend
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/kafka-consumer
kubectl -n law-chatbot wait --for=condition=available --timeout=120s deployment/data-ingest
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-master
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-worker
kubectl -n law-chatbot wait --for=condition=available --timeout=180s deployment/spark-job

echo ""
echo "==> Done! All services deployed in namespace 'law-chatbot':"
kubectl -n law-chatbot get all
echo ""
echo "==> Access services:"
echo "    Frontend:   minikube service frontend -n law-chatbot"
echo "    Grafana:    minikube service grafana -n law-chatbot (admin/admin123)"
echo "    Prometheus: minikube service prometheus -n law-chatbot"
echo "    Kibana:     minikube service kibana -n law-chatbot"
