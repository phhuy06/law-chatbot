#!/usr/bin/env bash
# Bring up the whole law-chatbot stack on a local minikube cluster.
# Idempotent: re-running picks up where the last run left off.
#
# Usage:
#   ./k8s/start.sh            # full bring-up + port-forwards
#   ./k8s/start.sh --rebuild  # force rebuild of all 5 app images
#   ./k8s/start.sh --stop     # tear down port-forwards (cluster keeps running)

set -euo pipefail

NS=law-chatbot
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

REBUILD=0
case "${1:-}" in
  --rebuild) REBUILD=1 ;;
  --stop)    pkill -f "kubectl.*port-forward.*${NS}" 2>/dev/null || true
             echo "Port-forwards stopped. Cluster still running."
             exit 0 ;;
  "") ;;
  *) echo "Unknown arg: $1"; exit 1 ;;
esac

# ----- 1. Docker -----
if ! docker info >/dev/null 2>&1; then
  echo "==> Starting Docker Desktop..."
  open -ga Docker
  until docker info >/dev/null 2>&1; do sleep 2; done
fi
echo "==> Docker ready."

# ----- 2. Minikube -----
if ! minikube status >/dev/null 2>&1; then
  echo "==> Starting minikube..."
  minikube start --driver=docker --cpus=4 --memory=8192
fi
echo "==> Minikube ready."

# ----- 3. Point docker CLI at minikube's daemon -----
eval "$(minikube docker-env)"

# ----- 4. Build images (skip if present unless --rebuild) -----
need_build() {
  [[ "$REBUILD" == 1 ]] && return 0
  ! docker image inspect "$1" >/dev/null 2>&1
}
build_if_needed() {
  local tag=$1 dockerfile=$2 context=$3
  if need_build "$tag"; then
    echo "==> Building $tag"
    docker build -t "$tag" -f "$dockerfile" "$context"
  else
    echo "==> $tag already present, skipping"
  fi
}
build_if_needed law-chatbot/backend:latest       backend/Dockerfile       .
build_if_needed law-chatbot/frontend:latest      frontend/Dockerfile      ./frontend
build_if_needed law-chatbot/spark:latest         spark/Dockerfile         .
build_if_needed law-chatbot/kafka:latest         kafka/Dockerfile         .
build_if_needed law-chatbot/elasticsearch:latest elasticsearch/Dockerfile elasticsearch

# ----- 5. Apply manifests -----
echo "==> Applying namespace + infrastructure manifests..."
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/infrastructure/redis.yaml
kubectl apply -f k8s/infrastructure/minio.yaml
kubectl apply -f k8s/infrastructure/kafka.yaml
kubectl apply -f k8s/infrastructure/elasticsearch.yaml
kubectl apply -f k8s/infrastructure/prometheus.yaml
kubectl apply -f k8s/infrastructure/grafana.yaml

echo "==> Grafana dashboards configmap (from monitoring/grafana-dashboards/*.json)..."
if compgen -G "monitoring/grafana-dashboards/*.json" >/dev/null; then
  kubectl -n "$NS" create configmap grafana-dashboards \
    --from-file=monitoring/grafana-dashboards \
    --dry-run=client -o yaml | kubectl apply -f -
  # Force Grafana to remount the configmap so dashboards refresh on JSON edits.
  kubectl -n "$NS" rollout restart deployment/grafana >/dev/null 2>&1 || true
else
  echo "    (no dashboards found, skipping)"
fi

echo "==> Kibana dashboards configmap + init job..."
kubectl -n "$NS" create configmap kibana-dashboards \
  --from-file=kibana/dashboard-export.ndjson \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/infrastructure/kibana-init.yaml

# Backend secret — read OPENAI_API_KEY from local .env, never from the repo.
echo "==> Creating backend-secret from .env..."
if [[ ! -f .env ]]; then
  echo "ERROR: .env not found in $PWD. Create one with OPENAI_API_KEY=sk-..." >&2
  exit 1
fi
OPENAI_KEY=$(grep -E '^OPENAI_API_KEY=' .env | head -n1 | cut -d= -f2- | tr -d '\r\n' | sed 's/^"//;s/"$//')
if [[ -z "${OPENAI_KEY}" || "${OPENAI_KEY}" == sk-change-me ]]; then
  echo "ERROR: .env has no valid OPENAI_API_KEY (got: ${OPENAI_KEY:-<empty>})" >&2
  exit 1
fi
kubectl -n "$NS" create secret generic backend-secret \
  --from-literal=OPENAI_API_KEY="$OPENAI_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
unset OPENAI_KEY

echo "==> Applying app manifests..."
kubectl apply -f k8s/app/backend.yaml
kubectl apply -f k8s/app/frontend.yaml
kubectl apply -f k8s/app/kafka-workers.yaml
kubectl apply -f k8s/app/spark.yaml

# ----- 6. Wait for deployments -----
echo "==> Waiting for deployments to become Available..."
kubectl -n "$NS" wait --for=condition=available --timeout=300s \
  deployment/redis deployment/minio deployment/zookeeper deployment/kafka \
  deployment/elasticsearch deployment/kibana deployment/prometheus deployment/grafana \
  deployment/backend deployment/frontend deployment/kafka-consumer deployment/data-ingest \
  deployment/spark-master deployment/spark-worker deployment/spark-job

# ----- 6b. Ensure Kafka topic exists -----
# Kafka has no persistent volume, so a pod/cluster restart wipes all topics.
# kafka-init is a one-shot Job that does NOT re-run, so re-assert the topic here
# (idempotent). Only bounce the consumers when the topic was actually missing.
echo "==> Ensuring Kafka topic 'van-ban-phap-luat' exists..."
if kubectl -n "$NS" exec deploy/kafka -- kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx van-ban-phap-luat; then
  echo "    topic already present"
else
  kubectl -n "$NS" exec deploy/kafka -- kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic van-ban-phap-luat --partitions 1 --replication-factor 1
  echo "    topic created — restarting spark-job + kafka-consumer to re-subscribe"
  kubectl -n "$NS" rollout restart deployment/spark-job deployment/kafka-consumer >/dev/null 2>&1 || true
  kubectl -n "$NS" rollout status deployment/spark-job --timeout=120s >/dev/null 2>&1 || true
fi

# ----- 7. Port-forwards -----
echo "==> Resetting port-forwards..."
pkill -f "kubectl.*port-forward.*${NS}" 2>/dev/null || true
sleep 1

BACKEND_LOCAL=8001  # 8000 often busy with a local dev backend on this machine
if ! lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1; then
  BACKEND_LOCAL=8000
fi

start_pf() {
  local svc=$1 local_port=$2 remote_port=$3
  kubectl -n "$NS" port-forward "svc/$svc" "${local_port}:${remote_port}" \
    >"/tmp/pf-${svc}.log" 2>&1 &
}
start_pf frontend     8080            80
start_pf backend      "$BACKEND_LOCAL" 8000
start_pf grafana      3000            3000
start_pf kibana       5601            5601
start_pf prometheus   9090            9090
start_pf minio        9001            9001   # web console
start_pf minio        9000            9000   # S3 API (used by bulk_load_to_minio.py)
start_pf elasticsearch 9200           9200   # ES API (used by demo + bulk scripts)
start_pf kafka        29092           9094   # external listener (used by host crawler)
sleep 2

# ----- 8. Verify port-forwards are actually listening (retry once) -----
# Port-forwards can die if their target pod restarts (e.g. grafana after the
# rollout above). Re-check each local port and retry any that aren't up.
echo "==> Verifying port-forwards..."
verify_pf() {
  local svc=$1 local_port=$2 remote_port=$3
  if ! nc -z localhost "$local_port" 2>/dev/null; then
    echo "    :$local_port ($svc) not up — retrying"
    kubectl -n "$NS" port-forward "svc/$svc" "${local_port}:${remote_port}" \
      >"/tmp/pf-${svc}-${local_port}.log" 2>&1 &
  fi
}
sleep 1
verify_pf frontend 8080 80
verify_pf backend "$BACKEND_LOCAL" 8000
verify_pf grafana 3000 3000
verify_pf kibana 5601 5601
verify_pf prometheus 9090 9090
verify_pf minio 9001 9001
verify_pf minio 9000 9000
verify_pf elasticsearch 9200 9200
verify_pf kafka 29092 9094
sleep 2

cat <<EOF

==> Stack is up. Access URLs:

    Frontend     http://localhost:8080
    Backend API  http://localhost:${BACKEND_LOCAL}   (docs at /docs)
    Grafana      http://localhost:3000           (admin / admin123)
    Kibana       http://localhost:5601
    Prometheus   http://localhost:9090
    MinIO        http://localhost:9001

To stop port-forwards:  ./k8s/start.sh --stop
To stop the cluster:    minikube stop
EOF
