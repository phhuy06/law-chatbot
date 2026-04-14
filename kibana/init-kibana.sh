#!/bin/bash
# Wait for Kibana to be ready, then import data view + dashboard
set -e

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
MAX_RETRIES=60
RETRY_INTERVAL=5

echo "[kibana-init] Waiting for Kibana at $KIBANA_URL ..."

for i in $(seq 1 $MAX_RETRIES); do
  if curl -sf "$KIBANA_URL/api/status" > /dev/null 2>&1; then
    echo "[kibana-init] Kibana is ready."
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "[kibana-init] ERROR: Kibana not ready after $((MAX_RETRIES * RETRY_INTERVAL))s, giving up."
    exit 1
  fi
  sleep $RETRY_INTERVAL
done

echo "[kibana-init] Importing saved objects (data view + dashboard) ..."

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@/kibana-export/dashboard-export.ndjson)

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" -eq 200 ]; then
  echo "[kibana-init] Dashboard imported successfully."
  echo "$BODY" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f\"  Success: {r.get('successCount', 0)} objects imported\")
    if r.get('errors'):
        for e in r['errors']:
            print(f\"  Error: {e['id']} - {e['error']['message']}\")
except: pass
"
else
  echo "[kibana-init] ERROR: Import failed (HTTP $HTTP_CODE)"
  echo "$BODY"
  exit 1
fi

echo "[kibana-init] Done."
