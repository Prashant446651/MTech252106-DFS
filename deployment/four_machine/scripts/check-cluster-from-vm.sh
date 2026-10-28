#!/usr/bin/env bash
set -u
META_LOCAL="${METADATA_LOCAL_URL:-http://localhost:9000}"
NODES=(
  "node1|http://10.0.10.199:8001"
  "node2|http://10.0.9.188:8002"
  "node3|http://10.0.10.198:8003"
  "node4|http://10.0.13.253:8004"
)

printf '\n===== METADATA HEALTH =====\n'
curl -sS --connect-timeout 3 --max-time 8 -i "$META_LOCAL/health" || true

printf '\n===== STORAGE NODE HEALTH =====\n'
for item in "${NODES[@]}"; do
  name="${item%%|*}"
  url="${item#*|}"
  printf '\n--- %s (%s) ---\n' "$name" "$url"
  curl -sS --connect-timeout 3 --max-time 8 -i "$url/health" || true
done

printf '\n===== REGISTERED NODES =====\n'
curl -sS --connect-timeout 3 --max-time 8 "$META_LOCAL/nodes" || true
printf '\n\n===== PENDING REPLICATION JOBS =====\n'
curl -sS --connect-timeout 3 --max-time 8 "$META_LOCAL/replication-queue" || true
printf '\n\n===== REPLICA EXCLUSIONS =====\n'
curl -sS --connect-timeout 3 --max-time 8 "$META_LOCAL/replica-exclusions" || true
printf '\n\n'
