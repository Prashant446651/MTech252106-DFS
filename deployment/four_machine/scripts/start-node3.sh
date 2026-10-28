#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/mtechproject"
mkdir -p "$HOME/mtechproject/data/node3"
METADATA_URL="${DFS_METADATA_URL:-http://10.0.11.149:9000}"
echo "Storage node: node3"
echo "Advertised URL: http://10.0.10.198:8003"
echo "Metadata URL: $METADATA_URL"
exec go run storage/main.go \
  --node "node3" \
  --port "8003" \
  --dir "$HOME/mtechproject/data/node3" \
  --metadata "$METADATA_URL" \
  --advertise "http://10.0.10.198:8003" \
  --peer "http://10.0.10.199:8001" \
  --peer "http://10.0.9.188:8002" \
  --peer "http://10.0.13.253:8004"
