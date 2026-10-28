#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/mtechproject"
# Use a fresh metadata database for the physical cluster so old localhost-only
# test records remain preserved in metadata.db.
export METADATA_DB="${METADATA_DB:-./metadata-cluster.db}"
export METADATA_PORT="${METADATA_PORT:-9000}"
echo "Metadata DB: $METADATA_DB"
echo "Metadata port: $METADATA_PORT"
exec go run metadata/main.go
