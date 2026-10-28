#!/usr/bin/env bash
set -euo pipefail
PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_DIR="${HOME}/mtechproject"
STAMP="$(date +%Y%m%d-%H%M%S)"

if [[ ! -d "$PROJECT_DIR/metadata" || ! -d "$PROJECT_DIR/storage" ]]; then
  echo "Expected project directories were not found under $PROJECT_DIR" >&2
  exit 1
fi

cp "$PROJECT_DIR/metadata/main.go" "$PROJECT_DIR/metadata/main.go.before-four-machine-$STAMP.bak"
cp "$PROJECT_DIR/storage/main.go" "$PROJECT_DIR/storage/main.go.before-four-machine-$STAMP.bak"
cp "$PACKAGE_DIR/metadata/main.go" "$PROJECT_DIR/metadata/main.go"
cp "$PACKAGE_DIR/storage/main.go" "$PROJECT_DIR/storage/main.go"

cd "$PROJECT_DIR"
gofmt -w metadata/main.go storage/main.go
go test ./...
go build ./...
echo "Updated source installed; original source files have timestamped backups."
echo "Existing go.mod/go.sum and metadata.db were not overwritten."
