#!/usr/bin/env bash

# Upload to Node 1 first, then try Node 2 and Node 3
# if a node cannot be contacted.

set -u

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file-path>"
    exit 1
fi

file="$1"

if [ ! -f "$file" ]; then
    echo "Error: File not found: $file"
    exit 1
fi

filename=$(basename -- "$file")

# Keep the name compatible with the storage server's
# single-filename validation.
if [[ ! "$filename" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "Error: Use only letters, numbers, dots, underscores, or hyphens in the filename."
    exit 1
fi

nodes=(
    "http://localhost:8001"
    "http://localhost:8002"
    "http://localhost:8003"
)

for node in "${nodes[@]}"; do
    echo
    echo "Trying $node ..."

    response_file=$(mktemp)

    http_code=$(curl -sS \
        --connect-timeout 2 \
        --max-time 60 \
        -o "$response_file" \
        -w "%{http_code}" \
        -X POST \
        --data-binary "@$file" \
        "$node/upload?name=$filename")

    curl_status=$?

    if [ "$curl_status" -ne 0 ]; then
        echo "Could not contact $node. Trying the next node."
        rm -f "$response_file"
        continue
    fi

    cat "$response_file"
    rm -f "$response_file"

    if [[ "$http_code" =~ ^2[0-9][0-9]$ ]]; then
        echo "Upload accepted by $node."

        if [ "$node" != "http://localhost:8001" ]; then
            echo "WARNING: A fallback node was used."
            echo "Verify the replica copies separately."
        fi

        exit 0
    fi

    echo "HTTP error $http_code from $node."
    echo "The file may already have been saved locally."
    echo "Check the replication status before retrying."
    exit 1
done

echo "Upload failed: no storage node could be contacted."
exit 1
