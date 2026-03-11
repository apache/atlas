#!/usr/bin/env bash
set -euo pipefail

# Setup gremlin-100 on the atlas pod for in-pod JanusGraph debugging.
# Downloads the gremlin-100 zip, extracts it, and writes a read-only config.
# Only needs to run once per pod restart (files live in /tmp).
#
# Usage: gremlin-setup.sh [kubectl-context]
#
# This script only writes to /tmp on the pod. No graph data is modified.

CTX="${1:-}"
NS="atlas"
POD="atlas-0"
CONTAINER="atlas-main"
GREMLIN_DIR="/tmp/gremlin-100-gremlin-1"
GREMLIN_ZIP_URL="https://github.com/nikhilbonte/gremlin-100/archive/refs/tags/gremlin-1.zip"

# Build kubectl command with optional context
K="kubectl"
if [ -n "$CTX" ]; then
  K="kubectl --context $CTX"
fi

echo "=== Gremlin-100 Setup on $POD ==="

# Check if already set up
if $K -n "$NS" exec "$POD" -c "$CONTAINER" -- test -d "$GREMLIN_DIR/lib" 2>/dev/null; then
  echo "gremlin-100 already exists at $GREMLIN_DIR"
  echo "To force re-setup, run: $K -n $NS exec $POD -c $CONTAINER -- rm -rf $GREMLIN_DIR"
  exit 0
fi

echo ""
echo "Step 1: Downloading gremlin-100 zip (~379MB)..."
$K -n "$NS" exec "$POD" -c "$CONTAINER" -- \
  bash -c "cd /tmp && wget -q --show-progress -O gremlin-1.zip '$GREMLIN_ZIP_URL'" || {
    echo "wget failed, trying curl..."
    $K -n "$NS" exec "$POD" -c "$CONTAINER" -- \
      bash -c "cd /tmp && curl -L -o gremlin-1.zip '$GREMLIN_ZIP_URL'"
  }

echo ""
echo "Step 2: Extracting with jar xf..."
$K -n "$NS" exec "$POD" -c "$CONTAINER" -- \
  bash -c 'cd /tmp && mkdir -p gremlin-extract && cd gremlin-extract && jar xf /tmp/gremlin-1.zip && mv gremlin-100-gremlin-1 /tmp/ && cd /tmp && rm -rf gremlin-extract gremlin-1.zip'

echo ""
echo "Step 3: Writing JanusGraph config..."
CONFIG_CONTENT='gremlin.graph=org.janusgraph.core.JanusGraphFactory
storage.backend=cql
storage.hostname=atlas-cassandra
storage.cql.keyspace=atlas
storage.cql.local-datacenter=datacenter1
cache.db-cache=false
index.search.backend=elasticsearch
index.search.hostname=atlas-elasticsearch-master:9200
index.search.elasticsearch.http.auth.type=none'
$K -n "$NS" exec "$POD" -c "$CONTAINER" -- \
  bash -c "echo '$CONFIG_CONTENT' > $GREMLIN_DIR/conf/janusgraph-cql-es.properties"

echo ""
echo "Step 4: Verifying installation..."
FILE_COUNT=$($K -n "$NS" exec "$POD" -c "$CONTAINER" -- bash -c "ls $GREMLIN_DIR/lib/*.jar 2>/dev/null | wc -l")
echo "Found $FILE_COUNT jars in $GREMLIN_DIR/lib/"

if [ "$FILE_COUNT" -gt 100 ]; then
  echo ""
  echo "Setup complete! Run scripts with:"
  echo "  gremlin-run.sh <script-name> ${CTX:+$CTX}"
else
  echo "ERROR: Expected 100+ jars, got $FILE_COUNT. Setup may have failed."
  exit 1
fi
