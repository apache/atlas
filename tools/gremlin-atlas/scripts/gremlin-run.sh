#!/usr/bin/env bash
set -euo pipefail

# Run a Groovy script on the atlas pod using gremlin-100's classpath.
#
# Usage:
#   gremlin-run.sh <script-name-or-path> [kubectl-context]
#
# Built-in scripts (no path needed):
#   sanity-check, check-vertex, index-status, index-fields,
#   check-property-in-index, schema-info
#
# Custom scripts: provide a local file path — it gets copied to the pod and executed.
#
# All built-in scripts are READ-ONLY. They never commit management transactions
# or modify graph data.

SCRIPT="${1:-}"
CTX="${2:-}"
NS="atlas"
POD="atlas-0"
CONTAINER="atlas-main"
GREMLIN_DIR="/tmp/gremlin-100-gremlin-1"
SKILL_DIR="$(cd "$(dirname "$0")" && pwd)"
GROOVY_DIR="$SKILL_DIR/groovy"

if [ -z "$SCRIPT" ]; then
  echo "Usage: gremlin-run.sh <script-name-or-path> [kubectl-context]"
  echo ""
  echo "Built-in scripts (all read-only):"
  if [ -d "$GROOVY_DIR" ]; then
    ls "$GROOVY_DIR"/*.groovy 2>/dev/null | xargs -I{} basename {} .groovy | sed 's/^/  /'
  fi
  exit 1
fi

# Build kubectl command with optional context
K="kubectl"
if [ -n "$CTX" ]; then
  K="kubectl --context $CTX"
fi

# Check gremlin-100 is set up on the pod
if ! $K -n "$NS" exec "$POD" -c "$CONTAINER" -- test -d "$GREMLIN_DIR/lib" 2>/dev/null; then
  echo "ERROR: gremlin-100 not found on pod. Run gremlin-setup.sh first."
  exit 1
fi

# Resolve script path
REMOTE_SCRIPT=""
if [ -f "$GROOVY_DIR/${SCRIPT}.groovy" ]; then
  # Built-in script
  LOCAL_SCRIPT="$GROOVY_DIR/${SCRIPT}.groovy"
  REMOTE_SCRIPT="/tmp/gremlin-skill-${SCRIPT}.groovy"
elif [ -f "$SCRIPT" ]; then
  # Custom local script
  LOCAL_SCRIPT="$SCRIPT"
  REMOTE_SCRIPT="/tmp/gremlin-skill-custom.groovy"
else
  echo "ERROR: Script not found: $SCRIPT"
  echo "Not a built-in script name or a valid file path."
  exit 1
fi

# Copy script to pod
$K -n "$NS" cp "$LOCAL_SCRIPT" "$POD:$REMOTE_SCRIPT" -c "$CONTAINER"

# Build classpath and run
$K -n "$NS" exec "$POD" -c "$CONTAINER" -- \
  bash -c "cd $GREMLIN_DIR && CP=\$(find lib -name '*.jar' | sort | tr '\n' ':'):conf && java -cp \"\$CP\" groovy.ui.GroovyMain $REMOTE_SCRIPT" 2>&1 | \
  grep -v "^SLF4J:" || true
