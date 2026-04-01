#!/bin/bash
# =============================================================================
# Atlas JanusGraph → Cassandra Migrator
#
# Packaged at /opt/apache-atlas/bin/atlas_migrate.sh inside the Atlas Docker image.
# Reads Cassandra/ES connection config from atlas-application.properties automatically.
# The migrator fat JAR ships in the WEB-INF/lib/ classpath.
#
# Usage:
#   /opt/apache-atlas/bin/atlas_migrate.sh                  # Full migration
#   /opt/apache-atlas/bin/atlas_migrate.sh --es-only        # ES reindex only
#   /opt/apache-atlas/bin/atlas_migrate.sh --validate-only  # Validate only
#   /opt/apache-atlas/bin/atlas_migrate.sh --fresh          # Clear state, restart
#   /opt/apache-atlas/bin/atlas_migrate.sh --dry-run        # Show config, don't run
#
# From kubectl:
#   kubectl exec -it atlas-0 -n atlas -c atlas-main -- \
#     /opt/apache-atlas/bin/atlas_migrate.sh --dry-run
# =============================================================================

set -euo pipefail

# ---- Configuration ----

ATLAS_HOME="${ATLAS_HOME:-/opt/apache-atlas}"
ATLAS_CONF="${ATLAS_HOME}/conf/atlas-application.properties"
MIGRATOR_JAR=""
PROPERTIES_FILE="/tmp/migrator.properties"
LOG_DIR="${ATLAS_HOME}/logs"
LOG_FILE="${LOG_DIR}/migrator-$(date +%Y%m%d-%H%M%S).log"

# Cassandra / ES hostnames — read from atlas-application.properties if available
CASSANDRA_HOST=""
CASSANDRA_PORT=""
CASSANDRA_DC=""
ES_HOST=""
ES_PORT=""
ES_PROTOCOL="http"

# Source JanusGraph keyspace (where edgestore lives)
SOURCE_KEYSPACE="${SOURCE_KEYSPACE:-atlas}"
SOURCE_EDGESTORE_TABLE="${SOURCE_EDGESTORE_TABLE:-edgestore}"

# Target keyspace (new Cassandra graph schema)
TARGET_KEYSPACE="${TARGET_KEYSPACE:-atlas_graph}"

# ES index names
# Source = JanusGraph index (copy mappings from), Target = CassandraGraph index (write docs to)
SOURCE_ES_INDEX="${SOURCE_ES_INDEX:-janusgraph_vertex_index}"
TARGET_ES_INDEX="${TARGET_ES_INDEX:-atlas_graph_vertex_index}"

# Tuning (in-cluster = fast, can be aggressive)
SCANNER_THREADS="${SCANNER_THREADS:-16}"
WRITER_THREADS="${WRITER_THREADS:-8}"
ES_BULK_SIZE="${ES_BULK_SIZE:-1000}"

# ID strategy and dedup claims
ID_STRATEGY="${ID_STRATEGY:-legacy}"          # "legacy" (UUID), "hash-jg" (SHA-256 of JG ID), or "deterministic" (identity-based SHA-256)
CLAIM_ENABLED="${CLAIM_ENABLED:-false}"       # LWT dedup claims during migration

# ---- Helpers ----

log()  { echo "[migrator] $*"; }
warn() { echo "[migrator] WARN: $*"; }
err()  { echo "[migrator] ERROR: $*"; exit 1; }

# Read a property value from atlas-application.properties
read_prop() {
    local key="$1"
    local default="${2:-}"
    if [ -f "$ATLAS_CONF" ]; then
        local val
        val=$(grep -E "^${key}=" "$ATLAS_CONF" 2>/dev/null | tail -1 | cut -d'=' -f2- | tr -d '[:space:]')
        if [ -n "$val" ]; then
            echo "$val"
            return
        fi
    fi
    echo "$default"
}

# ---- Find migrator JAR ----

find_migrator_jar() {
    local LIB_DIR="${ATLAS_HOME}/server/webapp/atlas/WEB-INF/lib"

    # Search in standard locations (priority order)
    local candidates=(
        "${ATLAS_HOME}/tools/atlas-graphdb-migrator.jar"
        "${LIB_DIR}/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
        "${ATLAS_HOME}/libext/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
        "/opt/jar-overrides/atlas-graphdb-migrator-3.0.0-SNAPSHOT.jar"
    )

    for candidate in "${candidates[@]}"; do
        if [ -f "$candidate" ]; then
            MIGRATOR_JAR="$candidate"
            return
        fi
    done

    # Glob search in WEB-INF/lib
    local found
    found=$(find "${ATLAS_HOME}/tools" "$LIB_DIR" "${ATLAS_HOME}/libext" /opt/jar-overrides \
            -name "atlas-graphdb-migrator*.jar" -not -name "original-*" 2>/dev/null | head -1)
    if [ -n "$found" ]; then
        MIGRATOR_JAR="$found"
        return
    fi

    err "Migrator JAR not found in ${LIB_DIR}/ or ${ATLAS_HOME}/libext/.
  Ensure the atlas-graphdb-migrator module is included in the build."
}

# ---- Read connection config from atlas-application.properties ----

read_atlas_config() {
    if [ ! -f "$ATLAS_CONF" ]; then
        err "atlas-application.properties not found at $ATLAS_CONF"
    fi

    log "Reading connection config from $ATLAS_CONF"

    # Cassandra — try the new lean-graph config keys first, then fall back to JanusGraph keys
    CASSANDRA_HOST=$(read_prop "atlas.cassandra.graph.hostname" "")
    if [ -z "$CASSANDRA_HOST" ]; then
        # Fall back to JanusGraph storage config
        CASSANDRA_HOST=$(read_prop "atlas.graph.storage.hostname" "atlas-cassandra")
    fi

    CASSANDRA_PORT=$(read_prop "atlas.cassandra.graph.port" "")
    if [ -z "$CASSANDRA_PORT" ]; then
        CASSANDRA_PORT=$(read_prop "atlas.graph.storage.port" "9042")
    fi

    CASSANDRA_DC=$(read_prop "atlas.cassandra.graph.datacenter" "")
    if [ -z "$CASSANDRA_DC" ]; then
        CASSANDRA_DC=$(read_prop "atlas.graph.storage.cql.local-datacenter" "datacenter1")
    fi

    # Elasticsearch
    local es_raw
    es_raw=$(read_prop "atlas.graph.index.search.hostname" "atlas-elasticsearch-master:9200")
    # Could be "host:port" or just "host"
    ES_HOST=$(echo "$es_raw" | cut -d':' -f1)
    local es_port_from_host
    es_port_from_host=$(echo "$es_raw" | grep -o ':[0-9]*' | tr -d ':' || true)
    ES_PORT="${es_port_from_host:-9200}"

    ES_PROTOCOL=$(read_prop "atlas.graph.index.search.elasticsearch.http.protocol" "http")

    # Target keyspace from lean-graph config
    local tgt_ks
    tgt_ks=$(read_prop "atlas.cassandra.graph.keyspace" "")
    if [ -n "$tgt_ks" ]; then
        TARGET_KEYSPACE="$tgt_ks"
    fi

    log "  Cassandra: ${CASSANDRA_HOST}:${CASSANDRA_PORT} (dc=${CASSANDRA_DC})"
    log "  ES:        ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
    log "  Source keyspace: ${SOURCE_KEYSPACE} (table: ${SOURCE_EDGESTORE_TABLE})"
    log "  Target keyspace: ${TARGET_KEYSPACE}"
    log "  Source ES index: ${SOURCE_ES_INDEX} (copy mappings from)"
    log "  Target ES index: ${TARGET_ES_INDEX} (write docs to)"
}

# ---- Generate migrator.properties ----

generate_properties() {
    log "Generating $PROPERTIES_FILE"

    cat > "$PROPERTIES_FILE" << EOF
# Auto-generated by atlas_migrate.sh at $(date)
# Source: atlas-application.properties at $ATLAS_CONF

# Source: JanusGraph schema resolution
source.janusgraph.config=${ATLAS_CONF}

# Source Cassandra (JanusGraph edgestore)
source.cassandra.hostname=${CASSANDRA_HOST}
source.cassandra.port=${CASSANDRA_PORT}
source.cassandra.keyspace=${SOURCE_KEYSPACE}
source.cassandra.datacenter=${CASSANDRA_DC}
source.cassandra.username=
source.cassandra.password=
source.cassandra.edgestore.table=${SOURCE_EDGESTORE_TABLE}

# Source Elasticsearch (copy mappings from JanusGraph index)
source.elasticsearch.index=${SOURCE_ES_INDEX}

# Target Cassandra (new schema)
target.cassandra.hostname=${CASSANDRA_HOST}
target.cassandra.port=${CASSANDRA_PORT}
target.cassandra.keyspace=${TARGET_KEYSPACE}
target.cassandra.datacenter=${CASSANDRA_DC}
target.cassandra.username=
target.cassandra.password=

# Target Elasticsearch (write migrated docs here)
target.elasticsearch.hostname=${ES_HOST}
target.elasticsearch.port=${ES_PORT}
target.elasticsearch.protocol=${ES_PROTOCOL}
target.elasticsearch.index=${TARGET_ES_INDEX}
target.elasticsearch.username=
target.elasticsearch.password=

# Tuning
migration.scanner.threads=${SCANNER_THREADS}
migration.writer.threads=${WRITER_THREADS}
migration.es.bulk.size=${ES_BULK_SIZE}
migration.scan.fetch.size=5000
migration.queue.capacity=10000
migration.resume=true

# ID strategy / dedup
migration.id.strategy=${ID_STRATEGY}
migration.claim.enabled=${CLAIM_ENABLED}
EOF

    log "Properties written to $PROPERTIES_FILE"
}

# ---- Preflight checks ----

preflight() {
    log "Running preflight checks..."

    # Check Java
    if ! java -version 2>&1 | head -1; then
        err "Java not found in PATH"
    fi

    # Check Cassandra connectivity
    if command -v nc >/dev/null 2>&1; then
        if nc -z -w3 "$CASSANDRA_HOST" "$CASSANDRA_PORT" 2>/dev/null; then
            log "  Cassandra reachable at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
        else
            warn "Cannot reach Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
        fi
    fi

    # Check ES connectivity
    if command -v curl >/dev/null 2>&1; then
        local es_status
        es_status=$(curl -s -o /dev/null -w "%{http_code}" "${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}/" 2>/dev/null || echo "000")
        if [ "$es_status" = "200" ]; then
            log "  ES reachable at ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
        else
            warn "ES returned HTTP $es_status at ${ES_PROTOCOL}://${ES_HOST}:${ES_PORT}"
        fi
    fi

    log "Preflight complete"
}

# ---- Run migrator ----

run() {
    local extra_args="${1:-}"

    log ""
    log "=========================================="
    log "  JanusGraph -> Cassandra Migrator"
    if [ -n "$extra_args" ]; then
        log "  Mode: $extra_args"
    else
        log "  Mode: full migration"
    fi
    log "  JAR:  $MIGRATOR_JAR"
    log "  Log:  $LOG_FILE"
    log "=========================================="
    log ""

    java \
        -Xmx4g -Xms2g \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        -jar "$MIGRATOR_JAR" \
        "$PROPERTIES_FILE" \
        $extra_args \
        2>&1 | tee "$LOG_FILE"

    log ""
    log "Migration log saved to $LOG_FILE"
}

# ---- Help ----

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Run from inside an Atlas pod. Reads Cassandra/ES config from atlas-application.properties."
    echo ""
    echo "Options:"
    echo "  (no args)          Full migration: scan + ES reindex + validate"
    echo "  --es-only          Re-index Elasticsearch only"
    echo "  --validate-only    Validate migration completeness"
    echo "  --fresh            Clear resume state, start from scratch"
    echo "  --dry-run          Generate properties and show config without running"
    echo "  --help             Show this help"
    echo ""
    echo "Environment overrides:"
    echo "  SOURCE_KEYSPACE        Source JanusGraph keyspace (default: atlas)"
    echo "  SOURCE_EDGESTORE_TABLE Edgestore table name (default: edgestore)"
    echo "  TARGET_KEYSPACE        Target keyspace (default: atlas_graph)"
    echo "  SOURCE_ES_INDEX        Source ES index to copy mappings from (default: janusgraph_vertex_index)"
    echo "  TARGET_ES_INDEX        Target ES index to write docs to (default: atlas_graph_vertex_index)"
    echo "  SCANNER_THREADS        Scanner parallelism (default: 16)"
    echo "  WRITER_THREADS         Writer parallelism (default: 8)"
    echo "  ID_STRATEGY            ID generation: legacy, hash-jg, or deterministic (identity-based SHA-256) (default: legacy)"
    echo "  CLAIM_ENABLED          LWT dedup claims during migration: true/false (default: false)"
    echo ""
    echo "Quick start (from kubectl):"
    echo "  kubectl exec -it atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh --dry-run"
    echo "  kubectl exec -it atlas-0 -n atlas -c atlas-main -- /opt/apache-atlas/bin/atlas_migrate.sh"
}

# ---- Main ----

case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --dry-run)
        find_migrator_jar
        read_atlas_config
        generate_properties
        preflight
        log ""
        log "Dry run complete. Properties at $PROPERTIES_FILE"
        log "To run: java -Xmx4g -jar $MIGRATOR_JAR $PROPERTIES_FILE"
        ;;
    --es-only|--validate-only|--fresh|"")
        find_migrator_jar
        read_atlas_config
        generate_properties
        preflight
        run "${1:-}"
        ;;
    *)
        err "Unknown option: $1 (use --help)"
        ;;
esac
