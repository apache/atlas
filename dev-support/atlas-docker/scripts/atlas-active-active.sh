#!/bin/bash
# =============================================================================
# Atlas Active-Active startup script
#
# Called by every Atlas container regardless of RUN_MODE.  The RUN_MODE
# environment variable drives which subsystems start:
#
#   INITIALIZER          One-shot: graph schema + type-defs + patches → exit 0
#   METADATA_SERVER      REST + search + entity CRUD (long-lived)
#   NOTIFICATION_PROCESSOR  Hook Kafka consumer (long-lived)
#   MONOLITHIC (default) Everything on one node (backward-compatible)
#
# The JVM receives RUN_MODE via -DRUN_MODE so AtlasRunMode.resolve() picks it
# up at class-load time (before Spring context is built).
#
# NOTE: no 'set -euo pipefail' — process management scripts use grep/ps/kill
#       commands that legitimately return non-zero (no match), and pipefail
#       would cause spurious exits.
# =============================================================================

RUN_MODE="${RUN_MODE:-MONOLITHIC}"
ATLAS_HOME="${ATLAS_HOME:-/opt/atlas}"
PROPS="${ATLAS_HOME}/conf/atlas-application.properties"
COMMON_PROPS="${ATLAS_HOME}/conf/atlas-application-common.properties"
BACKEND_PROPS="${ATLAS_HOME}/conf/atlas-application-backend.properties"
ATLAS_REBUILD_INDEX="${ATLAS_REBUILD_INDEX:-false}"
ATLAS_UPDATE_COMPOSITE_INDEX_STATUS="${ATLAS_UPDATE_COMPOSITE_INDEX_STATUS:-true}"
ATLAS_INDEX_RECOVERY_ENABLE="${ATLAS_INDEX_RECOVERY_ENABLE:-true}"
# Sentinel lives in /opt/atlas/data (the named volume mount point) so it
# persists across container restarts without shadowing the full installation.
SENTINEL="${ATLAS_HOME}/data/.setupDone"

echo "============================================================"
echo " Atlas Active-Active startup"
echo " RUN_MODE          = ${RUN_MODE}"
echo " ATLAS_HOME        = ${ATLAS_HOME}"
echo " ATLAS_VERSION     = ${ATLAS_VERSION:-unknown}"
echo " ATLAS_REBUILD_INDEX = ${ATLAS_REBUILD_INDEX}"
echo " ATLAS_UPDATE_COMPOSITE_INDEX_STATUS = ${ATLAS_UPDATE_COMPOSITE_INDEX_STATUS}"
echo " ATLAS_INDEX_RECOVERY_ENABLE = ${ATLAS_INDEX_RECOVERY_ENABLE}"
echo "============================================================"

remove_prop() {
    key="$1"
    awk -F= -v k="${key}" '$1 != k' "${PROPS}" > "${PROPS}.tmp" && mv "${PROPS}.tmp" "${PROPS}"
}

ensure_prop() {
    key="$1"
    value="$2"
    remove_prop "${key}"
    printf "\n%s=%s\n" "${key}" "${value}" >> "${PROPS}"
}

# ---------------------------------------------------------------------------
# One-time per-container configuration
# ---------------------------------------------------------------------------
if [ ! -f "${SENTINEL}" ]; then
    echo "[setup] First start — configuring atlas-application.properties…"

    encryptedPwd=$(${ATLAS_HOME}/bin/cputil.py -g -u admin -p atlasR0cks! -s | tail -1)
    echo "admin=ADMIN::${encryptedPwd}" > "${ATLAS_HOME}/conf/users-credentials.properties"

    chown -R atlas:atlas "${ATLAS_HOME}/"
    touch "${SENTINEL}"
    echo "[setup] Done — sentinel written to ${SENTINEL}"
else
    echo "[setup] Already configured (sentinel exists), skipping."
fi

# ---------------------------------------------------------------------------
# Build runtime atlas-application.properties from active-active layered files:
#   1) common properties
#   2) backend-specific overrides (hbase/postgres)
# This is active-active specific and avoids mutating bind-mounted source files.
# ---------------------------------------------------------------------------
if [ -f "${COMMON_PROPS}" ] && [ -f "${BACKEND_PROPS}" ]; then
    cat "${COMMON_PROPS}" "${BACKEND_PROPS}" > "${PROPS}"
else
    echo "[error] Missing layered active-active config files." >&2
    echo "[error] Expected: ${COMMON_PROPS} and ${BACKEND_PROPS}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Always reconcile required runtime properties.
# This prevents stale .setupDone state from leaving core backend properties
# unconfigured and breaking initializer startup.
# ---------------------------------------------------------------------------
ensure_prop "atlas.notification.embedded" "false"
ensure_prop "atlas.kafka.bootstrap.servers" "atlas-kafka.example.com:9092"
sed -i "/^atlas.kafka.zookeeper.connect=/d" "${PROPS}"
ensure_prop "atlas.rebuild.index" "${ATLAS_REBUILD_INDEX}"
ensure_prop "atlas.update.composite.index.status" "${ATLAS_UPDATE_COMPOSITE_INDEX_STATUS}"
if [ "${RUN_MODE}" = "METADATA_SERVER" ] || [ "${RUN_MODE}" = "MONOLITHIC" ]; then
    ensure_prop "atlas.index.recovery.enable" "${ATLAS_INDEX_RECOVERY_ENABLE}"
fi

# Ensure each container advertises a stable, unique Atlas HA server identity.
# This allows AtlasServerIdSelector to resolve node ID deterministically instead
# of falling back to "node-unknown"/hostname heuristics.
if [ "${RUN_MODE}" = "METADATA_SERVER" ] || [ "${RUN_MODE}" = "NOTIFICATION_PROCESSOR" ] || [ "${RUN_MODE}" = "MONOLITHIC" ]; then
    ATLAS_HTTP_PORT="${ATLAS_HTTP_PORT:-21000}"
    ATLAS_SERVER_HOST="${ATLAS_SERVER_HOST:-${HOSTNAME}}"
    ATLAS_SERVER_ID="${ATLAS_SERVER_ID:-${ATLAS_SERVER_HOST}}"
    ATLAS_SERVER_ID="$(printf "%s" "${ATLAS_SERVER_ID}" | tr -c '[:alnum:]_-' '_')"

    ensure_prop "atlas.server.ids" "${ATLAS_SERVER_ID}"
    ensure_prop "atlas.server.address.${ATLAS_SERVER_ID}" "${ATLAS_SERVER_HOST}:${ATLAS_HTTP_PORT}"
fi

# Header-based authentication — stateless, no session affinity needed.
# The client passes x-awc-username/x-awc-roles/x-awc-requestid headers
# and Atlas trusts them directly without maintaining server-side sessions.
if [ "${RUN_MODE}" = "METADATA_SERVER" ] || [ "${RUN_MODE}" = "MONOLITHIC" ]; then
    ensure_prop "atlas.authn.header.enabled" "true"
    ensure_prop "atlas.authn.header.username" "x-awc-username"
    ensure_prop "atlas.authn.header.roles" "x-awc-roles"
    ensure_prop "atlas.authn.header.requestid" "x-awc-requestid"
    # Active-active round-robin uses stateless auth headers; disable CSRF token
    # enforcement to avoid session-bound token mismatches across replicas.
    ensure_prop "atlas.rest-csrf.enabled" "false"
fi

# ---------------------------------------------------------------------------
# INITIALIZER: if initialization already completed in a prior run of this
# container, exit 0 immediately (Docker Compose may restart the container).
# ---------------------------------------------------------------------------
if [ "${RUN_MODE}" = "INITIALIZER" ]; then
    if grep -rl "initialization complete, exiting" "${ATLAS_HOME}/logs/" > /dev/null 2>&1; then
        echo "[initializer] Already completed in a prior run — exiting 0 immediately."
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# Pass RUN_MODE to the JVM
# ---------------------------------------------------------------------------
JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/java}"
if [ -z "${JAVA_BIN}" ] || [ ! -x "${JAVA_BIN}" ]; then
    JAVA_BIN="java"
fi

JAVA_MAJOR="$(${JAVA_BIN} -version 2>&1 | awk -F[\".] '/version/ {print $2}')"

# Keep JVM module opens aligned with main README guidance:
# - Java 8 / 11: no --add-opens flags
# - Java 17: required opens for reflective access used by graph initialization
if [ "${JAVA_MAJOR}" = "17" ]; then
    ATLAS_JAVA_OPEN_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED"
else
    ATLAS_JAVA_OPEN_OPTS=""
fi

export ATLAS_OPTS="${ATLAS_OPTS:-} ${ATLAS_JAVA_OPEN_OPTS} -DRUN_MODE=${RUN_MODE}"

echo "[start] Launching Atlas (RUN_MODE=${RUN_MODE})…"

if [ "${RUN_MODE}" = "INITIALIZER" ]; then
    # -------------------------------------------------------------------------
    # INITIALIZER: atlas_start.py blocks until the HTTP server responds, but in
    # INITIALIZER mode Atlas calls System.exit(0) after init — the JVM exits,
    # atlas_start.py times out, and returns AFTER the JVM is already gone.
    # Running atlas_start.py in the background lets us poll the log sentinel
    # directly without depending on atlas_start.py's return code or timing.
    # -------------------------------------------------------------------------
    su -c "cd ${ATLAS_HOME}/bin && ./atlas_start.py" atlas &
    ATLAS_START_PID=$!
    echo "[initializer] Atlas starting (bg PID=${ATLAS_START_PID})…"

    MAX_WAIT=900   # 15 minutes
    ELAPSED=0
    while [ ${ELAPSED} -lt ${MAX_WAIT} ]; do
        # Success: initialization complete sentinel in logs
        if grep -rl "initialization complete, exiting" "${ATLAS_HOME}/logs/" > /dev/null 2>&1; then
            echo "[initializer] Initialization complete — store is ready for peer nodes."
            exit 0
        fi

        # atlas_start.py exited (HTTP was ready) — that is expected.
        # The JVM may still be running and applying patches/types.
        # Keep waiting as long as the Atlas JVM process is alive.
        if ! kill -0 "${ATLAS_START_PID}" 2>/dev/null; then
            # Find the Atlas JVM process
            ATLAS_JVM_PID=$(ps -ef | grep -v grep | grep "org.apache.atlas.Atlas" | awk '{print $2}' | head -1)
            if [ -z "${ATLAS_JVM_PID}" ]; then
                # JVM also gone — do one final sentinel check
                if grep -rl "initialization complete, exiting" "${ATLAS_HOME}/logs/" > /dev/null 2>&1; then
                    echo "[initializer] Initialization complete — store is ready for peer nodes."
                    exit 0
                else
                    echo "[initializer][ERROR] Atlas JVM exited before initialization completed." >&2
                    exit 1
                fi
            fi
            # JVM still alive — keep polling (atlas_start.py exit is normal after HTTP is up)
        fi

        sleep 10
        ELAPSED=$((ELAPSED + 10))
        echo "[initializer] Initializing… (${ELAPSED}s / ${MAX_WAIT}s)"
    done
    echo "[initializer][ERROR] Initialization timed out after ${MAX_WAIT}s." >&2
    exit 1

else
    # -------------------------------------------------------------------------
    # Long-lived modes (METADATA_SERVER, NOTIFICATION_PROCESSOR, MONOLITHIC):
    # atlas_start.py returns after verifying HTTP is ready. Then we find the
    # JVM PID and keep the container alive.
    # -------------------------------------------------------------------------
    su -c "cd ${ATLAS_HOME}/bin && ./atlas_start.py" atlas

    ATLAS_PID=""
    i=0
    while [ $i -lt 12 ]; do
        if [ -f "${ATLAS_HOME}/logs/atlas.pid" ]; then
            _pid=$(cat "${ATLAS_HOME}/logs/atlas.pid" 2>/dev/null | tr -d '[:space:]')
            if [ -n "${_pid}" ] && kill -0 "${_pid}" 2>/dev/null; then
                ATLAS_PID="${_pid}"
                break
            fi
        fi
        _pid=$(ps -ef | grep -v grep | grep -i "org.apache.atlas.Atlas" | awk '{print $2}' | head -1)
        if [ -n "${_pid}" ]; then
            ATLAS_PID="${_pid}"
            break
        fi
        i=$((i + 1))
        sleep 5
    done

    if [ -z "${ATLAS_PID}" ]; then
        echo "[error] Atlas JVM did not start — check ${ATLAS_HOME}/logs/" >&2
        exit 1
    fi

    echo "[${RUN_MODE}] Atlas JVM started (PID=${ATLAS_PID})"
    tail --pid="${ATLAS_PID}" -f /dev/null
fi
