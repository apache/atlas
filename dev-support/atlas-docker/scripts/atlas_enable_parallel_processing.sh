#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="../atlas-docker/docker-compose.atlas-active-active.yml"
WORKDIR="../atlas-docker"
cd "$WORKDIR"
for cid in $(docker compose -f "$COMPOSE_FILE" ps -q atlas-notification-proc); do
  echo "Updating $cid ..."
  docker exec "$cid" bash -lc '
    PROPS=/opt/atlas/conf/atlas-atlas-application.properties
    upsert() {
      k="$1"; v="$2"
      if grep -q "^${k}=" "$PROPS"; then
        sed -i "s|^${k}=.*|${k}=${v}|" "$PROPS"
      else
        printf "\n%s=%s\n" "$k" "$v" >> "$PROPS"
      fi
    }
    upsert atlas.notification.parallel.processing.enabled true
    upsert atlas.notification.parallel.processing.input.topics "ATLAS_HOOK,ATLAS_SPARK_HOOK"
    upsert atlas.notification.processor.metadata.topic.count 5
    upsert atlas.notification.processor.lineage.topic.count 3
    upsert atlas.notification.hook.consumer.topic.names "ATLAS_METADATA_0,ATLAS_METADATA_1,ATLAS_METADATA_2,ATLAS_METADATA_3,ATLAS_METADATA_4,ATLAS_LINEAGE_0,ATLAS_LINEAGE_1,ATLAS_LINEAGE_2"
  '
done
docker compose -f "$COMPOSE_FILE" restart atlas-notification-proc
echo "Applied + restarted. Effective values:"
for cid in $(docker compose -f "$COMPOSE_FILE" ps -q atlas-notification-proc); do
  echo "---- $cid ----"
  docker exec "$cid" bash -lc \
    "grep -E '^atlas.notification.(parallel.processing|processor\\.|hook.consumer.topic.names)' /opt/atlas/conf/atlas-application.properties"
done
