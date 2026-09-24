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
    # remove parallel-processing properties
    sed -i "/^atlas.notification.parallel.processing.input.topics=/d" "$PROPS"
    sed -i "/^atlas.notification.processor.metadata.topic.count=/d" "$PROPS"
    sed -i "/^atlas.notification.processor.lineage.topic.count=/d" "$PROPS"
    sed -i "/^atlas.notification.hook.consumer.topic.names=ATLAS_METADATA_0,ATLAS_METADATA_1,ATLAS_METADATA_2,ATLAS_METADATA_3,ATLAS_METADATA_4,ATLAS_LINEAGE_0,ATLAS_LINEAGE_1,ATLAS_LINEAGE_2$/d" "$PROPS"
    # disable parallel processing
    upsert atlas.notification.parallel.processing.enabled false
    upsert atlas.notification.hook.consumer.topic.names "ATLAS_HOOK,ATLAS_SPARK_HOOK"
  '
done
docker compose -f "$COMPOSE_FILE" restart atlas-notification-proc
echo "Done. Effective values:"
for cid in $(docker compose -f "$COMPOSE_FILE" ps -q atlas-notification-proc); do
  echo "---- $cid ----"
  docker exec "$cid" bash -lc "grep -E '^atlas.notification.(parallel.processing.enabled|parallel.processing.input.topics|processor.metadata.topic.count|processor.lineage.topic.count|hook.consumer.topic.names)' /opt/atlas/conf/atlas-application.properties || true"
done
