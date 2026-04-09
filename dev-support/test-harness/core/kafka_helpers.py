"""Kafka notification verification for ATLAS_ENTITIES topic."""

import json
import time
import uuid

# Graceful import - kafka-python is optional
try:
    from kafka import KafkaConsumer, TopicPartition
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


ATLAS_ENTITIES_TOPIC = "ATLAS_ENTITIES"
POLL_TIMEOUT_S = 30
POLL_INTERVAL_S = 2


class KafkaVerifier:
    """Verifies entity notifications on ATLAS_ENTITIES topic.

    Gracefully degrades: if kafka-python is not installed or the broker
    is unreachable, all operations silently no-op.
    """

    def __init__(self, bootstrap_servers, seek_back_seconds=60):
        self._consumer = None
        self._enabled = False
        if not KAFKA_AVAILABLE:
            print("  [Kafka] kafka-python not installed, skipping Kafka verification")
            return
        if not bootstrap_servers:
            return
        try:
            group_id = f"test-harness-{uuid.uuid4().hex[:8]}"
            self._consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='latest',
                consumer_timeout_ms=2000,
                value_deserializer=lambda m: m,  # raw bytes
            )
            # Assign all partitions and seek back
            partitions = self._consumer.partitions_for_topic(ATLAS_ENTITIES_TOPIC)
            if not partitions:
                print(f"  [Kafka] Topic {ATLAS_ENTITIES_TOPIC} not found")
                return
            tps = [TopicPartition(ATLAS_ENTITIES_TOPIC, p) for p in partitions]
            self._consumer.assign(tps)
            # Seek to recent: end - 1000 as a rough "recent" window
            end_offsets = self._consumer.end_offsets(tps)
            begin_offsets = self._consumer.beginning_offsets(tps)
            for tp in tps:
                target = max(begin_offsets[tp], end_offsets[tp] - 1000)
                self._consumer.seek(tp, target)
            self._enabled = True
            print(f"  [Kafka] Connected, consuming {ATLAS_ENTITIES_TOPIC} ({len(partitions)} partitions)")
        except Exception as e:
            print(f"  [Kafka] Could not connect: {e}")

    @property
    def enabled(self):
        return self._enabled

    def find_notification(self, entity_guid, operation_type=None, timeout_s=POLL_TIMEOUT_S):
        """Poll for a notification matching entity_guid and optional operationType.

        Returns the parsed message dict if found, None otherwise.
        """
        if not self._enabled:
            return None
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            records = self._consumer.poll(timeout_ms=int(POLL_INTERVAL_S * 1000))
            for tp, messages in records.items():
                for msg in messages:
                    parsed = self._parse_message(msg.value)
                    if not parsed:
                        continue
                    msg_guid = self._extract_guid(parsed)
                    msg_op = self._extract_operation(parsed)
                    if msg_guid == entity_guid:
                        if operation_type is None or msg_op == operation_type:
                            return parsed
        return None

    def _parse_message(self, raw_bytes):
        try:
            text = raw_bytes.decode("utf-8")
            return json.loads(text)
        except Exception:
            return None

    def _extract_guid(self, msg):
        # AtlasNotificationMessage wraps the entity notification
        entity = msg.get("message", msg).get("entity", {})
        return entity.get("guid")

    def _extract_operation(self, msg):
        return msg.get("message", msg).get("operationType")

    def close(self):
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass


def assert_entity_in_kafka(ctx, entity_guid, operation_type, timeout_s=POLL_TIMEOUT_S):
    """Soft assertion: check entity notification exists on ATLAS_ENTITIES.

    Returns the message dict if found, None if Kafka is not available or
    message not found. Prints a warning but does NOT fail the test if not
    found (Kafka is best-effort).
    """
    verifier = ctx.get("kafka_verifier")
    if not verifier or not verifier.enabled:
        return None
    result = verifier.find_notification(entity_guid, operation_type, timeout_s)
    if result is None:
        print(f"    [Kafka] WARNING: No {operation_type} notification found for {entity_guid}")
    else:
        print(f"    [Kafka] OK: {operation_type} notification found for {entity_guid}")
    return result
