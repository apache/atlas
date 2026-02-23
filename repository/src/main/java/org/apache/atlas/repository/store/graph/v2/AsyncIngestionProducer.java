package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka producer for async ingestion events.
 * <p>
 * Publishes request payloads to a Kafka topic after the JanusGraph transaction
 * succeeds, so a shadow lean-graph consumer can replay the writes in parallel.
 * <p>
 * Design principles:
 * - Lazy producer initialization (created on first use via double-checked locking)
 * - Best-effort publish (logs errors, does NOT fail the HTTP request)
 * - Config-gated at the call site via DynamicConfigStore.isAsyncIngestionEnabled()
 */
@Service
public class AsyncIngestionProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIngestionProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROPERTY_PREFIX = "atlas.kafka";
    private static final String CONFIG_TOPIC = "atlas.async.ingestion.topic";
    private static final String DEFAULT_TOPIC = "ATLAS_ASYNC_ENTITIES";

    private volatile KafkaProducer<String, String> producer;
    private String topic;

    // Micrometer metrics
    private Counter sendSuccessCounter;
    private Counter sendFailureCounter;
    private Timer sendLatencyTimer;

    @PostConstruct
    public void init() {
        try {
            Configuration appConfig = ApplicationProperties.get();
            this.topic = appConfig.getString(CONFIG_TOPIC, DEFAULT_TOPIC);
        } catch (Exception e) {
            LOG.warn("Failed to read async ingestion config, using defaults", e);
            this.topic = DEFAULT_TOPIC;
        }

        try {
            io.micrometer.core.instrument.MeterRegistry registry = MetricUtils.getMeterRegistry();
            this.sendSuccessCounter = Counter.builder("async.ingestion.producer.send.success")
                    .description("Successful async ingestion Kafka publishes")
                    .register(registry);
            this.sendFailureCounter = Counter.builder("async.ingestion.producer.send.failure")
                    .description("Failed async ingestion Kafka publishes")
                    .register(registry);
            this.sendLatencyTimer = Timer.builder("async.ingestion.producer.send.latency")
                    .description("Latency of async ingestion Kafka send")
                    .register(registry);
        } catch (Exception e) {
            LOG.warn("Failed to register async ingestion metrics", e);
        }

        LOG.info("AsyncIngestionProducer initialized - topic: {}", topic);
    }

    /**
     * Generic publish method for all async ingestion events.
     * Best-effort: logs errors but does not throw.
     *
     * @param eventType         e.g. "BULK_CREATE_OR_UPDATE", "DELETE_BY_GUIDS"
     * @param operationMetadata query params / flags specific to the operation
     * @param payload           the serializable request body (entities, guids, objectIds, etc.)
     * @param requestMetadata   trace/user/method context
     * @return eventId (UUID) on success, null on failure
     */
    public String publishEvent(String eventType,
                               Map<String, Object> operationMetadata,
                               Object payload,
                               RequestMetadata requestMetadata) {
        String eventId = UUID.randomUUID().toString();
        try {
            ObjectNode envelope = MAPPER.createObjectNode();
            envelope.put("eventId", eventId);
            envelope.put("eventType", eventType);
            envelope.put("eventTime", System.currentTimeMillis());
            envelope.set("requestMetadata", MAPPER.valueToTree(requestMetadata));
            envelope.set("operationMetadata", MAPPER.valueToTree(operationMetadata));
            envelope.set("payload", MAPPER.valueToTree(payload));

            String json = MAPPER.writeValueAsString(envelope);

            KafkaProducer<String, String> p = getOrCreateProducer();
            if (p == null) {
                LOG.error("AsyncIngestionProducer: Kafka producer unavailable, skipping event {}", eventType);
                incrementFailure();
                return null;
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventId, json);

            Timer.Sample sample = sendLatencyTimer != null ? Timer.start() : null;

            String evtType = eventType;
            String evtId = eventId;
            p.send(record, (metadata, exception) -> {
                if (sample != null && sendLatencyTimer != null) {
                    sample.stop(sendLatencyTimer);
                }
                if (exception != null) {
                    LOG.error("AsyncIngestionProducer: failed to publish {} event {} (non-fatal)", evtType, evtId, exception);
                    incrementFailure();
                } else {
                    incrementSuccess();
                    LOG.debug("AsyncIngestionProducer: published {} event {} to {}@{}", evtType, evtId,
                            metadata.topic(), metadata.partition());
                }
            });
            return eventId;

        } catch (Exception e) {
            LOG.error("AsyncIngestionProducer: failed to publish {} event {} (non-fatal)", eventType, eventId, e);
            incrementFailure();
            return null;
        }
    }

    @PreDestroy
    public void shutdown() {
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(10));
                LOG.info("AsyncIngestionProducer: Kafka producer closed");
            } catch (Exception e) {
                LOG.warn("AsyncIngestionProducer: error closing Kafka producer", e);
            }
        }
    }

    private KafkaProducer<String, String> getOrCreateProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    try {
                        Configuration appConfig = ApplicationProperties.get();
                        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(appConfig, PROPERTY_PREFIX);

                        Properties props = ConfigurationConverter.getProperties(kafkaConf);

                        // Override serializers
                        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringSerializer");
                        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                "org.apache.kafka.common.serialization.StringSerializer");

                        // Ensure acks=all for durability
                        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");

                        // Bound how long send() blocks when buffer is full (prevents HTTP thread hang)
                        props.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");

                        // Bound total delivery time (send + retries) — callbacks fire after this
                        props.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");

                        // Per-request timeout to broker
                        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

                        // Limit retries to avoid prolonged send attempts
                        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, "3");

                        // Micro-batch for throughput
                        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "10");

                        producer = new KafkaProducer<>(props);
                        LOG.info("AsyncIngestionProducer: Kafka producer created for topic {}", topic);
                    } catch (Exception e) {
                        LOG.error("AsyncIngestionProducer: failed to create Kafka producer", e);
                        return null;
                    }
                }
            }
        }
        return producer;
    }

    private void incrementSuccess() {
        if (sendSuccessCounter != null) {
            sendSuccessCounter.increment();
        }
    }

    private void incrementFailure() {
        if (sendFailureCounter != null) {
            sendFailureCounter.increment();
        }
    }

    // Visible for testing
    String getTopic() {
        return topic;
    }
}
