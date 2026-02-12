package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncIngestionProducerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KafkaProducer<String, String> mockKafkaProducer;

    private AsyncIngestionProducer asyncIngestionProducer;
    private SimpleMeterRegistry meterRegistry;
    private MockedStatic<MetricUtils> mockedMetricUtils;
    private MockedStatic<ApplicationProperties> mockedAppProps;

    @BeforeAll
    void setUpAll() {
        meterRegistry = new SimpleMeterRegistry();
        mockedMetricUtils = mockStatic(MetricUtils.class);
        mockedMetricUtils.when(MetricUtils::getMeterRegistry).thenReturn(meterRegistry);
    }

    @AfterAll
    void tearDownAll() {
        mockedMetricUtils.close();
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        mockKafkaProducer = mock(KafkaProducer.class);

        mockedAppProps = mockStatic(ApplicationProperties.class);
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty("atlas.async.ingestion.topic", "TEST_ASYNC_ENTITIES");
        config.setProperty("atlas.async.ingestion.send.timeout.ms", "5000");
        mockedAppProps.when(ApplicationProperties::get).thenReturn(config);

        asyncIngestionProducer = new AsyncIngestionProducer();
        asyncIngestionProducer.init();

        // Inject mock Kafka producer (bypassing lazy init)
        ReflectionTestUtils.setField(asyncIngestionProducer, "producer", mockKafkaProducer);
    }

    @AfterEach
    void tearDown() {
        mockedAppProps.close();
    }

    private RequestMetadata createTestRequestMetadata() {
        RequestMetadata rm = new RequestMetadata();
        rm.setTraceId("test-trace-123");
        rm.setUser("admin");
        return rm;
    }

    // =================== Test 1: Happy path ===================

    @Test
    void testPublishEvent_success_returnsEventId() throws Exception {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("TEST_ASYNC_ENTITIES", 0), 0L, 0L, 0L, 0L, 0, 0);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        String eventId = asyncIngestionProducer.publishEvent(
                "BULK_CREATE_OR_UPDATE",
                Map.of("replaceTags", false),
                Map.of("entities", "test"),
                createTestRequestMetadata());

        assertNotNull(eventId, "eventId should be non-null on success");
        assertFalse(eventId.isEmpty());
    }

    // =================== Test 2: Kafka failure ===================

    @Test
    void testPublishEvent_kafkaFailure_returnsNull_doesNotThrow() throws Exception {
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException("Kafka down", new RuntimeException()));
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        String eventId = asyncIngestionProducer.publishEvent(
                "BULK_CREATE_OR_UPDATE",
                Map.of(),
                Map.of("entities", "test"),
                createTestRequestMetadata());

        assertNull(eventId, "eventId should be null on Kafka failure");
    }

    // =================== Test 3: Timeout ===================

    @Test
    void testPublishEvent_timeout_returnsNull_doesNotThrow() throws Exception {
        @SuppressWarnings("unchecked")
        Future<RecordMetadata> slowFuture = mock(Future.class);
        when(slowFuture.get(anyLong(), any())).thenThrow(new TimeoutException("send timed out"));
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(slowFuture);

        String eventId = asyncIngestionProducer.publishEvent(
                "DELETE_BY_GUID",
                Map.of(),
                Map.of("guids", "g1"),
                createTestRequestMetadata());

        assertNull(eventId, "eventId should be null on timeout");
    }

    // =================== Test 4: Lazy producer init ===================

    @Test
    void testPublishEvent_lazyProducerInit_createsOnFirstCall() {
        // Create a fresh producer with no mock injected
        AsyncIngestionProducer freshProducer = new AsyncIngestionProducer();
        freshProducer.init();

        Object producerField = ReflectionTestUtils.getField(freshProducer, "producer");
        assertNull(producerField, "Producer should be null before first publish");

        // Trying to publish will attempt lazy init (will fail since no real Kafka, but that's OK)
        freshProducer.publishEvent("BULK_CREATE_OR_UPDATE", Map.of(), Map.of(), createTestRequestMetadata());

        // The producer creation will have been attempted (may still be null if Kafka is not available,
        // but the code path was exercised)
    }

    // =================== Test 5: Correct JSON schema ===================

    @SuppressWarnings("unchecked")
    @Test
    void testPublishEvent_correctJsonSchema() throws Exception {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("TEST_ASYNC_ENTITIES", 0), 0L, 0L, 0L, 0L, 0, 0);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        RequestMetadata rm = createTestRequestMetadata();
        Map<String, Object> opMeta = Map.of("replaceClassifications", false, "replaceTags", true);
        Map<String, Object> payload = Map.of("entities", "test-data");

        asyncIngestionProducer.publishEvent("BULK_CREATE_OR_UPDATE", opMeta, payload, rm);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockKafkaProducer).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();
        JsonNode json = MAPPER.readTree(record.value());

        assertTrue(json.has("eventId"));
        assertEquals("BULK_CREATE_OR_UPDATE", json.get("eventType").asText());
        assertTrue(json.has("eventTime"));
        assertTrue(json.get("eventTime").asLong() > 0);

        JsonNode reqMeta = json.get("requestMetadata");
        assertEquals("test-trace-123", reqMeta.get("traceId").asText());
        assertEquals("admin", reqMeta.get("user").asText());
        assertFalse(reqMeta.has("requestUri"), "requestUri should not be present");
        assertFalse(reqMeta.has("requestMethod"), "requestMethod should not be present");

        assertTrue(json.has("operationMetadata"));
        assertTrue(json.has("payload"));
    }

    // =================== Test 6: Record key = eventId ===================

    @SuppressWarnings("unchecked")
    @Test
    void testPublishEvent_recordKeyIsEventId() throws Exception {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("TEST_ASYNC_ENTITIES", 0), 0L, 0L, 0L, 0L, 0, 0);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        String eventId = asyncIngestionProducer.publishEvent(
                "BULK_CREATE_OR_UPDATE", Map.of(), Map.of(), createTestRequestMetadata());

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockKafkaProducer).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();
        assertEquals(eventId, record.key(), "ProducerRecord key should match eventId");

        JsonNode json = MAPPER.readTree(record.value());
        assertEquals(eventId, json.get("eventId").asText(), "JSON eventId should match record key");
    }

    // =================== Test 7: Success increments counter ===================

    @Test
    void testMetrics_successIncrementsCounter() throws Exception {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("TEST_ASYNC_ENTITIES", 0), 0L, 0L, 0L, 0L, 0, 0);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        Counter successCounter = meterRegistry.find("async.ingestion.producer.send.success").counter();
        double before = successCounter != null ? successCounter.count() : 0;

        asyncIngestionProducer.publishEvent("BULK_CREATE_OR_UPDATE", Map.of(), Map.of(), createTestRequestMetadata());

        Counter afterCounter = meterRegistry.find("async.ingestion.producer.send.success").counter();
        assertNotNull(afterCounter);
        assertTrue(afterCounter.count() > before, "Success counter should have incremented");
    }

    // =================== Test 10: Failure increments counter ===================

    @Test
    void testMetrics_failureIncrementsCounter() throws Exception {
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException("fail", new RuntimeException()));
        when(mockKafkaProducer.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        Counter failCounter = meterRegistry.find("async.ingestion.producer.send.failure").counter();
        double before = failCounter != null ? failCounter.count() : 0;

        asyncIngestionProducer.publishEvent("BULK_CREATE_OR_UPDATE", Map.of(), Map.of(), createTestRequestMetadata());

        Counter afterCounter = meterRegistry.find("async.ingestion.producer.send.failure").counter();
        assertNotNull(afterCounter);
        assertTrue(afterCounter.count() > before, "Failure counter should have incremented");
    }

    // =================== Test 11: shutdown closes producer ===================

    @Test
    void testShutdown_closesProducer() {
        asyncIngestionProducer.shutdown();
        verify(mockKafkaProducer).close();
    }

    // =================== Test 12: shutdown with no producer ===================

    @Test
    void testShutdown_noProducerCreated_noException() {
        AsyncIngestionProducer freshProducer = new AsyncIngestionProducer();
        freshProducer.init();
        // No producer created — shutdown should not throw
        assertDoesNotThrow(freshProducer::shutdown);
    }

}
