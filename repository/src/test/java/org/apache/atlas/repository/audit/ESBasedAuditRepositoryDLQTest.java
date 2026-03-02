/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE;
import org.apache.atlas.exception.AtlasBaseException;

import java.lang.reflect.Constructor;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for ESBasedAuditRepository entity audit retry + DLQ behaviour (MS-642).
 *
 * Flow covered:
 * - Request thread: on audit failure, enqueue to in-memory queue only (no DLQ); request does not fail.
 * - Background thread: poll queue, retry with backoff; after max retries, publish to Kafka DLQ (or drop).
 *
 * Scenarios:
 * - null / empty events: no enqueue, no throw
 * - Retry disabled (DLQ disabled): log only, no enqueue
 * - Write fails with retry enabled: enqueue (request succeeds)
 * - Queue full: request still succeeds, events dropped with log
 * - Replay: entry re-queued on failure when retries left; after max retries entry not re-queued (drop/DLQ path)
 * - All-null-entity: empty bulk, no ES call, no enqueue
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ESBasedAuditRepositoryDLQTest {

    private static final String ENTITY_AUDIT_DLQ_ENABLED_PROP = "atlas.entity.audit.dlq.enabled";
    private static final String ENTITY_AUDIT_DLQ_QUEUE_CAPACITY_PROP = "atlas.entity.audit.dlq.queue.capacity";
    private static final String ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP = "atlas.entity.audit.dlq.max.retries";

    private Configuration config;
    private EntityGraphRetriever entityGraphRetriever;
    private ESBasedAuditRepository repo;

    @BeforeAll
    void initApplicationProperties() throws Exception {
        config = new PropertiesConfiguration();
        config.setProperty("atlas.graph.storage.hostname", "localhost");
        config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
        config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, true);
        config.setProperty(ENTITY_AUDIT_DLQ_QUEUE_CAPACITY_PROP, 10);
        config.setProperty(ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP, 2);
        ApplicationProperties.set(config);
    }

    @BeforeEach
    void setUp() {
        RequestContext.clear();
        RequestContext.get();
        config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, true);
        entityGraphRetriever = mock(EntityGraphRetriever.class);
        repo = new ESBasedAuditRepository(config, entityGraphRetriever);
        // Do not call start() so lowLevelClient stays null and putEventsV2Internal will throw on first ES call
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(10);
        ReflectionTestUtils.setField(repo, "auditDlqQueue", queue);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    private List<EntityAuditEventV2> oneEvent() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId("guid-1");
        event.setAction(ENTITY_CREATE);
        event.setDetails("Created: {}");
        event.setUser("test-user");
        event.setTimestamp(System.currentTimeMillis());
        event.setEntityQualifiedName("\"qn1\"");
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("qualifiedName", "qn1");
        entity.setAttribute("updateTime", new Date());
        event.setEntity(entity);
        return List.of(event);
    }

    private List<EntityAuditEventV2> eventWithNullEntity() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId("guid-2");
        event.setAction(ENTITY_CREATE);
        event.setDetails("Created: {}");
        event.setUser("test-user");
        event.setTimestamp(System.currentTimeMillis());
        event.setEntity(null);
        return List.of(event);
    }

    @Nested
    @DisplayName("putEventsV2 does not fail request on audit write failure")
    class RequestDoesNotFail {

        @Test
        @DisplayName("null events: no throw, no enqueue")
        void nullEvents() {
            assertDoesNotThrow(() -> repo.putEventsV2((List<EntityAuditEventV2>) null));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("empty events: no throw, no enqueue")
        void emptyEvents() {
            assertDoesNotThrow(() -> repo.putEventsV2(new ArrayList<>()));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("write fails with retry enabled: request succeeds, events enqueued to retry queue")
        void failureEnqueuedWhenRetryEnabled() {
            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));

            assertEquals(1, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("write fails with retry disabled: request succeeds, no enqueue")
        void failureNotEnqueuedWhenRetryDisabled() {
            config.setProperty(ENTITY_AUDIT_DLQ_ENABLED_PROP, false);
            ReflectionTestUtils.setField(repo, "auditDlqQueue", new LinkedBlockingQueue<>(10));

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));

            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("queue full: request still succeeds, events dropped with log")
        void queueFullRequestSucceeds() {
            LinkedBlockingQueue<Object> smallQueue = new LinkedBlockingQueue<>(1);
            ReflectionTestUtils.setField(repo, "auditDlqQueue", smallQueue);

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, smallQueue.size());

            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, smallQueue.size());
        }

        @Test
        @DisplayName("mapping/permanent ES error: detector returns true so events are NOT enqueued (poison-pill guard)")
        void permanentErrorNotEnqueued() {
            assertTrue(ESBasedAuditRepository.isMappingOrPermanentException(
                    new AtlasBaseException("mapper_parsing_exception: failed to parse field [type]")));
            assertTrue(ESBasedAuditRepository.isMappingOrPermanentException(
                    new RuntimeException("illegal_argument_exception: ...")));
            assertTrue(ESBasedAuditRepository.isMappingOrPermanentException(
                    new AtlasBaseException("index_not_found_exception: no such index [entity_audits]")));
            assertFalse(ESBasedAuditRepository.isMappingOrPermanentException(
                    new AtlasBaseException("circuit_breaking_exception: data too large")));
        }
    }

    @Nested
    @DisplayName("Retry queue / replay behaviour (background thread behaviour simulated)")
    class RetryReplay {

        @Test
        @DisplayName("processOneDlqEntryForTest: entry re-queued on failure when retries left")
        void replayRequeuesOnFailure() {
            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, repo.getAuditDlqQueueSize());

            boolean processed = repo.processOneDlqEntryForTest();
            assertTrue(processed);
            assertEquals(1, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("processOneDlqEntryForTest: no entry returns false")
        void processOneWhenEmptyReturnsFalse() {
            boolean processed = repo.processOneDlqEntryForTest();
            assertFalse(processed);
        }

        @Test
        @DisplayName("after max retries entry is not re-queued (drop or DLQ path)")
        void afterMaxRetriesEntryNotRequeued() {
            config.setProperty(ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP, 1);
            assertDoesNotThrow(() -> repo.putEventsV2(oneEvent()));
            assertEquals(1, repo.getAuditDlqQueueSize());

            repo.processOneDlqEntryForTest();
            assertEquals(1, repo.getAuditDlqQueueSize());

            repo.processOneDlqEntryForTest();
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("replay: events dropped after max retries exhausted (injected entry at retryCount == maxRetries)")
        void replayDropsAfterMaxRetries() throws Exception {
            config.setProperty(ENTITY_AUDIT_DLQ_MAX_RETRIES_PROP, 2);
            @SuppressWarnings("unchecked")
            BlockingQueue<Object> queue = (BlockingQueue<Object>) ReflectionTestUtils.getField(repo, "auditDlqQueue");
            queue.clear();

            Class<?> entryClass = null;
            for (Class<?> c : ESBasedAuditRepository.class.getDeclaredClasses()) {
                if ("EntityAuditDLQEntry".equals(c.getSimpleName())) {
                    entryClass = c;
                    break;
                }
            }
            assertNotNull(entryClass);
            Constructor<?> ctor = entryClass.getDeclaredConstructor(List.class, int.class);
            ctor.setAccessible(true);
            Object entry = ctor.newInstance(oneEvent(), 2);
            queue.offer(entry);

            boolean processed = repo.processOneDlqEntryForTest();
            assertTrue(processed);
            assertEquals(0, repo.getAuditDlqQueueSize());
        }
    }

    @Nested
    @DisplayName("putEventsV2Internal edge cases (no NPE / no empty bulk)")
    class InternalEdgeCases {

        @Test
        @DisplayName("events with all null entity: build empty bulk, return early, no ES call")
        void allNullEntitySkipsBulk() {
            assertDoesNotThrow(() -> repo.putEventsV2(eventWithNullEntity()));
            assertEquals(0, repo.getAuditDlqQueueSize());
        }

        @Test
        @DisplayName("details string shorter than prefix: no StringIndexOutOfBoundsException")
        void detailsShorterThanPrefixNoException() {
            assertDoesNotThrow(() -> repo.putEventsV2(eventWithShortDetails()));
        }
    }

    @Nested
    @DisplayName("DLQ thread lifecycle (start / stop)")
    class DlqThreadLifecycle {

        @Test
        @DisplayName("start() creates DLQ queue; stop() shuts down cleanly and nulls queue")
        void dlqThreadLifecycle() throws Exception {
            ESBasedAuditRepository lifecycleRepo = new ESBasedAuditRepository(config, entityGraphRetriever);
            ESBasedAuditRepository spyRepo = spy(lifecycleRepo);
            doNothing().when(spyRepo).startInternal();

            spyRepo.start();
            assertNotNull(ReflectionTestUtils.getField(spyRepo, "auditDlqQueue"));
            assertDoesNotThrow(() -> assertEquals(0, spyRepo.getAuditDlqQueueSize()));

            spyRepo.stop();
            assertNull(ReflectionTestUtils.getField(spyRepo, "auditDlqQueue"));
        }
    }

    private List<EntityAuditEventV2> eventWithShortDetails() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId("guid-1");
        event.setAction(ENTITY_CREATE);
        event.setDetails("");
        event.setUser("test-user");
        event.setTimestamp(System.currentTimeMillis());
        event.setEntityQualifiedName("\"qn1\"");
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("qualifiedName", "qn1");
        entity.setAttribute("updateTime", new Date());
        event.setEntity(entity);
        return List.of(event);
    }

}
