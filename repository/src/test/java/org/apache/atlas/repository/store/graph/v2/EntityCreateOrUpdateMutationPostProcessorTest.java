package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.model.ESDeferredOperation.OperationType;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EntityCreateOrUpdateMutationPostProcessor.
 *
 * Tests the MS-456 fix: when multiple ES operations target the same entity,
 * only the operation with the most complete data should be written.
 * Priority: ADD > UPDATE > DELETE (ADD runs last in commitChanges, has final state)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityCreateOrUpdateMutationPostProcessorTest {

    private EntityCreateOrUpdateMutationPostProcessor processor;

    // Static block runs before class loading - sets up config before TagDAOCassandraImpl/ESConnector.<clinit>
    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @BeforeAll
    void setUp() {
        processor = new EntityCreateOrUpdateMutationPostProcessor();
    }

    @AfterAll
    void tearDown() {
        ApplicationProperties.forceReload();
    }

    // =================== Single Operation Tests ===================

    @Test
    void testSingleAddOperation_writesWithUpsertTrue() {
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

            processor.executeESOperations(List.of(addOp));

            // ADD operations use upsert=true
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(true)), times(1));
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(false)), never());
        }
    }

    @Test
    void testSingleDeleteOperation_writesWithUpsertFalse() {
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

            processor.executeESOperations(List.of(deleteOp));

            // DELETE operations use upsert=false
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(false)), times(1));
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(true)), never());
        }
    }

    @Test
    void testSingleUpdateOperation_writesWithUpsertFalse() {
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");

            processor.executeESOperations(List.of(updateOp));

            // UPDATE operations use upsert=false
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(false)), times(1));
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(true)), never());
        }
    }

    // =================== Same Entity Deduplication Tests ===================

    @Test
    void testDeleteThenAdd_sameEntity_onlyAddWritten() {
        // Simulates: tags [A,C] -> [B] where DELETE runs before ADD in commitChanges
        // DELETE(A) creates payload "", ADD(B) creates payload "|B|"
        // Only ADD should be written (has complete final state)
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");
            ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

            processor.executeESOperations(List.of(deleteOp, addOp));

            // Only ADD batch written (upsert=true)
            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(true)), times(1));
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(false)), never());

            // Verify payload contains ADD's data
            Map<String, Map<String, Object>> writtenPayload = payloadCaptor.getValue();
            assertEquals("|TagB|", writtenPayload.get("e1").get("__classificationNames"));
        }
    }

    @Test
    void testAddThenDelete_sameEntity_onlyAddWritten() {
        // Order independence: even if ADD comes first in the list, it should still win
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");
            ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

            processor.executeESOperations(List.of(addOp, deleteOp));

            // ADD wins by priority, not by position
            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(true)), times(1));
            assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    @Test
    void testMultipleDeletes_sameEntity_lastDeleteWritten() {
        // Multiple DELETEs for same entity: later one has more recent Cassandra state
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation delete1 = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "|TagC|");
            ESDeferredOperation delete2 = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

            processor.executeESOperations(List.of(delete1, delete2));

            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(false)), times(1));
            // Last DELETE's payload should be used
            assertEquals("", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    @Test
    void testDeleteThenUpdate_sameEntity_updateWins() {
        // UPDATE runs after DELETE in commitChanges, so UPDATE has more recent state
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");
            ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");

            processor.executeESOperations(List.of(deleteOp, updateOp));

            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(false)), times(1));
            assertEquals("|TagA|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    @Test
    void testUpdateThenDelete_sameEntity_updateWins() {
        // UPDATE has higher priority than DELETE regardless of order
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");
            ESDeferredOperation deleteOp = createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", "");

            processor.executeESOperations(List.of(updateOp, deleteOp));

            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(false)), times(1));
            // UPDATE wins over DELETE
            assertEquals("|TagA|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    @Test
    void testUpdateThenAdd_sameEntity_addWins() {
        // ADD has highest priority
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            ESDeferredOperation updateOp = createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|");
            ESDeferredOperation addOp = createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|");

            processor.executeESOperations(List.of(updateOp, addOp));

            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(true)), times(1));
            assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    // =================== Multiple Entity Tests ===================

    @Test
    void testMultipleEntities_eachDeduplicatedIndependently() {
        // e1: DELETE + UPDATE -> UPDATE wins
        // e2: ADD only -> ADD written
        // e3: DELETE only -> DELETE written
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> addCaptor = ArgumentCaptor.forClass(Map.class);
            ArgumentCaptor<Map<String, Map<String, Object>>> otherCaptor = ArgumentCaptor.forClass(Map.class);

            List<ESDeferredOperation> ops = List.of(
                    createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", ""),
                    createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e2", "|TagX|"),
                    createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|"),
                    createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e3", "")
            );

            processor.executeESOperations(ops);

            // ADD batch (e2) with upsert=true
            mockedES.verify(() -> ESConnector.writeTagProperties(addCaptor.capture(), eq(true)), times(1));
            // Other batch (e1 UPDATE, e3 DELETE) with upsert=false
            mockedES.verify(() -> ESConnector.writeTagProperties(otherCaptor.capture(), eq(false)), times(1));

            // Verify ADD batch
            Map<String, Map<String, Object>> addPayload = addCaptor.getValue();
            assertEquals(1, addPayload.size());
            assertEquals("|TagX|", addPayload.get("e2").get("__classificationNames"));

            // Verify other batch
            Map<String, Map<String, Object>> otherPayload = otherCaptor.getValue();
            assertEquals(2, otherPayload.size());
            assertEquals("|TagA|", otherPayload.get("e1").get("__classificationNames")); // UPDATE won
            assertEquals("", otherPayload.get("e3").get("__classificationNames")); // DELETE
        }
    }

    // =================== Edge Cases ===================

    @Test
    void testEmptyOperationsList_noESCalls() {
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            processor.executeESOperations(Collections.emptyList());

            mockedES.verifyNoInteractions();
        }
    }

    @Test
    void testNullOperationsList_noESCalls() {
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            processor.executeESOperations(null);

            mockedES.verifyNoInteractions();
        }
    }

    @Test
    void testAllThreeOperationTypes_sameEntity_addWins() {
        // DELETE -> UPDATE -> ADD for same entity: ADD should win
        try (MockedStatic<ESConnector> mockedES = mockStatic(ESConnector.class)) {
            ArgumentCaptor<Map<String, Map<String, Object>>> payloadCaptor = ArgumentCaptor.forClass(Map.class);

            List<ESDeferredOperation> ops = List.of(
                    createOperation(OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS, "e1", ""),
                    createOperation(OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS, "e1", "|TagA|"),
                    createOperation(OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS, "e1", "|TagB|")
            );

            processor.executeESOperations(ops);

            // Only ADD batch
            mockedES.verify(() -> ESConnector.writeTagProperties(payloadCaptor.capture(), eq(true)), times(1));
            mockedES.verify(() -> ESConnector.writeTagProperties(anyMap(), eq(false)), never());

            assertEquals("|TagB|", payloadCaptor.getValue().get("e1").get("__classificationNames"));
        }
    }

    // =================== Helper Methods ===================

    private ESDeferredOperation createOperation(OperationType type, String entityId, String classificationNames) {
        Map<String, Object> entityPayload = new HashMap<>();
        entityPayload.put("__classificationNames", classificationNames);

        Map<String, Map<String, Object>> payload = new HashMap<>();
        payload.put(entityId, entityPayload);

        return new ESDeferredOperation(type, entityId, payload);
    }
}
