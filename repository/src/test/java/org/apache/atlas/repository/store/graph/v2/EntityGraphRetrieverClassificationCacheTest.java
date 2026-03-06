package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the classification prefetch mechanism in EntityGraphRetriever.
 *
 * Verifies:
 * 1. prefetchClassifications() returns a batch map from TagDAO (short-lived, not stored in RequestContext)
 * 2. prefetchClassifications() returns null when TagV2 is disabled or auth check is skipped
 * 3. Fallback to sync fetch when vertex is absent from the prefetched map
 * 4. Exception propagation from TagDAO
 * 5. getClassificationNames() routes to TagDAO when TagV2 is enabled, falls back to vertex properties otherwise
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityGraphRetrieverClassificationCacheTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private TagDAO tagDAO;

    private AutoCloseable closeable;
    private EntityGraphRetriever retriever;

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setup() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        ApplicationProperties.set(new PropertiesConfiguration());
        RequestContext.clear();
        RequestContext.get();

        retriever = mock(EntityGraphRetriever.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        setField(retriever, "graph", graph);
        setField(retriever, "typeRegistry", typeRegistry);
        setField(retriever, "tagDAO", tagDAO);
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    // =================== prefetchClassifications tests ===================

    /**
     * Test: prefetchClassifications returns a batch map from TagDAO.
     * Production path: toAtlasEntitiesWithExtInfo() calls prefetchClassifications() to batch-load
     * classifications for all entities in a single Cassandra call.
     */
    @Test
    void testPrefetchClassificationsReturnsBatchMap() throws Exception {
        AtlasVertex v1 = mockVertex("100");
        AtlasVertex v2 = mockVertex("200");

        Map<String, List<AtlasClassification>> batchResult = new HashMap<>();
        batchResult.put("100", Arrays.asList(new AtlasClassification("TAG_A")));
        batchResult.put("200", Collections.emptyList());

        when(tagDAO.getAllClassificationsForVertices(anyCollection())).thenReturn(batchResult);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            @SuppressWarnings("unchecked")
            Map<String, List<AtlasClassification>> result =
                    (Map<String, List<AtlasClassification>>) invokePrefetch(retriever, Arrays.asList(v1, v2));

            assertNotNull(result);
            assertEquals(1, result.get("100").size());
            assertEquals("TAG_A", result.get("100").get(0).getTypeName());
            assertNotNull(result.get("200"));
            assertTrue(result.get("200").isEmpty());
        }

        verify(tagDAO, times(1)).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: prefetchClassifications returns null when TagV2 is disabled.
     * Production path: When TagV2 is off, classification fetching falls back to
     * V1 vertex property reads via mapClassifications().
     */
    @Test
    void testPrefetchReturnsNullWhenTagV2Disabled() throws Exception {
        AtlasVertex v1 = mockVertex("100");

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            Object result = invokePrefetch(retriever, Arrays.asList(v1));
            assertNull(result);
        }

        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: prefetchClassifications returns null when skipAuthorizationCheck is true.
     * Production path: Internal system calls (e.g., task processing) skip auth and
     * use the V1 path regardless of TagV2 setting.
     */
    @Test
    void testPrefetchReturnsNullWhenAuthCheckSkipped() throws Exception {
        AtlasVertex v1 = mockVertex("100");
        RequestContext.get().setSkipAuthorizationCheck(true);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            Object result = invokePrefetch(retriever, Arrays.asList(v1));
            assertNull(result);
        }

        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: prefetchClassifications with empty vertex list returns null.
     * Avoids unnecessary Cassandra call when there are no entities to process.
     */
    @Test
    void testPrefetchWithEmptyVertexListReturnsNull() throws Exception {
        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            Object result = invokePrefetch(retriever, Collections.emptyList());
            assertNull(result);
        }

        verify(tagDAO, never()).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: When async batch fetch fails for one vertex, that vertex is absent from the result.
     * Production path: TagDAOCassandraImpl.getAllClassificationsForVertices() catches per-vertex
     * async failures and omits them from the result map. mapVertexToAtlasEntity() detects the
     * missing key and falls back to individual sync fetch.
     */
    @Test
    void testPartialBatchFailureProducesPartialMap() throws Exception {
        AtlasVertex v1 = mockVertex("500");
        AtlasVertex v2 = mockVertex("600");

        Map<String, List<AtlasClassification>> partialResult = new HashMap<>();
        partialResult.put("500", Arrays.asList(new AtlasClassification("TAG_OK")));
        // "600" is absent — simulating async failure for that vertex

        when(tagDAO.getAllClassificationsForVertices(anyCollection())).thenReturn(partialResult);

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            @SuppressWarnings("unchecked")
            Map<String, List<AtlasClassification>> result =
                    (Map<String, List<AtlasClassification>>) invokePrefetch(retriever, Arrays.asList(v1, v2));

            assertNotNull(result);
            assertEquals(1, result.get("500").size());
            assertEquals("TAG_OK", result.get("500").get(0).getTypeName());
            // v2 absent — caller should detect null and fall back to sync individual fetch
            assertNull(result.get("600"));
        }

        verify(tagDAO, times(1)).getAllClassificationsForVertices(anyCollection());
    }

    /**
     * Test: When TagDAO.getAllClassificationsForVertices throws, prefetch propagates the exception.
     * Production path: Cassandra connection failure propagates up to the REST layer.
     */
    @Test
    void testPrefetchPropagatesTagDAOException() throws Exception {
        AtlasVertex v1 = mockVertex("900");

        when(tagDAO.getAllClassificationsForVertices(anyCollection()))
                .thenThrow(new AtlasBaseException("Cassandra unavailable"));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            Exception ex = assertThrows(Exception.class, () ->
                    invokePrefetch(retriever, Arrays.asList(v1)));

            // InvocationTargetException wraps the AtlasBaseException from reflection
            assertTrue(ex.getCause() instanceof AtlasBaseException || ex instanceof AtlasBaseException);
        }
    }

    // =================== getAllClassifications_V2 tests ===================

    /**
     * Test: getAllClassifications_V2 fetches from TagDAO directly on each call.
     * Production path: Single-entity GET /v2/entity/guid/{guid} uses this method.
     * No batch prefetch — each call is an individual Cassandra query.
     */
    @Test
    void testGetAllClassificationsV2FetchesFromTagDAO() throws Exception {
        AtlasVertex vertex = mockVertex("400");

        when(tagDAO.getAllClassificationsForVertex("400"))
                .thenReturn(Arrays.asList(new AtlasClassification("SYNC_TAG")));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<AtlasClassification> result = retriever.getAllClassifications_V2(vertex);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("SYNC_TAG", result.get(0).getTypeName());
        }

        verify(tagDAO, times(1)).getAllClassificationsForVertex("400");
    }

    // =================== getClassificationNames tests ===================

    /**
     * Test: getClassificationNames queries Cassandra via TagDAO when TagV2 is enabled.
     * Production path: Index search with excludeClassifications=true calls this to populate
     * classificationNames without deserializing full classification JSON.
     */
    @Test
    void testGetClassificationNamesUsesTagDAOWhenTagV2Enabled() throws Exception {
        AtlasVertex vertex = mockVertex("500");

        when(tagDAO.getClassificationNamesForVertex("500", null))
                .thenReturn(Arrays.asList("PII", "Sensitive"));

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(true);

            List<String> names = retriever.getClassificationNames(vertex);

            assertNotNull(names);
            assertEquals(2, names.size());
            assertTrue(names.contains("PII"));
            assertTrue(names.contains("Sensitive"));
        }

        verify(tagDAO, times(1)).getClassificationNamesForVertex("500", null);
    }

    /**
     * Test: getClassificationNames falls back to vertex properties when TagV2 is disabled.
     * Production path: Legacy/V1 path reads pipe-delimited classification names from
     * JanusGraph vertex property (__classificationNames).
     */
    @Test
    void testGetClassificationNamesFallsBackWhenTagV2Disabled() throws Exception {
        AtlasVertex vertex = mockVertex("600");
        when(vertex.getProperty(org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY, String.class))
                .thenReturn("|PII|Internal|");

        try (MockedStatic<DynamicConfigStore> dcsMock = mockStatic(DynamicConfigStore.class)) {
            dcsMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            List<String> names = retriever.getClassificationNames(vertex);

            assertNotNull(names);
            assertEquals(2, names.size());
            assertTrue(names.contains("PII"));
            assertTrue(names.contains("Internal"));
        }

        // TagDAO should NOT be called when TagV2 is disabled
        verify(tagDAO, never()).getClassificationNamesForVertex(anyString(), any());
    }

    // =================== Helpers ===================

    private AtlasVertex mockVertex(String id) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getIdForDisplay()).thenReturn(id);
        return vertex;
    }

    private Object invokePrefetch(Object target, List<AtlasVertex> vertices) throws Exception {
        Method method = EntityGraphRetriever.class.getDeclaredMethod("prefetchClassifications", List.class);
        method.setAccessible(true);
        return method.invoke(target, vertices);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        while (clazz != null) {
            try {
                return clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
