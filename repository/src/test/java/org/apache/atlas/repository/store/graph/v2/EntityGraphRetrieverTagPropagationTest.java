package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for NPE fixes in tag propagation traversal methods (PR #6007).
 *
 * Covers:
 * 1. graph.getVertex() returning null (deleted vertex during traversal)
 * 2. getAdjacentVerticesIds returning empty set for unknown entity types
 * 3. getAdjacentVerticesIds returning empty set when tagPropagationEdges is null
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityGraphRetrieverTagPropagationTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

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

        // Create EntityGraphRetriever without calling constructor (avoids TagDAOCassandraImpl.getInstance())
        // Field initializers don't run when constructor is bypassed, so we set all needed fields via reflection.
        retriever = mock(EntityGraphRetriever.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
        setField(retriever, "graph", graph);
        setField(retriever, "typeRegistry", typeRegistry);
        // Use a same-thread executor so that CompletableFuture.supplyAsync runs on the test thread.
        // This is critical because MockedStatic is thread-local and won't apply on executor threads.
        setField(retriever, "executorService", newSameThreadExecutorService());
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    /**
     * Test: traverseImpactedVerticesByLevelV2 should not throw NPE when
     * graph.getVertex() returns null for a vertex ID (e.g., entity deleted concurrently).
     *
     * Before fix: graph.getVertex(t) returns null, then entityVertex.getIdForDisplay() throws NPE.
     * After fix: null vertex is skipped gracefully with a warning log.
     */
    @Test
    void testTraverseV2_nullVertexSkippedGracefully() {
        AtlasVertex startVertex = mock(AtlasVertex.class);
        when(startVertex.getIdForDisplay()).thenReturn("v-start");

        when(graph.getVertex("v-start")).thenReturn(startVertex);
        when(graph.getVertex("v-deleted")).thenReturn(null);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(entityType);

        AtlasEdge edge = createMockEdge("v-start", "v-deleted", "rel-1");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(edge));

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            setupEdgeMocks(ghMock, edge);
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        // v-deleted is discovered as adjacent and added to result.
        // When BFS expands it at the next level, graph.getVertex("v-deleted") returns null
        // and it's safely skipped (no NPE).
        assertTrue(result.contains("v-deleted"), "Adjacent vertex should be in traversal result");
    }

    /**
     * Test: traverseImpactedVerticesByLevelV2 should handle when start vertex is null.
     */
    @Test
    void testTraverseV2_nullStartVertex() {
        Set<String> result = new HashSet<>();

        retriever.traverseImpactedVerticesByLevelV2(
                null, null, "classif-1", result,
                null, false, null, null);

        assertTrue(result.isEmpty(), "Result should be empty when start vertex is null");
    }

    /**
     * Test: getAdjacentVerticesIds should return empty set (not null) when entity type
     * has no tag propagation edges configured.
     *
     * Before fix: returned null, causing NPE in addAll(null) in the BFS loop.
     */
    @Test
    void testTraverseV2_entityTypeWithNoTagPropagationEdges() {
        AtlasVertex startVertex = mock(AtlasVertex.class);
        when(startVertex.getIdForDisplay()).thenReturn("v-start");
        when(graph.getVertex("v-start")).thenReturn(startVertex);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("CustomType")).thenReturn(entityType);

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("CustomType");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.isEmpty(), "Result should be empty when entity type has no propagation edges");
    }

    /**
     * Test: getAdjacentVerticesIds should return empty set when typeRegistry returns null
     * for the entity type (unknown type).
     *
     * Before fix: entityType is null => tagPropagationEdges is null => returns null => NPE.
     */
    @Test
    void testTraverseV2_unknownEntityType() {
        AtlasVertex startVertex = mock(AtlasVertex.class);
        when(startVertex.getIdForDisplay()).thenReturn("v-start");
        when(graph.getVertex("v-start")).thenReturn(startVertex);

        when(typeRegistry.getEntityTypeByName("UnknownType")).thenReturn(null);

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("UnknownType");

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.isEmpty(), "Result should be empty for unknown entity type");
    }

    /**
     * Test: When multiple vertices are at the same level and some return null from
     * graph.getVertex(), only the valid ones should be processed.
     */
    @Test
    void testTraverseV2_mixOfValidAndDeletedVerticesAtSameLevel() {
        AtlasVertex startVertex = mock(AtlasVertex.class);
        when(startVertex.getIdForDisplay()).thenReturn("v-start");
        when(graph.getVertex("v-start")).thenReturn(startVertex);

        AtlasVertex validVertex = mock(AtlasVertex.class);
        when(validVertex.getIdForDisplay()).thenReturn("v-valid");
        when(graph.getVertex("v-valid")).thenReturn(validVertex);
        when(graph.getVertex("v-deleted")).thenReturn(null);

        AtlasEntityType tableType = mock(AtlasEntityType.class);
        when(tableType.getTagPropagationEdgesArray()).thenReturn(new String[]{"__Process.inputs"});
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(tableType);

        // v-valid has no propagation edges (terminates traversal)
        AtlasEntityType columnType = mock(AtlasEntityType.class);
        when(columnType.getTagPropagationEdgesArray()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName("Column")).thenReturn(columnType);

        AtlasEdge edge1 = createMockEdge("v-start", "v-valid", "rel-1");
        AtlasEdge edge2 = createMockEdge("v-start", "v-deleted", "rel-2");
        when(startVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Arrays.asList(edge1, edge2));
        when(validVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        Set<String> result = new HashSet<>();

        try (MockedStatic<GraphHelper> ghMock = mockStatic(GraphHelper.class)) {
            ghMock.when(() -> GraphHelper.getTypeName(startVertex)).thenReturn("Table");
            ghMock.when(() -> GraphHelper.getTypeName(validVertex)).thenReturn("Column");
            setupEdgeMocks(ghMock, edge1);
            setupEdgeMocks(ghMock, edge2);

            retriever.traverseImpactedVerticesByLevelV2(
                    startVertex, null, "classif-1", result,
                    null, false, null, null);
        }

        assertTrue(result.contains("v-valid"), "Valid vertex should be in result");
        assertTrue(result.contains("v-deleted"), "Deleted vertex should still be in result from discovery");
    }

    /**
     * Setup standard mock responses for an edge's GraphHelper static method calls.
     * Uses ONE_TO_TWO propagation so edges from out-vertex propagate tags to in-vertex.
     */
    private void setupEdgeMocks(MockedStatic<GraphHelper> ghMock, AtlasEdge edge) {
        ghMock.when(() -> GraphHelper.getPropagateTags(edge)).thenReturn(
                org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);
        ghMock.when(() -> GraphHelper.getRelationshipGuid(edge)).thenReturn("rel-guid");
        ghMock.when(() -> GraphHelper.getEdgeStatus(edge)).thenReturn(
                org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE);
        ghMock.when(() -> GraphHelper.getBlockedClassificationIds(edge)).thenReturn(Collections.emptyList());
    }

    private AtlasEdge createMockEdge(String outVertexId, String inVertexId, String edgeId) {
        AtlasEdge edge = mock(AtlasEdge.class);
        AtlasVertex outVertex = mock(AtlasVertex.class);
        AtlasVertex inVertex = mock(AtlasVertex.class);
        when(outVertex.getIdForDisplay()).thenReturn(outVertexId);
        when(inVertex.getIdForDisplay()).thenReturn(inVertexId);
        when(edge.getOutVertex()).thenReturn(outVertex);
        when(edge.getInVertex()).thenReturn(inVertex);
        when(edge.getIdForDisplay()).thenReturn(edgeId);
        return edge;
    }

    /**
     * Creates an ExecutorService that runs tasks on the calling thread.
     * This ensures MockedStatic (which is thread-local) works correctly
     * for code executed via CompletableFuture.supplyAsync.
     */
    private static ExecutorService newSameThreadExecutorService() {
        return new AbstractExecutorService() {
            private volatile boolean shutdown = false;

            @Override public void execute(Runnable command) { command.run(); }
            @Override public void shutdown() { shutdown = true; }
            @Override public List<Runnable> shutdownNow() { shutdown = true; return Collections.emptyList(); }
            @Override public boolean isShutdown() { return shutdown; }
            @Override public boolean isTerminated() { return shutdown; }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        };
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
