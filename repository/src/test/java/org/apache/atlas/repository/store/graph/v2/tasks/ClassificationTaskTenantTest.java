package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

/**
 * Tests for ClassificationTask tenant null-safety fix (PR #6007).
 *
 * When the DOMAIN_NAME environment variable is not set, System.getenv("DOMAIN_NAME")
 * returns null. Before the fix, this null tenant was passed to TaskMetricsService methods,
 * which could cause NPE. After the fix, it defaults to "default".
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassificationTaskTenantTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private DeleteHandlerDelegate deleteDelegate;

    @Mock
    private AtlasRelationshipStore relationshipStore;

    @Mock
    private TaskMetricsService taskMetricsService;

    private AutoCloseable closeable;

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
    }

    @AfterEach
    void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    /**
     * Test: When DOMAIN_NAME env var is not set and task params are empty,
     * perform() should use "default" as tenant in error metrics instead of null.
     *
     * Before fix: tenant would be null, potentially causing NPE in metrics service.
     */
    @Test
    void testPerform_nullDomainName_usesDefaultTenant() throws AtlasBaseException {
        AtlasTask taskDef = new AtlasTask();
        taskDef.setGuid("test-task-guid");
        taskDef.setType("CLASSIFICATION_PROPAGATION_ADD");
        // Leave params empty to trigger MISSING_PARAMS early return
        taskDef.setParameters(Collections.emptyMap());

        TestableClassificationTask task = new TestableClassificationTask(
                taskDef, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            AtlasTask.Status status = task.perform();

            assertEquals(AtlasTask.Status.FAILED, status, "Task should fail due to empty params");

            // The key assertion: tenant passed to recordTaskError should be "default", not null.
            // If DOMAIN_NAME is not set in the test environment (which is typical), tenant
            // would have been null before the fix. Now it should be "default".
            String expectedTenant = System.getenv("DOMAIN_NAME") != null
                    ? System.getenv("DOMAIN_NAME") : "default";
            verify(taskMetricsService).recordTaskError(
                    eq("CLASSIFICATION_PROPAGATION_ADD"),
                    eq("v1"),
                    eq(expectedTenant),
                    eq("MISSING_PARAMS"));
        }
    }

    /**
     * Test: When DOMAIN_NAME env var is not set and userName is empty,
     * perform() should use "default" as tenant in error metrics.
     */
    @Test
    void testPerform_missingUser_usesDefaultTenant() throws AtlasBaseException {
        AtlasTask taskDef = new AtlasTask();
        taskDef.setGuid("test-task-guid");
        taskDef.setType("CLASSIFICATION_PROPAGATION_ADD");

        Map<String, Object> params = new HashMap<>();
        params.put("entityGuid", "some-guid");
        taskDef.setParameters(params);
        // createdBy is null => empty username
        taskDef.setCreatedBy("");

        TestableClassificationTask task = new TestableClassificationTask(
                taskDef, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

        try (MockedStatic<DynamicConfigStore> configMock = mockStatic(DynamicConfigStore.class)) {
            configMock.when(DynamicConfigStore::isTagV2Enabled).thenReturn(false);

            AtlasTask.Status status = task.perform();

            assertEquals(AtlasTask.Status.FAILED, status, "Task should fail due to missing user");

            String expectedTenant = System.getenv("DOMAIN_NAME") != null
                    ? System.getenv("DOMAIN_NAME") : "default";
            verify(taskMetricsService).recordTaskError(
                    eq("CLASSIFICATION_PROPAGATION_ADD"),
                    eq("v1"),
                    eq(expectedTenant),
                    eq("MISSING_USER"));
        }
    }

    /**
     * Concrete test subclass of ClassificationTask since ClassificationTask is abstract.
     * The run() method is never reached in these tests because we trigger early returns
     * (empty params, missing user).
     */
    private static class TestableClassificationTask extends ClassificationTask {

        public TestableClassificationTask(AtlasTask task, AtlasGraph graph,
                                          EntityGraphMapper entityGraphMapper,
                                          DeleteHandlerDelegate deleteDelegate,
                                          AtlasRelationshipStore relationshipStore,
                                          TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            // No-op for tests that trigger early returns before reaching run()
        }
    }
}
