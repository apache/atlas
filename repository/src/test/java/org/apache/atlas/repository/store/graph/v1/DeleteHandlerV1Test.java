/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.TestRelationshipUtilsV2.DEPARTMENT_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.EMPLOYEE_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.MANAGER_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.getDepartmentEmployeeTypes;
import static org.apache.atlas.TestUtilsV2.NAME;
import static org.apache.atlas.type.AtlasTypeUtil.getAtlasObjectId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for ATLAS-4766: resilience of DeleteHandlerV1 when graph elements
 * are removed during iteration ({@link DeleteHandlerV1#isRelationshipEdge},
 * {@link DeleteHandlerV1#deleteVertex}, {@link DeleteHandlerV1#deleteTraitsAndVertices}, and
 * {@code deleteAllClassifications}).
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class DeleteHandlerV1Test extends AtlasTestBase {
    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private DeleteHandlerDelegate deleteDelegate;

    @BeforeClass
    public void setUp() throws Exception {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);

        super.initialize();

        AtlasTypesDef employeeTypes = getDepartmentEmployeeTypes();
        typeDefStore.createTypesDef(employeeTypes);
    }

    @AfterClass
    public void clear() throws Exception {
        Thread.sleep(1000);
        super.cleanup();
    }

    // ---------------------------------------------------------------
    // isRelationshipEdge resilience
    // ---------------------------------------------------------------

    @Test
    public void testIsRelationshipEdgeWithPurgedEndpoint() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("is_rel_edge", 1);
        String                 subGuid    = getGuidForName(createResp, "is_rel_edge_sub1");
        String                 mgrGuid    = getGuidForName(createResp, "is_rel_edge_mgr");

        assertNotNull(subGuid);
        assertNotNull(mgrGuid);

        AtlasVertex subVertex   = AtlasGraphUtilsV2.findByGuid(subGuid);
        AtlasEdge   managerEdge = pickRelationshipEdge(subVertex);

        assertNotNull(subVertex);
        assertNotNull(managerEdge, "Expected a relationship edge from subordinate to manager");

        DeleteHandlerV1 handler = deleteDelegate.getHandler();
        assertTrue(handler.isRelationshipEdge(managerEdge));

        softDeleteGuids(Collections.singletonList(mgrGuid));
        // Soft-deleted endpoint still exists in the graph; must not throw.
        handler.isRelationshipEdge(managerEdge);

        purgeGuids(Collections.singleton(mgrGuid));
        // After hard purge the stale edge handle may still report true (label starts with
        // "r:") when the surviving endpoint is readable. ATLAS-4766 only requires that
        // removed vertices do not cause IllegalStateException to propagate.
        assertDoesNotThrow(() -> handler.isRelationshipEdge(managerEdge));
    }

// ---------------------------------------------------------------
    // deleteTraitsAndVertices / deleteAllClassifications resilience
    // ---------------------------------------------------------------

    /**
     * After hard purge, a stale entity vertex handle must not cause
     * {@code IllegalStateException} when {@link DeleteHandlerV1#deleteTraitsAndVertices}
     * iterates the collection (e.g. classification edge lookup on a removed vertex).
     */
    @Test
    public void testDeleteTraitsAndVerticesWithPurgedVertex() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("traits_purge", 1);
        String                 mgrGuid    = getGuidForName(createResp, "traits_purge_mgr");
        String                 subGuid    = getGuidForName(createResp, "traits_purge_sub1");

        assertNotNull(mgrGuid);
        assertNotNull(subGuid);

        AtlasVertex mgrVertex = AtlasGraphUtilsV2.findByGuid(mgrGuid);
        assertNotNull(mgrVertex);

        softDeleteGuids(Arrays.asList(mgrGuid, subGuid));

        EntityMutationResponse purgeResp = purgeGuids(new HashSet<>(Arrays.asList(mgrGuid, subGuid)));
        assertEquals(purgeResp.getPurgedEntities().size(), 2);
        assertNull(AtlasGraphUtilsV2.findByGuid(mgrGuid), "Entity should be removed from graph after purge");

        DeleteHandlerV1 handler = deleteDelegate.getHandler();
        handler.deleteTraitsAndVertices(Collections.singleton(mgrVertex));
    }

    // ---------------------------------------------------------------
    // deleteVertex resilience during purge
    // ---------------------------------------------------------------

    /**
     * Manager has incoming subordinate edges. Purge one subordinate first, then purge
     * the manager. Remaining incoming edges from the already-purged subordinate must
     * be skipped via {@code outVertex.exists()} without throwing.
     */
    @Test
    public void testPurgeManagerAfterSubordinateAlreadyPurged() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("seq_purge", 2);
        String                 mgrGuid    = getGuidForName(createResp, "seq_purge_mgr");
        String                 sub1Guid   = getGuidForName(createResp, "seq_purge_sub1");
        String                 sub2Guid   = getGuidForName(createResp, "seq_purge_sub2");

        softDeleteGuids(Collections.singletonList(sub1Guid));
        assertEntityPurged(sub1Guid, purgeGuids(Collections.singleton(sub1Guid)));

        softDeleteGuids(Collections.singletonList(mgrGuid));
        assertEntityPurged(mgrGuid, purgeGuids(Collections.singleton(mgrGuid)));

        assertNotNull(AtlasGraphUtilsV2.findByGuid(sub2Guid),
                "Other subordinate should remain until explicitly deleted");
    }

    /**
     * Purge manager and multiple subordinates in a single transaction. While iterating
     * incoming edges during {@code deleteVertex()}, edges whose out-vertex was already
     * removed earlier in the same batch must be skipped safely.
     */
    @Test
    public void testBatchPurgeManagerAndSubordinates() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("batch_purge", 2);
        String                 mgrGuid    = getGuidForName(createResp, "batch_purge_mgr");
        String                 sub1Guid   = getGuidForName(createResp, "batch_purge_sub1");
        String                 sub2Guid   = getGuidForName(createResp, "batch_purge_sub2");

        Set<String> guidsToPurge = new HashSet<>();
        guidsToPurge.add(mgrGuid);
        guidsToPurge.add(sub1Guid);
        guidsToPurge.add(sub2Guid);

        softDeleteGuids(guidsToPurge);

        EntityMutationResponse purgeResp = purgeGuids(guidsToPurge);

        assertEquals(purgeResp.getPurgedEntities().size(), guidsToPurge.size());
        assertEntitiesPurged(guidsToPurge, purgeResp);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private EntityMutationResponse createManagerWithSubordinates(String prefix, int subordinateCount) throws Exception {
        AtlasEntitiesWithExtInfo batch = new AtlasEntitiesWithExtInfo();

        AtlasEntity dept = new AtlasEntity(DEPARTMENT_TYPE, "name", prefix + "_dept");

        AtlasEntity manager = new AtlasEntity(MANAGER_TYPE);
        manager.setAttribute(NAME, prefix + "_mgr");
        manager.setRelationshipAttribute("department", getAtlasObjectId(dept));

        batch.addEntity(dept);
        batch.addEntity(manager);

        for (int i = 1; i <= subordinateCount; i++) {
            AtlasEntity subordinate = new AtlasEntity(EMPLOYEE_TYPE);
            subordinate.setAttribute(NAME, prefix + "_sub" + i);
            subordinate.setRelationshipAttribute("department", getAtlasObjectId(dept));
            subordinate.setRelationshipAttribute("manager", getAtlasObjectId(manager));
            batch.addEntity(subordinate);
        }

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(batch), false);
        assertNotNull(response);
        assertTrue(response.getCreatedEntities().size() >= subordinateCount + 2);
        return response;
    }

    private String getGuidForName(EntityMutationResponse response, String name) {
        for (AtlasEntityHeader header : response.getCreatedEntities()) {
            if (name.equals(header.getAttribute(NAME))) {
                return header.getGuid();
            }
        }
        return null;
    }

    private AtlasEdge pickRelationshipEdge(AtlasVertex vertex) {
        Iterator<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH).iterator();
        while (edges.hasNext()) {
            AtlasEdge edge = edges.next();
            if (!edge.getLabel().startsWith("__")) {
                return edge;
            }
        }
        return null;
    }

    private void initRequestContext() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    private void softDeleteGuids(Collection<String> guids) throws Exception {
        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.SOFT);
        entityStore.deleteByIds(new ArrayList<>(guids));
    }

    private EntityMutationResponse purgeGuids(Set<String> guids) throws Exception {
        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);
        return entityStore.purgeByIds(guids);
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new AssertionError("Expected no exception but got: " + e.getMessage(), e);
        }
    }

    private void assertEntityPurged(String guid, EntityMutationResponse purgeResp) {
        assertNotNull(purgeResp);
        assertNotNull(purgeResp.getPurgedEntities());
        assertTrue(purgeResp.getPurgedEntities().stream().anyMatch(h -> guid.equals(h.getGuid())),
                "Expected guid " + guid + " in purged entities");
        assertNull(AtlasGraphUtilsV2.findByGuid(guid), "Entity should be removed from graph after purge");
    }

    private void assertEntitiesPurged(Set<String> expectedGuids, EntityMutationResponse purgeResp) {
        assertNotNull(purgeResp);
        assertNotNull(purgeResp.getPurgedEntities());

        Set<String> purgedGuids = purgeResp.getPurgedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toSet());

        assertEquals(purgedGuids, expectedGuids);

        for (String guid : expectedGuids) {
            assertNull(AtlasGraphUtilsV2.findByGuid(guid), "Entity " + guid + " should be removed from graph after purge");
        }
    }
}
