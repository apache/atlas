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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.DeleteType;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.services.PurgeService;
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
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.TestRelationshipUtilsV2.DEPARTMENT_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.EMPLOYEE_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.MANAGER_TYPE;
import static org.apache.atlas.TestRelationshipUtilsV2.getDepartmentEmployeeTypes;
import static org.apache.atlas.TestUtilsV2.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtilsV2.NAME;
import static org.apache.atlas.type.AtlasTypeUtil.getAtlasObjectId;
import static org.apache.atlas.utils.TestLoadModelUtils.loadBaseModel;
import static org.apache.atlas.utils.TestLoadModelUtils.loadHiveModel;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for ATLAS-4766: resilience of DeleteHandlerV1 when graph elements
 * are removed during iteration ({@link DeleteHandlerV1#isRelationshipEdge},
 * {@link DeleteHandlerV1#deleteVertex}, {@link DeleteHandlerV1#deleteTraitsAndVertices}, and
 * {@code deleteAllClassifications}), and empty owned reference vertices during delete traversal.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class DeleteHandlerV1Test extends AtlasTestBase {
    private static final String HIVE_DB_TYPE             = "hive_db";
    private static final String HIVE_TABLE_TYPE          = "hive_table";
    private static final String HIVE_COLUMN_TYPE         = "hive_column";
    private static final String HIVE_PROCESS_TYPE        = "hive_process";
    private static final String HIVE_COLUMN_LINEAGE_TYPE = "hive_column_lineage";
    private static final String HIVE_STORAGE_DESC_TYPE   = "hive_storagedesc";
    private static final String HIVE_CLUSTER             = "cl1";

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private AtlasGraph atlasGraph;

    @Inject
    private AtlasAuditService atlasAuditService;

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

        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);

        EntityMutationResponse purgeResp = entityStore.purgeEntitiesInBatch(new HashSet<>(Arrays.asList(mgrGuid, subGuid)));
        assertEquals(purgeResp.getPurgedEntities().size(), 2);
        assertNull(AtlasGraphUtilsV2.findByGuid(mgrGuid), "Entity should be removed from graph after purge");

        DeleteHandlerV1 handler = deleteDelegate.getHandler();
        handler.deleteTraitsAndVertices(Collections.singleton(mgrVertex));
    }

    /**
     * {@link DeleteHandlerV1#deleteTraitsAndVertices} must return only vertices actually deleted.
     * Stale handles for already-removed entities are skipped and excluded from the return value.
     */
    @Test
    public void testDeleteTraitsAndVerticesReturnsOnlyDeletedVertices() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("delete_return", 1);
        String                 mgrGuid    = getGuidForName(createResp, "delete_return_mgr");
        String                 subGuid    = getGuidForName(createResp, "delete_return_sub1");

        assertNotNull(mgrGuid);
        assertNotNull(subGuid);

        AtlasVertex mgrVertex = AtlasGraphUtilsV2.findByGuid(mgrGuid);
        AtlasVertex subVertex = AtlasGraphUtilsV2.findByGuid(subGuid);
        assertNotNull(mgrVertex);
        assertNotNull(subVertex);

        softDeleteGuids(Arrays.asList(mgrGuid, subGuid));

        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);

        entityStore.purgeEntitiesInBatch(Collections.singleton(subGuid));

        DeleteHandlerV1 handler = deleteDelegate.getHandler();
        Collection<AtlasVertex> deletedVertices = handler.deleteTraitsAndVertices(
                Arrays.asList(mgrVertex, subVertex));

        assertEquals(deletedVertices.size(), 1);
        assertTrue(deletedVertices.contains(mgrVertex));
        assertNull(findByGuidFresh(subGuid));
        assertNull(findByGuidFresh(mgrGuid));
    }

    /**
     * When a batch contains an already-purged GUID, it must be recorded as a skippable failure
     * rather than reported in {@code purgedEntities}.
     */
    @Test
    public void testPurgeEntitiesInBatchDoesNotReportUnconfirmedDeletes() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("unconfirmed", 1);
        String                 mgrGuid    = getGuidForName(createResp, "unconfirmed_mgr");
        String                 subGuid    = getGuidForName(createResp, "unconfirmed_sub1");

        assertNotNull(mgrGuid);
        assertNotNull(subGuid);

        softDeleteGuids(Arrays.asList(mgrGuid, subGuid));

        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);

        EntityMutationResponse subPurge = entityStore.purgeEntitiesInBatch(Collections.singleton(subGuid));
        assertEntityPurged(subGuid, subPurge);

        EntityMutationResponse batchResp = entityStore.purgeEntitiesInBatch(
                new LinkedHashSet<>(Arrays.asList(mgrGuid, subGuid)));

        assertNotNull(batchResp.getPurgedEntities());
        assertEquals(batchResp.getPurgedEntities().size(), 1);
        assertEquals(batchResp.getPurgedEntities().get(0).getGuid(), mgrGuid);
        assertNotNull(batchResp.getFailedEntities());
        assertEquals(batchResp.getFailedEntities().size(), 1);
        assertEquals(batchResp.getFailedEntities().get(0).getGuid(), subGuid);
        assertEquals(batchResp.getFailedEntities().get(0).getErrorCode(),
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode());
        assertNull(findByGuidFresh(mgrGuid));
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

        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);

        // Single-transaction batch purge (not WIM workers) — exercises DeleteHandlerV1 edge
        // iteration when manager and subordinates are removed in the same batch.
        EntityMutationResponse purgeResp = entityStore.purgeEntitiesInBatch(guidsToPurge);

        assertEquals(purgeResp.getPurgedEntities().size(), guidsToPurge.size());
        assertEntitiesPurged(guidsToPurge, purgeResp);
    }

    /**
     * Purge subordinates in a batch without the manager. The manager is soft-deleted but not
     * included in the purge batch (as can happen when WIM workers split related entities).
     * Inverse reference updates on the deleted manager must be skipped.
     */
    @Test
    public void testPurgeSubordinatesWithoutManagerInSameBatch() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("sub_only_purge", 2);
        String                 mgrGuid    = getGuidForName(createResp, "sub_only_purge_mgr");
        String                 sub1Guid   = getGuidForName(createResp, "sub_only_purge_sub1");
        String                 sub2Guid   = getGuidForName(createResp, "sub_only_purge_sub2");

        Set<String> allGuids = new HashSet<>(Arrays.asList(mgrGuid, sub1Guid, sub2Guid));
        softDeleteGuids(allGuids);

        initRequestContext();
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);

        EntityMutationResponse purgeResp = entityStore.purgeEntitiesInBatch(new HashSet<>(Arrays.asList(sub1Guid, sub2Guid)));

        assertEquals(purgeResp.getPurgedEntities().size(), 2);
        assertEntitiesPurged(new HashSet<>(Arrays.asList(sub1Guid, sub2Guid)), purgeResp);
        assertNotNull(AtlasGraphUtilsV2.findByGuid(mgrGuid), "Manager should remain until explicitly purged");

        assertEntityPurged(mgrGuid, purgeGuids(Collections.singleton(mgrGuid)));
    }

    /**
     * Delete must succeed when an owned column edge points to an empty reference vertex
     * (edge present, column vertex has no properties).
     */
    @Test
    public void testDeleteTableWithHollowOwnedColumnVertex() throws Exception {
        String prefix = "hollow_owned_col";

        EntityMutationResponse createResp = createHiveTableWithOwnedColumns(prefix, "tbl");
        String                 tableGuid  = getGuidForQualifiedName(createResp, qualifiedTableName(prefix, "tbl"));
        String                 col2Guid   = getGuidForQualifiedName(createResp, qualifiedColumnName(prefix, "tbl", "col2"));
        assertNotNull(tableGuid);
        assertNotNull(col2Guid);

        AtlasVertex hollowColumnVertex = AtlasGraphUtilsV2.findByGuid(col2Guid);
        assertNotNull(hollowColumnVertex);
        stripAllVertexProperties(hollowColumnVertex);
        assertTrue(hollowColumnVertex.getPropertyKeys().isEmpty());

        DeleteHandlerV1 handler = deleteDelegate.getHandler();
        handler.getOwnedVertices(AtlasGraphUtilsV2.findByGuid(tableGuid));

        initRequestContext();
        EntityMutationResponse deleteResp = entityStore.deleteById(tableGuid);
        assertNotNull(deleteResp);
        assertNotNull(deleteResp.getDeletedEntities());
        assertTrue(deleteResp.getDeletedEntities().stream().anyMatch(h -> tableGuid.equals(h.getGuid())));
    }

    /**
     * Delete must succeed when an upstream process vertex on {@code PROCESS_OUTPUTS_EDGE}
     * is an empty reference vertex.
     */
    @Test
    public void testDeleteOutputTableWhenProcessVertexIsHollow() throws Exception {
        String                 prefix      = "hollow_proc_out";
        long                   timestamp   = System.currentTimeMillis();
        EntityMutationResponse createResp  = createHiveProcessOutputLineage(prefix, timestamp);
        String                 processGuid = getGuidForQualifiedName(createResp, qualifiedProcessName(prefix, timestamp));
        String                 outputGuid  = getGuidForQualifiedName(createResp, qualifiedTableName(prefix, "dst"));
        assertNotNull(processGuid);
        assertNotNull(outputGuid);

        AtlasVertex processVertex = AtlasGraphUtilsV2.findByGuid(processGuid);
        assertNotNull(processVertex);
        stripAllVertexProperties(processVertex);
        assertTrue(processVertex.getPropertyKeys().isEmpty());

        initRequestContext();
        EntityMutationResponse deleteResp = entityStore.deleteById(outputGuid);
        assertNotNull(deleteResp);
        assertNotNull(deleteResp.getDeletedEntities());
        assertTrue(deleteResp.getDeletedEntities().stream().anyMatch(h -> outputGuid.equals(h.getGuid())));
    }

    /**
     * Process delete must succeed when a linked column-lineage vertex is an empty reference vertex.
     */
    @Test
    public void testDeleteProcessSkipsHollowColumnLineageVertex() throws Exception {
        String                 prefix            = "hollow_col_lineage";
        long                   timestamp           = System.currentTimeMillis();
        EntityMutationResponse createResp        = createHiveProcessWithColumnLineage(prefix, timestamp);
        String                 processGuid       = getGuidForQualifiedName(createResp, qualifiedProcessName(prefix, timestamp));
        String                 columnLineageGuid = getGuidForQualifiedName(createResp,
                prefix + "_column_lineage@" + HIVE_CLUSTER + ":" + timestamp);
        assertNotNull(processGuid);
        assertNotNull(columnLineageGuid);

        AtlasVertex columnLineageVertex = AtlasGraphUtilsV2.findByGuid(columnLineageGuid);
        assertNotNull(columnLineageVertex);
        stripAllVertexProperties(columnLineageVertex);
        assertTrue(columnLineageVertex.getPropertyKeys().isEmpty());

        initRequestContext();
        EntityMutationResponse deleteResp = entityStore.deleteById(processGuid);
        assertNotNull(deleteResp);
        assertNotNull(deleteResp.getDeletedEntities());
        assertTrue(deleteResp.getDeletedEntities().stream().anyMatch(h -> processGuid.equals(h.getGuid())));
    }

    /**
     * Subordinate delete must succeed when the manager endpoint on a relationship edge
     * is an empty reference vertex ({@link DeleteHandlerV1#deleteEdgeReference} skips inverse update).
     */
    @Test
    public void testDeleteSubordinateWhenManagerVertexIsHollow() throws Exception {
        EntityMutationResponse createResp = createManagerWithSubordinates("hollow_mgr", 1);
        String                 subGuid    = getGuidForName(createResp, "hollow_mgr_sub1");
        String                 mgrGuid    = getGuidForName(createResp, "hollow_mgr_mgr");

        assertNotNull(subGuid);
        assertNotNull(mgrGuid);

        AtlasVertex managerVertex = AtlasGraphUtilsV2.findByGuid(mgrGuid);
        assertNotNull(managerVertex);
        stripAllVertexProperties(managerVertex);
        assertTrue(managerVertex.getPropertyKeys().isEmpty());

        initRequestContext();
        EntityMutationResponse deleteResp = entityStore.deleteById(subGuid);
        assertNotNull(deleteResp);
        assertNotNull(deleteResp.getDeletedEntities());
        assertTrue(deleteResp.getDeletedEntities().stream().anyMatch(h -> subGuid.equals(h.getGuid())));
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private void ensureHiveModelRegistered() throws Exception {
        if (typeRegistry.getEntityTypeByName(HIVE_DB_TYPE) == null) {
            loadBaseModel(typeDefStore, typeRegistry);
            loadHiveModel(typeDefStore, typeRegistry);
        }
    }

    private EntityMutationResponse createHiveTableWithOwnedColumns(String prefix, String tableName) throws Exception {
        ensureHiveModelRegistered();

        AtlasEntity db     = createHiveDb(prefix);
        AtlasEntity sd     = createHiveStorageDesc(prefix + "_" + tableName);
        AtlasEntity table  = createHiveTable(prefix, tableName, db, sd);
        AtlasEntity col1   = createHiveColumn(prefix, tableName, "col1", table);
        AtlasEntity col2   = createHiveColumn(prefix, tableName, "col2", table);

        table.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(getAtlasObjectId(col1), getAtlasObjectId(col2)));

        AtlasEntitiesWithExtInfo batch = new AtlasEntitiesWithExtInfo(table);
        batch.addReferredEntity(db);
        batch.addReferredEntity(sd);
        batch.addReferredEntity(col1);
        batch.addReferredEntity(col2);

        return createEntities(batch);
    }

    private EntityMutationResponse createHiveProcessOutputLineage(String prefix, long timestamp) throws Exception {
        ensureHiveModelRegistered();

        AtlasEntity db          = createHiveDb(prefix);
        AtlasEntity srcSd       = createHiveStorageDesc(prefix + "_src_sd");
        AtlasEntity dstSd       = createHiveStorageDesc(prefix + "_dst_sd");
        AtlasEntity srcTable    = createHiveTable(prefix, "src", db, srcSd);
        AtlasEntity dstTable    = createHiveTable(prefix, "dst", db, dstSd);
        AtlasEntity process     = createHiveProcess(prefix, timestamp,
                Collections.singletonList(srcTable), Collections.singletonList(dstTable));

        AtlasEntitiesWithExtInfo batch = new AtlasEntitiesWithExtInfo(process);
        batch.addReferredEntity(db);
        batch.addReferredEntity(srcSd);
        batch.addReferredEntity(dstSd);
        batch.addReferredEntity(srcTable);
        batch.addReferredEntity(dstTable);

        return createEntities(batch);
    }

    private EntityMutationResponse createHiveProcessWithColumnLineage(String prefix, long timestamp) throws Exception {
        ensureHiveModelRegistered();

        String      columnLineageQn = prefix + "_column_lineage@" + HIVE_CLUSTER + ":" + timestamp;
        AtlasEntity process         = createHiveProcess(prefix, timestamp, Collections.emptyList(), Collections.emptyList());
        AtlasEntity columnLineage   = new AtlasEntity(HIVE_COLUMN_LINEAGE_TYPE);

        columnLineage.setAttribute(NAME, prefix + "_column_lineage");
        columnLineage.setAttribute("qualifiedName", columnLineageQn);
        columnLineage.setAttribute("depenendencyType", "SIMPLE");
        columnLineage.setRelationshipAttribute("query", getAtlasObjectId(process));

        AtlasEntitiesWithExtInfo batch = new AtlasEntitiesWithExtInfo(columnLineage);
        batch.addReferredEntity(process);

        return createEntities(batch);
    }

    private AtlasEntity createHiveDb(String prefix) {
        AtlasEntity db = new AtlasEntity(HIVE_DB_TYPE);

        db.setAttribute(NAME, prefix + "_db");
        db.setAttribute("qualifiedName", prefix + "_db@" + HIVE_CLUSTER);
        db.setAttribute("clusterName", HIVE_CLUSTER);

        return db;
    }

    private AtlasEntity createHiveStorageDesc(String name) {
        AtlasEntity sd = new AtlasEntity(HIVE_STORAGE_DESC_TYPE);

        sd.setAttribute("qualifiedName", name + "@" + HIVE_CLUSTER);
        sd.setAttribute("compressed", false);

        return sd;
    }

    private AtlasEntity createHiveTable(String prefix, String tableName, AtlasEntity db, AtlasEntity sd) {
        AtlasEntity table = new AtlasEntity(HIVE_TABLE_TYPE);

        table.setAttribute(NAME, tableName);
        table.setAttribute("qualifiedName", qualifiedTableName(prefix, tableName));
        table.setRelationshipAttribute("db", getAtlasObjectId(db));
        table.setRelationshipAttribute("sd", getAtlasObjectId(sd));
        sd.setRelationshipAttribute("table", getAtlasObjectId(table));

        return table;
    }

    private AtlasEntity createHiveColumn(String prefix, String tableName, String columnName, AtlasEntity table) {
        AtlasEntity column = new AtlasEntity(HIVE_COLUMN_TYPE);

        column.setAttribute(NAME, columnName);
        column.setAttribute("type", "int");
        column.setAttribute("qualifiedName", qualifiedColumnName(prefix, tableName, columnName));
        column.setRelationshipAttribute("table", getAtlasObjectId(table));

        return column;
    }

    private AtlasEntity createHiveProcess(String prefix, long timestamp, List<AtlasEntity> inputs, List<AtlasEntity> outputs) {
        AtlasEntity process = new AtlasEntity(HIVE_PROCESS_TYPE);

        process.setAttribute(NAME, prefix + "_process");
        process.setAttribute("qualifiedName", qualifiedProcessName(prefix, timestamp));
        process.setAttribute("startTime", new Date(timestamp));
        process.setAttribute("endTime", new Date(timestamp + 1000));
        process.setAttribute("userName", TestUtilsV2.TEST_USER);
        process.setAttribute("operationType", "CREATETABLE_AS_SELECT");
        process.setAttribute("queryText", "select 1");
        process.setAttribute("queryPlan", "Not Supported");
        process.setAttribute("queryId", prefix + "_query");

        if (!inputs.isEmpty()) {
            process.setRelationshipAttribute("inputs",
                    inputs.stream().map(e -> getAtlasObjectId(e)).collect(Collectors.toList()));
        }

        if (!outputs.isEmpty()) {
            process.setRelationshipAttribute("outputs",
                    outputs.stream().map(e -> getAtlasObjectId(e)).collect(Collectors.toList()));
        }

        return process;
    }

    private EntityMutationResponse createEntities(AtlasEntitiesWithExtInfo batch) throws Exception {
        initRequestContext();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(batch), false);
        assertNotNull(response);
        return response;
    }

    private String qualifiedTableName(String prefix, String tableName) {
        return prefix + "_db." + tableName + "@" + HIVE_CLUSTER;
    }

    private String qualifiedColumnName(String prefix, String tableName, String columnName) {
        return prefix + "_db." + tableName + "." + columnName + "@" + HIVE_CLUSTER;
    }

    private String qualifiedProcessName(String prefix, long timestamp) {
        return prefix + "_process@" + HIVE_CLUSTER + ":" + timestamp;
    }

    private String getGuidForQualifiedName(EntityMutationResponse response, String qualifiedName) {
        for (AtlasEntityHeader header : response.getCreatedEntities()) {
            if (qualifiedName.equals(header.getAttribute("qualifiedName"))) {
                return header.getGuid();
            }
        }
        return null;
    }

    private void stripAllVertexProperties(AtlasVertex vertex) {
        for (String propertyKey : new ArrayList<>(vertex.getPropertyKeys())) {
            vertex.removeProperty(propertyKey);
        }
    }

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
        return new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService).purgeByIds(guids);
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new AssertionError("Expected no exception but got: " + e.getMessage(), e);
        }
    }

    private AtlasVertex findByGuidFresh(String guid) {
        // WIM purge runs on worker threads; main-thread guidVertexCache can retain stale handles.
        GraphTransactionInterceptor.clearCache();
        return AtlasGraphUtilsV2.findByGuid(guid);
    }

    private void assertEntityPurged(String guid, EntityMutationResponse purgeResp) {
        assertNotNull(purgeResp);
        assertNotNull(purgeResp.getPurgedEntities());
        assertTrue(purgeResp.getPurgedEntities().stream().anyMatch(h -> guid.equals(h.getGuid())),
                "Expected guid " + guid + " in purged entities");
        assertNull(findByGuidFresh(guid), "Entity should be removed from graph after purge");
    }

    private void assertEntitiesPurged(Set<String> expectedGuids, EntityMutationResponse purgeResp) {
        assertNotNull(purgeResp);
        assertNotNull(purgeResp.getPurgedEntities());

        Set<String> purgedGuids = purgeResp.getPurgedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toSet());

        assertEquals(purgedGuids, expectedGuids);

        for (String guid : expectedGuids) {
            assertNull(findByGuidFresh(guid), "Entity " + guid + " should be removed from graph after purge");
        }
    }
}
