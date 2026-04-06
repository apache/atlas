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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.purge;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.*;
import org.apache.atlas.repository.store.graph.v2.tags.PaginatedTagResult;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static org.apache.atlas.authorize.AtlasAuthorizerFactory.ATLAS_AUTHORIZER_IMPL;
import static org.apache.atlas.authorize.AtlasAuthorizerFactory.CURRENT_AUTHORIZER_IMPL;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_REFRESH_PROPAGATION;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_ENTITY_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_CLASSIFICATION_NAME;
import org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTask;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE;
import static org.apache.atlas.type.Constants.CATEGORIES_PARENT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.CATEGORIES_PROPERTY_KEY;

/**
 * All per-batch inline cleanup methods for the BulkPurge subsystem.
 * Called from BulkPurgeService.processBatch() after commit.
 *
 * Uses the shared normal graph (not bulk-loading). JanusGraph gives each thread
 * its own ThreadLocal transaction, so concurrent workers are safe.
 *
 * Cleanup order:
 *  1. Tag cleanup from deleted source entities (Cassandra + graph)
 *  2. Zombie Cassandra tags for propagated-only entities
 *  3. Stale propagated tag repair on external lineage vertices
 *  4. Relay propagation refresh tasks for alive sources
 *  5. Lineage flag repair on external vertices
 *  6. Dataset reference cleanup (catalogDatasetGuid)
 *  7. Term association cleanup (async task creation)
 *  8. Category parent/term reference cleanup
 *  9. DataProduct port reference cleanup
 * 10. Persona cleanup (Keycloak role + ES alias)
 * 11. Purpose cleanup (ES alias)
 * 12. DataProduct read-policy cleanup
 * 13. QueryCollection cleanup (Keycloak roles)
 * 14. Connection inline cleanup (Keycloak role + policies, for QN_PREFIX mode)
 * 15. StakeholderTitle empty stakeholder auto-delete
 * 16. External vertex refresh (timestamp + modifiedBy + inverse refs)
 */
public class PurgeBatchCleanupService {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeBatchCleanupService.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final TaskManagement taskManagement;
    private final EntityDiscoveryService discovery;
    private final PurgeESOperations esOps;
    private final Supplier<TagDAO> tagDAOSupplier;

    public PurgeBatchCleanupService(AtlasGraph graph,
                                    AtlasTypeRegistry typeRegistry,
                                    TaskManagement taskManagement,
                                    EntityDiscoveryService discovery,
                                    PurgeESOperations esOps,
                                    Supplier<TagDAO> tagDAOSupplier) {
        this.graph          = graph;
        this.typeRegistry   = typeRegistry;
        this.taskManagement = taskManagement;
        this.discovery      = discovery;
        this.esOps          = esOps;
        this.tagDAOSupplier = tagDAOSupplier;
    }

    // ======================== ORCHESTRATOR ========================

    public void performBatchCleanup(PurgeContext ctx, BatchCleanupMetadata batchMeta) {
        if (ctx.cancelRequested) return;
        try {
            cleanBatchPropagatedTags(batchMeta.directTagVerts);
            cleanBatchPropagatedOnlyTags(batchMeta.propagatedOnlyTagVerts);
            repairBatchExternalTags(batchMeta.externalLineageVerts);
            triggerBatchRelayPropagation(ctx, batchMeta.externalLineageVerts);
            repairBatchLineage(batchMeta.externalLineageVerts);
            cleanBatchDatasetReferences(batchMeta.datasetGuids);
            cleanBatchTermAssociations(ctx, batchMeta.termInfo);
            cleanBatchCategoryReferences(batchMeta.categoryCleanup);
            cleanBatchDataProductPorts(batchMeta.deletedGuids);
            cleanBatchPersonas(batchMeta.personaCleanup);
            cleanBatchPurposes(batchMeta.purposeCleanup);
            cleanBatchDataProductPolicies(batchMeta.dataProductGuids);
            cleanBatchQueryCollections(batchMeta.collectionCleanup);
            cleanBatchConnectionsInline(batchMeta.connectionCleanup);
            cleanBatchStakeholderTitles(batchMeta.stakeholderTitleCleanup);
            refreshBatchExternalVertices(ctx, batchMeta.externalVerts);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Batch cleanup error for purgeKey={}", ctx.purgeKey, e);
            try { graph.rollback(); } catch (Exception re) { /* ignore */ }
        }
    }

    // ======================== TAG CLEANUP ========================

    private void cleanBatchPropagatedTags(Set<String> directTagVerts) {
        if (directTagVerts.isEmpty()) return;

        TagDAO tagDAO = tagDAOSupplier.get();
        int totalCleaned = 0;

        for (String deletedVertexId : directTagVerts) {
            try {
                List<Tag> allTags = tagDAO.getAllTagsByVertexId(deletedVertexId);
                if (allTags == null || allTags.isEmpty()) continue;

                for (Tag tag : allTags) {
                    if (tag.isPropagated()) continue;
                    if (!tag.getRemovePropagationsOnEntityDelete()) continue;

                    String pagingState = null;
                    List<Tag> propagatedTagsToDelete = new ArrayList<>();
                    List<AtlasVertex> verticesToUpdate = new ArrayList<>();

                    while (true) {
                        PaginatedTagResult result = tagDAO.getPropagationsForAttachmentBatchWithPagination(
                                deletedVertexId, tag.getTagTypeName(), pagingState, 10000);

                        for (Tag propagatedTag : result.getTags()) {
                            propagatedTagsToDelete.add(propagatedTag);
                            AtlasVertex extVertex = graph.getVertex(propagatedTag.getVertexId());
                            if (extVertex != null) {
                                verticesToUpdate.add(extVertex);
                            }
                        }

                        if (Boolean.TRUE.equals(result.isDone()) || !result.hasMorePages()) break;
                        pagingState = result.getPagingState();
                    }

                    if (!propagatedTagsToDelete.isEmpty()) {
                        tagDAO.deleteTags(propagatedTagsToDelete);
                    }

                    int batchCount = 0;
                    for (AtlasVertex extVertex : verticesToUpdate) {
                        try {
                            removePropagatedTraitFromVertex(extVertex, tag.getTagTypeName());
                            batchCount++;
                            if (batchCount % 50 == 0) {
                                try { graph.commit(); } catch (Exception commitEx) {
                                    LOG.warn("BulkPurge: Commit failed during batch tag cleanup", commitEx);
                                    try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("BulkPurge: Failed to update graph properties on vertex for tag {}", tag.getTagTypeName(), e);
                        }
                    }
                    if (batchCount % 50 != 0 && batchCount > 0) {
                        try { graph.commit(); } catch (Exception commitEx) {
                            LOG.warn("BulkPurge: Final commit failed during batch tag cleanup", commitEx);
                            try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                        }
                    }
                    totalCleaned += propagatedTagsToDelete.size();
                }

                tagDAO.deleteTags(allTags);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean propagated tags for deleted source {}", deletedVertexId, e);
            }
        }

        if (totalCleaned > 0) {
            LOG.debug("BulkPurge: Batch tag cleanup: cleaned {} propagated tags from {} source entities",
                    totalCleaned, directTagVerts.size());
        }
    }

    private void cleanBatchPropagatedOnlyTags(Set<String> propagatedOnlyTagVerts) {
        if (propagatedOnlyTagVerts.isEmpty()) return;

        TagDAO tagDAO = tagDAOSupplier.get();
        int totalCleaned = 0;

        for (String vertexId : propagatedOnlyTagVerts) {
            try {
                List<Tag> allTags = tagDAO.getAllTagsByVertexId(vertexId);
                if (allTags != null && !allTags.isEmpty()) {
                    tagDAO.deleteTags(allTags);
                    totalCleaned += allTags.size();
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean propagated-only tags for vertex {}", vertexId, e);
            }
        }

        if (totalCleaned > 0) {
            LOG.debug("BulkPurge: Cleaned {} zombie Cassandra tags from {} propagated-only entities",
                    totalCleaned, propagatedOnlyTagVerts.size());
        }
    }

    private void repairBatchExternalTags(Set<String> externalLineageVerts) {
        if (externalLineageVerts.isEmpty()) return;

        TagDAO tagDAO = tagDAOSupplier.get();

        for (String vertexId : externalLineageVerts) {
            try {
                AtlasVertex vertex = graph.getVertex(vertexId);
                if (vertex == null) continue;
                repairPropagatedClassificationsV2(vertex, vertexId, tagDAO);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to repair propagated classifications for vertex {}", vertexId, e);
                try { graph.rollback(); } catch (Exception re) { /* ignore */ }
            }
        }
    }

    private void triggerBatchRelayPropagation(PurgeContext ctx, Set<String> externalLineageVerts) {
        if (externalLineageVerts.isEmpty()) return;

        TagDAO tagDAO = tagDAOSupplier.get();
        Set<String> refreshKeys = new HashSet<>();
        List<String[]> tasksToCreate = new ArrayList<>();

        for (String extVertexId : externalLineageVerts) {
            try {
                collectRelaySourcesV2(extVertexId, tagDAO, refreshKeys, tasksToCreate);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to collect relay sources for vertex {}", extVertexId, e);
            }
        }

        for (String[] pair : tasksToCreate) {
            try {
                Map<String, Object> taskParams = new HashMap<>();
                taskParams.put(PARAM_ENTITY_GUID, pair[0]);
                taskParams.put(PARAM_CLASSIFICATION_NAME, pair[1]);
                taskManagement.createTaskV2(CLASSIFICATION_REFRESH_PROPAGATION, ctx.submittedBy,
                        taskParams, pair[1], pair[0]);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to create refresh task for [{}, {}]", pair[0], pair[1], e);
            }
        }
    }

    // ======================== LINEAGE REPAIR ========================

    private void repairBatchLineage(Set<String> externalLineageVerts) {
        if (externalLineageVerts.isEmpty()) return;

        String[] lineageLabels = {PROCESS_INPUTS, PROCESS_OUTPUTS};

        for (String vertexId : externalLineageVerts) {
            boolean repairSuccess = false;
            for (int attempt = 1; attempt <= 3 && !repairSuccess; attempt++) {
                try {
                    AtlasVertex vertex = graph.getVertex(vertexId);
                    if (vertex == null) { repairSuccess = true; break; }

                    boolean hasActiveLineage = false;
                    Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH, lineageLabels);
                    for (AtlasEdge edge : edges) {
                        if (ACTIVE_STATE_VALUE.equals(edge.getProperty(STATE_PROPERTY_KEY, String.class))) {
                            hasActiveLineage = true;
                            break;
                        }
                    }

                    if (!hasActiveLineage) {
                        AtlasGraphUtilsV2.setEncodedProperty(vertex, org.apache.atlas.type.Constants.HAS_LINEAGE, false);
                        graph.commit();
                    }
                    repairSuccess = true;
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Batch lineage repair attempt {}/3 failed for vertex {}", attempt, vertexId, e);
                    try { graph.rollback(); } catch (Exception re) { /* ignore */ }
                    if (attempt < 3) {
                        try { Thread.sleep(attempt * 500L); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt(); break;
                        }
                    }
                }
            }
        }
    }

    // ======================== DATASET / TERM / CATEGORY CLEANUP ========================

    private void cleanBatchDatasetReferences(Set<String> datasetGuids) {
        if (datasetGuids.isEmpty()) return;

        for (String datasetGuid : datasetGuids) {
            try {
                List<String> linkedVertexIds = esOps.findVerticesByTermQuery(CATALOG_DATASET_GUID_ATTR, datasetGuid);

                for (String vertexId : linkedVertexIds) {
                    try {
                        AtlasVertex assetVertex = graph.getVertex(vertexId);
                        if (assetVertex != null) {
                            AtlasGraphUtilsV2.setProperty(assetVertex, CATALOG_DATASET_GUID_ATTR, null);
                        }
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: Failed to clear catalogDatasetGuid on vertex {} for dataset {}", vertexId, datasetGuid, e);
                    }
                }

                if (!linkedVertexIds.isEmpty()) {
                    try { graph.commit(); } catch (Exception commitEx) {
                        LOG.warn("BulkPurge: Commit failed during dataset reference cleanup for {}", datasetGuid, commitEx);
                        try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                    }
                    LOG.debug("BulkPurge: Cleared catalogDatasetGuid from {} assets for deleted dataset {}",
                            linkedVertexIds.size(), datasetGuid);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean dataset references for guid {}", datasetGuid, e);
            }
        }
    }

    private void cleanBatchTermAssociations(PurgeContext ctx, List<String[]> termInfoList) {
        if (termInfoList.isEmpty()) return;

        for (String[] termInfo : termInfoList) {
            String termGuid = termInfo[0];
            String termQN   = termInfo[1];
            String termName = termInfo[2];

            try {
                List<AtlasEntityHeader> entityHeaders = discovery.searchUsingTermQualifiedName(0, 1, termQN, null, null);
                if (entityHeaders != null && !entityHeaders.isEmpty()) {
                    Map<String, Object> taskParams = MeaningsTask.toParameters(termName, termQN, termGuid);
                    taskManagement.createTask(UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE,
                            ctx.submittedBy, taskParams);
                    LOG.debug("BulkPurge: Created term cleanup task for term {} (guid={})", termQN, termGuid);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to create term cleanup task for term {} (guid={})", termQN, termGuid, e);
            }
        }
    }

    private void cleanBatchCategoryReferences(List<CategoryCleanupInfo> cleanupInfos) {
        if (cleanupInfos.isEmpty()) return;

        int cleaned = 0;
        for (CategoryCleanupInfo info : cleanupInfos) {
            for (String childId : info.childVertexIds) {
                try {
                    AtlasVertex childVertex = graph.getVertex(childId);
                    if (childVertex != null) {
                        childVertex.removeProperty(CATEGORIES_PARENT_PROPERTY_KEY);
                        cleaned++;
                    }
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to remove __parentCategory from child vertex {}", childId, e);
                }
            }

            if (info.categoryQN != null) {
                for (String termId : info.termVertexIds) {
                    try {
                        AtlasVertex termVertex = graph.getVertex(termId);
                        if (termVertex != null) {
                            termVertex.removePropertyValue(CATEGORIES_PROPERTY_KEY, info.categoryQN);
                            cleaned++;
                        }
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: Failed to remove category QN from __categories on term vertex {}", termId, e);
                    }
                }
            }
        }

        if (cleaned > 0) {
            try { graph.commit(); } catch (Exception commitEx) {
                LOG.warn("BulkPurge: Commit failed during category reference cleanup", commitEx);
                try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
            }
        }
    }

    private void cleanBatchDataProductPorts(Set<String> deletedGuids) {
        if (deletedGuids.isEmpty()) return;

        try {
            List<String> guidList = new ArrayList<>(deletedGuids);
            List<String> dataProductVertexIds = esOps.findDataProductsReferencingGuids(guidList);
            if (dataProductVertexIds.isEmpty()) return;

            for (String dpVertexId : dataProductVertexIds) {
                try {
                    AtlasVertex dpVertex = graph.getVertex(dpVertexId);
                    if (dpVertex == null) continue;

                    for (String deletedGuid : guidList) {
                        AtlasGraphUtilsV2.removeItemFromListPropertyValue(dpVertex, "daapOutputPortGuids", deletedGuid);
                        AtlasGraphUtilsV2.removeItemFromListPropertyValue(dpVertex, "daapInputPortGuids", deletedGuid);
                    }

                    AtlasGraphUtilsV2.setEncodedProperty(dpVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());
                    graph.commit();
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to clean port references on DataProduct vertex {}", dpVertexId, e);
                    try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                }
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to find DataProducts referencing deleted GUIDs in batch", e);
        }
    }

    // ======================== ACCESS CONTROL CLEANUP ========================

    private void cleanBatchPersonas(List<PersonaCleanupInfo> personaCleanupList) {
        if (personaCleanupList.isEmpty()) return;

        for (PersonaCleanupInfo info : personaCleanupList) {
            if (info.roleId != null) {
                try {
                    new KeycloakStore().removeRole(info.roleId);
                    LOG.debug("BulkPurge: Removed Keycloak role {} for Persona {}", info.roleId, info.guid);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to remove Keycloak role {} for Persona {}", info.roleId, info.guid, e);
                }
            }

            if (info.qualifiedName != null) {
                try {
                    String aliasName = AccessControlUtils.getESAliasName(info.qualifiedName);
                    IndexAliasStore aliasStore = new ESAliasStore(graph, new EntityGraphRetriever(graph, typeRegistry));
                    aliasStore.deleteAlias(aliasName);
                    LOG.debug("BulkPurge: Deleted ES alias {} for Persona {}", aliasName, info.guid);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to delete ES alias for Persona {} (qn={})", info.guid, info.qualifiedName, e);
                }
            }
        }
    }

    private void cleanBatchPurposes(List<PurposeCleanupInfo> purposeCleanupList) {
        if (purposeCleanupList.isEmpty()) return;

        for (PurposeCleanupInfo info : purposeCleanupList) {
            if (info.qualifiedName != null) {
                try {
                    String aliasName = AccessControlUtils.getESAliasName(info.qualifiedName);
                    IndexAliasStore aliasStore = new ESAliasStore(graph, new EntityGraphRetriever(graph, typeRegistry));
                    aliasStore.deleteAlias(aliasName);
                    LOG.debug("BulkPurge: Deleted ES alias {} for Purpose {}", aliasName, info.guid);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to delete ES alias for Purpose {} (qn={})", info.guid, info.qualifiedName, e);
                }
            }
        }
    }

    private void cleanBatchDataProductPolicies(List<String> dataProductGuids) {
        if (dataProductGuids.isEmpty()) return;

        for (String productGuid : dataProductGuids) {
            try {
                String policyQN = productGuid + "/read-policy";
                AtlasVertex policyVertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                        graph, POLICY_ENTITY_TYPE, QUALIFIED_NAME, policyQN);

                if (policyVertex != null) {
                    String policyGuid = policyVertex.getProperty(GUID_PROPERTY_KEY, String.class);

                    Iterable<AtlasEdge> edges = policyVertex.getEdges(AtlasEdgeDirection.BOTH);
                    for (AtlasEdge edge : edges) {
                        graph.removeEdge(edge);
                    }
                    graph.removeVertex(policyVertex);
                    graph.commit();

                    if (policyGuid != null) {
                        try {
                            List<String> esIds = new ArrayList<>();
                            esIds.add(LongEncoding.encode(Long.parseLong(policyVertex.getId().toString())));
                            esOps.deleteESDocsByIds(esOps.getEsClient(), esIds);
                        } catch (Exception esEx) {
                            LOG.debug("BulkPurge: Could not delete ES doc for DataProduct read-policy {}", policyQN, esEx);
                        }
                    }

                    LOG.debug("BulkPurge: Deleted read-policy {} for DataProduct {}", policyQN, productGuid);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to delete read-policy for DataProduct {}", productGuid, e);
                try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
            }
        }
    }

    private void cleanBatchQueryCollections(List<CollectionCleanupInfo> collectionCleanupList) {
        if (collectionCleanupList.isEmpty()) return;
        if (!ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) return;

        for (CollectionCleanupInfo info : collectionCleanupList) {
            String adminRoleName  = String.format("collection_admins_%s", info.guid);
            String viewerRoleName = String.format("collection_viewer_%s", info.guid);

            try {
                new KeycloakStore().removeRoleByName(adminRoleName);
                LOG.debug("BulkPurge: Removed Keycloak admin role for Collection {}", info.guid);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to remove Keycloak admin role {} for Collection {}", adminRoleName, info.guid, e);
            }

            try {
                new KeycloakStore().removeRoleByName(viewerRoleName);
                LOG.debug("BulkPurge: Removed Keycloak viewer role for Collection {}", info.guid);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to remove Keycloak viewer role {} for Collection {}", viewerRoleName, info.guid, e);
            }
        }
    }

    private void cleanBatchConnectionsInline(List<ConnectionCleanupInfo> connectionCleanupList) {
        if (connectionCleanupList.isEmpty()) return;
        if (!ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) return;

        for (ConnectionCleanupInfo info : connectionCleanupList) {
            try {
                String roleName = String.format("connection_admins_%s", info.guid);

                List<String> policyVertexIds = esOps.findConnectionPolicyVertexIds(info.guid, roleName);
                if (!policyVertexIds.isEmpty()) {
                    List<String> policyEsIds = new ArrayList<>();
                    for (String vertexId : policyVertexIds) {
                        try {
                            AtlasVertex policyVertex = graph.getVertex(vertexId);
                            if (policyVertex != null) {
                                policyEsIds.add(LongEncoding.encode(Long.parseLong(vertexId)));
                                Iterable<AtlasEdge> edges = policyVertex.getEdges(AtlasEdgeDirection.BOTH);
                                for (AtlasEdge edge : edges) {
                                    graph.removeEdge(edge);
                                }
                                graph.removeVertex(policyVertex);
                                graph.commit();
                            }
                        } catch (Exception e) {
                            LOG.warn("BulkPurge: Failed to delete inline policy vertex {} for Connection {}", vertexId, info.guid, e);
                            try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                        }
                    }
                    if (!policyEsIds.isEmpty()) {
                        try {
                            esOps.deleteESDocsByIds(esOps.getEsClient(), policyEsIds);
                        } catch (Exception e) {
                            LOG.debug("BulkPurge: Failed to delete inline policy ES docs for Connection {}", info.guid, e);
                        }
                    }
                }

                try {
                    new KeycloakStore().removeRoleByName(roleName);
                    LOG.debug("BulkPurge: Removed Keycloak role {} for inline Connection {}", roleName, info.guid);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to remove Keycloak role {} for inline Connection {}", roleName, info.guid, e);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed inline Connection cleanup for guid {}", info.guid, e);
            }
        }
    }

    // ======================== STAKEHOLDER TITLE CLEANUP ========================

    private void cleanBatchStakeholderTitles(List<StakeholderTitleCleanupInfo> stakeholderTitleCleanupList) {
        if (stakeholderTitleCleanupList.isEmpty()) return;

        for (StakeholderTitleCleanupInfo info : stakeholderTitleCleanupList) {
            try {
                List<String> linkedStakeholderVertexIds = esOps.findStakeholdersForTitle(info.guid);

                List<String> emptyStakeholderGuids = new ArrayList<>();
                for (String vertexId : linkedStakeholderVertexIds) {
                    try {
                        AtlasVertex stakeholderVertex = graph.getVertex(vertexId);
                        if (stakeholderVertex == null) continue;

                        String stakeholderGuid = stakeholderVertex.getProperty(GUID_PROPERTY_KEY, String.class);
                        String state = stakeholderVertex.getProperty(STATE_PROPERTY_KEY, String.class);
                        if (!ACTIVE_STATE_VALUE.equals(state)) continue;

                        List<String> personaUsers = stakeholderVertex.getMultiValuedProperty("personaUsers", String.class);
                        if (personaUsers == null || personaUsers.isEmpty()) {
                            if (stakeholderGuid != null) {
                                emptyStakeholderGuids.add(stakeholderGuid);
                            }
                        } else {
                            LOG.warn("BulkPurge: StakeholderTitle {} has linked Stakeholder {} with active users -- " +
                                    "orphaned stakeholder requires manual cleanup", info.guid, stakeholderGuid);
                        }
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: Failed to check stakeholder vertex {} for StakeholderTitle {}",
                                vertexId, info.guid, e);
                    }
                }

                if (!emptyStakeholderGuids.isEmpty()) {
                    for (String stakeholderGuid : emptyStakeholderGuids) {
                        try {
                            AtlasVertex sv = AtlasGraphUtilsV2.findByGuid(graph, stakeholderGuid);
                            if (sv != null) {
                                Iterable<AtlasEdge> edges = sv.getEdges(AtlasEdgeDirection.BOTH);
                                for (AtlasEdge edge : edges) {
                                    graph.removeEdge(edge);
                                }
                                graph.removeVertex(sv);
                            }
                        } catch (Exception e) {
                            LOG.warn("BulkPurge: Failed to auto-delete empty Stakeholder {}", stakeholderGuid, e);
                        }
                    }
                    try { graph.commit(); } catch (Exception commitEx) {
                        LOG.warn("BulkPurge: Commit failed during stakeholder auto-delete", commitEx);
                        try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                    }
                    LOG.info("BulkPurge: Auto-deleted {} empty Stakeholders for StakeholderTitle {}",
                            emptyStakeholderGuids.size(), info.guid);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed StakeholderTitle cleanup for guid {}", info.guid, e);
            }
        }
    }

    // ======================== EXTERNAL VERTEX REFRESH ========================

    private void refreshBatchExternalVertices(PurgeContext ctx, Set<String> externalVerts) {
        if (externalVerts.isEmpty()) return;

        Map<String, List<AtlasStructType.AtlasAttribute>> inverseRefCache = new HashMap<>();
        int batchCount = 0;

        for (String vertexId : externalVerts) {
            try {
                AtlasVertex vertex = graph.getVertex(vertexId);
                if (vertex == null) continue;

                AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());
                AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFIED_BY_KEY, ctx.submittedBy);

                String typeName = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
                if (typeName != null) {
                    List<AtlasStructType.AtlasAttribute> inverseAttrs = inverseRefCache.computeIfAbsent(
                            typeName, this::computeInverseRefAttributes);
                    for (AtlasStructType.AtlasAttribute attr : inverseAttrs) {
                        cleanStaleInverseProperty(vertex, attr);
                    }
                }

                batchCount++;
                if (batchCount % 50 == 0) {
                    try { graph.commit(); } catch (Exception commitEx) {
                        LOG.warn("BulkPurge: Commit failed during batch external vertex refresh", commitEx);
                        try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
                    }
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to refresh external vertex {}", vertexId, e);
            }
        }

        if (batchCount % 50 != 0 && batchCount > 0) {
            try { graph.commit(); } catch (Exception commitEx) {
                LOG.warn("BulkPurge: Final commit failed during batch external vertex refresh", commitEx);
                try { graph.rollback(); } catch (Exception rbEx) { /* ignore */ }
            }
        }
    }

    // ======================== TAG REPAIR HELPERS ========================

    private int repairPropagatedClassificationsV2(AtlasVertex vertex, String vertexId, TagDAO tagDAO) throws Exception {
        List<Tag> allTags = tagDAO.getAllTagsByVertexId(vertexId);
        if (allTags == null || allTags.isEmpty()) return 0;

        List<Tag> stalePropagatedTags = new ArrayList<>();

        for (Tag tag : allTags) {
            if (!tag.isPropagated()) continue;
            String sourceVertexId = tag.getSourceVertexId();
            if (sourceVertexId != null) {
                AtlasVertex sourceVertex = graph.getVertex(sourceVertexId);
                if (sourceVertex == null) {
                    stalePropagatedTags.add(tag);
                }
            }
        }

        if (!stalePropagatedTags.isEmpty()) {
            tagDAO.deleteTags(stalePropagatedTags);
            for (Tag tag : stalePropagatedTags) {
                removePropagatedTraitFromVertex(vertex, tag.getTagTypeName());
            }
            graph.commit();
            LOG.debug("BulkPurge: Cleaned {} stale propagated tags (V2) from vertex {}",
                    stalePropagatedTags.size(), vertexId);
        }

        return stalePropagatedTags.size();
    }

    private void collectRelaySourcesV2(String extVertexId, TagDAO tagDAO,
                                        Set<String> refreshKeys, List<String[]> tasksToCreate) throws Exception {
        List<Tag> allTags = tagDAO.getAllTagsByVertexId(extVertexId);
        if (allTags == null || allTags.isEmpty()) return;

        for (Tag tag : allTags) {
            if (!tag.isPropagated()) continue;
            String sourceVertexId = tag.getSourceVertexId();
            if (sourceVertexId == null) continue;

            AtlasVertex sourceVertex = graph.getVertex(sourceVertexId);
            if (sourceVertex == null) continue;

            String sourceGuid = sourceVertex.getProperty(GUID_PROPERTY_KEY, String.class);
            if (sourceGuid == null) continue;

            String key = sourceGuid + "|" + tag.getTagTypeName();
            if (refreshKeys.add(key)) {
                tasksToCreate.add(new String[]{sourceGuid, tag.getTagTypeName()});
            }
        }
    }

    private void removePropagatedTraitFromVertex(AtlasVertex vertex, String tagTypeName) {
        List<String> currentTraits = vertex.getMultiValuedProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, String.class);
        if (currentTraits == null || currentTraits.isEmpty()) return;

        List<String> updatedTraits = new ArrayList<>(currentTraits);
        if (!updatedTraits.remove(tagTypeName)) return;

        vertex.removeProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        for (String trait : updatedTraits) {
            vertex.addListProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, trait);
        }

        if (updatedTraits.isEmpty()) {
            vertex.removeProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        } else {
            StringBuilder sb = new StringBuilder();
            for (String trait : updatedTraits) {
                if (sb.length() > 0) sb.append("|");
                sb.append(trait);
            }
            AtlasGraphUtilsV2.setEncodedProperty(vertex, PROPAGATED_CLASSIFICATION_NAMES_KEY, sb.toString());
        }

        AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());
        vertex.removeProperty(CLASSIFICATION_TEXT_KEY);
    }

    // ======================== INVERSE REF HELPERS ========================

    private List<AtlasStructType.AtlasAttribute> computeInverseRefAttributes(String typeName) {
        try {
            AtlasType type = typeRegistry.getType(typeName);
            if (!(type instanceof AtlasStructType)) return Collections.emptyList();

            List<AtlasStructType.AtlasAttribute> result = null;
            for (AtlasStructType.AtlasAttribute attr : ((AtlasStructType) type).getAllAttributes().values()) {
                if (attr.getInverseRefAttributeName() != null) {
                    if (result == null) result = new ArrayList<>();
                    result.add(attr);
                }
            }
            return result != null ? result : Collections.emptyList();
        } catch (AtlasBaseException e) {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private boolean cleanStaleInverseProperty(AtlasVertex vertex, AtlasStructType.AtlasAttribute attr) {
        String propertyName = attr.getVertexPropertyName();
        if (propertyName == null) return false;

        Object value = vertex.getProperty(propertyName, Object.class);
        if (value == null) return false;

        if (value instanceof String) {
            if (graph.getEdge((String) value) == null) {
                vertex.removeProperty(propertyName);
                return true;
            }
        } else if (value instanceof List) {
            List<String> edgeIds = (List<String>) value;
            List<String> valid = new ArrayList<>();
            for (String edgeId : edgeIds) {
                if (graph.getEdge(edgeId) != null) {
                    valid.add(edgeId);
                }
            }
            if (valid.size() != edgeIds.size()) {
                if (valid.isEmpty()) {
                    vertex.removeProperty(propertyName);
                } else {
                    vertex.setProperty(propertyName, valid);
                }
                return true;
            }
        }
        return false;
    }
}
