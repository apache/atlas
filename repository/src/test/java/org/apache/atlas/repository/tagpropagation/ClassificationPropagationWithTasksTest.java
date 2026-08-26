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
package org.apache.atlas.repository.tagpropagation;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.ZipFileResourceTestUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.NONE;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertex;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatedTraitNames;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithNoParameters;
import static org.apache.atlas.utils.TestLoadModelUtils.loadModelFromJson;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ClassificationPropagationWithTasksTest extends AtlasTestBase {
    private static final String IMPORT_FILE = "tag-propagation-data.zip";

    private static final String IMPORT_DELETE_FILE = "deleted_tab_propagation.zip";

    private static final String HDFS_PATH_EMPLOYEES     = "a3955120-ac17-426f-a4af-972ec8690e5f";
    private static final String EMPLOYEES1_TABLE        = "cdf0040e-739e-4590-a137-964d10e73573";
    private static final String EMPLOYEES2_TABLE        = "0a3e66b6-472c-48b3-8453-abdd24f9494f";
    private static final String EMPLOYEES_UNION_TABLE   = "1ceac963-1a2b-476a-a269-10396187d406";
    private static final String EMPLOYEES_UNION_PROCESS = "470a2d1e-b1fd-47de-8f2d-8dfd0a0275a7";

    private static final String HIVE_TABLE     = "089c1ad4-9dde-4f9e-80c8-12a3046be337";
    private static final String HIVE_TABLE_CTAS = "e83551b7-bbef-45aa-99d5-3d98c0ac737b";

    private AtlasLineageInfo lineageInfo;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStore entityStore;

    @Inject
    private ImportService importService;

    @Inject
    private EntityGraphMapper entityGraphMapper;

    @Inject
    private TaskManagement tasksManagement;

    @Inject
    private AtlasRelationshipStore relationshipStore;

    @Inject
    private AtlasLineageService lineageService;

    @Inject
    private AtlasGraph graph;

    public static InputStream getZipSource(String fileName) throws IOException {
        return ZipFileResourceTestUtils.getFileInputStream(fileName);
    }

    @BeforeClass
    public void setup() throws Exception {
        RequestContext.clear();

        super.initialize();

        this.tasksManagement.start();
        entityGraphMapper.setTasksUseFlag(true);

        loadModelFilesAndImportTestData();
    }

    @Test
    public void parameterValidation() throws AtlasBaseException {
        try {
            entityGraphMapper.propagateClassification(null, null, null);
            entityGraphMapper.propagateClassification("unknown", "abcd", "xyz");
        } catch (AtlasBaseException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof EntityNotFoundException);
        }

        List<String> ret = entityGraphMapper.propagateClassification(HDFS_PATH_EMPLOYEES, StringUtils.EMPTY, StringUtils.EMPTY);

        assertNull(ret);

        ret = entityGraphMapper.deleteClassificationPropagation(StringUtils.EMPTY, StringUtils.EMPTY);

        assertNull(ret);

        AtlasEntity hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);

        ret = entityGraphMapper.propagateClassification(hdfsEmployees.getGuid(), StringUtils.EMPTY, StringUtils.EMPTY);

        assertNull(ret);
    }

    @Test
    public void add() throws AtlasBaseException {
        final String tagNameX = "tagX";
        final String tagNameY = "tagY";

        AtlasEntity hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);

        AtlasClassification tagX = new AtlasClassification(tagNameX);

        tagX.setEntityGuid(hdfsEmployees.getGuid());
        tagX.setPropagate(true);

        AtlasClassification tagY = new AtlasClassification(tagNameY);

        tagY.setEntityGuid(hdfsEmployees.getGuid());
        tagY.setPropagate(false);

        entityStore.addClassification(Collections.singletonList(HDFS_PATH_EMPLOYEES), tagX);
        entityStore.addClassification(Collections.singletonList(HDFS_PATH_EMPLOYEES), tagY);

        AtlasVertex entityVertex         = AtlasGraphUtilsV2.findByGuid(hdfsEmployees.getGuid());
        AtlasVertex classificationVertex = GraphHelper.getClassificationVertex(entityVertex, tagNameX);

        assertNotNull(entityVertex);
        assertNotNull(classificationVertex);

        AtlasEntity entityUpdated = getEntity(HDFS_PATH_EMPLOYEES);

        assertNotNull(entityUpdated.getPendingTasks());

        List<String> impactedEntities = entityGraphMapper.propagateClassification(hdfsEmployees.getGuid(), classificationVertex.getId().toString(), StringUtils.EMPTY);

        assertNotNull(impactedEntities);
    }

    @Test(dependsOnMethods = "add")
    public void update() throws AtlasBaseException {
        final String tagNameY = "tagY";

        AtlasEntity         hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasClassification tagY          = new AtlasClassification(tagNameY);

        tagY.setEntityGuid(hdfsEmployees.getGuid());
        tagY.setPropagate(true);

        entityStore.updateClassifications(hdfsEmployees.getGuid(), Collections.singletonList(tagY));

        AtlasVertex entityVertex         = AtlasGraphUtilsV2.findByGuid(hdfsEmployees.getGuid());
        AtlasVertex classificationVertex = GraphHelper.getClassificationVertex(entityVertex, tagNameY);

        assertNotNull(RequestContext.get().getQueuedTasks());
        assertFalse(RequestContext.get().getQueuedTasks().isEmpty(), "No tasks were queued!");

        assertNotNull(entityVertex);
        assertNotNull(classificationVertex);
    }

    @Test(dependsOnMethods = "update")
    public void delete() throws AtlasBaseException {
        final String tagName = "tagX";

        AtlasEntity hdfsEmployees = getEntity(HDFS_PATH_EMPLOYEES);

        entityGraphMapper.propagateClassification(hdfsEmployees.getGuid(), StringUtils.EMPTY, StringUtils.EMPTY);

        AtlasClassification tagX = new AtlasClassification(tagName);

        tagX.setEntityGuid(hdfsEmployees.getGuid());
        tagX.setPropagate(false);

        AtlasVertex entityVertex         = AtlasGraphUtilsV2.findByGuid(hdfsEmployees.getGuid());
        AtlasVertex classificationVertex = GraphHelper.getClassificationVertex(entityVertex, tagName);

        try {
            entityStore.deleteClassification(HDFS_PATH_EMPLOYEES, tagX.getTypeName());
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.DELETE_TAG_PROPAGATION_NOT_ALLOWED);
        }

        assertNotNull(entityVertex);
        assertNotNull(classificationVertex);

        List<String> impactedEntities = entityGraphMapper.deleteClassificationPropagation(hdfsEmployees.getGuid(), classificationVertex.getId().toString());

        assertNotNull(impactedEntities);
    }

    @Test(priority = 100)
    public void runImportForDeletedEntityLineage() throws Exception {
        runImportWithNoParameters(importService, getZipSource(IMPORT_DELETE_FILE));
        final String tagName = "classification1";

        AtlasEntity         hiveTable = getEntity(HIVE_TABLE);
        AtlasEntity         hiveTableCtas = getEntity(HIVE_TABLE_CTAS);

        AtlasVertex parentEntityVertex   = AtlasGraphUtilsV2.findByGuid(hiveTable.getGuid());

        AtlasVertex entityVertex         = AtlasGraphUtilsV2.findByGuid(hiveTableCtas.getGuid());

        AtlasVertex classificationVertex = getClassificationVertex(parentEntityVertex, tagName);
        assertNotNull(entityVertex);
        assertNotNull(parentEntityVertex);
        assertNotNull(classificationVertex);

        List<String> propagatedTraitNames = getPropagatedTraitNames(entityVertex);

        assertNotNull(propagatedTraitNames);
    }

    @Test(dependsOnMethods = "updateRelationship_afterTaskCompletion_entitiesAndPendingTasksCleared")
    public void updateRelationship_propagateTagsChange_ignoresBlockedInSamePut() throws Exception {
        setupRelationshipBlockScenario();

        AtlasRelationship relationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        assertEquals(relationship.getPropagateTags(), ONE_TO_TWO);
        assertFalse(relationship.getBlockedPropagatedClassifications().isEmpty());

        List<String> blockedIdsBefore = GraphHelper.getBlockedClassificationIds(
                new GraphHelper(graph).getEdgeForGUID(relationship.getGuid()));

        assertFalse(blockedIdsBefore.isEmpty());

        relationship.setPropagateTags(NONE);
        relationship.setBlockedPropagatedClassifications(Collections.emptySet());

        AtlasRelationship putReturn = relationshipStore.update(relationship);

        assertEquals(putReturn.getPropagateTags(), NONE);

        List<String> blockedIdsAfter = GraphHelper.getBlockedClassificationIds(
                new GraphHelper(graph).getEdgeForGUID(putReturn.getGuid()));

        assertEquals(blockedIdsAfter, blockedIdsBefore,
                "Blocked list must not change when propagateTags also changed in same PUT");
    }

    @Test
    public void updateRelationship_unblock_putAndGetBeforeTaskCompletion() throws Exception {
        setupRelationshipBlockScenario();

        AtlasEntity employees1 = getEntity(EMPLOYEES1_TABLE);
        AtlasEntity employees2 = getEntity(EMPLOYEES2_TABLE);

        AtlasClassification piiTag2 = new AtlasClassification("PII");
        piiTag2.setEntityGuid(employees1.getGuid());

        AtlasClassification piiTag3 = new AtlasClassification("PII");
        piiTag3.setEntityGuid(employees2.getGuid());

        AtlasRelationship relationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);

        relationship.setBlockedPropagatedClassifications(Collections.emptySet());

        AtlasRelationship putReturn = relationshipStore.update(relationship);

        assertNotNull(putReturn);
        assertTrue(putReturn.getBlockedPropagatedClassifications().isEmpty());
        assertClassificationExistInList(putReturn.getPropagatedClassifications(), piiTag2);
        assertClassificationExistInList(putReturn.getPropagatedClassifications(), piiTag3);

        AtlasRelationship getReturn = relationshipStore.getById(putReturn.getGuid());

        assertTrue(getReturn.getBlockedPropagatedClassifications().isEmpty());

        if (putReturn.getPendingTasks() != null && !putReturn.getPendingTasks().isEmpty()) {
            assertNotNull(getReturn.getPendingTasks());
            assertFalse(getReturn.getPendingTasks().isEmpty());
            assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
            assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);
        } else {
            assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
            assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);
        }
    }

    @Test(dependsOnMethods = "updateRelationship_unblock_putAndGetBeforeTaskCompletion")
    public void updateRelationship_afterTaskCompletion_entitiesAndPendingTasksCleared() throws Exception {
        setupRelationshipBlockScenario();

        AtlasEntity employees1 = getEntity(EMPLOYEES1_TABLE);
        AtlasEntity employees2 = getEntity(EMPLOYEES2_TABLE);

        AtlasClassification piiTag2 = new AtlasClassification("PII");
        piiTag2.setEntityGuid(employees1.getGuid());

        AtlasClassification piiTag3 = new AtlasClassification("PII");
        piiTag3.setEntityGuid(employees2.getGuid());

        AtlasRelationship relationship = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);
        PropagateTags propagateTags = relationship.getPropagateTags();
        List<String>  oldBlockedIds  = GraphHelper.getBlockedClassificationIds(
                new GraphHelper(graph).getEdgeForGUID(relationship.getGuid()));

        relationship.setBlockedPropagatedClassifications(Collections.emptySet());

        AtlasRelationship putReturn = relationshipStore.update(relationship);

        AtlasRelationship unblockRequest = relationshipStore.getById(putReturn.getGuid());
        unblockRequest.setBlockedPropagatedClassifications(Collections.emptySet());

        waitForRelationshipTasksToComplete(putReturn.getGuid(), unblockRequest, propagateTags, oldBlockedIds);

        AtlasRelationship finalRelationship = relationshipStore.getById(putReturn.getGuid());

        assertNull(finalRelationship.getPendingTasks());
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);
    }

    private void setupRelationshipBlockScenario() throws AtlasBaseException {
        AtlasEntity hdfsPath   = getEntity(HDFS_PATH_EMPLOYEES);
        AtlasEntity employees1 = getEntity(EMPLOYEES1_TABLE);
        AtlasEntity employees2 = getEntity(EMPLOYEES2_TABLE);

        AtlasClassification piiTag1 = new AtlasClassification("PII");
        piiTag1.setPropagate(true);
        piiTag1.setEntityGuid(hdfsPath.getGuid());
        piiTag1.setAttribute("type", "from hdfs_path entity");
        piiTag1.setAttribute("valid", true);

        AtlasClassification piiTag2 = new AtlasClassification("PII");
        piiTag2.setPropagate(true);
        piiTag2.setEntityGuid(employees1.getGuid());
        piiTag2.setAttribute("type", "from employees1 entity");
        piiTag2.setAttribute("valid", true);

        AtlasClassification piiTag3 = new AtlasClassification("PII");
        piiTag3.setPropagate(true);
        piiTag3.setEntityGuid(employees2.getGuid());
        piiTag3.setAttribute("type", "from employees2 entity");
        piiTag3.setAttribute("valid", true);

        addClassificationIfMissing(hdfsPath, piiTag1);
        addClassificationIfMissing(employees1, piiTag2);
        addClassificationIfMissing(employees2, piiTag3);

        propagateClassificationIfPresent(hdfsPath.getGuid(), piiTag1.getTypeName());
        propagateClassificationIfPresent(employees1.getGuid(), piiTag2.getTypeName());
        propagateClassificationIfPresent(employees2.getGuid(), piiTag3.getTypeName());

        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
        assertClassificationExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);

        AtlasRelationship relationship   = getRelationship(EMPLOYEES_UNION_PROCESS, EMPLOYEES_UNION_TABLE);
        PropagateTags       oldPropagateTags = relationship.getPropagateTags();
        List<String>        oldBlockedIds    = Collections.emptyList();

        Set<AtlasClassification> blockTags = selectPropagatedClassifications(relationship, employees1.getGuid(), employees2.getGuid());

        assertFalse(blockTags.isEmpty(), "Expected propagated PII on process->union relationship before block");

        relationship.setBlockedPropagatedClassifications(blockTags);

        AtlasRelationship blockReturn = relationshipStore.update(relationship);

        if (blockReturn.getPendingTasks() != null && !blockReturn.getPendingTasks().isEmpty()) {
            applyRelationshipTaskIfPending(blockReturn, oldPropagateTags, oldBlockedIds);
        }

        assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag2);
        assertClassificationNotExistInEntity(EMPLOYEES_UNION_TABLE, piiTag3);
    }

    private void addClassificationIfMissing(AtlasEntity entity, AtlasClassification classification) throws AtlasBaseException {
        List<AtlasClassification> classifications = entity.getClassifications();

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification existing : classifications) {
                if (existing.getTypeName().equals(classification.getTypeName())) {
                    return;
                }
            }
        }

        entityStore.addClassifications(entity.getGuid(), Collections.singletonList(classification));
    }

    private void propagateClassificationIfPresent(String entityGuid, String classificationName) throws AtlasBaseException {
        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityGuid);

        if (entityVertex == null) {
            return;
        }

        AtlasVertex classificationVertex = getClassificationVertex(entityVertex, classificationName);

        if (classificationVertex != null) {
            entityGraphMapper.propagateClassification(entityGuid, classificationVertex.getId().toString(), StringUtils.EMPTY);
        }
    }

    private Set<AtlasClassification> selectPropagatedClassifications(AtlasRelationship relationship, String... sourceEntityGuids) {
        Set<AtlasClassification> result     = new HashSet<>();
        Set<AtlasClassification> propagated = relationship.getPropagatedClassifications();

        if (CollectionUtils.isEmpty(propagated)) {
            return result;
        }

        Set<String> sourceGuids = new HashSet<>(Arrays.asList(sourceEntityGuids));

        for (AtlasClassification classification : propagated) {
            if ("PII".equals(classification.getTypeName()) && sourceGuids.contains(classification.getEntityGuid())) {
                result.add(classification);
            }
        }

        return result;
    }

    private void applyRelationshipTaskIfPending(AtlasRelationship updateRequest, PropagateTags oldPropagateTags,
                                              List<String> oldBlockedIds) throws AtlasBaseException {
        AtlasEdge edge = new GraphHelper(graph).getEdgeForGUID(updateRequest.getGuid());

        entityGraphMapper.updateTagPropagations(edge.getIdForDisplay(), updateRequest,
                oldPropagateTags != null ? oldPropagateTags.name() : null, oldBlockedIds);
    }

    private void waitForRelationshipTasksToComplete(String relationshipGuid, AtlasRelationship updateRequest,
                                                    PropagateTags oldPropagateTags, List<String> oldBlockedIds)
            throws AtlasBaseException, InterruptedException {
        AtlasRelationship relationship = relationshipStore.getById(relationshipGuid);

        if (relationship.getPendingTasks() != null && !relationship.getPendingTasks().isEmpty()) {
            applyRelationshipTaskIfPending(updateRequest, oldPropagateTags, oldBlockedIds);
            relationship = relationshipStore.getById(relationshipGuid);
        }

        if (relationship.getPendingTasks() == null || relationship.getPendingTasks().isEmpty()) {
            return;
        }

        for (int i = 0; i < 30; i++) {
            relationship = relationshipStore.getById(relationshipGuid);

            if (relationship.getPendingTasks() == null || relationship.getPendingTasks().isEmpty()) {
                return;
            }

            Thread.sleep(100);
        }

        fail("Relationship tasks did not complete in time for guid: " + relationshipGuid);
    }

    private void assertClassificationExistInList(Set<AtlasClassification> classifications, AtlasClassification expected) {
        if (classifications == null) {
            fail("Propagated classifications list is null");
        }

        for (AtlasClassification classification : classifications) {
            if (classification.getTypeName().equals(expected.getTypeName())
                    && classification.getEntityGuid().equals(expected.getEntityGuid())) {
                return;
            }
        }

        fail("Propagated classification not found in relationship response");
    }

    private void loadModelFilesAndImportTestData() {
        try {
            loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1020-fs_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);

            loadSampleClassificationDefs();

            runImportWithNoParameters(importService, getZipSource(IMPORT_FILE));

            initializeLineageInfo();
        } catch (AtlasBaseException | IOException e) {
            throw new SkipException("Model loading failed!");
        }
    }

    private void loadSampleClassificationDefs() throws AtlasBaseException {
        AtlasClassificationDef tagX = new AtlasClassificationDef("tagX");
        AtlasClassificationDef tagY = new AtlasClassificationDef("tagY");
        AtlasClassificationDef pii  = new AtlasClassificationDef("PII");

        pii.addAttribute(new AtlasAttributeDef("type", "string"));
        pii.addAttribute(new AtlasAttributeDef("valid", "boolean"));

        typeDefStore.createTypesDef(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(tagX, tagY, pii), Collections.emptyList(), Collections.emptyList()));
    }

    private void initializeLineageInfo() throws AtlasBaseException {
        lineageInfo = lineageService.getAtlasLineageInfo(HDFS_PATH_EMPLOYEES, LineageDirection.BOTH, 3);
    }

    private AtlasEntity getEntity(String entityGuid) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(entityGuid);

        return entityWithExtInfo.getEntity();
    }

    private AtlasRelationship getRelationship(String fromEntityGuid, String toEntityGuid) throws AtlasBaseException {
        String relationshipGuid = findRelationshipGuid(lineageInfo.getRelations(), fromEntityGuid, toEntityGuid);

        if (relationshipGuid == null) {
            relationshipGuid = findRelationshipGuid(lineageInfo.getRelations(), toEntityGuid, fromEntityGuid);
        }

        if (relationshipGuid == null) {
            AtlasLineageInfo outputLineage = lineageService.getAtlasLineageInfo(fromEntityGuid, LineageDirection.OUTPUT, 1);

            relationshipGuid = findRelationshipGuid(outputLineage.getRelations(), fromEntityGuid, toEntityGuid);

            if (relationshipGuid == null) {
                relationshipGuid = findRelationshipGuid(outputLineage.getRelations(), toEntityGuid, fromEntityGuid);
            }
        }

        assertNotNull(relationshipGuid, "Relationship not found between " + fromEntityGuid + " and " + toEntityGuid);

        return relationshipStore.getById(relationshipGuid);
    }

    private String findRelationshipGuid(Set<LineageRelation> relations, String fromEntityGuid, String toEntityGuid) {
        if (relations == null) {
            return null;
        }

        for (LineageRelation relation : relations) {
            if (relation.getFromEntityId().equals(fromEntityGuid) && relation.getToEntityId().equals(toEntityGuid)) {
                return relation.getRelationshipId();
            }
        }

        return null;
    }

    private void assertClassificationExistInEntity(String entityGuid, AtlasClassification classification) throws AtlasBaseException {
        List<AtlasClassification> classifications = getEntity(entityGuid).getClassifications();

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification c : classifications) {
                if (c.getTypeName().equals(classification.getTypeName())
                        && c.getEntityGuid().equals(classification.getEntityGuid())) {
                    return;
                }
            }
        }

        fail("Propagated classification is not present in entity!");
    }

    private void assertClassificationNotExistInEntity(String entityGuid, AtlasClassification classification) throws AtlasBaseException {
        List<AtlasClassification> classifications = getEntity(entityGuid).getClassifications();

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification c : classifications) {
                if (c.getTypeName().equals(classification.getTypeName())
                        && c.getEntityGuid().equals(classification.getEntityGuid())) {
                    fail("Propagated classification should not be present in entity!");
                }
            }
        }
    }
}
