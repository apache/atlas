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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.ZipFileResourceTestUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithNoParameters;
import static org.apache.atlas.utils.TestLoadModelUtils.loadModelFromJson;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ClassificationPropagationWithTasksTest extends AtlasTestBase {
    private static final String IMPORT_FILE = "tag-propagation-data.zip";

    private static final String HDFS_PATH_EMPLOYEES = "a3955120-ac17-426f-a4af-972ec8690e5f";

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

    private void loadModelFilesAndImportTestData() {
        try {
            loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1020-fs_model.json", typeDefStore, typeRegistry);
            loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);

            loadSampleClassificationDefs();

            runImportWithNoParameters(importService, getZipSource(IMPORT_FILE));
        } catch (AtlasBaseException | IOException e) {
            throw new SkipException("Model loading failed!");
        }
    }

    private void loadSampleClassificationDefs() throws AtlasBaseException {
        AtlasClassificationDef tagX = new AtlasClassificationDef("tagX");
        AtlasClassificationDef tagY = new AtlasClassificationDef("tagY");

        typeDefStore.createTypesDef(new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Arrays.asList(tagX, tagY), Collections.emptyList(), Collections.emptyList()));
    }

    private AtlasEntity getEntity(String entityGuid) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(entityGuid);

        return entityWithExtInfo.getEntity();
    }
}
