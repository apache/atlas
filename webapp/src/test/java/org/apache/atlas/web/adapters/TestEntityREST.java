/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.adapters;

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.rest.EntityREST;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Guice(modules = {RepositoryMetadataModule.class})
public class TestEntityREST {

    @Inject
    private AtlasTypeDefStore typeStore;

    @Inject
    private EntityREST entityREST;

    private AtlasEntity dbEntity;

    private String dbGuid;

    private AtlasClassification testClassification;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = TestUtilsV2.defineHiveTypes();
        typeStore.createTypesDef(typesDef);
        dbEntity = TestUtilsV2.createDBEntity();
    }

    @AfterClass
    public void tearDown() throws Exception {
        AtlasGraphProvider.cleanup();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        RequestContext.clear();
    }

    @Test
    public void testCreateOrUpdateEntity() throws Exception {
        final EntityMutationResponse response = entityREST.createOrUpdate(dbEntity);

        Assert.assertNotNull(response);
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE);

        Assert.assertNotNull(entitiesMutated);
        Assert.assertEquals(entitiesMutated.size(), 1);
        Assert.assertNotNull(entitiesMutated.get(0));
        dbGuid = entitiesMutated.get(0).getGuid();
        Assert.assertEquals(entitiesMutated.size(), 1);
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntity")
    public void testGetEntityById() throws Exception {

        final AtlasEntity response = entityREST.getById(dbGuid);

        Assert.assertNotNull(response);
        TestEntitiesREST.verifyAttributes(response.getAttributes(), dbEntity.getAttributes());
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntity")
    public void  testAddAndGetClassification() throws Exception {

        List<AtlasClassification> classifications = new ArrayList<>();
        testClassification = new AtlasClassification(TestUtilsV2.CLASSIFICATION, new HashMap<String, Object>() {{ put("tag", "tagName"); }});
        classifications.add(testClassification);
        entityREST.addClassifications(dbGuid, classifications);

        final AtlasClassification.AtlasClassifications retrievedClassifications = entityREST.getClassifications(dbGuid);
        Assert.assertNotNull(retrievedClassifications);
        final List<AtlasClassification> retrievedClassificationsList = retrievedClassifications.getList();
        Assert.assertNotNull(retrievedClassificationsList);

        Assert.assertEquals(classifications, retrievedClassificationsList);

        final AtlasClassification retrievedClassification = entityREST.getClassification(dbGuid, TestUtilsV2.CLASSIFICATION);

        Assert.assertNotNull(retrievedClassification);
        Assert.assertEquals(retrievedClassification, testClassification);

    }

    @Test(dependsOnMethods = "testAddAndGetClassification")
    public void  testGetEntityWithAssociations() throws Exception {

        AtlasEntityWithAssociations entity = entityREST.getWithAssociationsByGuid(dbGuid);
        final List<AtlasClassification> retrievedClassifications = entity.getClassifications();

        Assert.assertNotNull(retrievedClassifications);
        Assert.assertEquals(new ArrayList<AtlasClassification>() {{ add(testClassification); }}, retrievedClassifications);
    }

    @Test(dependsOnMethods = "testGetEntityWithAssociations")
    public void  testDeleteClassification() throws Exception {

        entityREST.deleteClassification(dbGuid, TestUtilsV2.CLASSIFICATION);
        final AtlasClassification.AtlasClassifications retrievedClassifications = entityREST.getClassifications(dbGuid);

        Assert.assertNotNull(retrievedClassifications);
        Assert.assertEquals(retrievedClassifications.getList().size(), 0);
    }

    @Test(dependsOnMethods = "testDeleteClassification")
    public void  testDeleteEntityById() throws Exception {

        EntityMutationResponse response = entityREST.deleteByGuid(dbGuid);
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE);
        Assert.assertNotNull(entitiesMutated);
        Assert.assertEquals(entitiesMutated.get(0).getGuid(), dbGuid);
    }

    @Test
    public void  testUpdateGetDeleteEntityByUniqueAttribute() throws Exception {

        AtlasEntity dbEntity = TestUtilsV2.createDBEntity();
        entityREST.createOrUpdate(dbEntity);

        final String prevDBName = (String) dbEntity.getAttribute(TestUtilsV2.NAME);
        final String updatedDBName = "updatedDBName";

        dbEntity.setAttribute(TestUtilsV2.NAME, updatedDBName);

        final EntityMutationResponse response = entityREST.partialUpdateByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, prevDBName, dbEntity);
        String dbGuid = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).get(0).getGuid();
        Assert.assertTrue(AtlasEntity.isAssigned(dbGuid));

        //Get By unique attribute
        AtlasEntity entity = entityREST.getByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, updatedDBName);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getGuid());
        Assert.assertEquals(entity.getGuid(), dbGuid);
        TestEntitiesREST.verifyAttributes(entity.getAttributes(), dbEntity.getAttributes());

        final EntityMutationResponse deleteResponse = entityREST.deleteByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, (String) dbEntity.getAttribute(TestUtilsV2.NAME));

        Assert.assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        Assert.assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 1);
        Assert.assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).get(0).getGuid(), dbGuid);
    }

}
