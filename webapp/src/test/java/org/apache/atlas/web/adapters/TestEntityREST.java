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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.rest.EntitiesREST;
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
import java.util.Map;

@Guice(modules = {RepositoryMetadataModule.class})
public class TestEntityREST {

    @Inject
    private AtlasTypeDefStore typeStore;

    @Inject
    private EntityREST entityREST;

    @Inject
    private EntitiesREST entitiesREST;

    private AtlasEntity dbEntity;

    private AtlasClassification testClassification;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = TestUtilsV2.defineHiveTypes();
        typeStore.createTypesDef(typesDef);
    }

    @AfterClass
    public void tearDown() throws Exception {
        AtlasGraphProvider.cleanup();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        RequestContext.clear();
    }

    private void createTestEntity() throws Exception {
        Map<String, AtlasEntity> dbEntityMap = TestUtilsV2.createDBEntity();
        AtlasEntity              dbEntity    = dbEntityMap.values().iterator().next();

        dbEntityMap.put(dbEntity.getGuid(), dbEntity);

        final EntityMutationResponse response = entitiesREST.createOrUpdate(dbEntityMap);

        Assert.assertNotNull(response);
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        Assert.assertNotNull(entitiesMutated);
        Assert.assertEquals(entitiesMutated.size(), 1);
        Assert.assertNotNull(entitiesMutated.get(0));
        dbEntity.setGuid(entitiesMutated.get(0).getGuid());

        this.dbEntity = dbEntity;
    }

    @Test
    public void testGetEntityById() throws Exception {
        createTestEntity();
        AtlasEntityWithExtInfo response = entityREST.getById(dbEntity.getGuid());

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getEntity());
        TestEntitiesREST.verifyAttributes(response.getEntity().getAttributes(), dbEntity.getAttributes());
    }

    @Test(dependsOnMethods = "testGetEntityById")
    public void  testAddAndGetClassification() throws Exception {

        List<AtlasClassification> classifications = new ArrayList<>();
        testClassification = new AtlasClassification(TestUtilsV2.CLASSIFICATION, new HashMap<String, Object>() {{ put("tag", "tagName"); }});
        classifications.add(testClassification);
        entityREST.addClassifications(dbEntity.getGuid(), classifications);

        final AtlasClassification.AtlasClassifications retrievedClassifications = entityREST.getClassifications(dbEntity.getGuid());
        Assert.assertNotNull(retrievedClassifications);
        final List<AtlasClassification> retrievedClassificationsList = retrievedClassifications.getList();
        Assert.assertNotNull(retrievedClassificationsList);

        Assert.assertEquals(classifications, retrievedClassificationsList);

        final AtlasClassification retrievedClassification = entityREST.getClassification(dbEntity.getGuid(), TestUtilsV2.CLASSIFICATION);

        Assert.assertNotNull(retrievedClassification);
        Assert.assertEquals(retrievedClassification, testClassification);

    }

    @Test(dependsOnMethods = "testAddAndGetClassification")
    public void  testGetEntityWithAssociations() throws Exception {

        AtlasEntityWithExtInfo entity = entityREST.getById(dbEntity.getGuid());
        final List<AtlasClassification> retrievedClassifications = entity.getEntity().getClassifications();

        Assert.assertNotNull(retrievedClassifications);
        Assert.assertEquals(new ArrayList<AtlasClassification>() {{ add(testClassification); }}, retrievedClassifications);
    }

    @Test(dependsOnMethods = "testGetEntityWithAssociations")
    public void  testDeleteClassification() throws Exception {

        entityREST.deleteClassification(dbEntity.getGuid(), TestUtilsV2.CLASSIFICATION);
        final AtlasClassification.AtlasClassifications retrievedClassifications = entityREST.getClassifications(dbEntity.getGuid());

        Assert.assertNotNull(retrievedClassifications);
        Assert.assertEquals(retrievedClassifications.getList().size(), 0);
    }

    @Test(dependsOnMethods = "testDeleteClassification")
    public void  testDeleteEntityById() throws Exception {

        EntityMutationResponse response = entityREST.deleteByGuid(dbEntity.getGuid());
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE);
        Assert.assertNotNull(entitiesMutated);
        Assert.assertEquals(entitiesMutated.get(0).getGuid(), dbEntity.getGuid());
    }

    @Test
    public void  testUpdateGetDeleteEntityByUniqueAttribute() throws Exception {
        Map<String, AtlasEntity> dbEntityMap = TestUtilsV2.createDBEntity();
        AtlasEntity              dbEntity    = dbEntityMap.values().iterator().next();
        EntityMutationResponse   response    = entitiesREST.createOrUpdate(dbEntityMap);
        String                   dbGuid      = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0).getGuid();

        Assert.assertTrue(AtlasEntity.isAssigned(dbGuid));

        final String prevDBName    = (String) dbEntity.getAttribute(TestUtilsV2.NAME);
        final String updatedDBName = prevDBName + ":updated";

        dbEntity.setAttribute(TestUtilsV2.NAME, updatedDBName);

        response = entityREST.partialUpdateByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, prevDBName, dbEntity);
        Assert.assertEquals(response.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).get(0).getGuid(), dbGuid);

        //Get By unique attribute
        List<AtlasEntity> entities = entityREST.getByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, updatedDBName);
        Assert.assertNotNull(entities);
        Assert.assertNotNull(entities.get(0).getGuid());
        Assert.assertEquals(entities.get(0).getGuid(), dbGuid);
        TestEntitiesREST.verifyAttributes(entities.get(0).getAttributes(), dbEntity.getAttributes());

        final EntityMutationResponse deleteResponse = entityREST.deleteByUniqueAttribute(TestUtilsV2.DATABASE_TYPE, TestUtilsV2.NAME, (String) dbEntity.getAttribute(TestUtilsV2.NAME));

        Assert.assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        Assert.assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 1);
        Assert.assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).get(0).getGuid(), dbGuid);
    }

}
