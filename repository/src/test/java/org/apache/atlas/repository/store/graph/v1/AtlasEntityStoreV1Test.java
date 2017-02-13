/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.randomString;
import static org.testng.Assert.assertEquals;

@Guice(modules = RepositoryMetadataModule.class)
public class AtlasEntityStoreV1Test {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1Test.class);

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    AtlasEntityStore entityStore;

    @Inject
    MetadataService metadataService;

    @Inject
    DeleteHandlerV1 deleteHandler;

    private AtlasEntitiesWithExtInfo deptEntity;
    private AtlasEntityWithExtInfo   dbEntity;
    private AtlasEntityWithExtInfo   tblEntity;


    @BeforeClass
    public void setUp() throws Exception {
        metadataService = TestUtils.addSessionCleanupWrapper(metadataService);
        new GraphBackedSearchIndexer(typeRegistry);

        AtlasTypesDef[] testTypesDefs = new AtlasTypesDef[] { TestUtilsV2.defineDeptEmployeeTypes(),
                                                              TestUtilsV2.defineHiveTypes()
                                                            };

        for (AtlasTypesDef typesDef : testTypesDefs) {
            AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

            if (!typesToCreate.isEmpty()) {
                typeDefStore.createTypesDef(typesToCreate);
            }
        }

        deptEntity = TestUtilsV2.createDeptEg2();
        dbEntity   = TestUtilsV2.createDBEntityV2();
        tblEntity  = TestUtilsV2.createTableEntityV2(dbEntity.getEntity());
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
        TestUtils.resetRequestContext();
    }

    @BeforeTest
    public void init() throws Exception {
        entityStore = new AtlasEntityStoreV1(deleteHandler, typeRegistry);

        RequestContextV1.clear();
    }

    @Test
    public void testCreate() throws Exception {
        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(deptEntity));

        validateMutationResponse(response, EntityOperation.CREATE, 5);
        AtlasEntityHeader dept1 = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DEPARTMENT_TYPE);
        validateEntity(deptEntity, getEntityFromStore(dept1), deptEntity.getEntities().get(0));

        final Map<EntityOperation, List<AtlasEntityHeader>> entitiesMutated = response.getEntitiesMutated();
        List<AtlasEntityHeader> entitiesCreated = entitiesMutated.get(EntityOperation.CREATE);

        Assert.assertTrue(entitiesCreated.size() >= deptEntity.getEntities().size());

        for (int i = 0; i < deptEntity.getEntities().size(); i++) {
            AtlasEntity expected = deptEntity.getEntities().get(i);
            AtlasEntity actual   = getEntityFromStore(entitiesCreated.get(i));

            validateEntity(deptEntity, actual, expected);
        }

        //Create DB
        init();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity));
        validateMutationResponse(dbCreationResponse, EntityOperation.CREATE, 1);

        AtlasEntityHeader db1 = dbCreationResponse.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);
        validateEntity(dbEntity, getEntityFromStore(db1));

        //Create Table
        //Update DB guid
        AtlasObjectId dbObjectId = (AtlasObjectId) tblEntity.getEntity().getAttribute("database");
        dbObjectId.setGuid(db1.getGuid());
        tblEntity.addReferredEntity(dbEntity.getEntity());

        init();
        EntityMutationResponse tableCreationResponse = entityStore.createOrUpdate(new AtlasEntityStream(tblEntity));
        validateMutationResponse(tableCreationResponse, EntityOperation.CREATE, 1);

        AtlasEntityHeader tableEntity = tableCreationResponse.getFirstCreatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(tblEntity, getEntityFromStore(tableEntity));
    }

    @Test(dependsOnMethods = "testCreate")
    public void testArrayOfEntityUpdate() throws Exception {
        AtlasEntity              tableEntity  = new AtlasEntity(tblEntity.getEntity());
        List<AtlasObjectId>      columns      = new ArrayList<>();
        AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntitiesWithExtInfo(tableEntity);


        AtlasEntity col1 = TestUtilsV2.createColumnEntity(tableEntity);
        col1.setAttribute(TestUtilsV2.NAME, "col1");

        AtlasEntity col2 = TestUtilsV2.createColumnEntity(tableEntity);
        col2.setAttribute(TestUtilsV2.NAME, "col2");

        columns.add(col1.getAtlasObjectId());
        columns.add(col2.getAtlasObjectId());

        tableEntity.setAttribute(TestUtilsV2.COLUMNS_ATTR_NAME, columns);

        entitiesInfo.addReferredEntity(dbEntity.getEntity());
        entitiesInfo.addReferredEntity(col1);
        entitiesInfo.addReferredEntity(col2);

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        AtlasEntityHeader updatedTable = response.getFirstUpdatedEntityByTypeName(tableEntity.getTypeName());
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //Complete update. Add  array elements - col3,col4
        AtlasEntity col3 = TestUtilsV2.createColumnEntity(tableEntity);
        col3.setAttribute(TestUtilsV2.NAME, "col3");

        AtlasEntity col4 = TestUtilsV2.createColumnEntity(tableEntity);
        col4.setAttribute(TestUtilsV2.NAME, "col4");

        columns.add(col3.getAtlasObjectId());
        columns.add(col4.getAtlasObjectId());

        entitiesInfo.addReferredEntity(col3);
        entitiesInfo.addReferredEntity(col4);

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        updatedTable = response.getFirstUpdatedEntityByTypeName(tableEntity.getTypeName());
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //Swap elements
        columns.clear();
        columns.add(col4.getAtlasObjectId());
        columns.add(col3.getAtlasObjectId());

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        updatedTable = response.getFirstUpdatedEntityByTypeName(tableEntity.getTypeName());
        Assert.assertEquals(((List<AtlasObjectId>) updatedTable.getAttribute(COLUMNS_ATTR_NAME)).size(), 2);

        assertEquals(response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 2);  // col1, col2 are deleted

        //Update array column to null
        tableEntity.setAttribute(COLUMNS_ATTR_NAME, null);

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        updatedTable = response.getFirstUpdatedEntityByTypeName(tableEntity.getTypeName());
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));
        assertEquals(response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 2);
    }

    @Test(dependsOnMethods = "testCreate")
    public void testUpdateEntityWithMap() throws Exception {
        AtlasEntity              tableEntity  = new AtlasEntity(tblEntity.getEntity());
        AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntitiesWithExtInfo(tableEntity);
        Map<String, AtlasStruct> partsMap     = new HashMap<>();
        partsMap.put("part0", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "test"));

        tableEntity.setAttribute("partitionsMap", partsMap);
        entitiesInfo.addReferredEntity(tableEntity);

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        AtlasEntityHeader tableDefinition1 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition1));
                
        Assert.assertTrue(partsMap.get("part0").equals(((Map<String, AtlasStruct>) tableDefinition1.getAttribute("partitionsMap")).get("part0")));

        //update map - add a map key
        partsMap.put("part1", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "test1"));
        tableEntity.setAttribute("partitionsMap", partsMap);

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        AtlasEntityHeader tableDefinition2 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition2));

        assertEquals(((Map<String, AtlasStruct>) tableDefinition2.getAttribute("partitionsMap")).size(), 2);
        Assert.assertTrue(partsMap.get("part1").equals(((Map<String, AtlasStruct>) tableDefinition2.getAttribute("partitionsMap")).get("part1")));

        //update map - remove a key and add another key
        partsMap.remove("part0");
        partsMap.put("part2", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "test2"));
        tableEntity.setAttribute("partitionsMap", partsMap);

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        AtlasEntityHeader tableDefinition3 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition3));

        assertEquals(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equals(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).get("part2")));

        //update struct value for existing map key
        AtlasStruct partition2 = partsMap.get("part2");
        partition2.setAttribute(TestUtilsV2.NAME, "test2Updated");

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));

        AtlasEntityHeader tableDefinition4 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition4));

        assertEquals(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equals(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).get("part2")));

        //Test map pointing to a class

        AtlasEntity col0 = new AtlasEntity(TestUtilsV2.COLUMN_TYPE, TestUtilsV2.NAME, "test1");
        col0.setAttribute("type", "string");
        col0.setAttribute("table", tableEntity.getAtlasObjectId());

        AtlasEntityWithExtInfo col0WithExtendedInfo = new AtlasEntityWithExtInfo(col0);
        col0WithExtendedInfo.addReferredEntity(tableEntity);
        col0WithExtendedInfo.addReferredEntity(dbEntity.getEntity());

        init();
        entityStore.createOrUpdate(new AtlasEntityStream(col0WithExtendedInfo));

        AtlasEntity col1 = new AtlasEntity(TestUtils.COLUMN_TYPE, TestUtilsV2.NAME, "test2");
        col1.setAttribute("type", "string");
        col1.setAttribute("table", tableEntity.getAtlasObjectId());

        AtlasEntityWithExtInfo col1WithExtendedInfo = new AtlasEntityWithExtInfo(col1);
        col1WithExtendedInfo.addReferredEntity(tableEntity);
        col1WithExtendedInfo.addReferredEntity(dbEntity.getEntity());

        init();
        entityStore.createOrUpdate(new AtlasEntityStream(col1WithExtendedInfo));

        Map<String, AtlasObjectId> columnsMap = new HashMap<String, AtlasObjectId>();
        columnsMap.put("col0", col0.getAtlasObjectId());
        columnsMap.put("col1", col1.getAtlasObjectId());

        tableEntity.setAttribute(TestUtils.COLUMNS_MAP, columnsMap);

        entitiesInfo.addReferredEntity(col0);
        entitiesInfo.addReferredEntity(col1);
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader tableDefinition5 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition5));

        //Swap elements
        columnsMap.clear();
        columnsMap.put("col0", col1.getAtlasObjectId());
        columnsMap.put("col1", col0.getAtlasObjectId());

        tableEntity.setAttribute(TestUtils.COLUMNS_MAP, columnsMap);
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader tableDefinition6 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition6));

        //Drop the first key and change the class type as well to col0
        columnsMap.clear();
        columnsMap.put("col0", col0.getAtlasObjectId());
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader tableDefinition7 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition7));

        //Clear state
        tableEntity.setAttribute(TestUtils.COLUMNS_MAP, null);
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader tableDefinition8 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(tableDefinition8));
    }

    @Test(dependsOnMethods = "testCreate")
    public void testMapOfPrimitivesUpdate() throws Exception {
        AtlasEntity              tableEntity  = new AtlasEntity(tblEntity.getEntity());
        AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntitiesWithExtInfo(tableEntity);

        entitiesInfo.addReferredEntity(tableEntity);

        //Add a new entry
        Map<String, String> paramsMap = (Map<String, String>) tableEntity.getAttribute("parametersMap");
        paramsMap.put("newParam", "value");
        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 1);
        AtlasEntityHeader updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //Remove an entry
        paramsMap.remove("key1");
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 1);
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));
    }

    @Test(dependsOnMethods = "testCreate")
    public void testArrayOfStructs() throws Exception {
        //Modify array of structs
        AtlasEntity              tableEntity  = new AtlasEntity(tblEntity.getEntity());
        AtlasEntitiesWithExtInfo entitiesInfo = new AtlasEntitiesWithExtInfo(tableEntity);

        List<AtlasStruct> partitions = new ArrayList<AtlasStruct>(){{
            add(new AtlasStruct(TestUtilsV2.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "part1"));
            add(new AtlasStruct(TestUtilsV2.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "part2"));
        }};
        tableEntity.setAttribute("partitions", partitions);

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //add a new element to array of struct
        partitions.add(new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "part3"));
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //remove one of the struct values
        init();
        partitions.remove(1);
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //Update struct value within array of struct
        init();
        partitions.get(0).setAttribute(TestUtilsV2.NAME, "part4");
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));


        //add a repeated element to array of struct
        partitions.add(new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE, TestUtilsV2.NAME, "part4"));
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        // Remove all elements. Should set array attribute to null
        partitions.clear();
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));
    }


    @Test(dependsOnMethods = "testCreate")
    public void testStructs() throws Exception {
        AtlasEntity              databaseEntity = dbEntity.getEntity();
        AtlasEntity              tableEntity    = new AtlasEntity(tblEntity.getEntity());
        AtlasEntitiesWithExtInfo entitiesInfo   = new AtlasEntitiesWithExtInfo(tableEntity);

        AtlasStruct serdeInstance = new AtlasStruct(TestUtils.SERDE_TYPE, TestUtilsV2.NAME, "serde1Name");
        serdeInstance.setAttribute("serde", "test");
        serdeInstance.setAttribute("description", "testDesc");
        tableEntity.setAttribute("serde1", serdeInstance);
        tableEntity.setAttribute("database", new AtlasObjectId(databaseEntity.getTypeName(), TestUtilsV2.NAME, databaseEntity.getAttribute(TestUtilsV2.NAME)));

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        AtlasEntityHeader updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //update struct attribute
        serdeInstance.setAttribute("serde", "testUpdated");
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));

        //set to null
        tableEntity.setAttribute("description", null);
        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesInfo));
        updatedTable = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        Assert.assertNull(updatedTable.getAttribute("description"));
        validateEntity(entitiesInfo, getEntityFromStore(updatedTable));
    }

//    private AtlasEntity clearSubOrdinates(List<AtlasObjectId> employees, int index) {
//
//        AtlasEntity ret = null;
//        AtlasObjectId employee = employees.get(index);
//        AtlasEntity subOrdClone = new ArrayList<>(subOrdinates);
//        ret = subOrdClone.remove(index);
//
//        employees.get(index).setAttribute("subordinates", subOrdClone);
//        return ret;
//    }
//
//    private int addSubordinate(AtlasEntity manager, AtlasEntity employee) {
//        List<AtlasEntity> subOrdinates = (List<AtlasEntity>) manager.getAttribute("subordinates");
//        subOrdinates.add(employee);
//
//        manager.setAttribute("subordinates", subOrdinates);
//        return subOrdinates.size() - 1;
//    }

    @Test(dependsOnMethods = "testCreate")
    public void testClassUpdate() throws Exception {

        init();
        //Create new db instance
        final AtlasEntity databaseInstance = TestUtilsV2.createDBEntity();

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(databaseInstance));
        final AtlasEntityHeader dbCreated = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

        init();
        Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        AtlasEntity tableClone = new AtlasEntity(tblEntity.getEntity());
        tableClone.setAttribute("database", new AtlasObjectId(dbCreated.getGuid(), TestUtils.DATABASE_TYPE));

        tableCloneMap.put(dbCreated.getGuid(), databaseInstance);
        tableCloneMap.put(tableClone.getGuid(), tableClone);

        response = entityStore.createOrUpdate(new InMemoryMapEntityStream(tableCloneMap));
        final AtlasEntityHeader tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        Assert.assertNotNull(tableDefinition.getAttribute("database"));
        Assert.assertEquals(((AtlasObjectId) tableDefinition.getAttribute("database")).getGuid(), dbCreated.getGuid());
    }

    @Test
    public void testCheckOptionalAttrValueRetention() throws Exception {

        AtlasEntity dbEntity = TestUtilsV2.createDBEntity();

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity));
        AtlasEntityHeader firstEntityCreated = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

        //The optional boolean attribute should have a non-null value
        final String isReplicatedAttr = "isReplicated";
        final String paramsAttr = "parameters";
        Assert.assertNotNull(firstEntityCreated.getAttribute(isReplicatedAttr));
        Assert.assertEquals(firstEntityCreated.getAttribute(isReplicatedAttr), Boolean.FALSE);
        Assert.assertNull(firstEntityCreated.getAttribute(paramsAttr));

        //Update to true
        dbEntity.setAttribute(isReplicatedAttr, Boolean.TRUE);
        //Update array
        final HashMap<String, String> params = new HashMap<String, String>() {{ put("param1", "val1"); put("param2", "val2"); }};
        dbEntity.setAttribute(paramsAttr, params);
        //Complete update

        init();
        response = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity));
        AtlasEntityHeader firstEntityUpdated = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

        Assert.assertNotNull(firstEntityUpdated);
        Assert.assertNotNull(firstEntityUpdated.getAttribute(isReplicatedAttr));
        Assert.assertEquals(firstEntityUpdated.getAttribute(isReplicatedAttr), Boolean.TRUE);
        Assert.assertEquals(firstEntityUpdated.getAttribute(paramsAttr), params);

        //TODO - enable test after GET API is ready
//        init();
//        //Complete update without setting the attribute
//        AtlasEntity newEntity = TestUtilsV2.createDBEntity();
//        //Reset name to the current DB name
//        newEntity.setAttribute(AtlasClient.NAME, firstEntityCreated.getAttribute(AtlasClient.NAME));
//        response = entityStore.createOrUpdate(newEntity);
//
//        firstEntityUpdated = response.getFirstEntityUpdated();
//        Assert.assertNotNull(firstEntityUpdated.getAttribute(isReplicatedAttr));
//        Assert.assertEquals(firstEntityUpdated.getAttribute(isReplicatedAttr), Boolean.TRUE);
//        Assert.assertEquals(firstEntityUpdated.getAttribute(paramsAttr), params);
    }

    @Test(enabled = false)
    //Titan doesn't allow some reserved chars in property keys. Verify that atlas encodes these
    //See GraphHelper.encodePropertyKey()
    //TODO : Failing in typedef creation
    public void testSpecialCharacters() throws Exception {
        //Verify that type can be created with reserved characters in typename, attribute name
        final String typeName = "test_type_"+ RandomStringUtils.randomAlphanumeric(10);
        String strAttrName = randomStrWithReservedChars();
        String arrayAttrName = randomStrWithReservedChars();
        String mapAttrName = randomStrWithReservedChars();

        AtlasEntityDef typeDefinition =
            AtlasTypeUtil.createClassTypeDef(typeName, "Special chars test type", ImmutableSet.<String>of(),
                AtlasTypeUtil.createOptionalAttrDef(strAttrName, "string"),
                AtlasTypeUtil.createOptionalAttrDef(arrayAttrName, "array<string>"),
                AtlasTypeUtil.createOptionalAttrDef(mapAttrName, "map<string,string>"));

        AtlasTypesDef atlasTypesDef = new AtlasTypesDef(null, null, null, Arrays.asList(typeDefinition));
        typeDefStore.createTypesDef(atlasTypesDef);

        //verify that entity can be created with reserved characters in string value, array value and map key and value
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(strAttrName, randomStrWithReservedChars());
        entity.setAttribute(arrayAttrName, new String[]{randomStrWithReservedChars()});
        entity.setAttribute(mapAttrName, new HashMap<String, String>() {{
            put(randomStrWithReservedChars(), randomStrWithReservedChars());
        }});

        AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntityWithExtInfo(entity);

        final EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entityWithExtInfo));
        final AtlasEntityHeader firstEntityCreated = response.getFirstEntityCreated();
        validateEntity(entityWithExtInfo, getEntityFromStore(firstEntityCreated));


        //Verify that search with reserved characters works - for string attribute
//        String query =
//            String.format("`%s` where `%s` = '%s'", typeName, strAttrName, entity.getAttribute(strAttrName));
//        String responseJson = discoveryService.searchByDSL(query, new QueryParams(1, 0));
//        JSONObject response = new JSONObject(responseJson);
//        assertEquals(response.getJSONArray("rows").length(), 1);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testCreateRequiredAttrNull() throws Exception {
        //Update required attribute
        Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        AtlasEntity tableEntity = new AtlasEntity(TestUtilsV2.TABLE_TYPE);
        tableEntity.setAttribute(TestUtilsV2.NAME, "table_" + TestUtils.randomString());
        tableCloneMap.put(tableEntity.getGuid(), tableEntity);

        entityStore.createOrUpdate(new InMemoryMapEntityStream(tableCloneMap));
        Assert.fail("Expected exception while creating with required attribute null");
    }


    private String randomStrWithReservedChars() {
        return randomString() + "\"${}%";
    }

    private void validateMutationResponse(EntityMutationResponse response, EntityMutations.EntityOperation op, int expectedNumCreated) {
        List<AtlasEntityHeader> entitiesCreated = response.getEntitiesByOperation(op);
        Assert.assertNotNull(entitiesCreated);
        Assert.assertEquals(entitiesCreated.size(), expectedNumCreated);
    }

    private void validateEntity(AtlasEntityExtInfo entityExtInfo, AtlasEntity actual) throws AtlasBaseException, AtlasException {
        validateEntity(entityExtInfo, actual, entityExtInfo.getEntity(actual.getGuid()));
    }

    private void validateEntity(AtlasEntityExtInfo entityExtInfo, AtlasStruct actual, AtlasStruct expected) throws AtlasBaseException, AtlasException {
        if (expected == null) {
            Assert.assertNull(actual, "expected null instance. Found " + actual);

            return;
        }

        Assert.assertNotNull(actual, "found null instance");

        AtlasStructType entityType = (AtlasStructType) typeRegistry.getType(actual.getTypeName());
        for (String attrName : expected.getAttributes().keySet()) {
            Object expectedVal = expected.getAttribute(attrName);
            Object actualVal   = actual.getAttribute(attrName);

            AtlasType attrType = entityType.getAttributeType(attrName);
            validateAttribute(entityExtInfo, actualVal, expectedVal, attrType, attrName);
        }
    }

    private void validateAttribute(AtlasEntityExtInfo entityExtInfo, Object actual, Object expected, AtlasType attributeType, String attrName) throws AtlasBaseException, AtlasException {
        switch(attributeType.getTypeCategory()) {
            case OBJECT_ID_TYPE:
                Assert.assertTrue(actual instanceof AtlasObjectId);
                String guid = ((AtlasObjectId) actual).getGuid();
                Assert.assertTrue(AtlasEntity.isAssigned(guid), "expected assigned guid. found " + guid);
                break;

            case PRIMITIVE:
            case ENUM:
                Assert.assertEquals(actual, expected);
                break;

            case MAP:
                AtlasMapType mapType     = (AtlasMapType) attributeType;
                AtlasType    valueType   = mapType.getValueType();
                Map          actualMap   = (Map) actual;
                Map          expectedMap = (Map) expected;

                if (MapUtils.isNotEmpty(expectedMap)) {
                    Assert.assertTrue(MapUtils.isNotEmpty(actualMap));

                    //actual map could have deleted entities. Hence size may not match.
                    Assert.assertTrue(actualMap.size() >= expectedMap.size());

                    for (Object key : expectedMap.keySet()) {
                        validateAttribute(entityExtInfo, actualMap.get(key), expectedMap.get(key), valueType, attrName);
                    }
                }
                break;

            case ARRAY:
                AtlasArrayType arrType      = (AtlasArrayType) attributeType;
                AtlasType      elemType     = arrType.getElementType();
                List           actualList   = (List) actual;
                List           expectedList = (List) expected;

                if (CollectionUtils.isNotEmpty(expectedList)) {
                    Assert.assertTrue(CollectionUtils.isNotEmpty(actualList));

                    //actual list could have deleted entities. Hence size may not match.
                    Assert.assertTrue(actualList.size() >= expectedList.size());

                    for (int i = 0; i < expectedList.size(); i++) {
                        validateAttribute(entityExtInfo, actualList.get(i), expectedList.get(i), elemType, attrName);
                    }
                }
                break;
            case STRUCT:
                AtlasStruct expectedStruct = (AtlasStruct) expected;
                AtlasStruct actualStruct   = (AtlasStruct) actual;

                validateEntity(entityExtInfo, actualStruct, expectedStruct);
                break;
            default:
                Assert.fail("Unknown type category");
        }
    }

    private AtlasEntity getEntityFromStore(AtlasEntityHeader header) throws AtlasBaseException {
        AtlasEntityWithExtInfo entity = header != null ? entityStore.getById(header.getGuid()) : null;

        return entity != null ? entity.getEntity() : null;
    }
}
