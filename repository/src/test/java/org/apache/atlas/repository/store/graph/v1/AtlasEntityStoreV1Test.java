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
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.TABLE_TYPE;
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
    
    private Map<String, AtlasEntity> deptEntityMap;
    private Map<String, AtlasEntity> dbEntityMap;
    private Map<String, AtlasEntity> tableEntityMap;

    private AtlasEntity deptEntity;
    private AtlasEntity dbEntity;
    private AtlasEntity tableEntity;

    @BeforeClass
    public void setUp() throws Exception {
        metadataService = TestUtils.addSessionCleanupWrapper(metadataService);
        new GraphBackedSearchIndexer(typeRegistry);
        final AtlasTypesDef deptTypesDef = TestUtilsV2.defineDeptEmployeeTypes();
        typeDefStore.createTypesDef(deptTypesDef);

        final AtlasTypesDef hiveTypesDef = TestUtilsV2.defineHiveTypes();
        typeDefStore.createTypesDef(hiveTypesDef);
        
        deptEntityMap = TestUtilsV2.createDeptEg1();
        dbEntityMap = TestUtilsV2.createDBEntity();
        tableEntityMap = TestUtilsV2.createTableEntity(dbEntityMap.keySet().iterator().next());

        deptEntity = deptEntityMap.values().iterator().next();
        dbEntity = dbEntityMap.values().iterator().next();
        tableEntity = tableEntityMap.values().iterator().next();
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
        TestUtils.resetRequestContext();
    }

    @BeforeTest
    public void init() throws Exception {
        final Class<? extends DeleteHandlerV1> deleteHandlerImpl = AtlasRepositoryConfiguration.getDeleteHandlerV1Impl();
        final Constructor<? extends DeleteHandlerV1> deleteHandlerImplConstructor = deleteHandlerImpl.getConstructor(AtlasTypeRegistry.class);
        DeleteHandlerV1 deleteHandler = deleteHandlerImplConstructor.newInstance(typeRegistry);
        ArrayVertexMapper arrVertexMapper = new ArrayVertexMapper(deleteHandler);
        MapVertexMapper mapVertexMapper = new MapVertexMapper(deleteHandler);


        entityStore = new AtlasEntityStoreV1(new EntityGraphMapper(arrVertexMapper, mapVertexMapper, deleteHandler));
        entityStore.init(typeRegistry);

        RequestContextV1.clear();
    }

    @Test
    public void testCreate() throws Exception {
        EntityMutationResponse response = entityStore.createOrUpdate(deptEntityMap);

        validateMutationResponse(response, EntityMutations.EntityOperation.CREATE, 5);
        AtlasEntityHeader deptEntity = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DEPARTMENT_TYPE);

        final Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> entitiesMutated = response.getEntitiesMutated();
        List<AtlasEntityHeader> entitiesCreated = entitiesMutated.get(EntityMutations.EntityOperation.CREATE);

        for (AtlasEntityHeader header : entitiesCreated) {
            validateAttributes(deptEntityMap, header);
        }

        //Create DB
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(dbEntityMap);
        validateMutationResponse(dbCreationResponse, EntityMutations.EntityOperation.CREATE, 1);

        AtlasEntityHeader dbEntity = dbCreationResponse.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);
        validateAttributes(dbEntityMap, dbEntity);

        //Create Table
        //Update DB guid
        AtlasObjectId dbId = (AtlasObjectId) tableEntity.getAttribute("database");
        dbId.setGuid(dbEntity.getGuid());
        tableEntityMap.put(dbId.getGuid(), dbEntityMap.values().iterator().next());
        tableEntity.setAttribute("database", dbId);

        EntityMutationResponse tableCreationResponse = entityStore.createOrUpdate(tableEntityMap);
        validateMutationResponse(tableCreationResponse, EntityMutations.EntityOperation.CREATE, 1);

        AtlasEntityHeader tableEntity = tableCreationResponse.getFirstCreatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableEntity);

    }

    @Test(dependsOnMethods = "testCreate")
    public void testArrayOfEntityUpdate() throws Exception {
        //clear state
        init();

//        Map<String, AtlasEntity> entityCloneMap = new HashMap<>();
//        AtlasEntity entityClone = new AtlasEntity(deptEntity);
//        List<AtlasObjectId> employees = (List<AtlasObjectId>) entityClone.getAttribute("employees");
//        AtlasEntity entityRemoved = clearSubOrdinates(employees, 1);
//        entityClone.setAttribute("employees", employees);
//        EntityMutationResponse response = entityStore.createOrUpdate(entityCloneMap);
//
//        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 5);
//        AtlasEntityHeader deptEntity = response.getFirstEntityUpdated();
//        Assert.assertEquals(((List<AtlasEntity>)(((List<AtlasEntity>) deptEntity.getAttribute("employees")).get(1).getAttribute("subordinates"))).size(), 1);
//
//        init();
//        //add  entity back
//        addSubordinate(employees.get(1), entityRemoved);
//        response = entityStore.createOrUpdate(entityCloneMap);
//        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 5);
//        deptEntity = response.getFirstEntityUpdated();
//        validateAttributes(deptEntity);


        Map<String, AtlasEntity> tableUpdatedMap = new HashMap<>();
        tableUpdatedMap.put(dbEntity.getGuid(), dbEntity);

        //test array of class with id
        final List<AtlasObjectId> columns = new ArrayList<>();

        AtlasEntity col1 = TestUtilsV2.createColumnEntity(tableEntity.getGuid());
        col1.setAttribute(TestUtilsV2.NAME, "col1");
        columns.add(col1.getAtlasObjectId());
        tableUpdatedMap.put(col1.getGuid(), col1);

        AtlasEntity tableUpdated = new AtlasEntity(tableEntity);
        tableUpdated.setAttribute(TestUtilsV2.COLUMNS_ATTR_NAME, columns);
        tableUpdatedMap.put(tableUpdated.getGuid(), tableUpdated);

        init();
        EntityMutationResponse response = entityStore.createOrUpdate(tableUpdatedMap);
        AtlasEntityHeader updatedTable = response.getFirstUpdatedEntityByTypeName(tableUpdated.getTypeName());
        validateAttributes(tableUpdatedMap, updatedTable);

        //Complete update. Add  array elements - col3,col4
        AtlasEntity col3 = TestUtilsV2.createColumnEntity(tableEntity.getGuid());
        col1.setAttribute(TestUtilsV2.NAME, "col3");
        columns.add(col3.getAtlasObjectId());
        tableUpdatedMap.put(col3.getGuid(), col3);

        AtlasEntity col4 = TestUtilsV2.createColumnEntity(tableEntity.getGuid());
        col1.setAttribute(TestUtilsV2.NAME, "col4");
        columns.add(col4.getAtlasObjectId());
        tableUpdatedMap.put(col4.getGuid(), col4);

        tableUpdated.setAttribute(COLUMNS_ATTR_NAME, columns);
        init();
        response = entityStore.createOrUpdate(tableUpdatedMap);
        updatedTable = response.getFirstUpdatedEntityByTypeName(tableUpdated.getTypeName());
        validateAttributes(tableUpdatedMap, updatedTable);

        //Swap elements
        tableUpdatedMap.clear();
        columns.clear();
        tableUpdated.setAttribute(COLUMNS_ATTR_NAME, columns);
        tableUpdatedMap.put(tableUpdated.getGuid(), tableUpdated);
        tableUpdatedMap.put(col3.getGuid(), col3);
        tableUpdatedMap.put(col4.getGuid(), col4);
        columns.add(col4.getAtlasObjectId());
        columns.add(col3.getAtlasObjectId());

        init();
        response = entityStore.createOrUpdate(tableUpdatedMap);
        updatedTable = response.getFirstUpdatedEntityByTypeName(tableUpdated.getTypeName());
        Assert.assertEquals(((List<AtlasObjectId>) updatedTable.getAttribute(COLUMNS_ATTR_NAME)).size(), 2);

        assertEquals(response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 1);  //col1 is deleted

        //Update array column to null
        tableUpdated.setAttribute(COLUMNS_ATTR_NAME, null);
        init();
        response = entityStore.createOrUpdate(tableUpdatedMap);
        updatedTable = response.getFirstUpdatedEntityByTypeName(tableUpdated.getTypeName());
        validateAttributes(tableUpdatedMap, updatedTable);
        assertEquals(response.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 2);

    }
    
    @Test(dependsOnMethods = "testCreate")
    public void testUpdateEntityWithMap() throws Exception {

        final Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        final AtlasEntity tableClone = new AtlasEntity(tableEntity);
        final Map<String, AtlasStruct> partsMap = new HashMap<>();
        partsMap.put("part0", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(TestUtilsV2.NAME, "test");
            }}));

        
        tableClone.setAttribute("partitionsMap", partsMap);
        tableCloneMap.put(tableClone.getGuid(), tableClone);


        init();
        EntityMutationResponse response = entityStore.createOrUpdate(tableCloneMap);
        final AtlasEntityHeader tableDefinition1 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition1);
                
        Assert.assertTrue(partsMap.get("part0").equals(((Map<String, AtlasStruct>) tableDefinition1.getAttribute("partitionsMap")).get("part0")));

        //update map - add a map key
        partsMap.put("part1", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(TestUtilsV2.NAME, "test1");
            }}));
        tableClone.setAttribute("partitionsMap", partsMap);

        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition2 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition2);

        assertEquals(((Map<String, AtlasStruct>) tableDefinition2.getAttribute("partitionsMap")).size(), 2);
        Assert.assertTrue(partsMap.get("part1").equals(((Map<String, AtlasStruct>) tableDefinition2.getAttribute("partitionsMap")).get("part1")));

        //update map - remove a key and add another key
        partsMap.remove("part0");
        partsMap.put("part2", new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(TestUtilsV2.NAME, "test2");
            }}));
        tableClone.setAttribute("partitionsMap", partsMap);

        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition3 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition3);

        assertEquals(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equals(((Map<String, AtlasStruct>) tableDefinition3.getAttribute("partitionsMap")).get("part2")));

        //update struct value for existing map key
        init();
        AtlasStruct partition2 = partsMap.get("part2");
        partition2.setAttribute(TestUtilsV2.NAME, "test2Updated");
        response = entityStore.createOrUpdate(tableCloneMap);
        final AtlasEntityHeader tableDefinition4 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition4);

        assertEquals(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).get("part0"));

        assertEquals(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equals(((Map<String, AtlasStruct>) tableDefinition4.getAttribute("partitionsMap")).get("part2")));

        //Test map pointing to a class

        final Map<String, AtlasObjectId> columnsMap = new HashMap<>();

        Map<String, AtlasEntity> col0TypeMap = new HashMap<>();
        AtlasEntity col0Type = new AtlasEntity(TestUtilsV2.COLUMN_TYPE,
            new HashMap<String, Object>() {{
                put(TestUtilsV2.NAME, "test1");
                put("type", "string");
                put("table", new AtlasObjectId(TABLE_TYPE, tableDefinition1.getGuid()));
            }});

        col0TypeMap.put(col0Type.getGuid(), col0Type);


        init();
        entityStore.createOrUpdate(col0TypeMap);

        Map<String, AtlasEntity> col1TypeMap = new HashMap<>();
        AtlasEntity col1Type = new AtlasEntity(TestUtils.COLUMN_TYPE,
            new HashMap<String, Object>() {{
                put(TestUtilsV2.NAME, "test2");
                put("type", "string");
                put("table", new AtlasObjectId(TABLE_TYPE, tableDefinition1.getGuid()));
            }});

        init();
        col1TypeMap.put(col1Type.getGuid(), col1Type);
        entityStore.createOrUpdate(col1TypeMap);

        AtlasObjectId col0Id = new AtlasObjectId(col0Type.getTypeName(), col0Type.getGuid());
        AtlasObjectId col1Id = new AtlasObjectId(col1Type.getTypeName(), col1Type.getGuid());

        columnsMap.put("col0", col0Id);
        columnsMap.put("col1", col1Id);
        tableCloneMap.put(col0Type.getGuid(), col0Type);
        tableCloneMap.put(col1Type.getGuid(), col1Type);

        tableClone.setAttribute(TestUtils.COLUMNS_MAP, columnsMap);

        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition5 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition5);

        //Swap elements
        columnsMap.clear();
        columnsMap.put("col0", col1Id);
        columnsMap.put("col1", col0Id);

        tableClone.setAttribute(TestUtils.COLUMNS_MAP, columnsMap);
        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition6 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition6);

        //Drop the first key and change the class type as well to col0
        columnsMap.clear();
        columnsMap.put("col0", col0Id);

        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition7 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition7);

        //Clear state
        tableClone.setAttribute(TestUtils.COLUMNS_MAP, null);
        init();
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition8 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition8);
    }

    @Test(dependsOnMethods = "testCreate")
    public void testMapOfPrimitivesUpdate() throws Exception {
        //clear state
        init();

        Map<String, AtlasEntity> entityCloneMap = new HashMap<>();
        AtlasEntity entityClone = new AtlasEntity(tableEntity);
        entityCloneMap.put(entityClone.getGuid(), entityClone);

        //Add a new entry
        Map<String, String> paramsMap = (Map<String, String>) entityClone.getAttribute("parametersMap");
        paramsMap.put("newParam", "value");
        entityClone.setAttribute("parametersMap", paramsMap);

        EntityMutationResponse response = entityStore.createOrUpdate(entityCloneMap);
        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 1);
        AtlasEntityHeader tableEntity = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(entityCloneMap, tableEntity);

        //clear state
        init();

        //Remove an entry
        paramsMap.remove("key1");
        entityClone.setAttribute("parametersMap", paramsMap);

        response = entityStore.createOrUpdate(entityCloneMap);
        validateMutationResponse(response, EntityMutations.EntityOperation.UPDATE, 1);
        tableEntity = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(entityCloneMap, tableEntity);
    }

    @Test(dependsOnMethods = "testCreate")
    public void testArrayOfStructs() throws Exception {
        //Modify array of structs
//        TestUtils.dumpGraph(TestUtils.getGraph());
        init();
        final AtlasStruct partition1 = new AtlasStruct(TestUtilsV2.PARTITION_STRUCT_TYPE);
        partition1.setAttribute(TestUtilsV2.NAME, "part1");
        final AtlasStruct partition2 = new AtlasStruct(TestUtilsV2.PARTITION_STRUCT_TYPE);
        partition2.setAttribute(TestUtilsV2.NAME, "part2");

        List<AtlasStruct> partitions = new ArrayList<AtlasStruct>(){{ add(partition1); add(partition2); }};
        tableEntity.setAttribute("partitions", partitions);

        EntityMutationResponse response = entityStore.createOrUpdate(tableEntityMap);
        AtlasEntityHeader tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);

        validateAttributes(tableEntityMap, tableDefinition);

        //add a new element to array of struct
        init();
        final AtlasStruct partition3 = new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE);
        partition3.setAttribute(TestUtilsV2.NAME, "part3");
        partitions.add(partition3);
        tableEntity.setAttribute("partitions", partitions);
        response = entityStore.createOrUpdate(tableEntityMap);
        tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableDefinition);

        //remove one of the struct values
        init();
        partitions.remove(1);
        tableEntity.setAttribute("partitions", partitions);
        response = entityStore.createOrUpdate(tableEntityMap);
        tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableDefinition);

        //Update struct value within array of struct
        init();
        partitions.get(0).setAttribute(TestUtilsV2.NAME, "part4");
        tableEntity.setAttribute("partitions", partitions);
        response = entityStore.createOrUpdate(tableEntityMap);
        tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableDefinition);


        //add a repeated element to array of struct
        init();
        final AtlasStruct partition4 = new AtlasStruct(TestUtils.PARTITION_STRUCT_TYPE);
        partition4.setAttribute(TestUtilsV2.NAME, "part4");
        partitions.add(partition4);
        tableEntity.setAttribute("partitions", partitions);
        response = entityStore.createOrUpdate(tableEntityMap);
        tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableDefinition);

        // Remove all elements. Should set array attribute to null
        init();
        partitions.clear();
        tableEntity.setAttribute("partitions", partitions);
        response = entityStore.createOrUpdate(tableEntityMap);
        tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableEntityMap, tableDefinition);
    }


    @Test(dependsOnMethods = "testCreate")
    public void testStructs() throws Exception {
        init();

        Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        AtlasEntity tableClone = new AtlasEntity(tableEntity);
        AtlasStruct serdeInstance = new AtlasStruct(TestUtils.SERDE_TYPE);
        serdeInstance.setAttribute(TestUtilsV2.NAME, "serde1Name");
        serdeInstance.setAttribute("serde", "test");
        serdeInstance.setAttribute("description", "testDesc");
        tableClone.setAttribute("serde1", serdeInstance);
        tableClone.setAttribute("database", new AtlasObjectId(dbEntity.getTypeName(), new HashMap<String, Object>() {{
            put(TestUtilsV2.NAME, dbEntity.getAttribute(TestUtilsV2.NAME));
        }}));

        tableCloneMap.put(tableClone.getGuid(), tableClone);

        EntityMutationResponse response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition1 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition1);

        //update struct attribute
        init();
        serdeInstance.setAttribute("serde", "testUpdated");
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition2 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        validateAttributes(tableCloneMap, tableDefinition2);

        //set to null
        init();
        tableClone.setAttribute("description", null);
        response = entityStore.createOrUpdate(tableCloneMap);
        AtlasEntityHeader tableDefinition3 = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        Assert.assertNull(tableDefinition3.getAttribute("description"));
        validateAttributes(tableCloneMap, tableDefinition3);
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

    private void validateMutationResponse(EntityMutationResponse response, EntityMutations.EntityOperation op, int expectedNumCreated) {
        List<AtlasEntityHeader> entitiesCreated = response.getEntitiesByOperation(op);
        Assert.assertNotNull(entitiesCreated);
        Assert.assertEquals(entitiesCreated.size(), expectedNumCreated);
    }

    private void validateAttributes(Map<String, AtlasEntity> entityMap, AtlasEntityHeader entity) throws AtlasBaseException, AtlasException {
        //TODO : Use the older API for get until new instance API is ready and validated
        ITypedReferenceableInstance instance = metadataService.getEntityDefinition(entity.getGuid());
        assertAttributes(entityMap, entity, instance);
    }

    private void assertAttributes(Map<String, AtlasEntity> entityMap, AtlasStruct entity, IInstance instance) throws AtlasBaseException, AtlasException {
        LOG.debug("Asserting type : " + entity.getTypeName());
        AtlasStructType entityType = (AtlasStructType) typeRegistry.getType(instance.getTypeName());
        for (String attrName : entity.getAttributes().keySet()) {
            Object actual = entity.getAttribute(attrName);
            Object expected = instance.get(attrName);

            AtlasType attrType = entityType.getAttributeType(attrName);
            assertAttribute(entityMap, actual, expected, attrType, attrName);
        }
    }

    private void assertAttribute(Map<String, AtlasEntity> actualEntityMap, Object actual, Object expected, AtlasType attributeType, String attrName) throws AtlasBaseException, AtlasException {
        LOG.debug("Asserting attribute : " + attrName);

        switch(attributeType.getTypeCategory()) {
        case ENTITY:
            if ( expected instanceof Id) {
                String guid = ((Id) expected)._getId();
                Assert.assertTrue(AtlasEntity.isAssigned(guid));
            } else {
                ReferenceableInstance expectedInstance = (ReferenceableInstance) expected;
                if (actual instanceof AtlasObjectId) {
                    AtlasEntity actualEntity = actualEntityMap.get(((AtlasObjectId) actual).getGuid());
                    if (actualEntity != null) {
                        assertAttributes(actualEntityMap, actualEntity, expectedInstance);
                    }
                } else {
                    AtlasEntity actualInstance = (AtlasEntity) actual;
                    if (actualInstance != null) {
                        assertAttributes(actualEntityMap , actualInstance, expectedInstance);
                    }
                }
            }
            break;
        case PRIMITIVE:
            Assert.assertEquals(actual, expected);
            break;
        case ENUM:
            EnumValue expectedEnumVal = (EnumValue) expected;
            if ( actual != null) {
                Assert.assertEquals(actual, expectedEnumVal.value);
            }
            break;
        case MAP:
            AtlasMapType mapType = (AtlasMapType) attributeType;
            AtlasType keyType = mapType.getKeyType();
            AtlasType valueType = mapType.getValueType();
            Map actualMap = (Map) actual;
            Map expectedMap = (Map) expected;

            if (expectedMap != null && actualMap != null) {
                Assert.assertEquals(actualMap.size(), expectedMap.size());
                for (Object key : actualMap.keySet()) {
                    assertAttribute(actualEntityMap, actualMap.get(key), expectedMap.get(key), valueType, attrName);
                }
            }
            break;
        case ARRAY:
            AtlasArrayType arrType = (AtlasArrayType) attributeType;
            AtlasType elemType = arrType.getElementType();
            List actualList = (List) actual;
            List expectedList = (List) expected;

            if (CollectionUtils.isNotEmpty(actualList)) {
                //actual list could have deleted entities . Hence size may not match.
                for (int i = 0; i < actualList.size(); i++) {
                    assertAttribute(actualEntityMap, actualList.get(i), expectedList.get(i), elemType, attrName);
                }
            }
            break;
        case STRUCT:
            StructInstance structInstance = (StructInstance) expected;
            AtlasStruct newStructVal = (AtlasStruct) actual;
            assertAttributes(actualEntityMap, newStructVal, structInstance);
            break;
        default:
            Assert.fail("Unknown type category");
        }
    }

    @Test(dependsOnMethods = "testCreate")
    public void testClassUpdate() throws Exception {

        init();
        //Create new db instance
        final Map<String, AtlasEntity> databaseInstance = TestUtilsV2.createDBEntity();

        EntityMutationResponse response = entityStore.createOrUpdate(databaseInstance);
        final AtlasEntityHeader dbCreated = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

        init();
        Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        AtlasEntity tableClone = new AtlasEntity(tableEntity);
        tableClone.setAttribute("database", new AtlasObjectId(TestUtils.DATABASE_TYPE, dbCreated.getGuid()));

        tableCloneMap.put(dbCreated.getGuid(), databaseInstance.values().iterator().next());
        tableCloneMap.put(tableClone.getGuid(), tableClone);

        response = entityStore.createOrUpdate(tableCloneMap);
        final AtlasEntityHeader tableDefinition = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.TABLE_TYPE);
        Assert.assertNotNull(tableDefinition.getAttribute("database"));
        Assert.assertEquals(((AtlasObjectId) tableDefinition.getAttribute("database")).getGuid(), dbCreated.getGuid());
    }

    @Test
    public void testCheckOptionalAttrValueRetention() throws Exception {

        Map<String, AtlasEntity> dbEntityMap = TestUtilsV2.createDBEntity();
        EntityMutationResponse response = entityStore.createOrUpdate(dbEntityMap);
        AtlasEntityHeader firstEntityCreated = response.getFirstCreatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

        //The optional boolean attribute should have a non-null value
        final String isReplicatedAttr = "isReplicated";
        final String paramsAttr = "parameters";
        Assert.assertNotNull(firstEntityCreated.getAttribute(isReplicatedAttr));
        Assert.assertEquals(firstEntityCreated.getAttribute(isReplicatedAttr), Boolean.FALSE);
        Assert.assertNull(firstEntityCreated.getAttribute(paramsAttr));

        //Update to true
        init();
        AtlasEntity dbEntity = dbEntityMap.values().iterator().next();
        dbEntity.setAttribute(isReplicatedAttr, Boolean.TRUE);
        //Update array
        final HashMap<String, String> params = new HashMap<String, String>() {{ put("param1", "val1"); put("param2", "val2"); }};
        dbEntity.setAttribute(paramsAttr, params);
        //Complete update

        dbEntityMap.put(dbEntity.getGuid(), dbEntity);
        response = entityStore.createOrUpdate(dbEntityMap);
        AtlasEntityHeader firstEntityUpdated = response.getFirstUpdatedEntityByTypeName(TestUtilsV2.DATABASE_TYPE);

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
        Map<String, AtlasEntity> entityCloneMap = new HashMap<>();

        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(strAttrName, randomStrWithReservedChars());
        entity.setAttribute(arrayAttrName, new String[]{randomStrWithReservedChars()});
        entity.setAttribute(mapAttrName, new HashMap<String, String>() {{
            put(randomStrWithReservedChars(), randomStrWithReservedChars());
        }});

        entityCloneMap.put(entity.getGuid(), entity);
        final EntityMutationResponse response = entityStore.createOrUpdate(entityCloneMap);
        final AtlasEntityHeader firstEntityCreated = response.getFirstEntityCreated();
        validateAttributes(entityCloneMap, firstEntityCreated);


        //Verify that search with reserved characters works - for string attribute
//        String query =
//            String.format("`%s` where `%s` = '%s'", typeName, strAttrName, entity.getAttribute(strAttrName));
//        String responseJson = discoveryService.searchByDSL(query, new QueryParams(1, 0));
//        JSONObject response = new JSONObject(responseJson);
//        assertEquals(response.getJSONArray("rows").length(), 1);
    }

    private String randomStrWithReservedChars() {
        return randomString() + "\"${}%";
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testCreateRequiredAttrNull() throws Exception {
        //Update required attribute
        Map<String, AtlasEntity> tableCloneMap = new HashMap<>();
        AtlasEntity tableEntity = new AtlasEntity(TestUtilsV2.TABLE_TYPE);
        tableEntity.setAttribute(TestUtilsV2.NAME, "table_" + TestUtils.randomString());
        tableCloneMap.put(tableEntity.getGuid(), tableEntity);

        entityStore.createOrUpdate(tableCloneMap);
        Assert.fail("Expected exception while creating with required attribute null");
    }
}
