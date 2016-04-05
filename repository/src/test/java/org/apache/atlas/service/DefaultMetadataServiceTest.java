/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.audit.HBaseBasedAuditRepository;
import org.apache.atlas.repository.audit.HBaseTestUtils;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtils;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = RepositoryMetadataModule.class)
public class DefaultMetadataServiceTest {
    @Inject
    private MetadataService metadataService;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private EntityAuditRepository repository;

    private Referenceable db = createDBEntity();

    private Id dbId;

    private Referenceable table;

    private Id tableId;
    
    private final String NAME = "name";
    private final String COLUMNS_ATTR_NAME = "columns";


    @BeforeTest
    public void setUp() throws Exception {
        if (repository instanceof HBaseBasedAuditRepository) {
            HBaseTestUtils.startCluster();
            ((HBaseBasedAuditRepository) repository).start();
        }
        RequestContext.createContext();
        RequestContext.get().setUser("testuser");

        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }

        String dbGUid = createInstance(db);
        dbId = new Id(dbGUid, 0, TestUtils.DATABASE_TYPE);

        table = createTableEntity(dbId);
        String tableGuid = createInstance(table);
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        table = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        tableId = new Id(tableGuid, 0, TestUtils.TABLE_TYPE);
    }

    @AfterTest
    public void shutdown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }

        if (repository instanceof HBaseBasedAuditRepository) {
            ((HBaseBasedAuditRepository) repository).stop();
            HBaseTestUtils.stopCluster();
        }
    }

    private String createInstance(Referenceable entity) throws Exception {
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.createEntities(entitiesJson.toString());
        JSONArray guids = new JSONArray(response);
        if (guids != null && guids.length() > 0) {
            return guids.getString(0);
        }
        return null;
    }

    private String updateInstance(Referenceable entity) throws Exception {
        ParamChecker.notNull(entity, "Entity");
        ParamChecker.notNull(entity.getId(), "Entity");
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.updateEntities(entitiesJson.toString());
        return new JSONArray(response).getString(0);
    }

    private Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(TestUtils.DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, dbName);
        entity.set("description", "us db");
        return entity;
    }

    private Referenceable createTableEntity(Id dbId) {
        Referenceable entity = new Referenceable(TestUtils.TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, tableName);
        entity.set("description", "random table");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", dbId);
        entity.set("created", new Date());
        return entity;
    }

    private Referenceable createColumnEntity() {
        Referenceable entity = new Referenceable(TestUtils.COLUMN_TYPE);
        entity.set(NAME, RandomStringUtils.randomAlphanumeric(10));
        entity.set("type", "VARCHAR(32)");
        return entity;
    }

    @Test(expectedExceptions = TypeNotFoundException.class)
    public void testCreateEntityWithUnknownDatatype() throws Exception {
        Referenceable entity = new Referenceable("Unknown datatype");
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, dbName);
        entity.set("description", "us db");
        createInstance(entity);
        Assert.fail(TypeNotFoundException.class.getSimpleName() + " was expected but none thrown.");
    }

    @Test
    public void testCreateEntityWithUniqueAttribute() throws Exception {
        //name is the unique attribute
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);
        assertAuditEvents(id, EntityAuditRepository.EntityAuditAction.ENTITY_CREATE);

        //using the same name should succeed, but not create another entity
        String newId = createInstance(entity);
        assertNull(newId);

        //Same entity, but different qualified name should succeed
        entity.set(NAME, TestUtils.randomString());
        newId = createInstance(entity);
        Assert.assertNotEquals(newId, id);
    }

    @Test
    public void testEntityAudit() throws Exception {
        //create entity
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);
        assertAuditEvents(id, EntityAuditRepository.EntityAuditAction.ENTITY_CREATE);

        Struct tag = new Struct(TestUtils.PII);
        metadataService.addTrait(id, InstanceSerialization.toJson(tag, true));
        assertAuditEvents(id, EntityAuditRepository.EntityAuditAction.TAG_ADD);

        metadataService.deleteTrait(id, TestUtils.PII);
        assertAuditEvents(id, EntityAuditRepository.EntityAuditAction.TAG_DELETE);

        metadataService.deleteEntities(Arrays.asList(id));
        assertAuditEvents(id, EntityAuditRepository.EntityAuditAction.ENTITY_DELETE);
    }

    private void assertAuditEvents(String id, EntityAuditRepository.EntityAuditAction action) throws Exception {
        List<EntityAuditRepository.EntityAuditEvent> events =
                repository.listEvents(id, System.currentTimeMillis(), (short) 10);
        for (EntityAuditRepository.EntityAuditEvent event : events) {
            if (event.getAction() == action) {
                return;
            }
        }
        fail("Didn't find " + action + " in audit events");
    }

    @Test
    public void testCreateEntityWithUniqueAttributeWithReference() throws Exception {
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable table = new Referenceable(TestUtils.TABLE_TYPE);
        table.set(NAME, TestUtils.randomString());
        table.set("description", "random table");
        table.set("type", "type");
        table.set("tableType", "MANAGED");
        table.set("database", new Id(dbId, 0, TestUtils.DATABASE_TYPE));
        table.set("databaseComposite", db);
        createInstance(table);

        //table create should re-use the db instance created earlier
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Referenceable actualDb = (Referenceable) tableDefinition.get("databaseComposite");
        Assert.assertEquals(actualDb.getId().id, dbId);
    }

    @Test
    public void testUpdateEntityByUniqueAttribute() throws Exception {
        final List<String> colNameList = ImmutableList.of("col1", "col2");
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", colNameList);
        }});
        metadataService.updateEntityByUniqueAttribute(table.getTypeName(), NAME, (String) table.get(NAME), tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<String> actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, colNameList);
    }

    @Test
    public void testUpdateEntityWithMap() throws Exception {

        final Map<String, Struct> partsMap = new HashMap<>();
        partsMap.put("part0", new Struct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(NAME, "test");
            }}));

        table.set("partitionsMap", partsMap);

        updateInstance(table);
        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertTrue(partsMap.get("part0").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0")));

        //update map - add a map key
        partsMap.put("part1", new Struct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(NAME, "test1");
            }}));
        table.set("partitionsMap", partsMap);

        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertTrue(partsMap.get("part1").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part1")));

        //update map - remove a key and add another key
        partsMap.remove("part0");
        partsMap.put("part2", new Struct(TestUtils.PARTITION_STRUCT_TYPE,
            new HashMap<String, Object>() {{
                put(NAME, "test2");
            }}));
        table.set("partitionsMap", partsMap);

        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part2")));

        //update struct value for existing map key
        Struct partition2 = (Struct)partsMap.get("part2");
        partition2.set(NAME, "test2Updated");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part2")));

        //Test map pointing to a class
        final Map<String, Struct> columnsMap = new HashMap<>();
        Referenceable col0Type = new Referenceable(TestUtils.COLUMN_TYPE,
            new HashMap<String, Object>() {{
                put(NAME, "test1");
                put("type", "string");
            }});

        columnsMap.put("col0", col0Type);

        Referenceable col1Type = new Referenceable(TestUtils.COLUMN_TYPE,
            new HashMap<String, Object>() {{
                put(NAME, "test2");
                put("type", "string");
            }});

        columnsMap.put("col1", col1Type);
        table.set(TestUtils.COLUMNS_MAP, columnsMap);
        updateInstance(table);
        verifyMapUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columnsMap, TestUtils.COLUMNS_MAP);

        //Swap elements
        columnsMap.clear();
        columnsMap.put("col0", col1Type);
        columnsMap.put("col1", col0Type);

        table.set(TestUtils.COLUMNS_MAP, columnsMap);
        updateInstance(table);
        verifyMapUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columnsMap, TestUtils.COLUMNS_MAP);

        //Drop the first key and change the class type as well to col0
        columnsMap.clear();
        columnsMap.put("col0", col0Type);

        table.set(TestUtils.COLUMNS_MAP, columnsMap);
        updateInstance(table);
        verifyMapUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columnsMap, TestUtils.COLUMNS_MAP);

        //Clear state
        table.setNull(TestUtils.COLUMNS_MAP);
        updateInstance(table);
        verifyMapUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), null, TestUtils.COLUMNS_MAP);
    }

    private void verifyMapUpdates(String typeName, String uniqAttrName, String uniqAttrValue, Map<String, Struct> expectedMap, String mapAttrName) throws AtlasException {
        String json =
            metadataService.getEntityDefinition(typeName, uniqAttrName, uniqAttrValue);
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(json, true);

        if(expectedMap == null) {
            Assert.assertNull(tableDefinition.get(TestUtils.COLUMNS_MAP));
        } else {
            Assert.assertEquals(((Map<String, Referenceable>)tableDefinition.get(mapAttrName)).size(), expectedMap.size());
            for (String key : expectedMap.keySet()) {
                Assert.assertTrue(((Map<String, Referenceable>) tableDefinition.get(mapAttrName)).get(key).equalsContents(expectedMap.get(key)));
            }
        }
    }

    @Test
    public void testUpdateEntityAddAndUpdateArrayAttr() throws Exception {
        //Update entity, add new array attribute
        //add array of primitives
        final List<String> colNameList = ImmutableList.of("col1", "col2");
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", colNameList);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<String> actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, colNameList);

        //update array of primitives
        final List<String> updatedColNameList = ImmutableList.of("col2", "col3");
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", updatedColNameList);
        }});
        metadataService.updateEntityPartialByGuid(tableId.getId()._getId(), tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, updatedColNameList);
    }

    @Test
    public void testUpdateEntityArrayOfClass() throws Exception {
        
        //test array of class with id
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put(NAME, "col1");
        values.put("type", "type");
        Referenceable col1 = new Referenceable(TestUtils.COLUMN_TYPE, values);
        columns.add(col1);
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put(COLUMNS_ATTR_NAME, columns);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columns, COLUMNS_ATTR_NAME);

        //Partial update. Add col5 But also update col1
        Map<String, Object> valuesCol5 = new HashMap<>();
        valuesCol5.put(NAME, "col5");
        valuesCol5.put("type", "type");
        Referenceable col2 = new Referenceable(TestUtils.COLUMN_TYPE, valuesCol5);
        //update col1
        col1.set("type", "type1");

        //add col5
        final List<Referenceable> updateColumns = Arrays.asList(col1, col2);

        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put(COLUMNS_ATTR_NAME, updateColumns);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), updateColumns, COLUMNS_ATTR_NAME);

        //Complete update. Add  array elements - col3,4
        Map<String, Object> values1 = new HashMap<>();
        values1.put(NAME, "col3");
        values1.put("type", "type");
        Referenceable ref1 = new Referenceable(TestUtils.COLUMN_TYPE, values1);
        columns.add(ref1);

        Map<String, Object> values2 = new HashMap<>();
        values2.put(NAME, "col4");
        values2.put("type", "type");
        Referenceable ref2 = new Referenceable(TestUtils.COLUMN_TYPE, values2);
        columns.add(ref2);

        table.set(COLUMNS_ATTR_NAME, columns);
        updateInstance(table);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columns, COLUMNS_ATTR_NAME);

        //Swap elements
        columns.clear();
        columns.add(ref2);
        columns.add(ref1);

        table.set(COLUMNS_ATTR_NAME, columns);
        updateInstance(table);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columns, COLUMNS_ATTR_NAME);

        //drop a single column
        columns.clear();
        columns.add(ref1);

        table.set(COLUMNS_ATTR_NAME, columns);
        updateInstance(table);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columns, COLUMNS_ATTR_NAME);

        //Remove a class reference/Id and insert another reference
        //Also covers isComposite case since columns is a composite
        values.clear();
        columns.clear();

        values.put(NAME, "col2");
        values.put("type", "type");
        col1 = new Referenceable(TestUtils.COLUMN_TYPE, values);
        columns.add(col1);
        table.set(COLUMNS_ATTR_NAME, columns);
        updateInstance(table);

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), columns, COLUMNS_ATTR_NAME);

        //Update array column to null
        table.setNull(COLUMNS_ATTR_NAME);
        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        verifyArrayUpdates(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME), null, COLUMNS_ATTR_NAME);
    }

    private void verifyArrayUpdates(String typeName, String uniqAttrName, String uniqAttrValue, List<? extends Struct> expectedArray, String arrAttrName) throws AtlasException {
        String json =
            metadataService.getEntityDefinition(typeName, uniqAttrName, uniqAttrValue);
        Referenceable entityDefinition = InstanceSerialization.fromJsonReferenceable(json, true);

        if (expectedArray == null) {
            Assert.assertNull(entityDefinition.get(arrAttrName));
        } else {
            Assert.assertEquals(((List<Referenceable>)entityDefinition.get(arrAttrName)).size(), expectedArray.size());
            for (int index = 0; index < expectedArray.size(); index++) {
                Assert.assertTrue(((List<Referenceable>) entityDefinition.get(arrAttrName)).get(index).equalsContents(expectedArray.get(index)));
            }
        }
    }

    private void assertReferenceables(Referenceable r1, Referenceable r2) {
        assertEquals(r1.getTypeName(), r2.getTypeName());
        assertTrue(r1.getTraits().equals(r2.getTraits()));
        for (String attr : r1.getValuesMap().keySet()) {
            assertTrue(r1.getValuesMap().get(attr).equals(r2.getValuesMap().get(attr)));
        }
        //TODO assert trait instances and complex attributes
    }

    @Test
    public void testStructs() throws Exception {
        Struct serdeInstance = new Struct(TestUtils.SERDE_TYPE);
        serdeInstance.set(NAME, "serde1Name");
        serdeInstance.set("serde", "test");
        serdeInstance.set("description", "testDesc");
        table.set("serde1", serdeInstance);

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNotNull(tableDefinition.get("serde1"));
        Assert.assertTrue(serdeInstance.equalsContents(tableDefinition.get("serde1")));

        //update struct attribute
        serdeInstance.set("serde", "testUpdated");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertTrue(serdeInstance.equalsContents(tableDefinition.get("serde1")));

        //set to null
        serdeInstance.setNull("description");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNull(((Struct) tableDefinition.get("serde1")).get("description"));
    }


    @Test
    public void testCreateEntityWithReferenceableHavingIdNoValue() throws Exception {
        //ATLAS-383 Test
        Referenceable sdReferenceable = new Referenceable(TestUtils.STORAGE_DESC_TYPE);
        sdReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, TestUtils.randomString());
        sdReferenceable.set("compressed", "false");
        sdReferenceable.set("location", "hdfs://tmp/hive-user");
        String sdGuid = createInstance(sdReferenceable);

        Referenceable sdRef2 = new Referenceable(sdGuid, TestUtils.STORAGE_DESC_TYPE, null);

        Referenceable partRef = new Referenceable(TestUtils.PARTITION_CLASS_TYPE);
        partRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "part-unique");
        partRef.set("values", ImmutableList.of("2014-10-01"));
        partRef.set("table", table);
        partRef.set("sd", sdRef2);

        String partGuid = createInstance(partRef);
        Assert.assertNotNull(partGuid);
    }

    @Test
    public void testClassUpdate() throws Exception {
        //Create new db instance
        final Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set(NAME, TestUtils.randomString());
        databaseInstance.set("description", "new database");

        String dbId = createInstance(databaseInstance);

        /*Update reference property with Id */
        metadataService.updateEntityAttributeByGuid(tableId._getId(), "database", dbId);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(dbId, (((Id)tableDefinition.get("database"))._getId()));

        /* Update with referenceable - TODO - Fails . Need to fix this */
        /*final String dbName = TestUtils.randomString();
        final Referenceable databaseInstance2 = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance2.set(NAME, dbName);
        databaseInstance2.set("description", "new database 2");

        Referenceable updateTable = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("database", databaseInstance2);
        }});
        metadataService.updateEntityAttributeByGuid(tableId._getId(), updateTable);

        tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        Referenceable tableDefinitionActual = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        String dbDefJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, NAME, dbName);
        Referenceable dbDef = InstanceSerialization.fromJsonReferenceable(dbDefJson, true);

        Assert.assertNotEquals(dbId, (((Id) tableDefinitionActual.get("database"))._getId()));
        Assert.assertEquals(dbDef.getId()._getId(), (((Id) tableDefinitionActual.get("database"))._getId())); */

    }

    @Test
    public void testArrayOfStructs() throws Exception {
        //Add array of structs
        TestUtils.dumpGraph(graphProvider.get());

        final Struct partition1 = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
        partition1.set(NAME, "part1");

        final Struct partition2 = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
        partition2.set(NAME, "part2");

        List<Struct> partitions = new ArrayList<Struct>(){{ add(partition1); add(partition2); }};
        table.set("partitions", partitions);

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        List<Struct> partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));

        //add a new element to array of struct
        final Struct partition3 = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
        partition3.set(NAME, "part3");
        partitions.add(partition3);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 3);
        Assert.assertTrue(partitions.get(2).equalsContents(partitionsActual.get(2)));

        //remove one of the struct values
        partitions.remove(1);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));
        Assert.assertTrue(partitions.get(1).equalsContents(partitionsActual.get(1)));

        //Update struct value within array of struct
        partition1.set(NAME, "part4");
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));

        //add a repeated element to array of struct
        final Struct partition4 = new Struct(TestUtils.PARTITION_STRUCT_TYPE);
        partition4.set(NAME, "part4");
        partitions.add(partition4);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 3);
        Assert.assertEquals(partitionsActual.get(2).get(NAME), "part4");
        Assert.assertEquals(partitionsActual.get(0).get(NAME), "part4");
        Assert.assertTrue(partitions.get(2).equalsContents(partitionsActual.get(2)));


        // Remove all elements. Should set array attribute to null
        partitions.clear();
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNull(tableDefinition.get("partitions"));
    }


    @Test(expectedExceptions = ValueConversionException.class)
    public void testUpdateRequiredAttrToNull() throws Exception {
        //Update required attribute
        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(tableDefinition.get("description"), "random table");
        table.setNull("description");

        updateInstance(table);
        Assert.fail("Expected exception while updating required attribute to null");
    }

    @Test
    public void testUpdateOptionalAttrToNull() throws Exception {

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        //Update optional Attribute
        Assert.assertNotNull(tableDefinition.get("created"));
        //Update optional attribute
        table.setNull("created");

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNull(tableDefinition.get("created"));
    }

    @Test
    public void testCreateEntityWithEnum() throws Exception {
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, NAME, (String) table.get(NAME));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        EnumValue tableType = (EnumValue) tableDefinition.get("tableType");

        Assert.assertEquals(tableType, new EnumValue("MANAGED", 1));
    }

    @Test
    public void testGetEntityByUniqueAttribute() throws Exception {
        Referenceable entity = createDBEntity();
        createInstance(entity);

        //get entity by valid qualified name
        String entityJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, NAME,
                (String) entity.get(NAME));
        Assert.assertNotNull(entityJson);
        Referenceable referenceable = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Assert.assertEquals(referenceable.get(NAME), entity.get(NAME));

        //get entity by invalid qualified name
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, NAME, "random");
            Assert.fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        //get entity by non-unique attribute
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "description",
                    (String) entity.get("description"));
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }
    
    @Test
    public void testDeleteEntities() throws Exception {
        // Create 2 table entities, each with 3 composite column entities
        Referenceable dbEntity = createDBEntity();
        String dbGuid = createInstance(dbEntity);
        Id dbId = new Id(dbGuid, 0, TestUtils.DATABASE_TYPE);
        Referenceable table1Entity = createTableEntity(dbId);
        Referenceable table2Entity = createTableEntity(dbId);
        Referenceable col1 = createColumnEntity();
        Referenceable col2 = createColumnEntity();
        Referenceable col3 = createColumnEntity();
        table1Entity.set(COLUMNS_ATTR_NAME, ImmutableList.of(col1, col2, col3));
        table2Entity.set(COLUMNS_ATTR_NAME, ImmutableList.of(col1, col2, col3));
        createInstance(table1Entity);
        createInstance(table2Entity);
        
        // Retrieve the table entities from the repository,
        // to get their guids and the composite column guids.
        String entityJson = metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, 
            NAME, (String)table1Entity.get(NAME));
        Assert.assertNotNull(entityJson);
        table1Entity = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Object val = table1Entity.get(COLUMNS_ATTR_NAME);
        Assert.assertTrue(val instanceof List);
        List<IReferenceableInstance> table1Columns = (List<IReferenceableInstance>) val;
        entityJson = metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, 
            NAME, (String)table2Entity.get(NAME));
        Assert.assertNotNull(entityJson);
        table2Entity = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        val = table2Entity.get(COLUMNS_ATTR_NAME);
        Assert.assertTrue(val instanceof List);
        List<IReferenceableInstance> table2Columns = (List<IReferenceableInstance>) val;

        // Register an EntityChangeListener to verify the notification mechanism
        // is working for deleteEntities().
        DeleteEntitiesChangeListener listener = new DeleteEntitiesChangeListener();
        metadataService.registerListener(listener);
        
        // Delete the table entities.  The deletion should cascade
        // to their composite columns.
        JSONArray deleteCandidateGuids = new JSONArray();
        deleteCandidateGuids.put(table1Entity.getId()._getId());
        deleteCandidateGuids.put(table2Entity.getId()._getId());
        List<String> deletedGuids = metadataService.deleteEntities(
            Arrays.asList(table1Entity.getId()._getId(), table2Entity.getId()._getId()));

        // Verify that deleteEntities() response has guids for tables and their composite columns. 
        Assert.assertTrue(deletedGuids.contains(table1Entity.getId()._getId()));
        Assert.assertTrue(deletedGuids.contains(table2Entity.getId()._getId()));
        for (IReferenceableInstance column : table1Columns) {
            Assert.assertTrue(deletedGuids.contains(column.getId()._getId()));
        }
        for (IReferenceableInstance column : table2Columns) {
            Assert.assertTrue(deletedGuids.contains(column.getId()._getId()));
        }
        
        // Verify that tables and their composite columns have been deleted from the repository.
        for (String guid : deletedGuids) {
            try {
                metadataService.getEntityDefinition(guid);
                Assert.fail(EntityNotFoundException.class.getSimpleName() + 
                    " expected but not thrown.  The entity with guid " + guid + 
                    " still exists in the repository after being deleted." );
            }
            catch(EntityNotFoundException e) {
                // The entity does not exist in the repository, so deletion was successful.
            }
        }
        
        // Verify that the listener was notified about the deleted entities.
        Collection<ITypedReferenceableInstance> deletedEntitiesFromListener = listener.getDeletedEntities();
        Assert.assertNotNull(deletedEntitiesFromListener);
        Assert.assertEquals(deletedEntitiesFromListener.size(), deletedGuids.size());
        List<String> deletedGuidsFromListener = new ArrayList<>(deletedGuids.size());
        for (ITypedReferenceableInstance deletedEntity : deletedEntitiesFromListener) {
            deletedGuidsFromListener.add(deletedEntity.getId()._getId());
        }
        Assert.assertEquals(deletedGuidsFromListener.size(), deletedGuids.size());
        Assert.assertTrue(deletedGuidsFromListener.containsAll(deletedGuids));
    }

    @Test
    public void testDeleteEntityByUniqueAttribute() throws Exception {
        // Create 2 table entities, each with 3 composite column entities
        Referenceable dbEntity = createDBEntity();
        String dbGuid = createInstance(dbEntity);
        Id dbId = new Id(dbGuid, 0, TestUtils.DATABASE_TYPE);
        Referenceable table1Entity = createTableEntity(dbId);
        Referenceable col1 = createColumnEntity();
        Referenceable col2 = createColumnEntity();
        Referenceable col3 = createColumnEntity();
        table1Entity.set(COLUMNS_ATTR_NAME, ImmutableList.of(col1, col2, col3));
        createInstance(table1Entity);

        // to get their guids and the composite column guids.
        String entityJson = metadataService.getEntityDefinition(TestUtils.TABLE_TYPE,
            NAME, (String)table1Entity.get(NAME));
        Assert.assertNotNull(entityJson);
        table1Entity = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Object val = table1Entity.get(COLUMNS_ATTR_NAME);
        Assert.assertTrue(val instanceof List);
        List<IReferenceableInstance> table1Columns = (List<IReferenceableInstance>) val;

        // Register an EntityChangeListener to verify the notification mechanism
        // is working for deleteEntityByUniqueAttribute().
        DeleteEntitiesChangeListener listener = new DeleteEntitiesChangeListener();
        metadataService.registerListener(listener);

        // Delete the table entities.  The deletion should cascade
        // to their composite columns.
        List<String> deletedGuids = metadataService.deleteEntityByUniqueAttribute(TestUtils.TABLE_TYPE, NAME, (String) table1Entity.get(NAME));

        // Verify that deleteEntities() response has guids for tables and their composite columns.
        Assert.assertTrue(deletedGuids.contains(table1Entity.getId()._getId()));
        for (IReferenceableInstance column : table1Columns) {
            Assert.assertTrue(deletedGuids.contains(column.getId()._getId()));
        }

        // Verify that tables and their composite columns have been deleted from the repository.
        for (String guid : deletedGuids) {
            try {
                metadataService.getEntityDefinition(guid);
                Assert.fail(EntityNotFoundException.class.getSimpleName() +
                    " expected but not thrown.  The entity with guid " + guid +
                    " still exists in the repository after being deleted." );
            }
            catch(EntityNotFoundException e) {
                // The entity does not exist in the repository, so deletion was successful.
            }
        }

        // Verify that the listener was notified about the deleted entities.
        Collection<ITypedReferenceableInstance> deletedEntitiesFromListener = listener.getDeletedEntities();
        Assert.assertNotNull(deletedEntitiesFromListener);
        Assert.assertEquals(deletedEntitiesFromListener.size(), deletedGuids.size());
        List<String> deletedGuidsFromListener = new ArrayList<>(deletedGuids.size());
        for (ITypedReferenceableInstance deletedEntity : deletedEntitiesFromListener) {
            deletedGuidsFromListener.add(deletedEntity.getId()._getId());
        }
        Assert.assertEquals(deletedGuidsFromListener.size(), deletedGuids.size());
        Assert.assertTrue(deletedGuidsFromListener.containsAll(deletedGuids));
    }

    @Test
    public void testTypeUpdateWithReservedAttributes() throws AtlasException, JSONException {
        String typeName = "test_type_"+ RandomStringUtils.randomAlphanumeric(10);
        HierarchicalTypeDefinition<ClassType> typeDef = TypesUtil.createClassTypeDef(
                typeName, ImmutableSet.<String>of(),
                TypesUtil.createUniqueRequiredAttrDef("test_type_attribute", DataTypes.STRING_TYPE));
        TypesDef typesDef = new TypesDef(typeDef, false);
        JSONObject type = metadataService.createType(TypesSerialization.toJson(typesDef));
        Assert.assertNotNull(type.get(AtlasClient.TYPES));

        HierarchicalTypeDefinition<ClassType> updatedTypeDef = TypesUtil.createClassTypeDef(
            typeName, ImmutableSet.<String>of(),
            TypesUtil.createUniqueRequiredAttrDef("test_type_attribute", DataTypes.STRING_TYPE),
            TypesUtil.createOptionalAttrDef("test_type_invalid_attribute$", DataTypes.STRING_TYPE));
        TypesDef updatedTypesDef = new TypesDef(updatedTypeDef, false);

        try {
            metadataService.updateType(TypesSerialization.toJson(updatedTypesDef));
            Assert.fail("Should not be able to update type with reserved character");
        } catch (AtlasException ae) {
            // pass.. expected
        }
        String typeDefinition = metadataService.getTypeDefinition(typeName);
        Assert.assertNotNull(typeDefinition);
    }

    private static class DeleteEntitiesChangeListener implements EntityChangeListener {
        
        private Collection<ITypedReferenceableInstance> deletedEntities_;
        
        @Override
        public void onEntitiesAdded(Collection<ITypedReferenceableInstance> entities)
            throws AtlasException {
        }

        @Override
        public void onEntitiesUpdated(Collection<ITypedReferenceableInstance> entities)
            throws AtlasException {
        }

        @Override
        public void onTraitAdded(ITypedReferenceableInstance entity, IStruct trait)
            throws AtlasException {
        }

        @Override
        public void onTraitDeleted(ITypedReferenceableInstance entity, String traitName)
            throws AtlasException {
        }

        @Override
        public void onEntitiesDeleted(Collection<ITypedReferenceableInstance> entities)
            throws AtlasException {
            deletedEntities_ = entities;
        }
        
        public Collection<ITypedReferenceableInstance> getDeletedEntities() {
            return deletedEntities_;
        }
    }
}
