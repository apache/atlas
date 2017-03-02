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

package org.apache.atlas.web.resources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.model.instance.GuidMapping;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.InstanceSerialization$;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;


/**
 * Integration tests for Entity Jersey Resource.
 */
@Guice(modules = NotificationModule.class)
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityJerseyResourceIT.class);

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    private static final String TRAITS = "traits";
    private Referenceable tableInstance;
    private Id tableId;
    private Id dbId;
    private String traitName;

    @Inject
    private NotificationInterface notificationInterface;
    private NotificationConsumer<EntityNotification> notificationConsumer;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitionsV1();
        Referenceable HiveDBInstance = createHiveDBInstanceBuiltIn(DATABASE_NAME);
        dbId = createInstance(HiveDBInstance);

        List<NotificationConsumer<EntityNotification>> consumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

        notificationConsumer = consumers.iterator().next();
    }

    @Test
    public void testCreateNestedEntities() throws Exception {

        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", "db1");
        databaseInstance.set("description", "foo database");

        int nTables = 5;
        int colsPerTable=3;
        List<Referenceable> tables = new ArrayList<>();
        List<Referenceable> allColumns = new ArrayList<>();

        for(int i = 0; i < nTables; i++) {
            String tableName = "db1-table-" + i;

            Referenceable tableInstance =
                    new Referenceable(HIVE_TABLE_TYPE);
            tableInstance.set("name", tableName);
            tableInstance.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
            tableInstance.set("db", databaseInstance);
            tableInstance.set("description", tableName + " table");
            tables.add(tableInstance);

            List<Referenceable> columns = new ArrayList<>();
            for(int j = 0; j < colsPerTable; j++) {
                Referenceable columnInstance = new Referenceable(COLUMN_TYPE);
                columnInstance.set("name", tableName + "-col-" + j);
                columnInstance.set("dataType", "String");
                columnInstance.set("comment", "column " + j + " for table " + i);
                allColumns.add(columnInstance);
                columns.add(columnInstance);
            }
            tableInstance.set("columns", columns);
        }

        //Create the tables.  The database and columns should be created automatically, since
        //the tables reference them.
        JSONArray entityArray = new JSONArray(tables.size());
        for(int i = 0; i < tables.size(); i++) {
            Referenceable table = tables.get(i);
            entityArray.put(InstanceSerialization.toJson(table, true));
        }
        String json = entityArray.toString();

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.CREATE_ENTITY, json);

        GuidMapping guidMapping = AtlasType.fromJson(response.toString(), GuidMapping.class);

        Map<String,String> guidsCreated = guidMapping.getGuidAssignments();
        assertEquals(guidsCreated.size(), nTables * colsPerTable + nTables + 1);
        assertNotNull(guidsCreated.get(databaseInstance.getId()._getId()));
        for(Referenceable r : allColumns) {
            assertNotNull(guidsCreated.get(r.getId()._getId()));
        }
        for(Referenceable r : tables) {
            assertNotNull(guidsCreated.get(r.getId()._getId()));
        }
    }


    @Test
    public void testSubmitEntity() throws Exception {
        tableInstance = createHiveTableInstanceBuiltIn(DATABASE_NAME, TABLE_NAME, dbId);
        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();
        try {
            Assert.assertNotNull(UUID.fromString(guid));
        } catch (IllegalArgumentException e) {
            Assert.fail("Response is not a guid, " + guid);
        }
    }

    @Test
    public void testRequestUser() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        entity.set("name", dbName);
        entity.set(QUALIFIED_NAME, dbName);
        entity.set("clusterName", randomString());
        entity.set("description", randomString());
        entity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        entity.set("owner", "user1");
        entity.set("clusterName", "cl1");
        entity.set("parameters", Collections.EMPTY_MAP);
        entity.set("location", "/tmp");


        String user = "admin";
        AtlasClient localClient = null;
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            localClient = new AtlasClient(atlasUrls, new String[]{"admin", "admin"});
        } else {
            localClient = new AtlasClient(atlasUrls);
        }
        String entityId = localClient.createEntity(entity).get(0);

        List<EntityAuditEvent> events = atlasClientV1.getEntityAuditEvents(entityId, (short) 10);
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).getUser(), user);
    }

    @Test
    //API should accept single entity (or jsonarray of entities)
    public void testSubmitSingleEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        databaseInstance.set("name", dbName);
        databaseInstance.set(QUALIFIED_NAME, dbName);
        databaseInstance.set("clusterName", randomString());
        databaseInstance.set("description", randomString());
        databaseInstance.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        databaseInstance.set("owner", "user1");
        databaseInstance.set("clusterName", "cl1");
        databaseInstance.set("parameters", Collections.EMPTY_MAP);
        databaseInstance.set("location", "/tmp");

        JSONObject response = atlasClientV1
                .callAPIWithBody(AtlasClient.API.CREATE_ENTITY, InstanceSerialization.toJson(databaseInstance, true));
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        AtlasClient.EntityResult entityResult = AtlasClient.EntityResult.fromString(response.toString());
        assertEquals(entityResult.getCreatedEntities().size(), 1);
        assertNotNull(entityResult.getCreatedEntities().get(0));
    }

    @Test
    public void testEntityDeduping() throws Exception {
        final Referenceable db = new Referenceable(DATABASE_TYPE_BUILTIN);
        final String dbName = "db" + randomString();
        Referenceable HiveDBInstance = createHiveDBInstanceBuiltIn(dbName);
        Id dbIdReference = createInstance(HiveDBInstance);
        final String dbId = dbIdReference._getId();

        assertEntityAudit(dbId, EntityAuditEvent.EntityAuditAction.ENTITY_CREATE);

        waitForNotification(notificationConsumer, MAX_WAIT_TIME, new NotificationPredicate() {
            @Override
            public boolean evaluate(EntityNotification notification) throws Exception {
                return notification != null && notification.getEntity().getId()._getId().equals(dbId);
            }
        });

        JSONArray results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE_BUILTIN, dbName));
        assertEquals(results.length(), 1);

        //create entity again shouldn't create another instance with same unique attribute value
        List<String> entityResults = atlasClientV1.createEntity(HiveDBInstance);
        assertEquals(entityResults.size(), 0);
        try {
            waitForNotification(notificationConsumer, MAX_WAIT_TIME, new NotificationPredicate() {
                @Override
                public boolean evaluate(EntityNotification notification) throws Exception {
                    return notification != null && notification.getEntity().getId()._getId().equals(dbId);
                }
            });
        } catch (Exception e) {
            //expected timeout
        }

        results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE_BUILTIN, dbName));
        assertEquals(results.length(), 1);

        //Test the same across references
        Referenceable table = new Referenceable(HIVE_TABLE_TYPE_BUILTIN);
        final String tableName = randomString();
        Referenceable tableInstance = createHiveTableInstanceBuiltIn(DATABASE_NAME, tableName, dbIdReference);
        atlasClientV1.createEntity(tableInstance);
        results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE_BUILTIN, dbName));
        assertEquals(results.length(), 1);
    }

    private void assertEntityAudit(String dbid, EntityAuditEvent.EntityAuditAction auditAction)
            throws Exception {
        List<EntityAuditEvent> events = atlasClientV1.getEntityAuditEvents(dbid, (short) 100);
        for (EntityAuditEvent event : events) {
            if (event.getAction() == auditAction) {
                return;
            }
        }
        fail("Expected audit event with action = " + auditAction);
    }

    @Test
    public void testEntityDefinitionAcrossTypeUpdate() throws Exception {
        //create type
        HierarchicalTypeDefinition<ClassType> typeDefinition = TypesUtil
                .createClassTypeDef(randomString(), ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        atlasClientV1.createType(TypesSerialization.toJson(typeDefinition, false));

        //create entity for the type
        Referenceable instance = new Referenceable(typeDefinition.typeName);
        instance.set("name", randomString());
        String guid = atlasClientV1.createEntity(instance).get(0);

        //update type - add attribute
        typeDefinition = TypesUtil.createClassTypeDef(typeDefinition.typeName, ImmutableSet.<String>of(),
                TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE));
        TypesDef typeDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(typeDefinition));
        atlasClientV1.updateType(typeDef);

        //Get definition after type update - new attributes should be null
        Referenceable entity = atlasClientV1.getEntity(guid);
        Assert.assertNull(entity.get("description"));
        Assert.assertEquals(entity.get("name"), instance.get("name"));
    }

    @DataProvider
    public Object[][] invalidAttrValues() {
        return new Object[][]{{null}, {""}};
    }

    @Test(dataProvider = "invalidAttrValues")
    public void testEntityInvalidValue(String value) throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE_BUILTIN);
        databaseInstance.set("name", randomString());
        databaseInstance.set("description", value);

        try {
            createInstance(databaseInstance);
            Assert.fail("Expected AtlasServiceException");
        } catch (AtlasServiceException e) {
            Assert.assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST);
        }
    }

    @Test
    public void testGetEntityByAttribute() throws Exception {
        Referenceable db1 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        db1.set(NAME, dbName);
        db1.set(DESCRIPTION, randomString());
        db1.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        db1.set("owner", "user1");
        db1.set(CLUSTER_NAME, "cl1");
        db1.set("parameters", Collections.EMPTY_MAP);
        db1.set("location", "/tmp");
        createInstance(db1);

        //get entity by attribute
        Referenceable referenceable = atlasClientV1.getEntity(DATABASE_TYPE_BUILTIN, QUALIFIED_NAME, dbName);
        Assert.assertEquals(referenceable.getTypeName(), DATABASE_TYPE_BUILTIN);
        Assert.assertEquals(referenceable.get(QUALIFIED_NAME), dbName);
    }

    @Test
    public void testSubmitEntityWithBadDateFormat() throws Exception {
        try {
            Referenceable tableInstance = createHiveTableInstanceBuiltIn("db" + randomString(), "table" + randomString(), dbId);
            tableInstance.set("lastAccessTime", "2014-07-11");
            tableId = createInstance(tableInstance);
            Assert.fail("Was expecting an  exception here ");
        } catch (AtlasServiceException e) {
            Assert.assertTrue(
                    e.getMessage().contains("\"error\":\"Cannot convert value '2014-07-11' to datatype date\""));
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddProperty() throws Exception {
        final String guid = tableId._getId();
        //add property
        String description = "bar table - new desc";
        addProperty(guid, "description", description);

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, guid);
        Assert.assertNotNull(response);

        tableInstance.set("description", description);

        //invalid property for the type
        try {
            addProperty(guid, "invalid_property", "bar table");
            Assert.fail("Expected AtlasServiceException");
        } catch (AtlasServiceException e) {
            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
        }

        String currentTime = String.valueOf(new DateTime());

        // updating date attribute as string not supported in v2
        // addProperty(guid, "createTime", currentTime);

        response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, guid);
        Assert.assertNotNull(response);

        tableInstance.set("createTime", currentTime);
    }

    @Test(dependsOnMethods = "testSubmitEntity", expectedExceptions = IllegalArgumentException.class)
    public void testAddNullProperty() throws Exception {
        final String guid = tableId._getId();
        //add property
        addProperty(guid, null, "foo bar");
        Assert.fail();
    }

    @Test(enabled = false)
    public void testAddNullPropertyValue() throws Exception {
        final String guid = tableId._getId();
        //add property
        try {
            addProperty(guid, "description", null);
            Assert.fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddReferenceProperty() throws Exception {
        //Create new db instance
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        databaseInstance.set(NAME, dbName);
        databaseInstance.set(QUALIFIED_NAME, dbName);
        databaseInstance.set(CLUSTER_NAME, randomString());
        databaseInstance.set("description", "new database");
        databaseInstance.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        databaseInstance.set("owner", "user1");
        databaseInstance.set(CLUSTER_NAME, "cl1");
        databaseInstance.set("parameters", Collections.EMPTY_MAP);
        databaseInstance.set("location", "/tmp");

        Id dbInstance = createInstance(databaseInstance);
        String dbId = dbInstance._getId();

        //Add reference property
        final String guid = tableId._getId();
        addProperty(guid, "db", dbId);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        final String guid = tableId._getId();
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, guid);

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final String definition = response.getString(AtlasClient.DEFINITION);
        Assert.assertNotNull(definition);
        LOG.debug("tableInstanceAfterGet = {}", definition);
        InstanceSerialization.fromJsonReferenceable(definition, true);
    }

    private void addProperty(String guid, String property, String value) throws AtlasServiceException {
        AtlasClient.EntityResult entityResult = atlasClientV1.updateEntityAttribute(guid, property, value);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertEquals(entityResult.getUpdateEntities().get(0), guid);
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testGetInvalidEntityDefinition() throws Exception {

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, "blah");

        Assert.assertNotNull(response);

        Assert.assertNotNull(response.get(AtlasClient.ERROR));
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() throws Exception {
        List<String> entities = atlasClientV1.listEntities(HIVE_TABLE_TYPE_BUILTIN);
        Assert.assertNotNull(entities);
        Assert.assertTrue(entities.contains(tableId._getId()));
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testGetEntityListForBadEntityType() throws Exception {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("type", "blah");

        JSONObject response = atlasClientV1.callAPIWithQueryParams(AtlasClient.API.GET_ENTITY, queryParams);
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.ERROR));
    }


    @Test
    public void testGetEntityListForNoInstances() throws Exception {
        String typeName = addNewType();

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("type", typeName);

        JSONObject response = atlasClientV1.callAPIWithQueryParams(AtlasClient.API.GET_ENTITY, queryParams);
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(list.length(), 0);
    }

    private String addNewType() throws Exception {
        String typeName = "test" + randomString();
        HierarchicalTypeDefinition<ClassType> testTypeDefinition = TypesUtil
                .createClassTypeDef(typeName, ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        String typesAsJSON = TypesSerialization.toJson(testTypeDefinition, false);
        createType(typesAsJSON);
        return typeName;
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitNames() throws Exception {
        final String guid = tableId._getId();

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.LIST_TRAITS, null, guid, TRAITS);
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(list.length(), 7);
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = {}", traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = {}", traitInstanceAsJSON);

        final String guid = tableId._getId();
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.ADD_TRAITS, traitInstanceAsJSON, guid, TRAITS);
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        assertEntityAudit(guid, EntityAuditEvent.EntityAuditAction.TAG_ADD);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testgetTraitDefinitionForEntity() throws Exception{
        traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = {}", traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = {}", traitInstanceAsJSON);

        final String guid = tableId._getId();
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.ADD_TRAITS, traitInstanceAsJSON, guid, TRAITS);
        assertNotNull(response);
        Struct traitDef = atlasClientV1.getTraitDefinition(guid, traitName);
        System.out.println(traitDef.toString());
        JSONObject responseAsJSON = new JSONObject(InstanceSerialization.toJson(traitDef, true));
        Assert.assertEquals(responseAsJSON.get("typeName"), traitName);


        List<Struct> allTraitDefs = atlasClientV1.listTraitDefinitions(guid);
        System.out.println(allTraitDefs.toString());
        Assert.assertEquals(allTraitDefs.size(), 9);
    }

    @Test(dependsOnMethods = "testAddTrait", expectedExceptions = AtlasServiceException.class)
    public void testAddExistingTrait() throws Exception {
        final String traitName = "PII_Trait" + randomString();

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = {}", traitInstanceAsJSON);

        final String guid = tableId._getId();
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.ADD_TRAITS, traitInstanceAsJSON, guid, TRAITS);
        assertNotNull(response);
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTraitWithAttribute() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil
                .createTraitTypeDef(traitName, ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = {}", traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        traitInstance.set("type", "SSN");
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = {}", traitInstanceAsJSON);

        final String guid = tableId._getId();
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.ADD_TRAITS, traitInstanceAsJSON, guid, TRAITS);
        assertNotNull(response);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        // verify the response
        response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, guid);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final String definition = response.getString(AtlasClient.DEFINITION);
        Assert.assertNotNull(definition);
        Referenceable entityRef = InstanceSerialization.fromJsonReferenceable(definition, true);
        IStruct traitRef = entityRef.getTrait(traitName);
        String type = (String) traitRef.get("type");
        Assert.assertEquals(type, "SSN");
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testAddTraitWithNoRegistration() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = {}", traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization$.MODULE$.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = {}", traitInstanceAsJSON);

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.CREATE_ENTITY, traitInstanceAsJSON, "random", TRAITS);
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = tableId._getId();

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.DELETE_TRAITS, null, guid, TRAITS, traitName);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get("traitName"));
        assertEntityAudit(guid, EntityAuditEvent.EntityAuditAction.TAG_DELETE);
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testDeleteTraitNonExistent() throws Exception {
        final String traitName = "blah_trait";
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.DELETE_TRAITS, null, "random", TRAITS);

        Assert.assertNotNull(response.get(AtlasClient.ERROR));
        Assert.assertEquals(response.getString(AtlasClient.ERROR),
                "trait=" + traitName + " should be defined in type system before it can be deleted");
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testDeleteExistentTraitNonExistentForEntity() throws Exception {

        final String guid = tableId._getId();
        final String traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil
                .createTraitTypeDef(traitName, ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        createType(traitDefinitionAsJSON);

        try {
            JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.DELETE_TRAITS, null, guid, TRAITS, traitName);
            fail("Call should've failed for deletion of invalid trait");
        } catch (AtlasServiceException e) {
            assertNotNull(e);
            assertNotNull(e.getStatus());
            assertEquals(e.getStatus(), ClientResponse.Status.NOT_FOUND);
        }
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTF8() throws Exception {
        //Type names cannot be arbitrary UTF8 characters. See org.apache.atlas.type.AtlasTypeUtil#validateType()
        String classType = randomString();
        String attrName = random();
        String attrValue = random();

        HierarchicalTypeDefinition<ClassType> classTypeDefinition = TypesUtil
                .createClassTypeDef(classType, ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(attrName, DataTypes.STRING_TYPE));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(classTypeDefinition));
        createType(typesDef);

        Referenceable instance = new Referenceable(classType);
        instance.set(attrName, attrValue);
        Id guid = createInstance(instance);

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, guid._getId());
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(response.getString(AtlasClient.DEFINITION), true);
        Assert.assertEquals(getReferenceable.get(attrName), attrValue);
    }


    @Test(dependsOnMethods = "testSubmitEntity")
    public void testPartialUpdate() throws Exception {
        String colName = "col1"+randomString();
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put(NAME, colName);
        values.put("comment", "col1 comment");
        values.put(QUALIFIED_NAME, "default.table.col1@"+colName);
        values.put("comment", "col1 comment");
        values.put("type", "string");
        values.put("owner", "user1");
        values.put("position", 0);
        values.put("description", "col1");
        values.put("table", tableId ); //table is a required reference, can't be null

        Referenceable ref = new Referenceable(BaseResourceIT.COLUMN_TYPE_BUILTIN, values);
        columns.add(ref);
        Referenceable tableUpdated = new Referenceable(BaseResourceIT.HIVE_TABLE_TYPE_BUILTIN, new HashMap<String, Object>() {{
            put("columns", columns);
        }});

        LOG.debug("Updating entity= {}", tableUpdated);
        AtlasClient.EntityResult entityResult = atlasClientV1.updateEntity(tableId._getId(), tableUpdated);
        assertEquals(entityResult.getUpdateEntities().size(), 2);
        assertEquals(entityResult.getUpdateEntities().get(0), tableId._getId());

        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, tableId._getId());
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(response.getString(AtlasClient.DEFINITION), true);
        List<Referenceable> refs = (List<Referenceable>) getReferenceable.get("columns");

        Assert.assertTrue(refs.get(0).equalsContents(columns.get(0)));

        //Update by unique attribute
        values.put("type", "int");
        ref = new Referenceable(BaseResourceIT.COLUMN_TYPE_BUILTIN, values);
        columns.set(0, ref);
        tableUpdated = new Referenceable(BaseResourceIT.HIVE_TABLE_TYPE_BUILTIN, new HashMap<String, Object>() {{
            put("columns", columns);
        }});

        LOG.debug("Updating entity= {}", tableUpdated);
        entityResult = atlasClientV1.updateEntity(BaseResourceIT.HIVE_TABLE_TYPE_BUILTIN, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                (String) tableInstance.get(QUALIFIED_NAME), tableUpdated);
        assertEquals(entityResult.getUpdateEntities().size(), 2);
        assertEquals(entityResult.getUpdateEntities().get(1), tableId._getId());

        response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, tableId._getId());
        getReferenceable = InstanceSerialization.fromJsonReferenceable(response.getString(AtlasClient.DEFINITION), true);
        refs = (List<Referenceable>) getReferenceable.get("columns");

        Assert.assertTrue(refs.get(0).getValuesMap().equals(values));
        Assert.assertEquals(refs.get(0).get("type"), "int");
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testCompleteUpdate() throws Exception {
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values1 = new HashMap<>();
        values1.put(NAME, "col3");
        values1.put(QUALIFIED_NAME, "default.table.col3@cl1");
        values1.put("comment", "col3 comment");
        values1.put("type", "string");
        values1.put("owner", "user1");
        values1.put("position", 0);
        values1.put("description", "col3");
        values1.put("table", tableId);


        Map<String, Object> values2 = new HashMap<>();
        values2.put(NAME, "col4");
        values2.put(QUALIFIED_NAME, "default.table.col4@cl1");
        values2.put("comment", "col4 comment");
        values2.put("type", "string");
        values2.put("owner", "user2");
        values2.put("position", 1);
        values2.put("description", "col4");
        values2.put("table", tableId);

        Referenceable ref1 = new Referenceable(BaseResourceIT.COLUMN_TYPE_BUILTIN, values1);
        Referenceable ref2 = new Referenceable(BaseResourceIT.COLUMN_TYPE_BUILTIN, values2);
        columns.add(ref1);
        columns.add(ref2);
        tableInstance.set("columns", columns);
        String entityJson = InstanceSerialization.toJson(tableInstance, true);
        JSONArray entityArray = new JSONArray(1);
        entityArray.put(entityJson);
        LOG.debug("Replacing entity= {}", tableInstance);

        JSONObject response = atlasClientV1.callAPIWithBody(AtlasClient.API.UPDATE_ENTITY, entityArray);

        // ATLAS-586: verify response entity can be parsed by GSON.
        Gson gson = new Gson();
        try {
            UpdateEntitiesResponse updateEntitiesResponse = gson.fromJson(response.toString(), UpdateEntitiesResponse.class);
        }
        catch (JsonSyntaxException e) {
            Assert.fail("Response entity from not parse-able by GSON", e);
        }

        response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.GET_ENTITY, null, tableId._getId());
        LOG.info("Response = {}", response.toString());
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(response.getString(AtlasClient.DEFINITION), true);
        List<Referenceable> refs = (List<Referenceable>) getReferenceable.get("columns");
        Assert.assertEquals(refs.size(), 2);

        Assert.assertTrue(refs.get(0).getValuesMap().equals(values1));
        Assert.assertTrue(refs.get(1).getValuesMap().equals(values2));
    }

    private static class UpdateEntitiesResponse {
        String requestId;
        AtlasClient.EntityResult entities;
        AtlasEntity definition;
    }

    private static class AtlasEntity {
        String typeName;
        final Map<String, Object> values = new HashMap<>();
    }

    @Test
    public void testDeleteEntitiesViaRestApi() throws Exception {
        // Create 2 database entities
        Referenceable db1 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        db1.set(NAME, dbName);
        db1.set(DESCRIPTION, randomString());
        db1.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        db1.set("owner", "user1");
        db1.set(CLUSTER_NAME, "cl1");
        db1.set("parameters", Collections.EMPTY_MAP);
        db1.set("location", "/tmp");
        Id db1Id = createInstance(db1);

        Referenceable db2 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName2 = randomString();
        db2.set(NAME, dbName2);
        db2.set(QUALIFIED_NAME, dbName2);
        db2.set(CLUSTER_NAME, randomString());
        db2.set(DESCRIPTION, randomString());
        db2.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName2);
        db2.set("owner", "user2");
        db2.set(CLUSTER_NAME, "cl1");
        db2.set("parameters", Collections.EMPTY_MAP);
        db2.set("location", "/tmp");
        Id db2Id = createInstance(db2);

        // Delete the database entities
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(AtlasClient.GUID.toLowerCase(), db1Id._getId());
        queryParams.add(AtlasClient.GUID.toLowerCase(), db2Id._getId());

        JSONObject response = atlasClientV1.callAPIWithQueryParams(AtlasClient.API.DELETE_ENTITIES, queryParams);
        List<String> deletedGuidsList = AtlasClient.EntityResult.fromString(response.toString()).getDeletedEntities();
        Assert.assertTrue(deletedGuidsList.contains(db1Id._getId()));
        Assert.assertTrue(deletedGuidsList.contains(db2Id._getId()));

        // Verify entities were deleted from the repository.
        for (String guid : deletedGuidsList) {
            Referenceable entity = atlasClientV1.getEntity(guid);
            assertEquals(entity.getId().getState(), Id.EntityState.DELETED);
        }
    }

    @Test
    public void testDeleteEntitiesViaClientApi() throws Exception {
        // Create 2 database entities
        Referenceable db1 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        db1.set("name", dbName);
        db1.set("description", randomString());
        db1.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        db1.set("owner", "user1");
        db1.set(CLUSTER_NAME, "cl1");
        db1.set("parameters", Collections.EMPTY_MAP);
        db1.set("location", "/tmp");
        Id db1Id = createInstance(db1);
        Referenceable db2 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName2 = randomString();
        db2.set("name", dbName2);
        db2.set(QUALIFIED_NAME, dbName2);
        db2.set(CLUSTER_NAME, randomString());
        db2.set("description", randomString());
        db2.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName2);
        db2.set("owner", "user2");
        db2.set("clusterName", "cl1");
        db2.set("parameters", Collections.EMPTY_MAP);
        db2.set("location", "/tmp");
        Id db2Id = createInstance(db2);

        // Delete the database entities
        List<String> deletedGuidsList =
                atlasClientV1.deleteEntities(db1Id._getId(), db2Id._getId()).getDeletedEntities();
        // Verify that deleteEntities() response has database entity guids
        Assert.assertEquals(deletedGuidsList.size(), 2);
        Assert.assertTrue(deletedGuidsList.contains(db1Id._getId()));
        Assert.assertTrue(deletedGuidsList.contains(db2Id._getId()));

        // Verify entities were deleted from the repository.
        for (String guid : deletedGuidsList) {
            Referenceable entity = atlasClientV1.getEntity(guid);
            assertEquals(entity.getId().getState(), Id.EntityState.DELETED);
        }
    }

    @Test
    public void testDeleteEntityByUniqAttribute() throws Exception {
        // Create database entity
        Referenceable db1 = new Referenceable(DATABASE_TYPE_BUILTIN);
        String dbName = randomString();
        db1.set(NAME, dbName);
        db1.set(QUALIFIED_NAME, dbName);
        db1.set(CLUSTER_NAME, randomString());
        db1.set(DESCRIPTION, randomString());
        db1.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        db1.set("owner", "user1");
        db1.set(CLUSTER_NAME, "cl1");
        db1.set("parameters", Collections.EMPTY_MAP);
        db1.set("location", "/tmp");
        Id db1Id = createInstance(db1);

        // Delete the database entity
        List<String> deletedGuidsList = atlasClientV1.deleteEntity(DATABASE_TYPE_BUILTIN, QUALIFIED_NAME, dbName).getDeletedEntities();

        // Verify that deleteEntities() response has database entity guids
        Assert.assertEquals(deletedGuidsList.size(), 1);
        Assert.assertTrue(deletedGuidsList.contains(db1Id._getId()));

        // Verify entities were deleted from the repository.
        for (String guid : deletedGuidsList) {
            Referenceable entity = atlasClientV1.getEntity(guid);
            assertEquals(entity.getId().getState(), Id.EntityState.DELETED);
        }
    }

}
