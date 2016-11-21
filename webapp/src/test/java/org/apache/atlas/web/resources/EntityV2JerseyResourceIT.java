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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;


/**
 * Integration tests for Entity Jersey Resource.
 */
@Guice(modules = {NotificationModule.class})
public class EntityV2JerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityV2JerseyResourceIT.class);

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    private static final String TRAITS = "traits";
    private static final String TRAIT_DEFINITION = "traitDefinitions";

    private String traitName;

    private AtlasEntity dbEntity;
    private AtlasEntityHeader dbEntityHeader;
    private AtlasEntityWithAssociations tableEntity;
    private AtlasEntityHeader tableEntityHeader;

    @Inject
    private NotificationInterface notificationInterface;
    private NotificationConsumer<EntityNotification> notificationConsumer;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitionsV2();

        List<NotificationConsumer<EntityNotification>> consumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

        notificationConsumer = consumers.iterator().next();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        TypeUtils.Pair dbAndTable = createDBAndTable(DATABASE_NAME, TABLE_NAME);
        assertNotNull(dbAndTable);
        assertNotNull(dbAndTable.left);
        assertNotNull(dbAndTable.right);
    }

    @Test
    public void testRequestUser() throws Exception {
        AtlasEntity hiveDBInstanceV2 = createHiveDB(DATABASE_NAME);
        List<EntityAuditEvent> events = atlasClientV1.getEntityAuditEvents(hiveDBInstanceV2.getGuid(), (short) 10);
        assertTrue(events.size() > 1);
        assertEquals(events.get(0).getUser(), "admin");
    }

    @Test
    public void testEntityDeduping() throws Exception {
        JSONArray results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, DATABASE_NAME));
        assertEquals(results.length(), 1);

        final AtlasEntity hiveDBInstanceV2 = createHiveDB(DATABASE_NAME);
        // Do the notification thing here
        waitForNotification(notificationConsumer, MAX_WAIT_TIME, new NotificationPredicate() {
            @Override
            public boolean evaluate(EntityNotification notification) throws Exception {
                return notification != null && notification.getEntity().getId()._getId().equals(hiveDBInstanceV2.getGuid());
            }
        });


        results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, DATABASE_NAME));
        assertEquals(results.length(), 1);

        //Test the same across references
        final String tableName = randomString();
        AtlasEntityWithAssociations hiveTableInstanceV2 = createHiveTableInstanceV2(hiveDBInstanceV2, tableName);
        hiveTableInstanceV2.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);

        EntityMutationResponse entity = entitiesClientV2.createEntity(hiveTableInstanceV2);
        assertNotNull(entity);
        assertNotNull(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
        results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, DATABASE_NAME));
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
        AtlasEntityDef entityDef = AtlasTypeUtil
                .createClassTypeDef(randomString(),
                        ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string")
                );
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entityDef);

        AtlasTypesDef created = typedefClientV2.createAtlasTypeDefs(typesDef);
        assertNotNull(created);
        assertNotNull(created.getEntityDefs());
        assertEquals(created.getEntityDefs().size(), 1);

        //create entity for the type
        AtlasEntity instance = new AtlasEntity(entityDef.getName());
        instance.setAttribute("name", randomString());
        EntityMutationResponse mutationResponse = entitiesClientV2.createEntity(instance);
        assertNotNull(mutationResponse);
        assertNotNull(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
        assertEquals(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).size(),1 );
        String guid = mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).get(0).getGuid();

        //update type - add attribute
        entityDef = AtlasTypeUtil.createClassTypeDef(entityDef.getName(), ImmutableSet.<String>of(),
                AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createOptionalAttrDef("description", "string"));

        typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entityDef);

        AtlasTypesDef updated = typedefClientV2.updateAtlasTypeDefs(typesDef);
        assertNotNull(updated);
        assertNotNull(updated.getEntityDefs());
        assertEquals(updated.getEntityDefs().size(), 1);

        //Get definition after type update - new attributes should be null
        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(guid);
        assertNull(entityByGuid.getAttribute("description"));
        assertEquals(entityByGuid.getAttribute("name"), instance.getAttribute("name"));
    }

    @DataProvider
    public Object[][] invalidAttrValues() {
        return new Object[][]{{null}, {""}};
    }

    @Test(dataProvider = "invalidAttrValues")
    public void testEntityInvalidValue(String value) throws Exception {
        AtlasEntity databaseInstance = new AtlasEntity(DATABASE_TYPE);
        String dbName = randomString();
        databaseInstance.setAttribute("name", dbName);
        databaseInstance.setAttribute("description", value);

        AtlasEntityHeader created = createEntity(databaseInstance);
        assertNull(created);
    }

    @Test
    public void testGetEntityByAttribute() throws Exception {
        AtlasEntity hiveDB = createHiveDB(DATABASE_NAME);
        String qualifiedName = (String) hiveDB.getAttribute(QUALIFIED_NAME);
        //get entity by attribute
        AtlasEntity byAttribute = entitiesClientV2.getEntityByAttribute(DATABASE_TYPE, QUALIFIED_NAME, qualifiedName);
        assertEquals(byAttribute.getTypeName(), DATABASE_TYPE);
        assertEquals(byAttribute.getAttribute(QUALIFIED_NAME), qualifiedName);
    }

    @Test
    public void testSubmitEntityWithBadDateFormat() throws Exception {
        AtlasEntity hiveDBInstance = createHiveDBInstanceV2("db" + randomString());
        AtlasEntityHeader entity = createEntity(hiveDBInstance);

        AtlasEntity tableInstance = createHiveTableInstanceV2(hiveDBInstance, "table" + randomString());
        tableInstance.setAttribute("lastAccessTime", "2014-07-11");
        AtlasEntityHeader tableEntityHeader = createEntity(tableInstance);
        assertNull(tableEntityHeader);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddProperty() throws Exception {
        //add property
        String description = "bar table - new desc";
        addProperty(tableEntity.getGuid(), "description", description);

        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(tableEntity.getGuid());
        Assert.assertNotNull(entityByGuid);

        entityByGuid.setAttribute("description", description);

        // TODO: This behavior should've been consistent across APIs
//        //invalid property for the type
//        try {
//            addProperty(table.getGuid(), "invalid_property", "bar table");
//            Assert.fail("Expected AtlasServiceException");
//        } catch (AtlasServiceException e) {
//            assertNotNull(e.getStatus());
//            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST);
//        }

        //non-string property, update
        String currentTime = String.valueOf(new DateTime());


        addProperty(tableEntity.getGuid(), "createTime", currentTime);

        entityByGuid = entitiesClientV2.getEntityByGuid(tableEntity.getGuid());
        Assert.assertNotNull(entityByGuid);
    }

    @Test
    public void testAddNullPropertyValue() throws Exception {
        // FIXME: Behavior has changed between v1 and v2
        //add property
//        try {
            addProperty(tableEntity.getGuid(), "description", null);
//            Assert.fail("Expected AtlasServiceException");
//        } catch(AtlasServiceException e) {
//            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
//        }
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testGetInvalidEntityDefinition() throws Exception {
        entitiesClientV2.getEntityByGuid("blah");
    }

    @Test(dependsOnMethods = "testSubmitEntity", enabled = false)
    public void testGetEntityList() throws Exception {
        // TODO: Can only be done when there's a search API exposed from entity REST
    }

    @Test(enabled = false)
    public void testGetEntityListForBadEntityType() throws Exception {
        // FIXME: Complete test when search interface is in place
    }

    @Test(enabled = false)
    public void testGetEntityListForNoInstances() throws Exception {
        // FIXME: Complete test when search interface is in place
        /*
        String typeName = "";

        ClientResponse clientResponse =
                service.path(ENTITIES).queryParam("type", typeName).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(list.length(), 0);
         */
    }

    private String addNewType() throws Exception {
        String typeName = "test" + randomString();
        AtlasEntityDef classTypeDef = AtlasTypeUtil
                .createClassTypeDef(typeName, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(classTypeDef);
        createType(typesDef);
        return typeName;
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitNames() throws Exception {
        AtlasClassifications classifications = entitiesClientV2.getClassifications(tableEntity.getGuid());
        assertNotNull(classifications);
        assertTrue(classifications.getList().size() > 0);
        assertEquals(classifications.getList().size(), 8);
    }

    private void addProperty(String guid, String property, String value) throws AtlasServiceException {

        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(guid);
        entityByGuid.setAttribute(property, value);
        EntityMutationResponse response = entitiesClientV2.updateEntity(entityByGuid);
        assertNotNull(response);
        assertNotNull(response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
    }

    private AtlasEntity createHiveDB() {
        if (dbEntity != null) {
            return dbEntity;
        } else {
            return createHiveDB(DATABASE_NAME);
        }
    }

    private AtlasEntity createHiveDB(String dbName) {
        AtlasEntity hiveDBInstanceV2 = createHiveDBInstanceV2(dbName);
        AtlasEntityHeader entityHeader = createEntity(hiveDBInstanceV2);
        assertNotNull(entityHeader);
        assertNotNull(entityHeader.getGuid());
        hiveDBInstanceV2.setGuid(entityHeader.getGuid());
        dbEntity = hiveDBInstanceV2;
        dbEntityHeader = entityHeader;
        return hiveDBInstanceV2;
    }

    private TypeUtils.Pair<AtlasEntity, AtlasEntityWithAssociations> createDBAndTable(String dbName, String tableName) throws Exception {
        AtlasEntity dbInstanceV2 = createHiveDB(dbName);
        AtlasEntityWithAssociations hiveTableInstanceV2 = createHiveTable(dbInstanceV2, tableName);
        return TypeUtils.Pair.of(dbInstanceV2, hiveTableInstanceV2);
    }

    private AtlasEntityWithAssociations createHiveTable() throws Exception {
        if (tableEntity != null) {
            return tableEntity;
        } else {
            return createHiveTable(createHiveDB(), TABLE_NAME);
        }
    }

    private AtlasEntityWithAssociations createHiveTable(AtlasEntity dbInstanceV2, String tableName) throws Exception {
        AtlasEntityWithAssociations hiveTableInstanceV2 = createHiveTableInstanceV2(dbInstanceV2, tableName);
        AtlasEntityHeader createdHeader = createEntity(hiveTableInstanceV2);
        assertNotNull(createdHeader);
        assertNotNull(createdHeader.getGuid());
        hiveTableInstanceV2.setGuid(createdHeader.getGuid());
        entitiesClientV2.addClassifications(createdHeader.getGuid(), hiveTableInstanceV2.getClassifications());
        tableEntity = hiveTableInstanceV2;
        tableEntityHeader = createdHeader;
        return hiveTableInstanceV2;
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        entitiesClientV2.addClassifications(tableEntity.getGuid(), ImmutableList.of(new AtlasClassification(piiTrait.getName())));

        assertEntityAudit(tableEntity.getGuid(), EntityAuditEvent.EntityAuditAction.TAG_ADD);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitDefinitionForEntity() throws Exception{
        traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        AtlasClassificationDef classificationByName = typedefClientV2.getClassificationByName(traitName);
        assertNotNull(classificationByName);

        assertEquals(tableEntity.getClassifications().size(), 7);

        AtlasClassification piiClassification = new AtlasClassification(piiTrait.getName());

        entitiesClientV2.addClassifications(tableEntity.getGuid(), Lists.newArrayList(piiClassification));

        AtlasClassifications classifications = entitiesClientV2.getClassifications(tableEntity.getGuid());
        assertNotNull(classifications);
        assertTrue(classifications.getList().size() > 0);
        assertEquals(classifications.getList().size(), 8);
    }


    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTraitWithAttribute() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait = AtlasTypeUtil
                .createTraitTypeDef(traitName, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        AtlasClassification traitInstance = new AtlasClassification(traitName);
        traitInstance.setAttribute("type", "SSN");

        final String guid = tableEntity.getGuid();
        entitiesClientV2.addClassifications(guid, ImmutableList.of(traitInstance));

        // verify the response
        AtlasEntityWithAssociations withAssociationByGuid = entitiesClientV2.getEntityWithAssociationByGuid(guid);
        assertNotNull(withAssociationByGuid);
        assertFalse(withAssociationByGuid.getClassifications().isEmpty());

        boolean found = false;
        for (AtlasClassification atlasClassification : withAssociationByGuid.getClassifications()) {
            String attribute = (String)atlasClassification.getAttribute("type");
            if (attribute != null && attribute.equals("SSN")) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testAddTraitWithNoRegistration() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());

        AtlasClassification traitInstance = new AtlasClassification(traitName);

        entitiesClientV2.addClassifications("random", ImmutableList.of(traitInstance));
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = tableEntity.getGuid();

        try {
            entitiesClientV2.deleteClassification(guid, traitName);
        } catch (AtlasServiceException ex) {
            fail("Deletion should've succeeded");
        }
        assertEntityAudit(guid, EntityAuditEvent.EntityAuditAction.TAG_DELETE);
    }

    @Test
    public void testDeleteTraitNonExistent() throws Exception {
        final String traitName = "blah_trait";

        try {
            entitiesClientV2.deleteClassification("random", traitName);
            fail("Deletion for bogus names shouldn't have succeeded");
        } catch (AtlasServiceException ex) {
            assertNotNull(ex.getStatus());
//            assertEquals(ex.getStatus(), ClientResponse.Status.NOT_FOUND);
            assertEquals(ex.getStatus(), ClientResponse.Status.BAD_REQUEST);
            // Should it be a 400 or 404
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testDeleteExistentTraitNonExistentForEntity() throws Exception {

        final String guid = tableEntity.getGuid();
        final String traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait = AtlasTypeUtil
                .createTraitTypeDef(traitName, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        try {
            entitiesClientV2.deleteClassification(guid, traitName);
            fail("Deletion should've failed for non-existent trait association");
        } catch (AtlasServiceException ex) {
            Assert.assertNotNull(ex.getStatus());
            assertEquals(ex.getStatus(), ClientResponse.Status.NOT_FOUND);
        }
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTF8() throws Exception {
        String classType = random();
        String attrName = random();
        String attrValue = random();

        AtlasEntityDef classTypeDef = AtlasTypeUtil
                .createClassTypeDef(classType, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef(attrName, "string"));
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(classTypeDef);
        createType(atlasTypesDef);

        AtlasEntity instance = new AtlasEntity(classType);
        instance.setAttribute(attrName, attrValue);
        AtlasEntityHeader entity = createEntity(instance);
        assertNotNull(entity);
        assertNotNull(entity.getGuid());

        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(entity.getGuid());
        assertEquals(entityByGuid.getAttribute(attrName), attrValue);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testPartialUpdate() throws Exception {
        final List<AtlasEntity> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put("name", "col1");
        values.put(QUALIFIED_NAME, "qualifiedName.col1");
        values.put("type", "string");
        values.put("comment", "col1 comment");

        AtlasEntity ref = new AtlasEntity(BaseResourceIT.COLUMN_TYPE, values);
        columns.add(ref);

        AtlasEntityWithAssociations tableUpdated = tableEntity;
        tableEntity.setAttribute("columns", columns);

        LOG.debug("Updating entity= " + tableUpdated);
        EntityMutationResponse updateResult = entitiesClientV2.updateEntity(tableEntity.getGuid(), tableUpdated);
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).size() > 0);

        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(tableEntity.getGuid());
        assertNotNull(entityByGuid);
        List<AtlasEntity> columns1 = (List<AtlasEntity>) entityByGuid.getAttribute("columns");

        //Update by unique attribute
        values.put("type", "int");
        ref = new AtlasEntity(BaseResourceIT.COLUMN_TYPE, values);
        columns.set(0, ref);
        tableUpdated = tableEntity;
        tableUpdated.setAttribute("columns", columns);

        LOG.debug("Updating entity= " + tableUpdated);
        EntityMutationResponse updateResponse = entitiesClientV2.updateEntityByAttribute(BaseResourceIT.HIVE_TABLE_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                (String) tableEntity.getAttribute("name"), tableUpdated);
        assertNotNull(updateResponse);
        assertNotNull(updateResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
        assertTrue(updateResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).size() > 0);

        entityByGuid = entitiesClientV2.getEntityByGuid(tableEntity.getGuid());
        assertNotNull(entityByGuid);
        columns1 = (List<AtlasEntity>) entityByGuid.getAttribute("columns");
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testCompleteUpdate() throws Exception {
        final List<AtlasEntity> columns = new ArrayList<>();
        Map<String, Object> values1 = new HashMap<>();
        values1.put("name", "col3");
        values1.put(QUALIFIED_NAME, "qualifiedName.col3");
        values1.put("type", "string");
        values1.put("comment", "col3 comment");

        Map<String, Object> values2 = new HashMap<>();
        values2.put("name", "col4");
        values2.put(QUALIFIED_NAME, "qualifiedName.col4");
        values2.put("type", "string");
        values2.put("comment", "col4 comment");

        AtlasEntity ref1 = new AtlasEntity(BaseResourceIT.COLUMN_TYPE, values1);
        AtlasEntity ref2 = new AtlasEntity(BaseResourceIT.COLUMN_TYPE, values2);
        columns.add(ref1);
        columns.add(ref2);
        tableEntity.setAttribute("columns", columns);
        EntityMutationResponse updateEntityResult = entitiesClientV2.updateEntity(tableEntity);
        assertNotNull(updateEntityResult);
        assertNotNull(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE));
        assertEquals(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE).size(), 3);

        AtlasEntity entityByGuid = entitiesClientV2.getEntityByGuid(tableEntity.getGuid());
        List<AtlasEntity> refs = (List<AtlasEntity>) entityByGuid.getAttribute("columns");
        assertEquals(refs.size(), 2);
    }

    @Test
    public void testDeleteEntities() throws Exception {
        // Create 2 database entities
        AtlasEntity db1 = new AtlasEntity(DATABASE_TYPE);
        String dbName1 = randomString();
        db1.setAttribute("name", dbName1);
        db1.setAttribute(QUALIFIED_NAME, dbName1);
        db1.setAttribute("clusterName", randomString());
        db1.setAttribute("description", randomString());
        AtlasEntityHeader entity1Header = createEntity(db1);
        AtlasEntity db2 = new AtlasEntity(DATABASE_TYPE);
        String dbName2 = randomString();
        db2.setAttribute("name", dbName2);
        db2.setAttribute(QUALIFIED_NAME, dbName2);
        db2.setAttribute("clusterName", randomString());
        db2.setAttribute("description", randomString());
        AtlasEntityHeader entity2Header = createEntity(db2);

        // Delete the database entities
        EntityMutationResponse deleteResponse = entitiesClientV2.deleteEntityByGuid(ImmutableList.of(entity1Header.getGuid(), entity2Header.getGuid()));

        // Verify that deleteEntities() response has database entity guids
        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 2);

        // Verify entities were deleted from the repository.
    }

    @Test
    public void testDeleteEntityByUniqAttribute() throws Exception {
        // Create database entity
        AtlasEntity hiveDB = createHiveDB(DATABASE_NAME + random());
        String qualifiedName = (String) hiveDB.getAttribute(QUALIFIED_NAME);

        // Delete the database entity
        EntityMutationResponse deleteResponse = entitiesClientV2.deleteEntityByAttribute(DATABASE_TYPE, QUALIFIED_NAME, qualifiedName);

        // Verify that deleteEntities() response has database entity guids
        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 1);

        // Verify entities were deleted from the repository.
    }

}
