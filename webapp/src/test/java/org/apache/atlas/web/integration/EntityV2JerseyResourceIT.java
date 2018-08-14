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

package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;


/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityV2JerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityV2JerseyResourceIT.class);

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    private String traitName;

    private AtlasEntity dbEntity;
    private AtlasEntity tableEntity;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitionsV2();

    }

    @Test
    public void testSubmitEntity() throws Exception {
        TypesUtil.Pair dbAndTable = createDBAndTable();
        assertNotNull(dbAndTable);
        assertNotNull(dbAndTable.left);
        assertNotNull(dbAndTable.right);
    }

    @Test
    public void testCreateNestedEntities() throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();

        AtlasEntity databaseInstance = new AtlasEntity(DATABASE_TYPE_V2, "name", "db1");
        databaseInstance.setAttribute("name", "db1");
        databaseInstance.setAttribute("description", "foo database");
        databaseInstance.setAttribute("owner", "user1");
        databaseInstance.setAttribute("locationUri", "/tmp");
        databaseInstance.setAttribute("createTime",1000);
        entities.addEntity(databaseInstance);

        int nTables = 5;
        int colsPerTable=3;

        for(int i = 0; i < nTables; i++) {
            String tableName = "db1-table-" + i;

            AtlasEntity tableInstance = new AtlasEntity(HIVE_TABLE_TYPE_V2, "name", tableName);
            tableInstance.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
            tableInstance.setAttribute("db", AtlasTypeUtil.getAtlasObjectId(databaseInstance));
            tableInstance.setAttribute("description", tableName + " table");
            entities.addEntity(tableInstance);

            List<AtlasObjectId> columns = new ArrayList<>();
            for(int j = 0; j < colsPerTable; j++) {
                AtlasEntity columnInstance = new AtlasEntity(COLUMN_TYPE_V2);
                columnInstance.setAttribute("name", tableName + "-col-" + j);
                columnInstance.setAttribute("dataType", "String");
                columnInstance.setAttribute("comment", "column " + j + " for table " + i);

                columns.add(AtlasTypeUtil.getAtlasObjectId(columnInstance));

                entities.addReferredEntity(columnInstance);
            }
            tableInstance.setAttribute("columns", columns);
        }

        //Create the tables.  The database and columns should be created automatically, since
        //the tables reference them.

        EntityMutationResponse response = atlasClientV2.createEntities(entities);
        Assert.assertNotNull(response);

        Map<String,String> guidsCreated = response.getGuidAssignments();
        assertEquals(guidsCreated.size(), nTables * colsPerTable + nTables + 1);
        assertNotNull(guidsCreated.get(databaseInstance.getGuid()));

        for(AtlasEntity r : entities.getEntities()) {
            assertNotNull(guidsCreated.get(r.getGuid()));
        }

        for(AtlasEntity r : entities.getReferredEntities().values()) {
            assertNotNull(guidsCreated.get(r.getGuid()));
        }
    }

    @Test
    public void testRequestUser() throws Exception {
        AtlasEntity hiveDBInstanceV2 = createHiveDB(randomString());
        List<EntityAuditEvent> events = atlasClientV1.getEntityAuditEvents(hiveDBInstanceV2.getGuid(), (short) 10);
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).getUser(), "admin");
    }

    @Test
    public void testEntityDeduping() throws Exception {
        ArrayNode results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE_V2, DATABASE_NAME));
        assertEquals(results.size(), 1);

        final AtlasEntity hiveDBInstanceV2 = createHiveDB();

        results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE_V2, DATABASE_NAME));
        assertEquals(results.size(), 1);

        //Test the same across references
        final String tableName = randomString();
        AtlasEntity hiveTableInstanceV2 = createHiveTableInstanceV2(hiveDBInstanceV2, tableName);
        hiveTableInstanceV2.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);

        EntityMutationResponse entity = atlasClientV2.createEntity(new AtlasEntityWithExtInfo(hiveTableInstanceV2));
        assertNotNull(entity);
        assertNotNull(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE));
        results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE_V2, DATABASE_NAME));
        assertEquals(results.size(), 1);
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
                        Collections.<String>emptySet(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string")
                );
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entityDef);

        AtlasTypesDef created = atlasClientV2.createAtlasTypeDefs(typesDef);
        assertNotNull(created);
        assertNotNull(created.getEntityDefs());
        assertEquals(created.getEntityDefs().size(), 1);

        //create entity for the type
        AtlasEntity instance = new AtlasEntity(entityDef.getName());
        instance.setAttribute("name", randomString());
        EntityMutationResponse mutationResponse = atlasClientV2.createEntity(new AtlasEntityWithExtInfo(instance));
        assertNotNull(mutationResponse);
        assertNotNull(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE));
        assertEquals(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).size(),1 );
        String guid = mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0).getGuid();

        //update type - add attribute
        entityDef = AtlasTypeUtil.createClassTypeDef(entityDef.getName(), Collections.<String>emptySet(),
                AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createOptionalAttrDef("description", "string"));

        typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entityDef);

        AtlasTypesDef updated = atlasClientV2.updateAtlasTypeDefs(typesDef);
        assertNotNull(updated);
        assertNotNull(updated.getEntityDefs());
        assertEquals(updated.getEntityDefs().size(), 1);

        //Get definition after type update - new attributes should be null
        AtlasEntity entityByGuid = getEntityByGuid(guid);
        assertNull(entityByGuid.getAttribute("description"));
        assertEquals(entityByGuid.getAttribute("name"), instance.getAttribute("name"));
    }

    @Test
    public void testEntityInvalidValue() throws Exception {
        AtlasEntity databaseInstance = new AtlasEntity(DATABASE_TYPE_V2);
        String dbName = randomString();
        String nullString = null;
        String emptyString = "";
        databaseInstance.setAttribute("name", dbName);
        databaseInstance.setAttribute("description", nullString);
        AtlasEntityHeader created = createEntity(databaseInstance);

        // null valid value for required attr - description
        assertNull(created);

        databaseInstance.setAttribute("description", emptyString);
        created = createEntity(databaseInstance);

        // empty string valid value for required attr
        assertNotNull(created);

        databaseInstance.setGuid(created.getGuid());
        databaseInstance.setAttribute("owner", nullString);
        databaseInstance.setAttribute("locationUri", emptyString);

        created = updateEntity(databaseInstance);

        // null/empty string valid value for optional attr
        assertNotNull(created);
    }

    @Test
    public void testGetEntityByAttribute() throws Exception {
        AtlasEntity hiveDB = createHiveDB();
        String qualifiedName = (String) hiveDB.getAttribute(NAME);
        //get entity by attribute

        AtlasEntity byAttribute = atlasClientV2.getEntityByAttribute(DATABASE_TYPE_V2, toMap(NAME, qualifiedName)).getEntity();
        assertEquals(byAttribute.getTypeName(), DATABASE_TYPE_V2);
        assertEquals(byAttribute.getAttribute(NAME), qualifiedName);
    }

    @Test
    public void testSubmitEntityWithBadDateFormat() throws Exception {
        AtlasEntity       hiveDBEntity = createHiveDBInstanceV2("db" + randomString());
        AtlasEntityHeader hiveDBHeader = createEntity(hiveDBEntity);
        hiveDBEntity.setGuid(hiveDBHeader.getGuid());

        AtlasEntity tableInstance = createHiveTableInstanceV2(hiveDBEntity, "table" + randomString());
        //Dates with an invalid format are simply nulled out.  This does not produce
        //an error.  See AtlasBuiltInTypes.AtlasDateType.getNormalizedValue().
        tableInstance.setAttribute("lastAccessTime", 1107201407);
        AtlasEntityHeader tableEntityHeader = createEntity(tableInstance);
        assertNotNull(tableEntityHeader);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddProperty() throws Exception {
        //add property
        String description = "bar table - new desc";
        addProperty(createHiveTable().getGuid(), "description", description);

        AtlasEntity entityByGuid = getEntityByGuid(createHiveTable().getGuid());
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
        Object currentTime = new Date(System.currentTimeMillis());
        addProperty(createHiveTable().getGuid(), "createTime", currentTime);

        entityByGuid = getEntityByGuid(createHiveTable().getGuid());
        Assert.assertNotNull(entityByGuid);
    }

    @Test
    public void testAddNullPropertyValue() throws Exception {
        // FIXME: Behavior has changed between v1 and v2
        //add property
//        try {
            addProperty(createHiveTable().getGuid(), "description", null);
//            Assert.fail("Expected AtlasServiceException");
//        } catch(AtlasServiceException e) {
//            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
//        }
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testGetInvalidEntityDefinition() throws Exception {
        getEntityByGuid("blah");
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
                .createClassTypeDef(typeName, Collections.<String>emptySet(),
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(classTypeDef);
        createType(typesDef);
        return typeName;
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitNames() throws Exception {
        AtlasClassifications classifications = atlasClientV2.getClassifications(createHiveTable().getGuid());
        assertNotNull(classifications);
        assertTrue(classifications.getList().size() > 0);
        assertEquals(classifications.getList().size(), 8);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testCommonAttributes() throws Exception{
        AtlasEntity entity = getEntityByGuid(createHiveTable().getGuid());
        Assert.assertNotNull(entity.getStatus());
        Assert.assertNotNull(entity.getVersion());
        Assert.assertNotNull(entity.getCreatedBy());
        Assert.assertNotNull(entity.getCreateTime());
        Assert.assertNotNull(entity.getUpdatedBy());
        Assert.assertNotNull(entity.getUpdateTime());
    }

    private void addProperty(String guid, String property, Object value) throws AtlasServiceException {

        AtlasEntity entityByGuid = getEntityByGuid(guid);
        entityByGuid.setAttribute(property, value);
        EntityMutationResponse response = atlasClientV2.updateEntity(new AtlasEntityWithExtInfo(entityByGuid));
        assertNotNull(response);
        assertNotNull(response.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
    }

    private AtlasEntity createHiveDB() {
        if (dbEntity == null) {
            dbEntity = createHiveDB(DATABASE_NAME);
        }
        return dbEntity;
    }

    private AtlasEntity createHiveDB(String dbName) {
        AtlasEntity hiveDBInstanceV2 = createHiveDBInstanceV2(dbName);
        AtlasEntityHeader entityHeader = createEntity(hiveDBInstanceV2);
        assertNotNull(entityHeader);
        assertNotNull(entityHeader.getGuid());
        hiveDBInstanceV2.setGuid(entityHeader.getGuid());
        return hiveDBInstanceV2;
    }

    private TypesUtil.Pair<AtlasEntity, AtlasEntity> createDBAndTable() throws Exception {
        AtlasEntity dbInstanceV2 = createHiveDB();
        AtlasEntity hiveTableInstanceV2 = createHiveTable();
        return TypesUtil.Pair.of(dbInstanceV2, hiveTableInstanceV2);
    }

    private AtlasEntity createHiveTable() throws Exception {
        if (tableEntity == null) {
            tableEntity = createHiveTable(createHiveDB(), TABLE_NAME);
        }
        return tableEntity;

    }

    private AtlasEntity createHiveTable(AtlasEntity dbInstanceV2, String tableName) throws Exception {
        AtlasEntity hiveTableInstanceV2 = createHiveTableInstanceV2(dbInstanceV2, tableName);
        AtlasEntityHeader createdHeader = createEntity(hiveTableInstanceV2);
        assertNotNull(createdHeader);
        assertNotNull(createdHeader.getGuid());
        hiveTableInstanceV2.setGuid(createdHeader.getGuid());
        tableEntity = hiveTableInstanceV2;
        return hiveTableInstanceV2;
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef(traitName, Collections.<String>emptySet());
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        atlasClientV2.addClassifications(createHiveTable().getGuid(), Collections.singletonList(new AtlasClassification(piiTrait.getName())));

        assertEntityAudit(createHiveTable().getGuid(), EntityAuditEvent.EntityAuditAction.TAG_ADD);
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTraitWithValidityPeriod() throws Exception {
        traitName = "PII_Trait" + randomString();

        AtlasClassificationDef piiTrait = AtlasTypeUtil.createTraitTypeDef(traitName, Collections.<String>emptySet());
        AtlasTypesDef          typesDef = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.singletonList(piiTrait), Collections.emptyList());

        createType(typesDef);

        String              tableGuid      = createHiveTable().getGuid();
        AtlasClassification classification = new AtlasClassification(piiTrait.getName());
        TimeBoundary        validityPeriod = new TimeBoundary("2018/03/01 00:00:00", "2018/04/01 00:00:00", "GMT");

        classification.setEntityGuid(tableGuid);
        classification.addValityPeriod(validityPeriod);
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(true);

        atlasClientV2.addClassifications(tableGuid, Collections.singletonList(classification));

        assertEntityAudit(tableGuid, EntityAuditEvent.EntityAuditAction.TAG_ADD);

        AtlasClassifications classifications = atlasClientV2.getClassifications(tableGuid);

        assertNotNull(classifications);
        assertNotNull(classifications.getList());
        assertTrue(classifications.getList().size() > 1);

        boolean foundClassification = false;
        for (AtlasClassification entityClassification : classifications.getList()) {
            if (StringUtils.equalsIgnoreCase(entityClassification.getTypeName(), piiTrait.getName())) {
                foundClassification = true;

                assertEquals(entityClassification.getTypeName(), piiTrait.getName());
                assertNotNull(entityClassification.getValidityPeriods());
                assertEquals(entityClassification.getValidityPeriods().size(), 1);
                assertEquals(entityClassification.getValidityPeriods().get(0), validityPeriod);
                assertEquals(entityClassification, classification);

                break;
            }
        }

        assertTrue(foundClassification, "classification '" + piiTrait.getName() + "' is missing for entity '" + tableGuid + "'");
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitDefinitionForEntity() throws Exception{
        traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef(traitName, Collections.<String>emptySet());
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        AtlasClassificationDef classificationByName = atlasClientV2.getClassificationDefByName(traitName);
        assertNotNull(classificationByName);

        AtlasEntity hiveTable = createHiveTable();
        assertEquals(hiveTable.getClassifications().size(), 7);

        AtlasClassification piiClassification = new AtlasClassification(piiTrait.getName());

        atlasClientV2.addClassifications(hiveTable.getGuid(), Lists.newArrayList(piiClassification));

        AtlasClassifications classifications = atlasClientV2.getClassifications(hiveTable.getGuid());
        assertNotNull(classifications);
        assertTrue(classifications.getList().size() > 0);
        assertEquals(classifications.getList().size(), 8);
    }


    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTraitWithAttribute() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait = AtlasTypeUtil
                .createTraitTypeDef(traitName, Collections.<String>emptySet(),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        AtlasClassification traitInstance = new AtlasClassification(traitName);
        traitInstance.setAttribute("type", "SSN");

        final String guid = createHiveTable().getGuid();
        atlasClientV2.addClassifications(guid, Collections.singletonList(traitInstance));

        // verify the response
        AtlasEntity withAssociationByGuid = atlasClientV2.getEntityByGuid(guid).getEntity();
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
        AtlasTypeUtil.createTraitTypeDef(traitName, Collections.<String>emptySet());

        AtlasClassification traitInstance = new AtlasClassification(traitName);

        atlasClientV2.addClassifications("random", Collections.singletonList(traitInstance));
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = createHiveTable().getGuid();

        try {
            atlasClientV2.deleteClassification(guid, traitName);
        } catch (AtlasServiceException ex) {
            fail("Deletion should've succeeded");
        }
        assertEntityAudit(guid, EntityAuditEvent.EntityAuditAction.TAG_DELETE);
    }

    @Test
    public void testDeleteTraitNonExistent() throws Exception {
        final String traitName = "blah_trait";

        try {
            atlasClientV2.deleteClassification("random", traitName);
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

        final String guid = createHiveTable().getGuid();
        final String traitName = "PII_Trait" + randomString();
        AtlasClassificationDef piiTrait = AtlasTypeUtil
                .createTraitTypeDef(traitName, Collections.<String>emptySet(),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(piiTrait);
        createType(typesDef);

        try {
            atlasClientV2.deleteClassification(guid, traitName);
            fail("Deletion should've failed for non-existent trait association");
        } catch (AtlasServiceException ex) {
            Assert.assertNotNull(ex.getStatus());
            assertEquals(ex.getStatus(), ClientResponse.Status.BAD_REQUEST);
        }
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTF8() throws Exception {
        String classType = randomString();
        String attrName = random();
        String attrValue = random();

        AtlasEntityDef classTypeDef = AtlasTypeUtil
                .createClassTypeDef(classType, Collections.<String>emptySet(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef(attrName, "string"));
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(classTypeDef);
        createType(atlasTypesDef);

        AtlasEntity instance = new AtlasEntity(classType);
        instance.setAttribute(attrName, attrValue);
        AtlasEntityHeader entity = createEntity(instance);
        assertNotNull(entity);
        assertNotNull(entity.getGuid());

        AtlasEntity entityByGuid = getEntityByGuid(entity.getGuid());
        assertEquals(entityByGuid.getAttribute(attrName), attrValue);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testPartialUpdate() throws Exception {
        final List<AtlasEntity> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put("name", "col1");
        values.put(NAME, "qualifiedName.col1");
        values.put("type", "string");
        values.put("comment", "col1 comment");

        AtlasEntity colEntity = new AtlasEntity(BaseResourceIT.COLUMN_TYPE_V2, values);
        columns.add(colEntity);
        AtlasEntity hiveTable = createHiveTable();
        AtlasEntity tableUpdated = hiveTable;

        hiveTable.setAttribute("columns", AtlasTypeUtil.toObjectIds(columns));

        AtlasEntityWithExtInfo entityInfo = new AtlasEntityWithExtInfo(tableUpdated);
        entityInfo.addReferredEntity(colEntity);

        LOG.debug("Full Update entity= " + tableUpdated);
        EntityMutationResponse updateResult = atlasClientV2.updateEntity(entityInfo);
        assertNotNull(updateResult);
        assertNotNull(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertTrue(updateResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);

        String guid = hiveTable.getGuid();
        AtlasEntity entityByGuid1 = getEntityByGuid(guid);
        assertNotNull(entityByGuid1);
        entityByGuid1.getAttribute("columns");

        values.put("type", "int");
        colEntity = new AtlasEntity(BaseResourceIT.COLUMN_TYPE_V2, values);
        columns.clear();
        columns.add(colEntity);

        tableUpdated = new AtlasEntity(HIVE_TABLE_TYPE_V2, "name", entityByGuid1.getAttribute("name"));
        tableUpdated.setGuid(entityByGuid1.getGuid());
        tableUpdated.setAttribute("columns", AtlasTypeUtil.toObjectIds(columns));

        // tableUpdated = hiveTable;
        // tableUpdated.setAttribute("columns", AtlasTypeUtil.toObjectIds(columns));

        LOG.debug("Partial Update entity by unique attributes= " + tableUpdated);
        Map<String, String> uniqAttributes = new HashMap<>();
        uniqAttributes.put(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, (String) hiveTable.getAttribute("name"));

        entityInfo = new AtlasEntityWithExtInfo(tableUpdated);
        entityInfo.addReferredEntity(colEntity);

        EntityMutationResponse updateResponse = atlasClientV2.updateEntityByAttribute(BaseResourceIT.HIVE_TABLE_TYPE_V2, uniqAttributes, entityInfo);

        assertNotNull(updateResponse);
        assertNotNull(updateResponse.getEntitiesByOperation(EntityMutations.EntityOperation.PARTIAL_UPDATE));
        assertTrue(updateResponse.getEntitiesByOperation(EntityMutations.EntityOperation.PARTIAL_UPDATE).size() > 0);

        AtlasEntity entityByGuid2 = getEntityByGuid(guid);
        assertNotNull(entityByGuid2);
    }

    private AtlasEntity getEntityByGuid(String guid) throws AtlasServiceException {
        return atlasClientV2.getEntityByGuid(guid).getEntity();
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testCompleteUpdate() throws Exception {
        final List<AtlasEntity> columns = new ArrayList<>();
        Map<String, Object> values1 = new HashMap<>();
        values1.put("name", "col3");
        values1.put(NAME, "qualifiedName.col3");
        values1.put("type", "string");
        values1.put("comment", "col3 comment");

        Map<String, Object> values2 = new HashMap<>();
        values2.put("name", "col4");
        values2.put(NAME, "qualifiedName.col4");
        values2.put("type", "string");
        values2.put("comment", "col4 comment");

        AtlasEntity colEntity1 = new AtlasEntity(BaseResourceIT.COLUMN_TYPE_V2, values1);
        AtlasEntity colEntity2 = new AtlasEntity(BaseResourceIT.COLUMN_TYPE_V2, values2);
        columns.add(colEntity1);
        columns.add(colEntity2);
        AtlasEntity hiveTable = createHiveTable();
        hiveTable.setAttribute("columns", AtlasTypeUtil.toObjectIds(columns));

        AtlasEntityWithExtInfo entityInfo = new AtlasEntityWithExtInfo(hiveTable);
        entityInfo.addReferredEntity(colEntity1);
        entityInfo.addReferredEntity(colEntity2);

        EntityMutationResponse updateEntityResult = atlasClientV2.updateEntity(entityInfo);
        assertNotNull(updateEntityResult);
        assertNotNull(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
        assertNotNull(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE));
        //2 columns are being created, and 1 hiveTable is being updated
        assertEquals(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size(), 1);
        assertEquals(updateEntityResult.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).size(), 2);

        AtlasEntity entityByGuid = getEntityByGuid(hiveTable.getGuid());
        List<AtlasObjectId> refs = (List<AtlasObjectId>) entityByGuid.getAttribute("columns");
        assertEquals(refs.size(), 2);
    }

    @Test
    public void testDeleteEntities() throws Exception {
        // Create 2 database entities
        AtlasEntity db1 = new AtlasEntity(DATABASE_TYPE_V2);
        String dbName1 = randomString();
        db1.setAttribute("name", dbName1);
        db1.setAttribute(NAME, dbName1);
        db1.setAttribute("clusterName", randomString());
        db1.setAttribute("description", randomString());
        AtlasEntityHeader entity1Header = createEntity(db1);
        AtlasEntity db2 = new AtlasEntity(DATABASE_TYPE_V2);
        String dbName2 = randomString();
        db2.setAttribute("name", dbName2);
        db2.setAttribute(NAME, dbName2);
        db2.setAttribute("clusterName", randomString());
        db2.setAttribute("description", randomString());
        AtlasEntityHeader entity2Header = createEntity(db2);

        // Delete the database entities
        EntityMutationResponse deleteResponse = atlasClientV2.deleteEntitiesByGuids(Arrays.asList(entity1Header.getGuid(), entity2Header.getGuid()));

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

        // Delete the database entity
        EntityMutationResponse deleteResponse = atlasClientV2.deleteEntityByAttribute(DATABASE_TYPE_V2, toMap(NAME, (String) hiveDB.getAttribute(NAME)));

        // Verify that deleteEntities() response has database entity guids
        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE));
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).size(), 1);

        // Verify entities were deleted from the repository.
    }

    private Map<String, String> toMap(final String name, final String value) {
        return new HashMap<String, String>() {{
            put(name, value);
        }};
    }
}
