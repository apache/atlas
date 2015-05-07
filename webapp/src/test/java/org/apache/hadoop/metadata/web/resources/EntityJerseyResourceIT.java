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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization$;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization$;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.EnumValue;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeUtils;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityJerseyResourceIT.class);

    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table_type";
    private static final String TABLE_NAME = "bar";
    private static final String TRAITS = "traits";
    private static final String TRAIT = "trait";

    private Referenceable tableInstance;
    private Id tableId;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createHiveTypes();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        tableInstance = createHiveTableInstance();
        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();
        try {
            Assert.assertNotNull(UUID.fromString(guid));
        } catch (IllegalArgumentException e) {
            Assert.fail("Response is not a guid, " + guid);
        }
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testAddProperty() throws Exception {
        final String guid = tableId._getId();
        //add property
        String description = "bar table - new desc";
        ClientResponse clientResponse = addProperty(guid, "description", description);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String entityRef = getEntityDefinition(getEntityDefinition(guid));
        Assert.assertNotNull(entityRef);

        tableInstance.set("description", description);

        //invalid property for the type
        clientResponse = addProperty(guid, "invalid_property", "bar table");
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        //non-string property, update
        clientResponse = addProperty(guid, "level", "4");
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        entityRef = getEntityDefinition(getEntityDefinition(guid));
        Assert.assertNotNull(entityRef);

        tableInstance.set("level", 4);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testAddReferenceProperty() throws Exception {
        //Create new db instance
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", "newdb");
        databaseInstance.set("description", "new database");

        Id dbInstance = createInstance(databaseInstance);
        String dbId = dbInstance._getId();

        //Add reference property
        final String guid = tableId._getId();
        ClientResponse clientResponse = addProperty(guid, "database", dbId);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        final String guid = tableId._getId();
        ClientResponse clientResponse = getEntityDefinition(guid);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        final String definition = response.getString(MetadataServiceClient.RESULTS);
        Assert.assertNotNull(definition);
        LOG.debug("tableInstanceAfterGet = " + definition);
    }

    private ClientResponse addProperty(String guid, String property, String value) {
        WebResource resource = service
                .path("api/metadata/entities")
                .path(guid);

        return resource.queryParam("property", property).queryParam("value", value)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.PUT, ClientResponse.class);
    }

    private ClientResponse getEntityDefinition(String guid) {
        WebResource resource = service
                .path("api/metadata/entities")
                .path(guid);
        return resource.accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
    }

    private String getEntityDefinition(ClientResponse clientResponse) throws Exception {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        JSONObject response = new JSONObject(clientResponse.getEntity(String.class));
        final String definition = response.getString(MetadataServiceClient.RESULTS);
        Assert.assertNotNull(definition);

        return definition;
    }

    @Test
    public void testGetInvalidEntityDefinition() throws Exception {
        WebResource resource = service
                .path("api/metadata/entities")
                .path("blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() throws Exception {
        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .queryParam("type", TABLE_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(MetadataServiceClient.RESULTS);
        Assert.assertNotNull(list);
        Assert.assertEquals(list.length(), 1);
    }

    @Test
    public void testGetEntityListForBadEntityType() throws Exception {
        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .queryParam("type", "blah")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(),
                Response.Status.BAD_REQUEST.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
    }

    @Test
    public void testGetEntityListForNoInstances() throws Exception {
        addNewType();

        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .queryParam("type", "test")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(MetadataServiceClient.RESULTS);
        Assert.assertEquals(list.length(), 0);
    }

    private void addNewType() throws Exception {
        HierarchicalTypeDefinition<ClassType> testTypeDefinition =
                TypesUtil.createClassTypeDef("test",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        String typesAsJSON = TypesSerialization.toJson(testTypeDefinition);
        createType(typesAsJSON);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetTraitNames() throws Exception {
        final String guid = tableId._getId();
        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .path(guid)
                .path(TRAITS)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));
        Assert.assertNotNull(response.get("GUID"));

        final JSONArray list = response.getJSONArray(MetadataServiceClient.RESULTS);
        Assert.assertEquals(list.length(), 7);
    }

    @Test (dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        final String traitName = "PII_Trait";
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableList.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = " + traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        final String guid = tableId._getId();
        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .path(guid)
                .path(TRAITS)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));
        Assert.assertNotNull(response.get("GUID"));
        Assert.assertNotNull(response.get("traitInstance"));
    }

    @Test
    public void testAddTraitWithNoRegistration() throws Exception {
        final String traitName = "PII_Trait_Blah";
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableList.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = " + traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization$.MODULE$.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .path("random")
                .path(TRAITS)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(),
                Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test (dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String traitName = "PII_Trait";
        final String guid = tableId._getId();

        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .path(guid)
                .path(TRAIT)
                .path(traitName)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.DELETE, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));
        Assert.assertNotNull(response.get("GUID"));
        Assert.assertNotNull(response.get("traitName"));
    }

    @Test
    public void testDeleteTraitNonExistent() throws Exception {
        final String traitName = "blah_trait";

        ClientResponse clientResponse = service
                .path("api/metadata/entities")
                .path("random")
                .path(TRAIT)
                .path(traitName)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.DELETE, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(),
                Response.Status.BAD_REQUEST.getStatusCode());
    }

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                TypesUtil.createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition =
                new StructTypeDefinition("serdeType",
                        new AttributeDefinition[]{
                                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                                TypesUtil.createRequiredAttrDef("serde", DataTypes.STRING_TYPE)
                        });

        EnumValue values[] = {
                new EnumValue("MANAGED", 1),
                new EnumValue("EXTERNAL", 2),
        };

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", values);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                TypesUtil.createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("level", DataTypes.INT_TYPE),
                        new AttributeDefinition("tableType", "tableType",
                                Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde1",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde2",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("database",
                                DATABASE_TYPE, Multiplicity.REQUIRED, true, null));

        HierarchicalTypeDefinition<TraitType> classificationTraitDefinition =
                TypesUtil.createTraitTypeDef("classification",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef("pii", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef("phi", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef("pci", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef("sox", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef("sec", ImmutableList.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef("finance", ImmutableList.<String>of());

        TypesDef typesDef = TypeUtils.getTypesDef(
                ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition),
                ImmutableList.of(classificationTraitDefinition, piiTrait, phiTrait, pciTrait,
                        soxTrait, secTrait, financeTrait),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));
        createType(typesDef);
    }

    private Referenceable createHiveTableInstance() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance = new Referenceable(TABLE_TYPE,
                "classification", "pii", "phi", "pci", "sox", "sec", "finance");
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("level", 2);
        tableInstance.set("tableType", 1); // enum
        tableInstance.set("database", databaseInstance);

        Struct traitInstance = (Struct) tableInstance.getTrait("classification");
        traitInstance.set("tag", "foundation_etl");

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set("name", "serde1");
        serde1Instance.set("serde", "serde1");
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set("name", "serde2");
        serde2Instance.set("serde", "serde2");
        tableInstance.set("serde2", serde2Instance);

        List<String> traits = tableInstance.getTraits();
        Assert.assertEquals(traits.size(), 7);

        return tableInstance;
    }
}
