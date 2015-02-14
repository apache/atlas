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
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.ITypedStruct;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.json.TypesSerialization;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.AttributeInfo;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.types.EnumValue;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.types.TraitType;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityJerseyResourceIT.class);

    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table";
    private static final String TABLE_NAME = "bar";

    private ITypedReferenceableInstance tableInstance;
    private String guid;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createHiveTypes();
        submitTypes();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        tableInstance = createHiveTableInstance();
        String tableInstanceAsJSON = Serialization$.MODULE$.toJson(tableInstance);
        LOG.debug("tableInstance = " + tableInstanceAsJSON);

        WebResource resource = service
                .path("api/metadata/entities/submit")
                .path(TABLE_TYPE);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, tableInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("requestId"));

        guid = response.get("GUID").toString();
        Assert.assertNotNull(guid);

        try {
            Assert.assertNotNull(UUID.fromString(guid));
        } catch (IllegalArgumentException e) {
            Assert.fail("Response is not a guid, " + response);
        }
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path(guid);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("requestId"));

        final String definition = response.getString("definition");
        Assert.assertNotNull(definition);
        LOG.debug("tableInstanceAfterGet = " + definition);

        // todo - this fails with type error, strange
        ITypedReferenceableInstance tableInstanceAfterGet = Serialization$.MODULE$.fromJson(definition);
        Assert.assertTrue(areEqual(tableInstance, tableInstanceAfterGet));
    }

    private boolean areEqual(ITypedInstance actual,
                             ITypedInstance expected) throws Exception {
/*
        Assert.assertEquals(Serialization$.MODULE$.toJson(actual),
                Serialization$.MODULE$.toJson(expected));
*/

        for (AttributeInfo attributeInfo : actual.fieldMapping().fields.values()) {
            final DataTypes.TypeCategory typeCategory = attributeInfo.dataType().getTypeCategory();
            if (typeCategory == DataTypes.TypeCategory.STRUCT
                    || typeCategory == DataTypes.TypeCategory.TRAIT
                    || typeCategory == DataTypes.TypeCategory.CLASS) {
                areEqual((ITypedStruct) actual.get(attributeInfo.name),
                        (ITypedStruct) expected.get(attributeInfo.name));
            } else if (typeCategory == DataTypes.TypeCategory.PRIMITIVE
                    || typeCategory == DataTypes.TypeCategory.ENUM) {
                Assert.assertEquals(actual.get(attributeInfo.name),
                        expected.get(attributeInfo.name));
            }
        }

        return true;
    }

    @Test
    public void testGetInvalidEntityDefinition() throws Exception {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path("blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() throws Exception {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/")
                .path(TABLE_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("requestId"));

        final JSONArray list = response.getJSONArray("list");
        Assert.assertNotNull(list);
        Assert.assertEquals(list.length(), 1);
    }

    @Test
    public void testGetEntityListForBadEntityType() throws Exception {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/blah")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
    }

    @Test
    public void testGetEntityListForNoInstances() throws Exception {
        addNewType();

        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/test")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("requestId"));

        final JSONArray list = response.getJSONArray("list");
        Assert.assertEquals(list.length(), 0);
    }

    private void addNewType() throws Exception {
        HierarchicalTypeDefinition<ClassType> testTypeDefinition =
                createClassTypeDef("test",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));
        typeSystem.defineClassType(testTypeDefinition);

        @SuppressWarnings("unchecked")
        String typesAsJSON = TypesSerialization.toJson(typeSystem,
                Arrays.asList(new String[]{"test"}));
        sumbitType(typesAsJSON, "test");
    }

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition =
                new StructTypeDefinition("serdeType",
                        new AttributeDefinition[] {
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("serde", DataTypes.STRING_TYPE)
                        });

        EnumValue values[] = {
                new EnumValue("MANAGED", 1),
                new EnumValue("EXTERNAL", 2),
        };

        EnumTypeDefinition enumTypeDefinition =  new EnumTypeDefinition("tableType", values);
        typeSystem.defineEnumType(enumTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        new AttributeDefinition("tableType", "tableType",
                                Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde1",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde2",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("database",
                                DATABASE_TYPE, Multiplicity.REQUIRED, true, null));

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef("classification",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        typeSystem.defineTypes(
                ImmutableList.of(structTypeDefinition),
                ImmutableList.of(classificationTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));
    }

    private void submitTypes() throws Exception {
        @SuppressWarnings("unchecked")
        String typesAsJSON = TypesSerialization.toJson(typeSystem,
                Arrays.asList(new String[]{
                        "tableType",
                        DATABASE_TYPE,
                        TABLE_TYPE,
                        "serdeType",
                        "classification"}));
        sumbitType(typesAsJSON, TABLE_TYPE);
    }

    private void sumbitType(String typesAsJSON, String type) throws JSONException {
        WebResource resource = service
                .path("api/metadata/types/submit")
                .path(type);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertEquals(response.get("typeName"), type);
        Assert.assertNotNull(response.get("types"));
        Assert.assertNotNull(response.get("requestId"));
    }

    private ITypedReferenceableInstance createHiveTableInstance() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance = new Referenceable(TABLE_TYPE, "classification");
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
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

        ClassType tableType = typeSystem.getDataType(ClassType.class, TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }
}
