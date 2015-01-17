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
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.json.TypesSerialization;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.types.TraitType;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table";
    private static final String TABLE_NAME = "bar";
    private static final String TRAIT_TYPE = "hive_fetl";

    private String guid;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        List<HierarchicalTypeDefinition> typeDefinitions = createHiveTypes();
        submitTypes(typeDefinitions);
    }

    @Test
    public void testSubmitEntity() throws Exception {
        ITypedReferenceableInstance tableInstance = createHiveTableInstance();

        String instanceAsJSON = Serialization$.MODULE$.toJson(tableInstance);

        WebResource resource = service
                .path("api/metadata/entities/submit")
                .path(TABLE_TYPE);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, instanceAsJSON);
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
    public void testGetEntityDefinition() {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path(guid);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    @Test
    public void testGetInvalidEntityDefinition() {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path("blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    @Test (dependsOnMethods = "testSubmitEntity", enabled = false)
    public void testGetEntityList() {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/")
                .path(TABLE_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    @Test (enabled = false) // todo: enable this later
    public void testGetEntityListForBadEntityType() {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/blah")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    private List<HierarchicalTypeDefinition> createHiveTypes() throws Exception {
        ArrayList<HierarchicalTypeDefinition> typeDefinitions = new ArrayList<>();

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));
        typeDefinitions.add(databaseTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        new AttributeDefinition(DATABASE_TYPE,
                                DATABASE_TYPE, Multiplicity.REQUIRED, true, DATABASE_TYPE));
        typeDefinitions.add(tableTypeDefinition);

        HierarchicalTypeDefinition<TraitType> fetlTypeDefinition =
                createTraitTypeDef(TRAIT_TYPE,
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));
        typeDefinitions.add(fetlTypeDefinition);

        typeSystem.defineTypes(
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(fetlTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));

        return typeDefinitions;
    }

    private void submitTypes(List<HierarchicalTypeDefinition> typeDefinitions) throws Exception {
        for (HierarchicalTypeDefinition typeDefinition : typeDefinitions) {
            String typesAsJSON = TypesSerialization.toJson(
                    typeSystem, typeDefinition.typeName);

            WebResource resource = service
                    .path("api/metadata/types/submit")
                    .path(typeDefinition.typeName);

            ClientResponse clientResponse = resource
                    .accept(MediaType.APPLICATION_JSON)
                    .type(MediaType.APPLICATION_JSON)
                    .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
            Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

            String responseAsString = clientResponse.getEntity(String.class);
            Assert.assertNotNull(responseAsString);

            JSONObject response = new JSONObject(responseAsString);
            Assert.assertEquals(response.get("typeName"), typeDefinition.typeName);
            Assert.assertNotNull(response.get("types"));
            Assert.assertNotNull(response.get("requestId"));
        }
    }

    protected ITypedReferenceableInstance createHiveTableInstance() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance = new Referenceable(TABLE_TYPE, TRAIT_TYPE);
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set(DATABASE_TYPE, databaseInstance);

        Struct traitInstance = (Struct) tableInstance.getTrait(TRAIT_TYPE);
        traitInstance.set("level", 1);

        ClassType tableType = typeSystem.getDataType(ClassType.class, TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }
}
