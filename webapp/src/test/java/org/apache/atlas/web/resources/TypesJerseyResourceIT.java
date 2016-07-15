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

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Integration test for types jersey resource.
 */
public class TypesJerseyResourceIT extends BaseResourceIT {

    private List<HierarchicalTypeDefinition> typeDefinitions;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        typeDefinitions = createHiveTypes();
    }

    @AfterClass
    public void tearDown() throws Exception {
        typeDefinitions.clear();
    }

    @Test
    public void testSubmit() throws Exception {
        for (HierarchicalTypeDefinition typeDefinition : typeDefinitions) {
            String typesAsJSON = TypesSerialization.toJson(typeDefinition);
            System.out.println("typesAsJSON = " + typesAsJSON);

            WebResource resource = service.path("api/atlas/types");

            ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                    .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
            assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

            String responseAsString = clientResponse.getEntity(String.class);
            Assert.assertNotNull(responseAsString);

            JSONObject response = new JSONObject(responseAsString);
            JSONArray typesAdded = response.getJSONArray(AtlasClient.TYPES);
            assertEquals(typesAdded.length(), 1);
            assertEquals(typesAdded.getJSONObject(0).getString("name"), typeDefinition.typeName);
            Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        }
    }

    @Test
    public void testDuplicateSubmit() throws Exception {
        HierarchicalTypeDefinition<ClassType> type = TypesUtil.createClassTypeDef(randomString(),
                ImmutableSet.<String>of(), TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        TypesDef typesDef =
                TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                        ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.of(type));
        serviceClient.createType(typesDef);

        try {
            serviceClient.createType(typesDef);
            fail("Expected 409");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.CONFLICT.getStatusCode());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        HierarchicalTypeDefinition<ClassType> typeDefinition = TypesUtil
                .createClassTypeDef(randomString(), ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        List<String> typesCreated = serviceClient.createType(TypesSerialization.toJson(typeDefinition, false));
        assertEquals(typesCreated.size(), 1);
        assertEquals(typesCreated.get(0), typeDefinition.typeName);

        //Add attribute description
        typeDefinition = TypesUtil.createClassTypeDef(typeDefinition.typeName,
            ImmutableSet.<String>of(),
                TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE));
        TypesDef typeDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(typeDefinition));
        List<String> typesUpdated = serviceClient.updateType(typeDef);
        assertEquals(typesUpdated.size(), 1);
        Assert.assertTrue(typesUpdated.contains(typeDefinition.typeName));

        HierarchicalTypeDefinition<ClassType>
                updatedType = serviceClient.getType(typeDefinition.typeName).classTypesAsJavaList().get(0);
        assertEquals(updatedType.attributeDefinitions.length, 2);
    }

    @Test(dependsOnMethods = "testSubmit")
    public void testGetDefinition() throws Exception {
        for (HierarchicalTypeDefinition typeDefinition : typeDefinitions) {
            System.out.println("typeName = " + typeDefinition.typeName);

            WebResource resource = service.path("api/atlas/types").path(typeDefinition.typeName);

            ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                    .method(HttpMethod.GET, ClientResponse.class);
            assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

            String responseAsString = clientResponse.getEntity(String.class);
            Assert.assertNotNull(responseAsString);
            JSONObject response = new JSONObject(responseAsString);
            Assert.assertNotNull(response.get(AtlasClient.DEFINITION));
            Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

            String typesJson = response.getString(AtlasClient.DEFINITION);
            final TypesDef typesDef = TypesSerialization.fromJson(typesJson);
            List<HierarchicalTypeDefinition<ClassType>> hierarchicalTypeDefinitions = typesDef.classTypesAsJavaList();
            for (HierarchicalTypeDefinition<ClassType> classType : hierarchicalTypeDefinitions) {
                for (AttributeDefinition attrDef : classType.attributeDefinitions) {
                    if ("name".equals(attrDef.name)) {
                        assertEquals(attrDef.isIndexable, true);
                        assertEquals(attrDef.isUnique, true);
                    }
                }
            }
        }
    }

    @Test
    public void testGetDefinitionForNonexistentType() throws Exception {
        WebResource resource = service.path("api/atlas/types").path("blah");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test(dependsOnMethods = "testSubmit")
    public void testGetTypeNames() throws Exception {
        WebResource resource = service.path("api/atlas/types");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertNotNull(list);

        //Verify that primitive and core types are not returned
        String typesString = list.join(" ");
        Assert.assertFalse(typesString.contains(" \"__IdType\" "));
        Assert.assertFalse(typesString.contains(" \"string\" "));
    }

    @Test
    public void testGetTraitNames() throws Exception {
        String[] traitsAdded = addTraits();

        WebResource resource = service.path("api/atlas/types");

        ClientResponse clientResponse =
                resource.queryParam("type", DataTypes.TypeCategory.TRAIT.name()).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.GET, ClientResponse.class);
        assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertNotNull(list);
        Assert.assertTrue(list.length() >= traitsAdded.length);
    }

    private String[] addTraits() throws Exception {
        String[] traitNames = {"class_trait", "secure_trait", "pii_trait", "ssn_trait", "salary_trait", "sox_trait",};

        for (String traitName : traitNames) {
            HierarchicalTypeDefinition<TraitType> traitTypeDef =
                    TypesUtil.createTraitTypeDef(traitName, ImmutableSet.<String>of());
            String json = TypesSerialization$.MODULE$.toJson(traitTypeDef, true);
            createType(json);
        }

        return traitNames;
    }

    private List<HierarchicalTypeDefinition> createHiveTypes() throws Exception {
        ArrayList<HierarchicalTypeDefinition> typeDefinitions = new ArrayList<>();

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition = TypesUtil
                .createClassTypeDef("database", ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));
        typeDefinitions.add(databaseTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition = TypesUtil
                .createClassTypeDef("table", ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("columnNames", DataTypes.arrayTypeName(DataTypes.STRING_TYPE)),
                        TypesUtil.createOptionalAttrDef("created", DataTypes.DATE_TYPE), TypesUtil
                                .createOptionalAttrDef("parameters",
                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE)),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        new AttributeDefinition("database", "database", Multiplicity.REQUIRED, false, "database"));
        typeDefinitions.add(tableTypeDefinition);

        HierarchicalTypeDefinition<TraitType> fetlTypeDefinition = TypesUtil
                .createTraitTypeDef("fetl", ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("level", DataTypes.INT_TYPE));
        typeDefinitions.add(fetlTypeDefinition);

        return typeDefinitions;
    }
}
