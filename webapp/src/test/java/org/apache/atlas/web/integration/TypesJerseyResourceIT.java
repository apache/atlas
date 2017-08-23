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

package org.apache.atlas.web.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.core.util.MultivaluedMapImpl;
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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
            try{
                atlasClientV1.getType(typeDefinition.typeName);
            } catch (AtlasServiceException ase){
                String typesAsJSON = TypesSerialization.toJson(typeDefinition, false);
                System.out.println("typesAsJSON = " + typesAsJSON);

                JSONObject response = atlasClientV1.callAPIWithBody(AtlasClient.API.CREATE_TYPE, typesAsJSON);
                Assert.assertNotNull(response);


                JSONArray typesAdded = response.getJSONArray(AtlasClient.TYPES);
                assertEquals(typesAdded.length(), 1);
                assertEquals(typesAdded.getJSONObject(0).getString(NAME), typeDefinition.typeName);
                Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));}
        }
    }

    @Test
    public void testDuplicateSubmit() throws Exception {
        HierarchicalTypeDefinition<ClassType> type = TypesUtil.createClassTypeDef(randomString(),
                ImmutableSet.<String>of(), TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE));
        TypesDef typesDef =
                TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                        ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.of(type));
        atlasClientV1.createType(typesDef);

        try {
            atlasClientV1.createType(typesDef);
            fail("Expected 409");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.CONFLICT.getStatusCode());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        HierarchicalTypeDefinition<ClassType> typeDefinition = TypesUtil
                .createClassTypeDef(randomString(), null, "1.0", ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE));
        List<String> typesCreated = atlasClientV1.createType(TypesSerialization.toJson(typeDefinition, false));
        assertEquals(typesCreated.size(), 1);
        assertEquals(typesCreated.get(0), typeDefinition.typeName);

        //Add attribute description
        typeDefinition = TypesUtil.createClassTypeDef(typeDefinition.typeName, null, "2.0",
                ImmutableSet.<String>of(),
                TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                createOptionalAttrDef(DESCRIPTION, DataTypes.STRING_TYPE));
        TypesDef typeDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(typeDefinition));
        List<String> typesUpdated = atlasClientV1.updateType(typeDef);
        assertEquals(typesUpdated.size(), 1);
        Assert.assertTrue(typesUpdated.contains(typeDefinition.typeName));

        TypesDef updatedTypeDef = atlasClientV1.getType(typeDefinition.typeName);
        assertNotNull(updatedTypeDef);

        HierarchicalTypeDefinition<ClassType> updatedType = updatedTypeDef.classTypesAsJavaList().get(0);
        assertEquals(updatedType.attributeDefinitions.length, 2);
    }

    @Test(dependsOnMethods = "testSubmit")
    public void testGetDefinition() throws Exception {
        for (HierarchicalTypeDefinition typeDefinition : typeDefinitions) {
            System.out.println("typeName = " + typeDefinition.typeName);

            JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.LIST_TYPES, null, typeDefinition.typeName);

            Assert.assertNotNull(response);
            Assert.assertNotNull(response.get(AtlasClient.DEFINITION));
            Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

            String typesJson = response.getString(AtlasClient.DEFINITION);
            final TypesDef typesDef = TypesSerialization.fromJson(typesJson);
            List<HierarchicalTypeDefinition<ClassType>> hierarchicalTypeDefinitions = typesDef.classTypesAsJavaList();
            for (HierarchicalTypeDefinition<ClassType> classType : hierarchicalTypeDefinitions) {
                for (AttributeDefinition attrDef : classType.attributeDefinitions) {
                    if (NAME.equals(attrDef.name)) {
                        assertEquals(attrDef.isIndexable, true);
                        assertEquals(attrDef.isUnique, true);
                    }
                }
            }
        }
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void testGetDefinitionForNonexistentType() throws Exception {
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.LIST_TYPES, null, "blah");
    }

    @Test(dependsOnMethods = "testSubmit")
    public void testGetTypeNames() throws Exception {
        JSONObject response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API.LIST_TYPES, null, (String[]) null);
        Assert.assertNotNull(response);

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

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("type", DataTypes.TypeCategory.TRAIT.name());

        JSONObject response = atlasClientV1.callAPIWithQueryParams(AtlasClient.API.LIST_TYPES, queryParams);
        Assert.assertNotNull(response);

        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertNotNull(list);
        Assert.assertTrue(list.length() >= traitsAdded.length);
    }

    @Test
    public void testListTypesByFilter() throws Exception {
        AttributeDefinition attr = TypesUtil.createOptionalAttrDef("attr", DataTypes.STRING_TYPE);
        String a = createType(TypesSerialization.toJson(
                TypesUtil.createClassTypeDef("A" + randomString(), ImmutableSet.<String>of(), attr), false)).get(0);
        String a1 = createType(TypesSerialization.toJson(
                TypesUtil.createClassTypeDef("A1" + randomString(), ImmutableSet.of(a), attr), false)).get(0);
        String b = createType(TypesSerialization.toJson(
                TypesUtil.createClassTypeDef("B" + randomString(), ImmutableSet.<String>of(), attr), false)).get(0);
        String c = createType(TypesSerialization.toJson(
                TypesUtil.createClassTypeDef("C" + randomString(), ImmutableSet.of(a, b), attr), false)).get(0);

        List<String> results = atlasClientV1.listTypes(DataTypes.TypeCategory.CLASS, a, b);
        assertEquals(results, Arrays.asList(a1), "Results: " + results);
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
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef(DESCRIPTION, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef(QUALIFIED_NAME, DataTypes.STRING_TYPE));
        typeDefinitions.add(databaseTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition = TypesUtil
                .createClassTypeDef("table", ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef(DESCRIPTION, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef(QUALIFIED_NAME, DataTypes.STRING_TYPE),
                        createOptionalAttrDef("columnNames", DataTypes.arrayTypeName(DataTypes.STRING_TYPE)),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        createOptionalAttrDef("parameters",
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
