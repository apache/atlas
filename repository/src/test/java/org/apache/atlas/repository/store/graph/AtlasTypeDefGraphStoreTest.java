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
package org.apache.atlas.repository.store.graph;

import com.google.inject.Inject;
import org.apache.atlas.TestOnlyModule;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.*;

@Guice(modules = TestOnlyModule.class)
public class AtlasTypeDefGraphStoreTest {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefGraphStoreTest.class);

    @Inject
    private
    AtlasTypeDefStore typeDefStore;

    @AfterClass
    public void clear(){
        AtlasGraphProvider.cleanup();
    }

    @Test(priority = 1)
    public void testGet() {
        try {
            AtlasTypesDef typesDef = typeDefStore.searchTypesDef(new SearchFilter());
            assertNotNull(typesDef.getEnumDefs());
            assertEquals(typesDef.getStructDefs().size(), 0);
            assertNotNull(typesDef.getStructDefs());
            assertEquals(typesDef.getClassificationDefs().size(), 0);
            assertNotNull(typesDef.getClassificationDefs());
            assertEquals(typesDef.getEntityDefs().size(), 0);
            assertNotNull(typesDef.getEntityDefs());
        } catch (AtlasBaseException e) {
            fail("Search of types shouldn't have failed");
        }
    }

    @Test(dataProvider = "invalidGetProvider", priority = 2)
    public void testInvalidGet(String name, String guid){
        try {
            assertNull(typeDefStore.getEnumDefByName(name));
            fail("Exception expected for invalid name");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getEnumDefByGuid(guid));
            fail("Exception expected for invalid guid");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getStructDefByName(name));
            fail("Exception expected for invalid name");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getStructDefByGuid(guid));
            fail("Exception expected for invalid guid");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getClassificationDefByName(name));
            fail("Exception expected for invalid name");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getClassificationDefByGuid(guid));
            fail("Exception expected for invalid guid");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getEntityDefByName(name));
            fail("Exception expected for invalid name");
        } catch (AtlasBaseException e) {
        }

        try {
            assertNull(typeDefStore.getEntityDefByGuid(guid));
            fail("Exception expected for invalid guid");
        } catch (AtlasBaseException e) {
        }
    }

    @DataProvider
    public Object[][] invalidGetProvider(){
        return new Object[][] {
                {"name1", "guid1"},
                {"", ""},
                {null, null}
        };
    }

    @DataProvider
    public Object[][] validCreateDeptTypes(){
        return new Object[][] {
                {TestUtilsV2.defineDeptEmployeeTypes()}
        };
    }

    @DataProvider
    public Object[][] validUpdateDeptTypes(){
        AtlasTypesDef typesDef = TestUtilsV2.defineValidUpdatedDeptEmployeeTypes();
        return new Object[][] {
                {typesDef}
        };
    }

    @DataProvider
    public Object[][] allCreatedTypes(){
        // Capture all the types that are getting created or updated here.
        AtlasTypesDef updatedTypeDefs = TestUtilsV2.defineValidUpdatedDeptEmployeeTypes();
        AtlasTypesDef allTypeDefs = new AtlasTypesDef();
        allTypeDefs.getEnumDefs().addAll(updatedTypeDefs.getEnumDefs());
        allTypeDefs.getStructDefs().addAll(updatedTypeDefs.getStructDefs());
        allTypeDefs.getClassificationDefs().addAll(updatedTypeDefs.getClassificationDefs());
        allTypeDefs.getEntityDefs().addAll(updatedTypeDefs.getEntityDefs());
        allTypeDefs.getEntityDefs().addAll(TestUtilsV2.getEntityWithValidSuperType());
        return new Object[][] {{allTypeDefs}};
    }

    @DataProvider
    public Object[][] invalidCreateTypes(){
        // TODO: Create invalid type in TestUtilsV2
        return new Object[][] {
        };
    }

    @DataProvider
    public Object[][] invalidUpdateTypes(){
        return new Object[][] {
                {TestUtilsV2.defineInvalidUpdatedDeptEmployeeTypes()}
        };
    }

    @Test(dependsOnMethods = {"testGet"}, dataProvider = "validCreateDeptTypes")
    public void testCreateDept(AtlasTypesDef atlasTypesDef) {
        AtlasTypesDef existingTypesDef = null;
        try {
            existingTypesDef = typeDefStore.searchTypesDef(new SearchFilter());
        } catch (AtlasBaseException e) {
            // ignore
        }

        assertNotEquals(atlasTypesDef, existingTypesDef, "Types to be created already exist in the system");
        AtlasTypesDef createdTypesDef = null;
        try {
            createdTypesDef = typeDefStore.createTypesDef(atlasTypesDef);
            assertNotNull(createdTypesDef);
            assertTrue(createdTypesDef.getEnumDefs().containsAll(atlasTypesDef.getEnumDefs()), "EnumDefs create failed");
            assertTrue(createdTypesDef.getClassificationDefs().containsAll(atlasTypesDef.getClassificationDefs()), "ClassificationDef create failed");
            assertTrue(createdTypesDef.getStructDefs().containsAll(atlasTypesDef.getStructDefs()), "StructDef creation failed");
            Assert.assertEquals(createdTypesDef.getEntityDefs(), atlasTypesDef.getEntityDefs());

        } catch (AtlasBaseException e) {
            fail("Creation of Types should've been a success", e);
        }
    }

    @Test(dependsOnMethods = {"testCreateDept"}, dataProvider = "validUpdateDeptTypes")
    public void testUpdate(AtlasTypesDef atlasTypesDef){
        try {
            AtlasTypesDef updatedTypesDef = typeDefStore.updateTypesDef(atlasTypesDef);
            assertNotNull(updatedTypesDef);

            assertEquals(updatedTypesDef.getEnumDefs().size(), atlasTypesDef.getEnumDefs().size(), "EnumDefs update failed");
            assertEquals(updatedTypesDef.getClassificationDefs().size(), atlasTypesDef.getClassificationDefs().size(), "ClassificationDef update failed");
            assertEquals(updatedTypesDef.getStructDefs().size(), atlasTypesDef.getStructDefs().size(), "StructDef update failed");
            assertEquals(updatedTypesDef.getEntityDefs().size(), atlasTypesDef.getEntityDefs().size(), "EntityDef update failed");

            // Try another update round by name and GUID
            for (AtlasEnumDef enumDef : updatedTypesDef.getEnumDefs()) {
                AtlasEnumDef updated = typeDefStore.updateEnumDefByGuid(enumDef.getGuid(), enumDef);
                assertNotNull(updated);
            }
            for (AtlasEnumDef enumDef : atlasTypesDef.getEnumDefs()) {
                AtlasEnumDef updated = typeDefStore.updateEnumDefByName(enumDef.getName(), enumDef);
                assertNotNull(updated);
            }

            // Try another update round by name and GUID
            for (AtlasClassificationDef classificationDef : updatedTypesDef.getClassificationDefs()) {
                AtlasClassificationDef updated = typeDefStore.updateClassificationDefByGuid(classificationDef.getGuid(), classificationDef);
                assertNotNull(updated);
            }
            for (AtlasClassificationDef classificationDef : atlasTypesDef.getClassificationDefs()) {
                AtlasClassificationDef updated = typeDefStore.updateClassificationDefByName(classificationDef.getName(), classificationDef);
                assertNotNull(updated);
            }

            // Try another update round by name and GUID
            for (AtlasStructDef structDef : updatedTypesDef.getStructDefs()) {
                AtlasStructDef updated = typeDefStore.updateStructDefByGuid(structDef.getGuid(), structDef);
                assertNotNull(updated);
            }
            for (AtlasStructDef structDef : atlasTypesDef.getStructDefs()) {
                AtlasStructDef updated = typeDefStore.updateStructDefByName(structDef.getName(), structDef);
                assertNotNull(updated);
            }

            // Try another update round by name and GUID
            for (AtlasEntityDef entityDef : updatedTypesDef.getEntityDefs()) {
                AtlasEntityDef updated = typeDefStore.updateEntityDefByGuid(entityDef.getGuid(), entityDef);
                assertNotNull(updated);
            }
            for (AtlasEntityDef entityDef : atlasTypesDef.getEntityDefs()) {
                AtlasEntityDef updated = typeDefStore.updateEntityDefByName(entityDef.getName(), entityDef);
                assertNotNull(updated);
            }

        } catch (AtlasBaseException e) {
            fail("TypeDef updates should've succeeded");
        }
    }

    @Test(enabled = false, dependsOnMethods = {"testCreateDept"})
    public void testUpdateWithMandatoryFields(){
        AtlasTypesDef atlasTypesDef = TestUtilsV2.defineInvalidUpdatedDeptEmployeeTypes();
        List<AtlasEnumDef> enumDefsToUpdate = atlasTypesDef.getEnumDefs();
        List<AtlasClassificationDef> classificationDefsToUpdate = atlasTypesDef.getClassificationDefs();
        List<AtlasStructDef> structDefsToUpdate = atlasTypesDef.getStructDefs();
        List<AtlasEntityDef> entityDefsToUpdate = atlasTypesDef.getEntityDefs();

        AtlasTypesDef onlyEnums = new AtlasTypesDef(enumDefsToUpdate,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        AtlasTypesDef onlyStructs = new AtlasTypesDef(Collections.EMPTY_LIST,
                structDefsToUpdate, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        AtlasTypesDef onlyClassification = new AtlasTypesDef(Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, classificationDefsToUpdate, Collections.EMPTY_LIST);

        AtlasTypesDef onlyEntities = new AtlasTypesDef(Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, entityDefsToUpdate);

        try {
            AtlasTypesDef updated = typeDefStore.updateTypesDef(onlyEnums);
            assertNotNull(updated);
        } catch (AtlasBaseException ignored) {}

        try {
            AtlasTypesDef updated = typeDefStore.updateTypesDef(onlyClassification);
            assertNotNull(updated);
            assertEquals(updated.getClassificationDefs().size(), 0, "Updates should've failed");
        } catch (AtlasBaseException ignored) {}

        try {
            AtlasTypesDef updated = typeDefStore.updateTypesDef(onlyStructs);
            assertNotNull(updated);
            assertEquals(updated.getStructDefs().size(), 0, "Updates should've failed");
        } catch (AtlasBaseException ignored) {}

        try {
            AtlasTypesDef updated = typeDefStore.updateTypesDef(onlyEntities);
            assertNotNull(updated);
            assertEquals(updated.getEntityDefs().size(), 0, "Updates should've failed");
        } catch (AtlasBaseException ignored) {}
    }

    // This should run after all the update calls
    @Test(dependsOnMethods = {"testUpdate"}, dataProvider = "allCreatedTypes")
    public void testDelete(AtlasTypesDef atlasTypesDef){
        try {
            typeDefStore.deleteTypesDef(atlasTypesDef);
        } catch (AtlasBaseException e) {
            fail("Deletion should've succeeded");
        }
    }

    @Test(dependsOnMethods = "testGet")
    public void testCreateWithValidAttributes(){
        AtlasTypesDef hiveTypes = TestUtilsV2.defineHiveTypes();
        try {
            AtlasTypesDef createdTypes = typeDefStore.createTypesDef(hiveTypes);
            assertEquals(hiveTypes.getEnumDefs(), createdTypes.getEnumDefs(), "Data integrity issue while persisting");
            assertEquals(hiveTypes.getStructDefs(), createdTypes.getStructDefs(), "Data integrity issue while persisting");
            assertEquals(hiveTypes.getClassificationDefs(), createdTypes.getClassificationDefs(), "Data integrity issue while persisting");
            assertEquals(hiveTypes.getEntityDefs(), createdTypes.getEntityDefs(), "Data integrity issue while persisting");
        } catch (AtlasBaseException e) {
            fail("Hive Type creation should've succeeded");
        }
    }

    @Test(enabled = false)
    public void testCreateWithInvalidAttributes(){
    }

    @Test(dependsOnMethods = "testGet")
    public void testCreateWithValidSuperTypes(){
        // Test Classification with supertype
        List<AtlasClassificationDef> classificationDefs = TestUtilsV2.getClassificationWithValidSuperType();

        AtlasTypesDef toCreate = new AtlasTypesDef(Collections.<AtlasEnumDef>emptyList(),
                Collections.<AtlasStructDef>emptyList(),
                classificationDefs,
                Collections.<AtlasEntityDef>emptyList());
        try {
            AtlasTypesDef created = typeDefStore.createTypesDef(toCreate);
            assertEquals(created.getClassificationDefs(), toCreate.getClassificationDefs(),
                    "Classification creation with valid supertype should've succeeded");
        } catch (AtlasBaseException e) {
            fail("Classification creation with valid supertype should've succeeded");
        }

        // Test Entity with supertype
        List<AtlasEntityDef> entityDefs = TestUtilsV2.getEntityWithValidSuperType();
        toCreate = new AtlasTypesDef(Collections.<AtlasEnumDef>emptyList(),
                Collections.<AtlasStructDef>emptyList(),
                Collections.<AtlasClassificationDef>emptyList(),
                entityDefs);
        try {
            AtlasTypesDef created = typeDefStore.createTypesDef(toCreate);
            assertEquals(created.getEntityDefs(), toCreate.getEntityDefs(),
                    "Entity creation with valid supertype should've succeeded");
        } catch (AtlasBaseException e) {
            fail("Entity creation with valid supertype should've succeeded");
        }
    }

    @Test(dependsOnMethods = "testGet")
    public void testCreateWithInvalidSuperTypes(){
        AtlasTypesDef typesDef;

        // Test Classification with supertype
        AtlasClassificationDef classificationDef = TestUtilsV2.getClassificationWithInvalidSuperType();
        typesDef = new AtlasTypesDef();
        typesDef.getClassificationDefs().add(classificationDef);
        try {
            AtlasTypesDef created = typeDefStore.createTypesDef(typesDef);
            fail("Classification creation with invalid supertype should've failed");
        } catch (AtlasBaseException e) {
            typesDef = null;
        }

        // Test Entity with supertype
        AtlasEntityDef entityDef = TestUtilsV2.getEntityWithInvalidSuperType();
        typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entityDef);
        try {
            AtlasTypesDef created = typeDefStore.createTypesDef(typesDef);
            fail("Entity creation with invalid supertype should've failed");
        } catch (AtlasBaseException e) {}

    }

    @Test(dependsOnMethods = "testGet")
    public void testSearchFunctionality() {
        SearchFilter searchFilter = new SearchFilter();
        searchFilter.setParam(SearchFilter.PARAM_SUPERTYPE, "Person");

        try {
            AtlasTypesDef typesDef = typeDefStore.searchTypesDef(searchFilter);
            assertNotNull(typesDef);
            assertNotNull(typesDef.getEntityDefs());
            assertEquals(typesDef.getEntityDefs().size(), 3);
        } catch (AtlasBaseException e) {
            fail("Search should've succeeded", e);
        }
    }

    @Test
    public void testTypeDeletionAndRecreate() {
        AtlasClassificationDef aTag = new AtlasClassificationDef("testTag");
        AtlasAttributeDef attributeDef = new AtlasAttributeDef("testAttribute", "string", true,
                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, true,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
        aTag.addAttribute(attributeDef);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setClassificationDefs(Arrays.asList(aTag));

        try {
            typeDefStore.createTypesDef(typesDef);
        } catch (AtlasBaseException e) {
            fail("Tag creation should've succeeded");
        }

        try {
            typeDefStore.deleteTypesDef(typesDef);
        } catch (AtlasBaseException e) {
            fail("Tag deletion should've succeeded");
        }

        aTag = new AtlasClassificationDef("testTag");
        attributeDef = new AtlasAttributeDef("testAttribute", "int", true,
                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                false, true,
                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
        aTag.addAttribute(attributeDef);
        typesDef.setClassificationDefs(Arrays.asList(aTag));

        try {
            typeDefStore.createTypesDef(typesDef);
        } catch (AtlasBaseException e) {
            fail("Tag re-creation should've succeeded");
        }
    }

}
