/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_MAX_STRING_LENGTH;
import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;
import static org.apache.atlas.utils.TestLoadModelUtils.loadBaseModel;
import static org.apache.atlas.utils.TestLoadModelUtils.loadFsModel;
import static org.apache.atlas.utils.TestLoadModelUtils.loadHiveModel;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/* Please note that for these tests, since the typeRegistry can be injected only once,
 * any new tests should make sure that they flush the type registry at the end of the test.
 * testNG does not provide a way to execute a method after each test has completed the run, hence
 * we have to manually make sure that the flushTypeRegistry method is invoked.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasBusinessMetadataDefStoreV2Test {
    private static final String TEST_BUSINESS_METADATA = "test_businessMetadata";

    private static int randomCount;

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefGraphStoreV2 typeDefStore;

    private AtlasTypesDef typesDefs;
    private String         businessMetadataName;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);

        loadBaseModel(typeDefStore, typeRegistry);
        loadFsModel(typeDefStore, typeRegistry);
        loadHiveModel(typeDefStore, typeRegistry);

        typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        businessMetadataName = TEST_BUSINESS_METADATA;

        randomCount = 1;
    }

    @BeforeMethod
    public void setTypeDefs() {
        typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        randomCount++;
        businessMetadataName = TEST_BUSINESS_METADATA + randomCount;
    }

    @Test(priority = -1)
    public void createBusinessMetadataDef() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        assertEquals(typeRegistry.getAllBusinessMetadataDefs().size(), 1);

        AtlasEntityType                                  entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, Map<String, AtlasBusinessAttribute>> m1         = entityType.getBusinessAttributes();

        assertEquals(m1.get(businessMetadataName).size(), 2);
    }

    @Test
    public void createBusinessMetadataDefWithoutAttributes() throws AtlasBaseException {
        createBusinessMetadataTypesWithoutAttributes(businessMetadataName);

        AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(businessMetadataName);

        assertTrue(businessMetadataType.getAllAttributes() == null || businessMetadataType.getAllAttributes().isEmpty());
    }

    @Test
    public void createBusinessMetadataDefIsOptionalIsUnique() throws AtlasBaseException {
        createBusinessMetadataTypesIsOptionalIsUnique(businessMetadataName);

        AtlasBusinessMetadataType        businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(businessMetadataName);
        AtlasStructType.AtlasAttribute   atlasAttribute       = businessMetadataType.getAttribute("test_business_attribute1");
        AtlasStructDef.AtlasAttributeDef atlasAttributeDef    = atlasAttribute.getAttributeDef();

        assertFalse(atlasAttributeDef.getIsOptional());
        assertTrue(atlasAttributeDef.getIsUnique());
    }

    @Test
    public void createBusinessMetadataDefParentApplicableType() throws AtlasBaseException {
        createBusinessMetadataTypesParentApplicableType(businessMetadataName);

        AtlasEntityType                  entityType        = typeRegistry.getEntityTypeByName("hive_table");
        AtlasBusinessAttribute           businessAttribute = entityType.getBusinessAttribute(businessMetadataName, "test_business_attribute_asset_type");
        AtlasStructDef.AtlasAttributeDef attributeDef      = businessAttribute.getAttributeDef();
        String                           applicableType    = attributeDef.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);

        assertEquals(applicableType, "[\"Asset\"]");
    }

    @Test
    public void createBusinessMetadataDefMultivaluedAttributes() throws AtlasBaseException {
        createEnumTypes();
        createBusinessMetadataTypesMultivaluedAttributes(businessMetadataName);

        AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(businessMetadataName);

        assertEquals(businessMetadataType.getAllAttributes().size(), 10);

        Map<String, AtlasStructType.AtlasAttribute> attributeMap = businessMetadataType.getAllAttributes();

        for (Map.Entry<String, AtlasStructType.AtlasAttribute> e : attributeMap.entrySet()) {
            AtlasStructType.AtlasAttribute   atlasAttribute    = e.getValue();
            AtlasStructDef.AtlasAttributeDef atlasAttributeDef = atlasAttribute.getAttributeDef();

            assertTrue(atlasAttributeDef.getTypeName().startsWith("array<"));
        }
    }

    @Test
    public void deleteBusinessMetadataDefs() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        for (AtlasBusinessMetadataDef atlasBusinessMetaDataDef : typesDefs.getBusinessMetadataDefs()) {
            if (atlasBusinessMetaDataDef.getName().equals(businessMetadataName)) {
                typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

                typesDefs.setBusinessMetadataDefs(Collections.singletonList(atlasBusinessMetaDataDef));

                typeDefStore.deleteTypesDef(typesDefs);
            }
        }

        for (AtlasBusinessMetadataDef businessMetadataDef : typeRegistry.getAllBusinessMetadataDefs()) {
            assertNotEquals(businessMetadataDef.getName(), businessMetadataName);
        }
    }

    @Test
    public void deleteBusinessMetadataDefWithNoAssignedTypes() throws AtlasBaseException {
        createBusinessMetadataTypesWithoutAssignedTypes(businessMetadataName);

        for (AtlasBusinessMetadataDef atlasBusinessMetaDataDef : typesDefs.getBusinessMetadataDefs()) {
            if (atlasBusinessMetaDataDef.getName().equals(businessMetadataName)) {
                typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

                typesDefs.setBusinessMetadataDefs(Collections.singletonList(atlasBusinessMetaDataDef));

                typeDefStore.deleteTypesDef(typesDefs);
            }
        }

        for (AtlasBusinessMetadataDef businessMetadataDef : typeRegistry.getAllBusinessMetadataDefs()) {
            assertNotEquals(businessMetadataDef.getName(), businessMetadataName);
        }
    }

    @Test
    public void updateBusinessMetadataDefs() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);

        assertNotNull(businessMetadataDef);

        addBusinessAttribute(businessMetadataDef, "test_businessMetadata_attribute3", Collections.singleton("hive_table"), String.format("array<%s>", "string"), AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        updateBusinessMetadataDefs(businessMetadataDef);

        typeDefStore.updateTypesDef(typesDefs);

        AtlasEntityType                                  entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, Map<String, AtlasBusinessAttribute>> m1         = entityType.getBusinessAttributes();

        assertEquals(m1.get(businessMetadataName).size(), 3);
    }

    /**
     * Test to verify that we cannot delete attribute defs from a businessMetadata definition
     * @throws AtlasBaseException
     */
    @Test
    public void updateTypeDefsWithoutApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);

        assertNotNull(businessMetadataDef);

        AtlasStructDef.AtlasAttributeDef businessAttributeDef = businessMetadataDef.getAttributeDefs().iterator().next();

        businessMetadataDef.setAttributeDefs(Collections.singletonList(businessAttributeDef));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Collections.singletonList(businessMetadataDef));

            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateTypeDefsDeleteApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);

        assertNotNull(businessMetadataDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it                    = businessMetadataDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef           businessAttributeDef  = it.next();
        AtlasStructDef.AtlasAttributeDef           businessAttributeDef2 = it.next();

        businessAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.emptySet()));

        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef, businessAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Collections.singletonList(businessMetadataDef));

            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefDeleteApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);

        assertNotNull(businessMetadataDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it                    = businessMetadataDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef           businessAttributeDef  = it.next();
        AtlasStructDef.AtlasAttributeDef           businessAttributeDef2 = it.next();

        businessAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));

        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef, businessAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Collections.singletonList(businessMetadataDef));

            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefAddApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef         businessMetadataDef   = findBusinessMetadataDef(businessMetadataName);
        AtlasStructDef.AtlasAttributeDef businessAttributeDef1 = businessMetadataDef.getAttributeDefs().get(0);
        AtlasStructDef.AtlasAttributeDef businessAttributeDef2 = businessMetadataDef.getAttributeDefs().get(1);
        Set<String>                      applicableEntityTypes = AtlasType.fromJson(businessAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        if (applicableEntityTypes == null) {
            applicableEntityTypes = new HashSet<>();
        }

        applicableEntityTypes.add("hive_column");

        businessAttributeDef1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef1, businessAttributeDef2));

        updateBusinessMetadataDefs(businessMetadataDef);

        typeDefStore.updateTypesDef(typesDefs);

        businessMetadataDef   = findBusinessMetadataDef(businessMetadataName);
        businessAttributeDef1 = businessMetadataDef.getAttributeDefs().get(0);

        applicableEntityTypes = AtlasType.fromJson(businessAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        assertEquals(applicableEntityTypes == null ? 0 : applicableEntityTypes.size(), 3);
    }

    @Test
    public void validateMaxStringLengthForStringTypes() {
        AtlasTypesDef            existingTypeDefs     = typesDefs;
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "string", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        typesDefs.setBusinessMetadataDefs(Collections.singletonList(businessMetadataDef1));

        try {
            typeDefStore.createTypesDef(typesDefs);
        } catch (AtlasBaseException exception) {
            assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    private void createBusinessMetadataTypesWithoutAttributes(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDefWithoutAttributes(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        typeDefStore.createTypesDef(typesDefs);
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDefWithoutAttributes(String businessMetadataName) {
        return new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);
    }

    private void createBusinessMetadataTypesIsOptionalIsUnique(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDefIsOptionalIsUnique(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        typeDefStore.createTypesDef(typesDefs);
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDefIsOptionalIsUnique(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);

        addBusinessAttribute(businessMetadataDef, "test_business_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false, true);

        return businessMetadataDef;
    }

    private void addBusinessAttribute(AtlasBusinessMetadataDef businessMetadataDef, String name, Set<String> applicableEntityTypes, String typeName, AtlasStructDef.AtlasAttributeDef.Cardinality cardinality, boolean isOptional, boolean isUnique) {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef(name, typeName);

        attributeDef.setCardinality(cardinality);
        attributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        attributeDef.setIsOptional(isOptional);
        attributeDef.setIsUnique(isUnique);

        businessMetadataDef.addAttribute(attributeDef);
    }

    private void createBusinessMetadataTypesParentApplicableType(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDefParentApplicableType(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        typeDefStore.createTypesDef(typesDefs);
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDefParentApplicableType(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);

        addBusinessAttribute(businessMetadataDef, "test_business_attribute_asset_type", new HashSet<>(Collections.singletonList("Asset")), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        return businessMetadataDef;
    }

    private void createEnumTypes() {
        List<AtlasEnumDef> atlasEnumDefs = new ArrayList<>(typesDefs.getEnumDefs());
        String             description   = "_description";
        AtlasEnumDef myEnum = new AtlasEnumDef("ENUM_1", "ENUM_1" + description, "1.0",
                        Arrays.asList(new AtlasEnumDef.AtlasEnumElementDef("USER", "Element" + description, 1),
                                new AtlasEnumDef.AtlasEnumElementDef("ROLE", "Element" + description, 2),
                                new AtlasEnumDef.AtlasEnumElementDef("GROUP", "Element" + description, 3)));

        atlasEnumDefs.add(myEnum);

        typesDefs.setEnumDefs(atlasEnumDefs);
    }

    private void createBusinessMetadataTypesMultivaluedAttributes(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDefMultivaluedAttributes(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        typeDefStore.createTypesDef(typesDefs);
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDefMultivaluedAttributes(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);

        addBusinessAttribute(businessMetadataDef, "test_business_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<boolean>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute2", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<byte>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute3", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<short>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute4", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<int>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute5", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<long>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute6", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<float>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute7", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<double>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute8", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<string>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute9", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<date>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef, "test_business_attribute10", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "array<ENUM_1>", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        return businessMetadataDef;
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDef(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);

        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute2", Collections.singleton("hive_table"), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        TestUtilsV2.populateSystemAttributes(businessMetadataDef1);

        return businessMetadataDef1;
    }

    private void createBusinessMetadataTypesWithoutAssignedTypes(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDefWithoutAssignedTypes(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        AtlasTypesDef createdTypesDef = typeDefStore.createTypesDef(typesDefs);

        assertEquals(createdTypesDef.getBusinessMetadataDefs(), businessMetadataDefs, "Data integrity issue while persisting");
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDefWithoutAssignedTypes(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_no_attributes", null);

        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", Collections.emptySet(), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        TestUtilsV2.populateSystemAttributes(businessMetadataDef1);

        return businessMetadataDef1;
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDef2(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);

        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", Collections.emptySet(), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute2", Collections.singleton("hive_table"), "int", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        return businessMetadataDef1;
    }

    private void createBusinessMetadataTypes(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList<>(typesDefs.getBusinessMetadataDefs());

        businessMetadataDefs.add(createBusinessMetadataDef(businessMetadataName));

        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);

        AtlasTypesDef createdTypesDef = typeDefStore.createTypesDef(typesDefs);

        assertEquals(createdTypesDef.getBusinessMetadataDefs(), businessMetadataDefs, "Data integrity issue while persisting");
    }

    private void addBusinessAttribute(AtlasBusinessMetadataDef businessMetadataDef, String name, Set<String> applicableEntityTypes, String typeName, AtlasStructDef.AtlasAttributeDef.Cardinality cardinality) {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef(name, typeName);

        attributeDef.setCardinality(cardinality);
        attributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));

        if (typeName.contains(AtlasBaseTypeDef.ATLAS_TYPE_STRING)) {
            attributeDef.setOption(ATTR_MAX_STRING_LENGTH, "20");
        }

        attributeDef.setIsOptional(true);
        attributeDef.setValuesMinCount(0);
        attributeDef.setValuesMaxCount(1);
        attributeDef.setIsUnique(false);
        attributeDef.setDisplayName(name);

        businessMetadataDef.addAttribute(attributeDef);
    }

    private AtlasBusinessMetadataDef findBusinessMetadataDef(String businessMetadataName) {
        for (AtlasBusinessMetadataDef atlasBusinessMetaDataDef : typesDefs.getBusinessMetadataDefs()) {
            if (atlasBusinessMetaDataDef.getName().equals(businessMetadataName)) {
                return atlasBusinessMetaDataDef;
            }
        }

        return null;
    }

    private void updateBusinessMetadataDefs(AtlasBusinessMetadataDef atlasBusinessMetaDataDef) {
        for (int i = 0; i < typesDefs.getBusinessMetadataDefs().size(); i++) {
            if (typesDefs.getBusinessMetadataDefs().get(i).getName().equals(businessMetadataName)) {
                typesDefs.getBusinessMetadataDefs().set(i, atlasBusinessMetaDataDef);
            }
        }
    }
}
