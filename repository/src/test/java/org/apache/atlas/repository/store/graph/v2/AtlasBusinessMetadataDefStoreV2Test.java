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
package org.apache.atlas.repository.store.graph.v2;

import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;

import static org.apache.atlas.utils.TestLoadModelUtils.loadBaseModel;
import static org.apache.atlas.utils.TestLoadModelUtils.loadFsModel;
import static org.apache.atlas.utils.TestLoadModelUtils.loadHiveModel;

/* Please note that for these tests, since the typeRegistry can be injected only once,
 * any new tests should make sure that they flush the type registry at the end of the test.
 * testNG does not provide a way to execute a method after each test has completed the run, hence
 * we have to manually make sure that the flushTypeRegistry method is invoked.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasBusinessMetadataDefStoreV2Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefGraphStoreV2 typeDefStore;

    private AtlasTypesDef typesDefs;

    private static int randomCount;
    private static final String TEST_BUSINESS_METADATA = "test_businessMetadata";
    private String businessMetadataName;
    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
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

    @Test
    public void createBusinessMetadataDef() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);
        Assert.assertEquals(typeRegistry.getAllBusinessMetadataDefs().size(), 1);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, Map<String, AtlasBusinessAttribute>> m1 = entityType.getBusinessAttributes();
        Assert.assertEquals(m1.get(businessMetadataName).size(), 2);
    }

    @Test
    public void deleteBusinessMetadataDefs() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);
        for (AtlasBusinessMetadataDef atlasBusinessMetaDataDef : typesDefs.getBusinessMetadataDefs()) {
            if (atlasBusinessMetaDataDef.getName().equals(businessMetadataName)) {
                typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyList());
                typesDefs.setBusinessMetadataDefs(Arrays.asList(atlasBusinessMetaDataDef));
                typeDefStore.deleteTypesDef(typesDefs);
            }
        }

        for (AtlasBusinessMetadataDef businessMetadataDef : typeRegistry.getAllBusinessMetadataDefs()) {
            Assert.assertNotEquals(businessMetadataDef.getName(), businessMetadataName);
        }
    }

    @Test
    public void updateBusinessMetadataDefs() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);
        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);
        Assert.assertNotNull(businessMetadataDef);

        addBusinessAttribute(businessMetadataDef, "test_businessMetadata_attribute3", Collections.singleton("hive_table"),
                String.format("array<%s>", "string"), AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        updateBusinessMetadataDefs(businessMetadataDef);
        typeDefStore.updateTypesDef(typesDefs);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, Map<String, AtlasBusinessAttribute>> m1 = entityType.getBusinessAttributes();
        Assert.assertEquals(m1.get(businessMetadataName).size(), 3);
    }

    /**
     * Test to verify that we cannot delete attribute defs from a businessMetadata definition
     * @throws AtlasBaseException
     */
    @Test
    public void updateTypeDefsWithoutApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);
        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);
        Assert.assertNotNull(businessMetadataDef);

        AtlasStructDef.AtlasAttributeDef businessAttributeDef = businessMetadataDef.getAttributeDefs().iterator().next();
        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Arrays.asList(businessMetadataDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateTypeDefsDeleteApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);
        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);
        Assert.assertNotNull(businessMetadataDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it = businessMetadataDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef businessAttributeDef = it.next();
        AtlasStructDef.AtlasAttributeDef businessAttributeDef2 = it.next();

        businessAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.emptySet()));

        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef, businessAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Arrays.asList(businessMetadataDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefDeleteApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef = findBusinessMetadataDef(businessMetadataName);
        Assert.assertNotNull(businessMetadataDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it = businessMetadataDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef businessAttributeDef = it.next();
        AtlasStructDef.AtlasAttributeDef businessAttributeDef2 = it.next();

        businessAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));

        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef, businessAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setBusinessMetadataDefs(Arrays.asList(businessMetadataDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefAddApplicableEntityTypes() throws AtlasBaseException {
        createBusinessMetadataTypes(businessMetadataName);

        AtlasBusinessMetadataDef businessMetadataDef           = findBusinessMetadataDef(businessMetadataName);
        AtlasStructDef.AtlasAttributeDef businessAttributeDef1 = businessMetadataDef.getAttributeDefs().get(0);
        AtlasStructDef.AtlasAttributeDef businessAttributeDef2 = businessMetadataDef.getAttributeDefs().get(1);
        Set<String>                      applicableEntityTypes  = AtlasType.fromJson(businessAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        if (applicableEntityTypes == null) {
            applicableEntityTypes = new HashSet<>();
        }

        applicableEntityTypes.add("hive_column");
        businessAttributeDef1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        businessMetadataDef.setAttributeDefs(Arrays.asList(businessAttributeDef1, businessAttributeDef2));

        updateBusinessMetadataDefs(businessMetadataDef);

        typeDefStore.updateTypesDef(typesDefs);

        businessMetadataDef = findBusinessMetadataDef(businessMetadataName);
        businessAttributeDef1 = businessMetadataDef.getAttributeDefs().get(0);

        applicableEntityTypes  = AtlasType.fromJson(businessAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        Assert.assertEquals(applicableEntityTypes == null ? 0 : applicableEntityTypes.size(), 3);
    }

    @Test
    public void validateMaxStringLengthForStringTypes() throws AtlasBaseException {
        AtlasTypesDef existingTypeDefs = typesDefs;
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "string",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        typesDefs.setBusinessMetadataDefs(Arrays.asList(businessMetadataDef1));
        try {
            typeDefStore.createTypesDef(typesDefs);
        } catch (AtlasBaseException exception) {
            Assert.assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDef(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute2", Collections.singleton("hive_table"), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        return businessMetadataDef1;
    }

    private AtlasBusinessMetadataDef createBusinessMetadataDef2(String businessMetadataName) {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef(businessMetadataName, "test_description", null);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute1", Collections.emptySet(), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addBusinessAttribute(businessMetadataDef1, "test_businessMetadata_attribute2", Collections.singleton("hive_table"), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        return businessMetadataDef1;
    }

    private void createBusinessMetadataTypes(String businessMetadataName) throws AtlasBaseException {
        List<AtlasBusinessMetadataDef> businessMetadataDefs = new ArrayList(typesDefs.getBusinessMetadataDefs());
        businessMetadataDefs.add(createBusinessMetadataDef(businessMetadataName));
        typesDefs.setBusinessMetadataDefs(businessMetadataDefs);
        typeDefStore.createTypesDef(typesDefs);
    }

    private void addBusinessAttribute(AtlasBusinessMetadataDef businessMetadataDef, String name, Set<String> applicableEntityTypes,
                                      String typeName, AtlasStructDef.AtlasAttributeDef.Cardinality cardinality) {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef(name, typeName);

        attributeDef.setCardinality(cardinality);
        attributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        attributeDef.setIsOptional(true);
        attributeDef.setIsUnique(false);

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