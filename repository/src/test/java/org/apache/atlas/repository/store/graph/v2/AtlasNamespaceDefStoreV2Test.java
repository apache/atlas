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
import org.apache.atlas.model.typedef.AtlasNamespaceDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasNamespaceType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.model.typedef.AtlasNamespaceDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.*;

/* Please note that for these tests, since the typeRegistry can be injected only once,
 * any new tests should make sure that they flush the type registry at the end of the test.
 * testNG does not provide a way to execute a method after each test has completed the run, hence
 * we have to manually make sure that the flushTypeRegistry method is invoked.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasNamespaceDefStoreV2Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefGraphStoreV2 typeDefStore;

    private AtlasTypesDef typesDefs;

    private static int randomCount;
    private static final String TEST_NAMESPACE = "test_namespace";
    private String namespaceName;
    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
        loadFsModel(typeDefStore, typeRegistry);
        loadHiveModel(typeDefStore, typeRegistry);

        typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        namespaceName = TEST_NAMESPACE;

        randomCount = 1;
    }

    @BeforeMethod
    public void setTypeDefs() {
        typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        randomCount++;
        namespaceName = TEST_NAMESPACE + randomCount;
    }

    @Test
    public void createNamespaceTypeDef() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);
        Assert.assertEquals(typeRegistry.getAllNamespaceDefs().size(), 1);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, List<AtlasNamespaceType.AtlasNamespaceAttribute>> m1 = entityType.getNamespaceAttributes();
        Assert.assertEquals(m1.get(namespaceName).size(), 2);
    }

    @Test
    public void deleteNamespaceTypeDefs() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);
        for (AtlasNamespaceDef atlasNamespaceDef : typesDefs.getNamespaceDefs()) {
            if (atlasNamespaceDef.getName().equals(namespaceName)) {
                typesDefs = new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyList());
                typesDefs.setNamespaceDefs(Arrays.asList(atlasNamespaceDef));
                typeDefStore.deleteTypesDef(typesDefs);
            }
        }

        for (AtlasNamespaceDef namespaceDef : typeRegistry.getAllNamespaceDefs()) {
            Assert.assertNotEquals(namespaceDef.getName(), namespaceName);
        }
    }

    @Test
    public void updateNamespaceTypeDefs() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);
        AtlasNamespaceDef namespaceDef = findNamespaceDef(namespaceName);
        Assert.assertNotNull(namespaceDef);

        addNamespaceAttribute(namespaceDef, "test_namespace_attribute3", Collections.singleton("hive_table"),
                String.format("array<%s>", "string"), AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        updateNamespaceTypeDefs(namespaceDef);
        typeDefStore.updateTypesDef(typesDefs);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName("hive_table");
        Map<String, List<AtlasNamespaceType.AtlasNamespaceAttribute>> m1 = entityType.getNamespaceAttributes();
        Assert.assertEquals(m1.get(namespaceName).size(), 3);
    }

    /**
     * Test to verify that we cannot delete attribute defs from a namespace definition
     * @throws AtlasBaseException
     */
    @Test
    public void updateTypeDefsWithoutApplicableEntityTypes() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);
        AtlasNamespaceDef namespaceDef = findNamespaceDef(namespaceName);
        Assert.assertNotNull(namespaceDef);

        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef = namespaceDef.getAttributeDefs().iterator().next();
        namespaceDef.setAttributeDefs(Arrays.asList(namespaceAttributeDef));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setNamespaceDefs(Arrays.asList(namespaceDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateTypeDefsDeleteApplicableEntityTypes() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);
        AtlasNamespaceDef namespaceDef = findNamespaceDef(namespaceName);
        Assert.assertNotNull(namespaceDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it = namespaceDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef = it.next();
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef2 = it.next();

        namespaceAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.emptySet()));

        namespaceDef.setAttributeDefs(Arrays.asList(namespaceAttributeDef, namespaceAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setNamespaceDefs(Arrays.asList(namespaceDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    /**
     * Test to verify that we cannot have an empty applicable entity types in an attribute definition
     * @throws AtlasBaseException
     */
    @Test
    public void createNsAttrDefWithoutApplicableEntityTypes() {
        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setNamespaceDefs(Arrays.asList(createNamespaceDef2(namespaceName)));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefDeleteApplicableEntityTypes() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);

        AtlasNamespaceDef namespaceDef = findNamespaceDef(namespaceName);
        Assert.assertNotNull(namespaceDef);

        Iterator<AtlasStructDef.AtlasAttributeDef> it = namespaceDef.getAttributeDefs().iterator();
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef = it.next();
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef2 = it.next();

        namespaceAttributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));

        namespaceDef.setAttributeDefs(Arrays.asList(namespaceAttributeDef, namespaceAttributeDef2));

        AtlasTypesDef existingTypeDefs = typesDefs;

        try {
            typesDefs.setNamespaceDefs(Arrays.asList(namespaceDef));
            typeDefStore.updateTypesDef(typesDefs);
        } catch (AtlasBaseException e) {
            Assert.assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    @Test
    public void updateNsAttrDefAddApplicableEntityTypes() throws AtlasBaseException {
        createNamespaceTypes(namespaceName);

        AtlasNamespaceDef                namespaceDef           = findNamespaceDef(namespaceName);
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef1 = namespaceDef.getAttributeDefs().get(0);
        AtlasStructDef.AtlasAttributeDef namespaceAttributeDef2 = namespaceDef.getAttributeDefs().get(1);
        Set<String>                      applicableEntityTypes  = AtlasType.fromJson(namespaceAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        if (applicableEntityTypes == null) {
            applicableEntityTypes = new HashSet<>();
        }

        applicableEntityTypes.add("hive_column");
        namespaceAttributeDef1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        namespaceDef.setAttributeDefs(Arrays.asList(namespaceAttributeDef1, namespaceAttributeDef2));

        updateNamespaceTypeDefs(namespaceDef);

        typeDefStore.updateTypesDef(typesDefs);

        namespaceDef = findNamespaceDef(namespaceName);
        namespaceAttributeDef1 = namespaceDef.getAttributeDefs().get(0);

        applicableEntityTypes  = AtlasType.fromJson(namespaceAttributeDef1.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

        Assert.assertEquals(applicableEntityTypes == null ? 0 : applicableEntityTypes.size(), 3);
    }

    @Test
    public void validateMaxStringLengthForStringTypes() throws AtlasBaseException {
        AtlasTypesDef existingTypeDefs = typesDefs;
        AtlasNamespaceDef namespaceDef1 = new AtlasNamespaceDef(namespaceName, "test_description", null);
        addNamespaceAttribute(namespaceDef1, "test_namespace_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "string",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        typesDefs.setNamespaceDefs(Arrays.asList(namespaceDef1));
        try {
            typeDefStore.createTypesDef(typesDefs);
        } catch (AtlasBaseException exception) {
            Assert.assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE);
        } finally {
            typesDefs = existingTypeDefs;
        }
    }

    private AtlasNamespaceDef createNamespaceDef(String namespaceName) {
        AtlasNamespaceDef namespaceDef1 = new AtlasNamespaceDef(namespaceName, "test_description", null);
        addNamespaceAttribute(namespaceDef1, "test_namespace_attribute1", new HashSet<>(Arrays.asList("hive_table", "fs_path")), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addNamespaceAttribute(namespaceDef1, "test_namespace_attribute2", Collections.singleton("hive_table"), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        return namespaceDef1;
    }

    private AtlasNamespaceDef createNamespaceDef2(String namespaceName) {
        AtlasNamespaceDef namespaceDef1 = new AtlasNamespaceDef(namespaceName, "test_description", null);
        addNamespaceAttribute(namespaceDef1, "test_namespace_attribute1", Collections.emptySet(), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        addNamespaceAttribute(namespaceDef1, "test_namespace_attribute2", Collections.singleton("hive_table"), "int",
                AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        return namespaceDef1;
    }

    private void createNamespaceTypes(String namespaceName) throws AtlasBaseException {
        List<AtlasNamespaceDef> namespaceDefs = new ArrayList(typesDefs.getNamespaceDefs());
        namespaceDefs.add(createNamespaceDef(namespaceName));
        typesDefs.setNamespaceDefs(namespaceDefs);
        typeDefStore.createTypesDef(typesDefs);
    }

    private void addNamespaceAttribute(AtlasNamespaceDef namespaceDef, String name, Set<String> applicableEntityTypes,
                                       String typeName, AtlasStructDef.AtlasAttributeDef.Cardinality cardinality) {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef(name, typeName);

        attributeDef.setCardinality(cardinality);
        attributeDef.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(applicableEntityTypes));
        attributeDef.setIsOptional(true);
        attributeDef.setIsUnique(false);

        namespaceDef.addAttribute(attributeDef);
    }

    private AtlasNamespaceDef findNamespaceDef(String namespaceName) {
        for (AtlasNamespaceDef atlasNamespaceDef : typesDefs.getNamespaceDefs()) {
            if (atlasNamespaceDef.getName().equals(namespaceName)) {
                return atlasNamespaceDef;
            }
        }

        return null;
    }

    private void updateNamespaceTypeDefs(AtlasNamespaceDef atlasNamespaceDef) {
        for (int i = 0; i < typesDefs.getNamespaceDefs().size(); i++) {
            if (typesDefs.getNamespaceDefs().get(i).getName().equals(namespaceName)) {
                typesDefs.getNamespaceDefs().set(i, atlasNamespaceDef);
            }
        }
    }
}