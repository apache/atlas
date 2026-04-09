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
package org.apache.atlas.type;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAtlasTypeRegistry {
    /*
     *             L0
     *          /      \
     *         /         \
     *      L1_1----      L1_2
     *      /  \    \    /   \
     *     /    \    \  /     \
     *   L2_1  L2_2   L2_3   L2_4
     */
    @Test
    public void testClassificationDefValidHierarchy() {
        AtlasClassificationDef classifiL0   = new AtlasClassificationDef("L0");
        AtlasClassificationDef classifiL1d1 = new AtlasClassificationDef("L1-1");
        AtlasClassificationDef classifiL1d2 = new AtlasClassificationDef("L1-2");
        AtlasClassificationDef classifiL2d1 = new AtlasClassificationDef("L2-1");
        AtlasClassificationDef classifiL2d2 = new AtlasClassificationDef("L2-2");
        AtlasClassificationDef classifiL2d3 = new AtlasClassificationDef("L2-3");
        AtlasClassificationDef classifiL2d4 = new AtlasClassificationDef("L2-4");

        classifiL1d1.addSuperType(classifiL0.getName());
        classifiL1d2.addSuperType(classifiL0.getName());
        classifiL2d1.addSuperType(classifiL1d1.getName());
        classifiL2d2.addSuperType(classifiL1d1.getName());
        classifiL2d3.addSuperType(classifiL1d1.getName());
        classifiL2d3.addSuperType(classifiL1d2.getName());
        classifiL2d4.addSuperType(classifiL1d2.getName());

        classifiL0.addAttribute(new AtlasAttributeDef("L0_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL1d1.addAttribute(new AtlasAttributeDef("L1-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL1d2.addAttribute(new AtlasAttributeDef("L1-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2d1.addAttribute(new AtlasAttributeDef("L2-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2d2.addAttribute(new AtlasAttributeDef("L2-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2d3.addAttribute(new AtlasAttributeDef("L2-3_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2d4.addAttribute(new AtlasAttributeDef("L2-4_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getClassificationDefs().add(classifiL0);
        typesDef.getClassificationDefs().add(classifiL1d1);
        typesDef.getClassificationDefs().add(classifiL1d2);
        typesDef.getClassificationDefs().add(classifiL2d1);
        typesDef.getClassificationDefs().add(classifiL2d2);
        typesDef.getClassificationDefs().add(classifiL2d3);
        typesDef.getClassificationDefs().add(classifiL2d4);

        AtlasTypeRegistry typeRegistry = null;

        try {
            typeRegistry = new AtlasTypeRegistry(typesDef);
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        }
        assertNotNull(typeRegistry);

        validateAllSuperTypes(typeRegistry, "L0", new HashSet<>());
        validateAllSuperTypes(typeRegistry, "L1-1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSuperTypes(typeRegistry, "L1-2", new HashSet<>(Collections.singletonList("L0")));
        validateAllSuperTypes(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateAllSuperTypes(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateAllSuperTypes(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L1-1", "L0", "L1-2")));
        validateAllSuperTypes(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L1-2", "L0")));

        validateSubTypes(typeRegistry, "L0", new HashSet<>(Arrays.asList("L1-1", "L1-2")));
        validateSubTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L2-1", "L2-2", "L2-3")));
        validateSubTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L2-3", "L2-4")));
        validateSubTypes(typeRegistry, "L2-1", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-2", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-3", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-4", new HashSet<>());

        validateAllSubTypes(typeRegistry, "L0", new HashSet<>(Arrays.asList("L1-1", "L1-2", "L2-1", "L2-2", "L2-3", "L2-4")));
        validateAllSubTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L2-1", "L2-2", "L2-3")));
        validateAllSubTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L2-3", "L2-4")));
        validateAllSubTypes(typeRegistry, "L2-1", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-2", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-3", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-4", new HashSet<>());

        validateAttributeNames(typeRegistry, "L0", new HashSet<>(Collections.singletonList("L0_a1")));
        validateAttributeNames(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1")));
        validateAttributeNames(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1")));
        validateAttributeNames(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-1_a1")));
        validateAttributeNames(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-2_a1")));
        validateAttributeNames(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L1-2_a1", "L2-3_a1")));
        validateAttributeNames(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1", "L2-4_a1")));
    }

    @Test
    public void testClassificationDefInvalidHierarchy_Self() {
        AtlasClassificationDef classifiDef1 = new AtlasClassificationDef("classifiDef-1");

        classifiDef1.addSuperType(classifiDef1.getName());

        try {
            AtlasTypeRegistry ignored = new AtlasTypeRegistry(new AtlasTypesDef(null, null, Collections.singletonList(classifiDef1), null, null));

            fail("expected invalid supertype failure");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.CIRCULAR_REFERENCE);
        }
    }

    /*
     *       L2_3
     *           \
     *             L0
     *          /      \
     *         /         \
     *      L1_1----      L1_2
     *      /  \    \    /   \
     *     /    \    \  /     \
     *   L2_1  L2_2   L2_3   L2_4
     */
    @Test
    public void testClassificationDefInvalidHierarchy_CircularRef() {
        AtlasClassificationDef classifiL0   = new AtlasClassificationDef("L0");
        AtlasClassificationDef classifiL1d1 = new AtlasClassificationDef("L1-1");
        AtlasClassificationDef classifiL1d2 = new AtlasClassificationDef("L1-2");
        AtlasClassificationDef classifiL2d1 = new AtlasClassificationDef("L2-1");
        AtlasClassificationDef classifiL2d2 = new AtlasClassificationDef("L2-2");
        AtlasClassificationDef classifiL2d3 = new AtlasClassificationDef("L2-3");
        AtlasClassificationDef classifiL2d4 = new AtlasClassificationDef("L2-4");

        classifiL1d1.addSuperType(classifiL0.getName());
        classifiL1d2.addSuperType(classifiL0.getName());
        classifiL2d1.addSuperType(classifiL1d1.getName());
        classifiL2d2.addSuperType(classifiL1d1.getName());
        classifiL2d3.addSuperType(classifiL1d1.getName());
        classifiL2d3.addSuperType(classifiL1d2.getName());
        classifiL2d4.addSuperType(classifiL1d2.getName());
        classifiL0.addSuperType(classifiL2d3.getName()); // circular-ref

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getClassificationDefs().add(classifiL0);
        typesDef.getClassificationDefs().add(classifiL1d1);
        typesDef.getClassificationDefs().add(classifiL1d2);
        typesDef.getClassificationDefs().add(classifiL2d1);
        typesDef.getClassificationDefs().add(classifiL2d2);
        typesDef.getClassificationDefs().add(classifiL2d3);
        typesDef.getClassificationDefs().add(classifiL2d4);

        try {
            AtlasTypeRegistry ignored = new AtlasTypeRegistry(typesDef);

            fail("expected invalid supertype failure");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.CIRCULAR_REFERENCE);
        }
    }

    /*
     *             L0        L0_1
     *          /      \
     *         /         \
     *      L1_1----      L1_2
     *      /  \    \    /   \
     *     /    \    \  /     \
     *   L2_1  L2_2   L2_3   L2_4
     */
    @Test
    public void testEntityDefValidHierarchy() {
        AtlasEntityDef entL0   = new AtlasEntityDef("L0");
        AtlasEntityDef entL0d1 = new AtlasEntityDef("L0-1");
        AtlasEntityDef entL1d1 = new AtlasEntityDef("L1-1");
        AtlasEntityDef entL1d2 = new AtlasEntityDef("L1-2");
        AtlasEntityDef entL2d1 = new AtlasEntityDef("L2-1");
        AtlasEntityDef entL2d2 = new AtlasEntityDef("L2-2");
        AtlasEntityDef entL2d3 = new AtlasEntityDef("L2-3");
        AtlasEntityDef entL2d4 = new AtlasEntityDef("L2-4");

        entL1d1.addSuperType(entL0.getName());
        entL1d2.addSuperType(entL0.getName());
        entL2d1.addSuperType(entL1d1.getName());
        entL2d2.addSuperType(entL1d1.getName());
        entL2d3.addSuperType(entL1d1.getName());
        entL2d3.addSuperType(entL1d2.getName());
        entL2d4.addSuperType(entL1d2.getName());

        entL0.addAttribute(new AtlasAttributeDef("L0_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1d1.addAttribute(new AtlasAttributeDef("L1-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1d2.addAttribute(new AtlasAttributeDef("L1-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2d1.addAttribute(new AtlasAttributeDef("L2-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2d2.addAttribute(new AtlasAttributeDef("L2-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2d3.addAttribute(new AtlasAttributeDef("L2-3_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2d4.addAttribute(new AtlasAttributeDef("L2-4_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        // set displayNames in L0, L1_1, L2_1
        entL0.setOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "L0_a1");
        entL1d1.setOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "L1-1_a1");
        entL2d1.setOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "L2-1_a1");
        entL2d4.setOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "non-existing-attr");

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL0d1);
        typesDef.getEntityDefs().add(entL1d1);
        typesDef.getEntityDefs().add(entL1d2);
        typesDef.getEntityDefs().add(entL2d1);
        typesDef.getEntityDefs().add(entL2d2);
        typesDef.getEntityDefs().add(entL2d3);
        typesDef.getEntityDefs().add(entL2d4);

        AtlasTypeRegistry typeRegistry = null;

        try {
            typeRegistry = new AtlasTypeRegistry(typesDef);
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        }
        assertNotNull(typeRegistry);

        validateAllSuperTypes(typeRegistry, "L0", new HashSet<>());
        validateAllSuperTypes(typeRegistry, "L1-1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSuperTypes(typeRegistry, "L1-2", new HashSet<>(Collections.singletonList("L0")));
        validateAllSuperTypes(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateAllSuperTypes(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateAllSuperTypes(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L1-1", "L0", "L1-2")));
        validateAllSuperTypes(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L1-2", "L0")));

        validateSubTypes(typeRegistry, "L0", new HashSet<>(Arrays.asList("L1-1", "L1-2")));
        validateSubTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L2-1", "L2-2", "L2-3")));
        validateSubTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L2-3", "L2-4")));
        validateSubTypes(typeRegistry, "L2-1", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-2", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-3", new HashSet<>());
        validateSubTypes(typeRegistry, "L2-4", new HashSet<>());

        validateAllSubTypes(typeRegistry, "L0", new HashSet<>(Arrays.asList("L1-1", "L1-2", "L2-1", "L2-2", "L2-3", "L2-4")));
        validateAllSubTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L2-1", "L2-2", "L2-3")));
        validateAllSubTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L2-3", "L2-4")));
        validateAllSubTypes(typeRegistry, "L2-1", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-2", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-3", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L2-4", new HashSet<>());

        validateAttributeNames(typeRegistry, "L0", new HashSet<>(Collections.singletonList("L0_a1")));
        validateAttributeNames(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1")));
        validateAttributeNames(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1")));
        validateAttributeNames(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-1_a1")));
        validateAttributeNames(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-2_a1")));
        validateAttributeNames(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L1-2_a1", "L2-3_a1")));
        validateAttributeNames(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1", "L2-4_a1")));

        validateDisplayNameAttribute(typeRegistry, "L0", "L0_a1");     // directly assigned for this type
        validateDisplayNameAttribute(typeRegistry, "L0-1");            // not assigned for this type
        validateDisplayNameAttribute(typeRegistry, "L1-1", "L1-1_a1"); // directly assigned for this type
        validateDisplayNameAttribute(typeRegistry, "L1-2", "L0_a1");   // inherits from L0
        validateDisplayNameAttribute(typeRegistry, "L2-1", "L2-1_a1"); // directly assigned for this type
        validateDisplayNameAttribute(typeRegistry, "L2-2", "L1-1_a1"); // inherits from L1-1
        validateDisplayNameAttribute(typeRegistry, "L2-3", "L1-1_a1"); // inherits from L1-1 or L0
        validateDisplayNameAttribute(typeRegistry, "L2-4", "L0_a1");   // invalid-name ignored, inherits from L0
    }

    @Test
    public void testEntityDefInvalidHierarchy_Self() {
        AtlasEntityDef entDef1 = new AtlasEntityDef("entDef-1");

        entDef1.addSuperType(entDef1.getName());

        try {
            AtlasTypeRegistry ignored = new AtlasTypeRegistry(new AtlasTypesDef(null, null, null, Collections.singletonList(entDef1), null));

            fail("expected invalid supertype failure");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.CIRCULAR_REFERENCE);
        }
    }

    /*
     *       L2_3
     *           \
     *             L0
     *          /      \
     *         /         \
     *      L1_1----      L1_2
     *      /  \    \    /   \
     *     /    \    \  /     \
     *   L2_1  L2_2   L2_3   L2_4
     */
    @Test
    public void testEntityDefInvalidHierarchy_CircularRef() {
        AtlasEntityDef entL0   = new AtlasEntityDef("L0");
        AtlasEntityDef entL1d1 = new AtlasEntityDef("L1-1");
        AtlasEntityDef entL1d2 = new AtlasEntityDef("L1-2");
        AtlasEntityDef entL2d1 = new AtlasEntityDef("L2-1");
        AtlasEntityDef entL2d2 = new AtlasEntityDef("L2-2");
        AtlasEntityDef entL2d3 = new AtlasEntityDef("L2-3");
        AtlasEntityDef entL2d4 = new AtlasEntityDef("L2-4");

        entL1d1.addSuperType(entL0.getName());
        entL1d2.addSuperType(entL0.getName());
        entL2d1.addSuperType(entL1d1.getName());
        entL2d2.addSuperType(entL1d1.getName());
        entL2d3.addSuperType(entL1d1.getName());
        entL2d3.addSuperType(entL1d2.getName());
        entL2d4.addSuperType(entL1d2.getName());
        entL0.addSuperType(entL2d3.getName()); // circular-ref

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL1d1);
        typesDef.getEntityDefs().add(entL1d2);
        typesDef.getEntityDefs().add(entL2d1);
        typesDef.getEntityDefs().add(entL2d2);
        typesDef.getEntityDefs().add(entL2d3);
        typesDef.getEntityDefs().add(entL2d4);

        try {
            AtlasTypeRegistry ignored = new AtlasTypeRegistry(typesDef);

            fail("expected invalid supertype failure");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.CIRCULAR_REFERENCE);
        }
    }

    @Test
    public void testNestedUpdates() {
        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = null;
        boolean                    commit       = false;
        AtlasClassificationDef     testTag1     = new AtlasClassificationDef("testTag1");
        AtlasClassificationDef     testTag2     = new AtlasClassificationDef("testTag2");

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();

            ttr.addType(testTag1);

            // changes should not be seen in typeRegistry until lock is released
            assertFalse(typeRegistry.isRegisteredType(testTag1.getName()),
                    "type added should be seen in typeRegistry only after commit");

            boolean isNestedUpdateSuccess = addType(typeRegistry, testTag2);

            assertTrue(isNestedUpdateSuccess);

            // changes made in nested commit, inside addType(), should not be seen in typeRegistry until lock is released here
            assertFalse(typeRegistry.isRegisteredType(testTag2.getName()),
                    "type added within nested commit should be seen in typeRegistry only after outer commit");

            commit = true;
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }
        assertTrue(typeRegistry.isRegisteredType(testTag1.getName()));
        assertTrue(typeRegistry.isRegisteredType(testTag2.getName()));
    }

    @Test
    public void testParallelUpdates() {
        final int    numOfThreads         = 3;
        final int    numOfTypesPerKind    = 30;
        final String enumTypePrefix       = "testEnum-";
        final String structTypePrefix     = "testStruct-";
        final String classificationPrefix = "testTag-";
        final String entityTypePrefix     = "testEntity-";

        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);

        final AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // update typeRegistry simultaneously in multiple threads
        for (int threadIdx = 0; threadIdx < numOfThreads; threadIdx++) {
            executor.submit(() -> {
                for (int i = 0; i < numOfTypesPerKind; i++) {
                    addType(typeRegistry, new AtlasEnumDef(enumTypePrefix + i));
                }

                for (int i = 0; i < numOfTypesPerKind; i++) {
                    addType(typeRegistry, new AtlasStructDef(structTypePrefix + i));
                }

                for (int i = 0; i < numOfTypesPerKind; i++) {
                    addType(typeRegistry, new AtlasClassificationDef(classificationPrefix + i));
                }

                for (int i = 0; i < numOfTypesPerKind; i++) {
                    addType(typeRegistry, new AtlasEntityDef(entityTypePrefix + i));
                }

                return null;
            });
        }

        executor.shutdown();

        try {
            boolean isCompleted = executor.awaitTermination(60, TimeUnit.SECONDS);

            assertTrue(isCompleted, "threads did not complete updating types");
        } catch (InterruptedException excp) {
            // ignore?
        }

        // verify that all types added are present in the typeRegistry
        for (int i = 0; i < numOfTypesPerKind; i++) {
            String enumType           = enumTypePrefix + i;
            String structType         = structTypePrefix + i;
            String classificationType = classificationPrefix + i;
            String entityType         = entityTypePrefix + i;

            assertNotNull(typeRegistry.getEnumDefByName(enumType), enumType + ": enum not found");
            assertNotNull(typeRegistry.getStructDefByName(structType), structType + ": struct not found");
            assertNotNull(typeRegistry.getClassificationDefByName(classificationType), classificationType + ": classification not found");
            assertNotNull(typeRegistry.getEntityDefByName(entityType), entityType + ": entity not found");
        }
    }

    /* create 2 entity types: L0 and L1, with L0 as superType of L1
     * add entity type L2, with L0, L1 and L2 as super-types - this should fail due to L2 self-referencing itself in super-types
     * verify that after the update failure, the registry still has correct super-type/sub-type information for L0 and L1
     */
    @Test
    public void testRegistryValidityOnInvalidUpdate() {
        AtlasEntityDef entL0 = new AtlasEntityDef("L0");
        AtlasEntityDef entL1 = new AtlasEntityDef("L1");

        entL1.addSuperType(entL0.getName());

        entL0.addAttribute(new AtlasAttributeDef("L0_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1.addAttribute(new AtlasAttributeDef("L1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL1);

        AtlasTypeRegistry typeRegistry = null;

        try {
            typeRegistry = new AtlasTypeRegistry(typesDef);
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        }
        assertNotNull(typeRegistry);

        validateAllSuperTypes(typeRegistry, "L0", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L0", new HashSet<>(Collections.singletonList("L1")));

        validateAllSuperTypes(typeRegistry, "L1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSubTypes(typeRegistry, "L1", new HashSet<>());

        // create a circular reference
        AtlasEntityDef entL2 = new AtlasEntityDef("L2");
        entL2.addSuperType(entL0.getName());
        entL2.addSuperType(entL1.getName());
        entL2.addSuperType(entL2.getName());

        typesDef.clear();
        typesDef.getEntityDefs().add(entL2);

        try {
            typeRegistry.updateTypes(typesDef);

            fail("type update should have failed");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.CIRCULAR_REFERENCE);
        }

        assertNull(typeRegistry.getEntityTypeByName("L2"));

        validateAllSuperTypes(typeRegistry, "L0", new HashSet<>());
        validateAllSubTypes(typeRegistry, "L0", new HashSet<>(Collections.singletonList("L1")));

        validateAllSuperTypes(typeRegistry, "L1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSubTypes(typeRegistry, "L1", new HashSet<>());
    }

    /* create 2 entity types: L0 and L1, with L0 as superType of L1
     * Create entity type L2 with same attribute as in L0.
     * Add L2 as superType of L1 - this should fail as "attr1" already exists in other supertype L0
     * verify that after the update failure, the registry still has correct super-type/sub-type information for L1
     */
    @Test
    public void testRegistryValiditySuperTypesUpdateWithExistingAttribute() {
        AtlasEntityDef entL0 = new AtlasEntityDef("L0");
        AtlasEntityDef entL1 = new AtlasEntityDef("L1");
        AtlasEntityDef entL2 = new AtlasEntityDef("L2");

        entL1.addSuperType(entL0.getName());

        entL0.addAttribute(new AtlasAttributeDef("attr1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1.addAttribute(new AtlasAttributeDef("attr2", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2.addAttribute(new AtlasAttributeDef("attr1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL1);
        typesDef.getEntityDefs().add(entL2);

        AtlasTypeRegistry typeRegistry = null;

        try {
            typeRegistry = new AtlasTypeRegistry(typesDef);
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        }

        validateAllSuperTypes(typeRegistry, "L1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSubTypes(typeRegistry, "L1", new HashSet<>());

        //Add L2 as supertype for L1
        entL1.addSuperType(entL2.getName());

        try {
            typeRegistry.updateTypes(typesDef);

            fail("type update should have failed");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_NAME_ALREADY_EXISTS_IN_ANOTHER_PARENT_TYPE);
        }

        validateAllSuperTypes(typeRegistry, "L1", new HashSet<>(Collections.singletonList("L0")));
        validateAllSubTypes(typeRegistry, "L1", new HashSet<>());
    }

    @Test
    public void testRegistryValiditySuperTypesUpdateWithExistingAttributeForClassification() {
        AtlasClassificationDef class0 = new AtlasClassificationDef("class0");
        AtlasClassificationDef class1 = new AtlasClassificationDef("class1");
        AtlasClassificationDef class2 = new AtlasClassificationDef("class2");

        class1.addSuperType(class0.getName());

        class0.addAttribute(new AtlasAttributeDef("attr1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        class1.addAttribute(new AtlasAttributeDef("attr2", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        class2.addAttribute(new AtlasAttributeDef("attr1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getClassificationDefs().add(class0);
        typesDef.getClassificationDefs().add(class1);
        typesDef.getClassificationDefs().add(class2);

        AtlasTypeRegistry typeRegistry = null;

        try {
            typeRegistry = new AtlasTypeRegistry(typesDef);
        } catch (AtlasBaseException excp) {
            fail("unexpected failure", excp);
        }
        assertNotNull(typeRegistry);

        validateAllSuperTypes(typeRegistry, "class1", new HashSet<>(Collections.singletonList("class0")));
        validateAllSubTypes(typeRegistry, "class1", new HashSet<>());

        //Add class2 as supertype for class1
        class1.addSuperType(class2.getName());

        try {
            typeRegistry.updateTypes(typesDef);

            fail("type update should have failed");
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_NAME_ALREADY_EXISTS_IN_ANOTHER_PARENT_TYPE);
        }

        validateAllSuperTypes(typeRegistry, "class1", new HashSet<>(Collections.singletonList("class0")));
        validateAllSubTypes(typeRegistry, "class1", new HashSet<>());
    }

    private boolean addType(AtlasTypeRegistry typeRegistry, AtlasBaseTypeDef typeDef) {
        try {
            typeRegistry.addType(typeDef);

            return true;
        } catch (AtlasBaseException excp) {
            // ignore
        }

        return false;
    }

    private void validateAllSuperTypes(AtlasTypeRegistry typeRegistry, String typeName, Set<String> expectedSuperTypes) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
            // ignored
        }

        Set<String> superTypes = null;

        if (type != null) {
            if (type instanceof AtlasEntityType) {
                superTypes = ((AtlasEntityType) type).getAllSuperTypes();
            } else if (type instanceof AtlasClassificationType) {
                superTypes = ((AtlasClassificationType) type).getAllSuperTypes();
            }
        }

        assertEquals(superTypes, expectedSuperTypes);
    }

    private void validateAllSubTypes(AtlasTypeRegistry typeRegistry, String typeName, Set<String> expectedSubTypes) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
            // ignored
        }

        Set<String> subTypes = null;

        if (type != null) {
            if (type instanceof AtlasEntityType) {
                subTypes = ((AtlasEntityType) type).getAllSubTypes();
            } else if (type instanceof AtlasClassificationType) {
                subTypes = ((AtlasClassificationType) type).getAllSubTypes();
            }
        }

        assertEquals(subTypes, expectedSubTypes);
    }

    private void validateSubTypes(AtlasTypeRegistry typeRegistry, String typeName, Set<String> expectedSubTypes) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
            // ignored
        }

        Set<String> subTypes = null;

        if (type != null) {
            if (type instanceof AtlasEntityType) {
                subTypes = ((AtlasEntityType) type).getSubTypes();
            } else if (type instanceof AtlasClassificationType) {
                subTypes = ((AtlasClassificationType) type).getSubTypes();
            }
        }

        assertEquals(subTypes, expectedSubTypes);
    }

    private void validateAttributeNames(AtlasTypeRegistry typeRegistry, String typeName, Set<String> attributeNames) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
            // ignored
        }

        Map<String, AtlasStructType.AtlasAttribute> attributes = null;

        if (type != null) {
            if (type instanceof AtlasEntityType) {
                attributes = ((AtlasEntityType) type).getAllAttributes();
            } else if (type instanceof AtlasClassificationType) {
                attributes = ((AtlasClassificationType) type).getAllAttributes();
            }
        }

        assertNotNull(attributes);
        assertEquals(attributes.keySet(), attributeNames);
    }

    private void validateDisplayNameAttribute(AtlasTypeRegistry typeRegistry, String entityTypeName, String... displayNameAttributes) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

        if (displayNameAttributes == null || displayNameAttributes.length == 0) {
            assertNull(entityType.getDisplayTextAttribute());
        } else {
            List<String> validValues = Arrays.asList(displayNameAttributes);

            assertTrue(validValues.contains(entityType.getDisplayTextAttribute()), entityTypeName + ": invalid displayNameAttribute " + entityType.getDisplayTextAttribute() + ". Valid values: " + validValues);
        }
    }

    @Test
    public void testBuiltInTypes() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // Test that built-in types are registered
        assertTrue(typeRegistry.isRegisteredType("string"));
        assertTrue(typeRegistry.isRegisteredType("int"));
        assertTrue(typeRegistry.isRegisteredType("boolean"));
        assertTrue(typeRegistry.isRegisteredType("float"));
        assertTrue(typeRegistry.isRegisteredType("double"));
        assertTrue(typeRegistry.isRegisteredType("date"));
        assertTrue(typeRegistry.isRegisteredType("long"));
        assertTrue(typeRegistry.isRegisteredType("short"));
        assertTrue(typeRegistry.isRegisteredType("byte"));

        // Test getting built-in types
        try {
            assertNotNull(typeRegistry.getType("string"));
            assertNotNull(typeRegistry.getType("int"));
            assertNotNull(typeRegistry.getType("boolean"));
        } catch (AtlasBaseException e) {
            // Should not happen for built-in types
        }
    }

    @Test
    public void testArrayAndMapTypes() throws AtlasBaseException {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // Test array type creation
        AtlasType arrayType = typeRegistry.getType("array<string>");
        assertNotNull(arrayType);
        assertTrue(arrayType instanceof AtlasArrayType);
        assertEquals(arrayType.getTypeName(), "array<string>");

        // Test map type creation
        AtlasType mapType = typeRegistry.getType("map<string,int>");
        assertNotNull(mapType);
        assertTrue(mapType instanceof AtlasMapType);
        assertEquals(mapType.getTypeName(), "map<string,int>");

        // Test nested array type
        arrayType = typeRegistry.getType("array<array<string>>");
        assertNotNull(arrayType);
        assertTrue(arrayType instanceof AtlasArrayType);

        // Test nested map type
        mapType = typeRegistry.getType("map<string,array<int>>");
        assertNotNull(mapType);
        assertTrue(mapType instanceof AtlasMapType);
    }

    @Test
    public void testInvalidTypeName() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        try {
            typeRegistry.getType("NonExistentType");
            assertFalse(true, "Expected AtlasBaseException for non-existent type");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_NOT_FOUND);
        }
    }

    @Test
    public void testEnumTypeOperations() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnum", "Test enum", "1.0",
                Arrays.asList(
                        new AtlasEnumElementDef("VALUE1", "First value", 1),
                        new AtlasEnumElementDef("VALUE2", "Second value", 2),
                        new AtlasEnumElementDef("VALUE3", "Third value", 3)));
        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(enumDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Test enum type retrieval
        AtlasEnumDef retrievedEnum = typeRegistry.getEnumDefByName("TestEnum");
        assertNotNull(retrievedEnum);
        assertEquals(retrievedEnum.getName(), "TestEnum");
        assertEquals(retrievedEnum.getElementDefs().size(), 3);

        AtlasEnumType enumType = typeRegistry.getEnumTypeByName("TestEnum");
        assertNotNull(enumType);
        assertEquals(enumType.getTypeName(), "TestEnum");

        // Test getAllEnumDefs
        assertTrue(typeRegistry.getAllEnumDefs().contains(retrievedEnum));
    }

    @Test
    public void testStructTypeOperations() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "Test struct", "1.0");
        structDef.addAttribute(new AtlasAttributeDef("attr1", "string", true, Cardinality.SINGLE, 0, 1, false, false, false, Collections.emptyList()));
        structDef.addAttribute(new AtlasAttributeDef("attr2", "int", false, Cardinality.SINGLE, 1, 1, false, false, false, Collections.emptyList()));

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(structDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Test struct type retrieval
        AtlasStructDef retrievedStruct = typeRegistry.getStructDefByName("TestStruct");
        assertNotNull(retrievedStruct);
        assertEquals(retrievedStruct.getName(), "TestStruct");
        assertEquals(retrievedStruct.getAttributeDefs().size(), 2);

        AtlasStructType structType = typeRegistry.getStructTypeByName("TestStruct");
        assertNotNull(structType);
        assertEquals(structType.getTypeName(), "TestStruct");

        // Test getAllStructDefs
        assertTrue(typeRegistry.getAllStructDefs().contains(retrievedStruct));
    }

    @Test
    public void testRelationshipTypeOperations() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasEntityDef entity1 = new AtlasEntityDef("Entity1", "First entity", "1.0");
        AtlasEntityDef entity2 = new AtlasEntityDef("Entity2", "Second entity", "1.0");

        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("TestRelationship", "Test relationship", "1.0",
                RelationshipCategory.ASSOCIATION, PropagateTags.NONE,
                new AtlasRelationshipEndDef("Entity1", "entity1", Cardinality.SINGLE),
                new AtlasRelationshipEndDef("Entity2", "entity2", Cardinality.SINGLE));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(entity1);
        typesDef.getEntityDefs().add(entity2);
        typesDef.getRelationshipDefs().add(relationshipDef);

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addTypes(typesDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Test relationship type retrieval
        AtlasRelationshipDef retrievedRelationship = typeRegistry.getRelationshipDefByName("TestRelationship");
        assertNotNull(retrievedRelationship);
        assertEquals(retrievedRelationship.getName(), "TestRelationship");

        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName("TestRelationship");
        assertNotNull(relationshipType);
        assertEquals(relationshipType.getTypeName(), "TestRelationship");

        // Test getAllRelationshipDefs
        assertTrue(typeRegistry.getAllRelationshipDefs().contains(retrievedRelationship));
    }

    @Test
    public void testBusinessMetadataTypeOperations() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasBusinessMetadataDef bmDef = new AtlasBusinessMetadataDef("TestBM", "Test business metadata", "1.0");
        bmDef.addAttribute(new AtlasAttributeDef("bmAttr1", "string", true, Cardinality.SINGLE, 0, 1, false, false, false, Collections.emptyList()));

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(bmDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Test business metadata type retrieval
        AtlasBusinessMetadataDef retrievedBM = typeRegistry.getBusinessMetadataDefByName("TestBM");
        if (retrievedBM != null) {
            assertEquals(retrievedBM.getName(), "TestBM");

            AtlasBusinessMetadataType bmType = typeRegistry.getBusinessMetadataTypeByName("TestBM");
            assertNotNull(bmType);
            assertEquals(bmType.getTypeName(), "TestBM");

            assertTrue(typeRegistry.getAllBusinessMetadataDefs().contains(retrievedBM));
        } else {
            assertNotNull(typeRegistry.getAllBusinessMetadataDefs());
        }
    }

    @Test
    public void testTypeDefsByGuid() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnumGuid", "Test enum with guid", "1.0");
        enumDef.setGuid("test-enum-guid-123");

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(enumDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        AtlasBaseTypeDef retrievedTypeDef = typeRegistry.getTypeDefByGuid("test-enum-guid-123");
        assertNotNull(retrievedTypeDef);
        assertEquals(retrievedTypeDef.getName(), "TestEnumGuid");
        assertEquals(retrievedTypeDef.getGuid(), "test-enum-guid-123");

        // Test with non-existent GUID
        retrievedTypeDef = typeRegistry.getTypeDefByGuid("non-existent-guid");
        assertNull(retrievedTypeDef);
    }

    @Test
    public void testGetAllTypeNames() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // Should include built-in types
        assertTrue(typeRegistry.getAllTypeNames().contains("string"));
        assertTrue(typeRegistry.getAllTypeNames().contains("int"));
        assertTrue(typeRegistry.getAllTypeNames().contains("boolean"));
    }

    @Test
    public void testGetAllTypes() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // Should include built-in types
        assertTrue(typeRegistry.getAllTypes().size() > 0);

        boolean foundStringType = false;
        for (AtlasType type : typeRegistry.getAllTypes()) {
            if ("string".equals(type.getTypeName())) {
                foundStringType = true;
                break;
            }
        }
        assertTrue(foundStringType);
    }

    @Test
    public void testRemoveTypeByName() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnumToRemove", "Test enum to remove", "1.0");

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(enumDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Verify type was added
        assertTrue(typeRegistry.isRegisteredType("TestEnumToRemove"));

        try {
            commit = false;
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.removeTypeByName("TestEnumToRemove");
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        assertFalse(typeRegistry.isRegisteredType("TestEnumToRemove"));
    }

    @Test
    public void testUpdateType() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasStructDef originalStruct = new AtlasStructDef("TestStructToUpdate", "Original description", "1.0");
        originalStruct.addAttribute(new AtlasAttributeDef("attr1", "string", true, Cardinality.SINGLE, 0, 1, false, false, false, Collections.emptyList()));

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(originalStruct);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Update the struct with new attribute
        AtlasStructDef updatedStruct = new AtlasStructDef("TestStructToUpdate", "Updated description", "2.0");
        updatedStruct.addAttribute(new AtlasAttributeDef("attr1", "string", true, Cardinality.SINGLE, 0, 1, false, false, false, Collections.emptyList()));
        updatedStruct.addAttribute(new AtlasAttributeDef("attr2", "int", false, Cardinality.SINGLE, 1, 1, false, false, false, Collections.emptyList()));

        try {
            commit = false;
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.updateType(updatedStruct);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        AtlasStructDef retrievedStruct = typeRegistry.getStructDefByName("TestStructToUpdate");
        assertNotNull(retrievedStruct);
        assertEquals(retrievedStruct.getDescription(), "Updated description");
        assertEquals(retrievedStruct.getTypeVersion(), "2.0");
        assertEquals(retrievedStruct.getAttributeDefs().size(), 2);
    }

    @Test
    public void testGetAllServiceTypes() {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        // Should return empty set for default registry
        Set<String> serviceTypes = typeRegistry.getAllServiceTypes();
        assertNotNull(serviceTypes);
    }

    @Test
    public void testConcurrentReadAccess() throws InterruptedException {
        final AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        final int numThreads = 10;
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // Add a type first
        AtlasTransientTypeRegistry ttr = null;
        boolean commit = false;

        AtlasEnumDef enumDef = new AtlasEnumDef("ConcurrentTestEnum", "Test enum for concurrent access", "1.0");

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate();
            ttr.addType(enumDef);
            commit = true;
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commit);
        }

        // Now test concurrent read access
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    // These should all work concurrently
                    assertTrue(typeRegistry.isRegisteredType("ConcurrentTestEnum"));
                    assertNotNull(typeRegistry.getType("ConcurrentTestEnum"));
                    assertNotNull(typeRegistry.getEnumDefByName("ConcurrentTestEnum"));
                    assertTrue(typeRegistry.getAllTypeNames().contains("ConcurrentTestEnum"));
                } catch (AtlasBaseException e) {
                    // Should not happen
                }
                return null;
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testProtectedConstructor() {
        AtlasTypeRegistry baseRegistry = new AtlasTypeRegistry();

        AtlasTransientTypeRegistry ttr = null;
        try {
            ttr = baseRegistry.lockTypeRegistryForUpdate();
            assertNotNull(ttr);
            assertTrue(ttr instanceof AtlasTransientTypeRegistry);
        } catch (AtlasBaseException e) {
            // Should not happen
        } finally {
            if (ttr != null) {
                baseRegistry.releaseTypeRegistryForUpdate(ttr, false);
            }
        }
    }
}
