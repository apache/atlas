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
package org.apache.atlas.type;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

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
    public void testClassificationDefValidSuperTypes() {
        AtlasClassificationDef classifiL0   = new AtlasClassificationDef("L0");
        AtlasClassificationDef classifiL1_1 = new AtlasClassificationDef("L1-1");
        AtlasClassificationDef classifiL1_2 = new AtlasClassificationDef("L1-2");
        AtlasClassificationDef classifiL2_1 = new AtlasClassificationDef("L2-1");
        AtlasClassificationDef classifiL2_2 = new AtlasClassificationDef("L2-2");
        AtlasClassificationDef classifiL2_3 = new AtlasClassificationDef("L2-3");
        AtlasClassificationDef classifiL2_4 = new AtlasClassificationDef("L2-4");

        classifiL1_1.addSuperType(classifiL0.getName());
        classifiL1_2.addSuperType(classifiL0.getName());
        classifiL2_1.addSuperType(classifiL1_1.getName());
        classifiL2_2.addSuperType(classifiL1_1.getName());
        classifiL2_3.addSuperType(classifiL1_1.getName());
        classifiL2_3.addSuperType(classifiL1_2.getName());
        classifiL2_4.addSuperType(classifiL1_2.getName());

        classifiL0.addAttribute(new AtlasAttributeDef("L0_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL1_1.addAttribute(new AtlasAttributeDef("L1-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL1_2.addAttribute(new AtlasAttributeDef("L1-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2_1.addAttribute(new AtlasAttributeDef("L2-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2_2.addAttribute(new AtlasAttributeDef("L2-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2_3.addAttribute(new AtlasAttributeDef("L2-3_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        classifiL2_4.addAttribute(new AtlasAttributeDef("L2-4_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getClassificationDefs().add(classifiL0);
        typesDef.getClassificationDefs().add(classifiL1_1);
        typesDef.getClassificationDefs().add(classifiL1_2);
        typesDef.getClassificationDefs().add(classifiL2_1);
        typesDef.getClassificationDefs().add(classifiL2_2);
        typesDef.getClassificationDefs().add(classifiL2_3);
        typesDef.getClassificationDefs().add(classifiL2_4);

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addTypes(typesDef);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNull(failureMsg);

        typeRegistry.commitTransientTypeRegistry(ttr);

        validateSuperTypes(typeRegistry, "L0", new HashSet<String>());
        validateSuperTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0")));
        validateSuperTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0")));
        validateSuperTypes(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateSuperTypes(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateSuperTypes(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L1-1", "L0", "L1-2")));
        validateSuperTypes(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L1-2", "L0")));

        validateAttributeNames(typeRegistry, "L0", new HashSet<>(Arrays.asList("L0_a1")));
        validateAttributeNames(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1")));
        validateAttributeNames(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1")));
        validateAttributeNames(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-1_a1")));
        validateAttributeNames(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-2_a1")));
        validateAttributeNames(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L1-2_a1", "L2-3_a1")));
        validateAttributeNames(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1", "L2-4_a1")));
    }

    @Test
    public void testClassificationDefInvalidSuperTypes_Self() {
        AtlasClassificationDef classifiDef1 = new AtlasClassificationDef("classifiDef-1");

        classifiDef1.addSuperType(classifiDef1.getName());

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addType(classifiDef1);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNotNull(failureMsg, "expected invalid supertype failure");
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
    public void testClassificationDefInvalidSuperTypes_CircularRef() {
        AtlasClassificationDef classifiL0   = new AtlasClassificationDef("L0");
        AtlasClassificationDef classifiL1_1 = new AtlasClassificationDef("L1-1");
        AtlasClassificationDef classifiL1_2 = new AtlasClassificationDef("L1-2");
        AtlasClassificationDef classifiL2_1 = new AtlasClassificationDef("L2-1");
        AtlasClassificationDef classifiL2_2 = new AtlasClassificationDef("L2-2");
        AtlasClassificationDef classifiL2_3 = new AtlasClassificationDef("L2-3");
        AtlasClassificationDef classifiL2_4 = new AtlasClassificationDef("L2-4");

        classifiL1_1.addSuperType(classifiL0.getName());
        classifiL1_2.addSuperType(classifiL0.getName());
        classifiL2_1.addSuperType(classifiL1_1.getName());
        classifiL2_2.addSuperType(classifiL1_1.getName());
        classifiL2_3.addSuperType(classifiL1_1.getName());
        classifiL2_3.addSuperType(classifiL1_2.getName());
        classifiL2_4.addSuperType(classifiL1_2.getName());
        classifiL0.addSuperType(classifiL2_3.getName()); // circular-ref

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getClassificationDefs().add(classifiL0);
        typesDef.getClassificationDefs().add(classifiL1_1);
        typesDef.getClassificationDefs().add(classifiL1_2);
        typesDef.getClassificationDefs().add(classifiL2_1);
        typesDef.getClassificationDefs().add(classifiL2_2);
        typesDef.getClassificationDefs().add(classifiL2_3);
        typesDef.getClassificationDefs().add(classifiL2_4);

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addTypes(typesDef);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNotNull(failureMsg, "expected invalid supertype failure");
    }

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
    public void testEntityDefValidSuperTypes() {
        AtlasEntityDef entL0   = new AtlasEntityDef("L0");
        AtlasEntityDef entL1_1 = new AtlasEntityDef("L1-1");
        AtlasEntityDef entL1_2 = new AtlasEntityDef("L1-2");
        AtlasEntityDef entL2_1 = new AtlasEntityDef("L2-1");
        AtlasEntityDef entL2_2 = new AtlasEntityDef("L2-2");
        AtlasEntityDef entL2_3 = new AtlasEntityDef("L2-3");
        AtlasEntityDef entL2_4 = new AtlasEntityDef("L2-4");

        entL1_1.addSuperType(entL0.getName());
        entL1_2.addSuperType(entL0.getName());
        entL2_1.addSuperType(entL1_1.getName());
        entL2_2.addSuperType(entL1_1.getName());
        entL2_3.addSuperType(entL1_1.getName());
        entL2_3.addSuperType(entL1_2.getName());
        entL2_4.addSuperType(entL1_2.getName());

        entL0.addAttribute(new AtlasAttributeDef("L0_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1_1.addAttribute(new AtlasAttributeDef("L1-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL1_2.addAttribute(new AtlasAttributeDef("L1-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2_1.addAttribute(new AtlasAttributeDef("L2-1_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2_2.addAttribute(new AtlasAttributeDef("L2-2_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2_3.addAttribute(new AtlasAttributeDef("L2-3_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        entL2_4.addAttribute(new AtlasAttributeDef("L2-4_a1", AtlasBaseTypeDef.ATLAS_TYPE_INT));

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL1_1);
        typesDef.getEntityDefs().add(entL1_2);
        typesDef.getEntityDefs().add(entL2_1);
        typesDef.getEntityDefs().add(entL2_2);
        typesDef.getEntityDefs().add(entL2_3);
        typesDef.getEntityDefs().add(entL2_4);

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addTypes(typesDef);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNull(failureMsg);

        typeRegistry.commitTransientTypeRegistry(ttr);

        validateSuperTypes(typeRegistry, "L0", new HashSet<String>());
        validateSuperTypes(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0")));
        validateSuperTypes(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0")));
        validateSuperTypes(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateSuperTypes(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L1-1", "L0")));
        validateSuperTypes(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L1-1", "L0", "L1-2")));
        validateSuperTypes(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L1-2", "L0")));

        validateAttributeNames(typeRegistry, "L0", new HashSet<>(Arrays.asList("L0_a1")));
        validateAttributeNames(typeRegistry, "L1-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1")));
        validateAttributeNames(typeRegistry, "L1-2", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1")));
        validateAttributeNames(typeRegistry, "L2-1", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-1_a1")));
        validateAttributeNames(typeRegistry, "L2-2", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L2-2_a1")));
        validateAttributeNames(typeRegistry, "L2-3", new HashSet<>(Arrays.asList("L0_a1", "L1-1_a1", "L1-2_a1", "L2-3_a1")));
        validateAttributeNames(typeRegistry, "L2-4", new HashSet<>(Arrays.asList("L0_a1", "L1-2_a1", "L2-4_a1")));
    }

    @Test
    public void testEntityDefInvalidSuperTypes_Self() {
        AtlasEntityDef entDef1 = new AtlasEntityDef("entDef-1");

        entDef1.addSuperType(entDef1.getName());

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addType(entDef1);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNotNull(failureMsg, "expected invalid supertype failure");
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
    public void testEntityDefInvalidSuperTypes_CircularRef() {
        AtlasEntityDef entL0   = new AtlasEntityDef("L0");
        AtlasEntityDef entL1_1 = new AtlasEntityDef("L1-1");
        AtlasEntityDef entL1_2 = new AtlasEntityDef("L1-2");
        AtlasEntityDef entL2_1 = new AtlasEntityDef("L2-1");
        AtlasEntityDef entL2_2 = new AtlasEntityDef("L2-2");
        AtlasEntityDef entL2_3 = new AtlasEntityDef("L2-3");
        AtlasEntityDef entL2_4 = new AtlasEntityDef("L2-4");

        entL1_1.addSuperType(entL0.getName());
        entL1_2.addSuperType(entL0.getName());
        entL2_1.addSuperType(entL1_1.getName());
        entL2_2.addSuperType(entL1_1.getName());
        entL2_3.addSuperType(entL1_1.getName());
        entL2_3.addSuperType(entL1_2.getName());
        entL2_4.addSuperType(entL1_2.getName());
        entL0.addSuperType(entL2_3.getName()); // circular-ref

        AtlasTypesDef typesDef = new AtlasTypesDef();

        typesDef.getEntityDefs().add(entL0);
        typesDef.getEntityDefs().add(entL1_1);
        typesDef.getEntityDefs().add(entL1_2);
        typesDef.getEntityDefs().add(entL2_1);
        typesDef.getEntityDefs().add(entL2_2);
        typesDef.getEntityDefs().add(entL2_3);
        typesDef.getEntityDefs().add(entL2_4);

        AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
        AtlasTransientTypeRegistry ttr          = typeRegistry.createTransientTypeRegistry();
        String                     failureMsg   = null;

        try {
            ttr.addTypes(typesDef);
        } catch (AtlasBaseException excp) {
            failureMsg = excp.getMessage();
        }
        assertNotNull(failureMsg, "expected invalid supertype failure");
    }

    private void validateSuperTypes(AtlasTypeRegistry typeRegistry, String typeName, Set<String> expectedSuperTypes) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
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

    private void validateAttributeNames(AtlasTypeRegistry typeRegistry, String typeName, Set<String> attributeNames) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);
        } catch (AtlasBaseException excp) {
        }

        Map<String, AtlasAttributeDef> attributeDefs = null;

        if (type != null) {
            if (type instanceof AtlasEntityType) {
                attributeDefs = ((AtlasEntityType) type).getAllAttributeDefs();
            } else if (type instanceof AtlasClassificationType) {
                attributeDefs = ((AtlasClassificationType) type).getAllAttributeDefs();
            }
        }

        assertNotNull(attributeDefs);
        assertEquals(attributeDefs.keySet(), attributeNames);
    }
}
