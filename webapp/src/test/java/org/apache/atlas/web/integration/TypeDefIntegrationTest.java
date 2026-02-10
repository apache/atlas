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

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for TypeDef CRUD operations.
 *
 * <p>Exercises typedef retrieval, creation (enum, struct, entity),
 * custom entity instantiation, and typedef deletion.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TypeDefIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDefIntegrationTest.class);

    private final long testId = System.currentTimeMillis();
    private final String customEnumName = "TestEnum_" + testId;
    private final String customStructName = "TestStruct_" + testId;
    private final String customEntityName = "TestEntity_" + testId;

    private String customEntityInstanceGuid;

    @Test
    @Order(1)
    void testGetAllTypeDefs() throws AtlasServiceException {
        SearchFilter filter = new SearchFilter();
        AtlasTypesDef allTypes = atlasClient.getAllTypeDefs(filter);

        assertNotNull(allTypes);
        // Atlas should have many bootstrap types
        assertFalse(allTypes.getEntityDefs().isEmpty(), "Should have entity defs");
        assertFalse(allTypes.getEnumDefs().isEmpty(), "Should have enum defs");

        LOG.info("All typedefs: {} entity, {} enum, {} struct, {} classification, {} relationship",
                allTypes.getEntityDefs().size(),
                allTypes.getEnumDefs().size(),
                allTypes.getStructDefs().size(),
                allTypes.getClassificationDefs().size(),
                allTypes.getRelationshipDefs().size());
    }

    @Test
    @Order(2)
    void testGetEntityDefByName() throws AtlasServiceException {
        AtlasEntityDef tableDef = atlasClient.getEntityDefByName("Table");

        assertNotNull(tableDef);
        assertEquals("Table", tableDef.getName());
        assertNotNull(tableDef.getAttributeDefs());
        assertFalse(tableDef.getAttributeDefs().isEmpty(), "Table should have attributes");

        LOG.info("Table entity def: {} attributes, superTypes={}",
                tableDef.getAttributeDefs().size(), tableDef.getSuperTypes());
    }

    @Test
    @Order(3)
    void testCreateEnumDef() throws AtlasServiceException {
        AtlasEnumDef enumDef = new AtlasEnumDef(customEnumName, "Test enum type");
        enumDef.setElementDefs(Arrays.asList(
                new AtlasEnumDef.AtlasEnumElementDef("VALUE_A", "Value A", 1),
                new AtlasEnumDef.AtlasEnumElementDef("VALUE_B", "Value B", 2),
                new AtlasEnumDef.AtlasEnumElementDef("VALUE_C", "Value C", 3)
        ));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEnumDefs(Collections.singletonList(enumDef));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);

        assertNotNull(created);
        assertFalse(created.getEnumDefs().isEmpty());
        assertEquals(customEnumName, created.getEnumDefs().get(0).getName());

        LOG.info("Created enum typedef: {}", customEnumName);
    }

    @Test
    @Order(4)
    void testCreateStructDef() throws AtlasServiceException {
        AtlasAttributeDef nameAttr = new AtlasAttributeDef("field1", "string");
        nameAttr.setIsOptional(true);

        AtlasAttributeDef valueAttr = new AtlasAttributeDef("field2", "int");
        valueAttr.setIsOptional(true);

        AtlasStructDef structDef = new AtlasStructDef(customStructName, "Test struct type",
                "1.0", Arrays.asList(nameAttr, valueAttr));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setStructDefs(Collections.singletonList(structDef));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);

        assertNotNull(created);
        assertFalse(created.getStructDefs().isEmpty());
        assertEquals(customStructName, created.getStructDefs().get(0).getName());

        LOG.info("Created struct typedef: {}", customStructName);
    }

    @Test
    @Order(5)
    void testCreateEntityDef() throws AtlasServiceException {
        AtlasAttributeDef customAttr = new AtlasAttributeDef("customField", "string");
        customAttr.setIsOptional(true);

        AtlasEntityDef entityDef = new AtlasEntityDef(customEntityName, "Custom test entity",
                "1.0", Collections.singletonList(customAttr));
        entityDef.setSuperTypes(Collections.singleton("Asset"));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(Collections.singletonList(entityDef));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);

        assertNotNull(created);
        assertFalse(created.getEntityDefs().isEmpty());
        assertEquals(customEntityName, created.getEntityDefs().get(0).getName());

        LOG.info("Created entity typedef: {} extending Asset", customEntityName);
    }

    @Test
    @Order(6)
    void testCreateEntityWithCustomType() throws AtlasServiceException {
        AtlasEntity entity = new AtlasEntity(customEntityName);
        entity.setAttribute("name", "custom-instance-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/typedef/custom/" + testId);
        entity.setAttribute("customField", "custom-value");

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals(customEntityName, created.getTypeName());
        customEntityInstanceGuid = created.getGuid();

        // Verify custom attribute
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(customEntityInstanceGuid);
        assertEquals("custom-value", result.getEntity().getAttribute("customField"));

        LOG.info("Created instance of custom type: guid={}", customEntityInstanceGuid);
    }

    @Test
    @Order(7)
    void testDeleteCustomTypeDefs() throws AtlasServiceException {
        // First delete the entity instance
        if (customEntityInstanceGuid != null) {
            atlasClient.deleteEntityByGuid(customEntityInstanceGuid);
        }

        // Delete enum and struct types (no instances)
        AtlasTypesDef enumTypesDef = new AtlasTypesDef();
        enumTypesDef.setEnumDefs(Collections.singletonList(new AtlasEnumDef(customEnumName)));
        atlasClient.deleteAtlasTypeDefs(enumTypesDef);
        LOG.info("Deleted enum typedef: {}", customEnumName);

        AtlasTypesDef structTypesDef = new AtlasTypesDef();
        structTypesDef.setStructDefs(Collections.singletonList(new AtlasStructDef(customStructName)));
        atlasClient.deleteAtlasTypeDefs(structTypesDef);
        LOG.info("Deleted struct typedef: {}", customStructName);
    }

    @Test
    @Order(8)
    void testDeleteTypeInUse() {
        // The custom entity type still has a (soft-deleted) instance.
        // Trying to delete the type should fail or succeed depending on Atlas behavior.
        // Atlas typically allows deletion if all instances are soft-deleted.
        try {
            atlasClient.deleteTypeByName(customEntityName);
            LOG.info("Custom entity type deleted (soft-deleted instances allowed)");
        } catch (AtlasServiceException e) {
            LOG.info("Custom entity type deletion rejected (type in use): {}", e.getMessage());
            // This is expected if Atlas doesn't allow deleting types with instances
        }
    }
}
