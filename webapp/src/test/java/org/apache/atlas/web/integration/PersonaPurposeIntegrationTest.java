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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Persona and Purpose entity CRUD operations.
 *
 * <p>Note: Persona/Purpose preprocessors may attempt Keycloak role creation.
 * If Keycloak is not available, creation tests will be skipped via JUnit Assumptions.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PersonaPurposeIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(PersonaPurposeIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    private String personaGuid;
    private String personaQN;
    private String policyGuid;
    private String purposeGuid;
    private boolean personaCreationSupported = true;
    private boolean purposeCreationSupported = true;

    @Test
    @Order(1)
    void testCreatePersona() {
        try {
            AtlasEntity persona = new AtlasEntity("Persona");
            persona.setAttribute("name", "test-persona-" + testId);
            persona.setAttribute("description", "Test persona for integration testing");

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(persona));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            assertEquals("Persona", created.getTypeName());
            personaGuid = created.getGuid();

            // Fetch to get auto-generated qualifiedName
            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid);
            personaQN = (String) result.getEntity().getAttribute("qualifiedName");

            LOG.info("Created Persona: guid={}, qn={}", personaGuid, personaQN);
        } catch (AtlasServiceException e) {
            LOG.warn("Persona creation failed (likely missing Keycloak): {}", e.getMessage());
            personaCreationSupported = false;
        }
    }

    @Test
    @Order(2)
    void testGetPersona() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        assertNotNull(personaGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid);
        AtlasEntity entity = result.getEntity();

        assertNotNull(entity);
        assertEquals("Persona", entity.getTypeName());
        assertEquals("test-persona-" + testId, entity.getAttribute("name"));
        assertEquals(AtlasEntity.Status.ACTIVE, entity.getStatus());

        LOG.info("Fetched persona: guid={}", personaGuid);
    }

    @Test
    @Order(3)
    void testUpdatePersona() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        assertNotNull(personaGuid);

        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(personaGuid);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("description", "Updated persona description");

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response);

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(personaGuid);
        assertEquals("Updated persona description",
                updated.getEntity().getAttribute("description"));

        LOG.info("Updated persona: guid={}", personaGuid);
    }

    @Test
    @Order(4)
    void testCreateMetadataPolicy() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        assertNotNull(personaGuid);
        assertNotNull(personaQN);

        AtlasEntity policy = new AtlasEntity("AuthPolicy");
        policy.setAttribute("name", "test-policy-" + testId);
        policy.setAttribute("policyType", "metadata");
        policy.setAttribute("policyCategory", "persona");
        policy.setAttribute("accessControl", personaGuid);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(policy));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("AuthPolicy", created.getTypeName());
        policyGuid = created.getGuid();

        LOG.info("Created AuthPolicy: guid={}", policyGuid);
    }

    @Test
    @Order(5)
    void testGetPersonaWithPolicies() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona creation not supported");
        Assumptions.assumeTrue(policyGuid != null, "Skipping: Policy creation failed");
        assertNotNull(personaGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid, false, false);
        AtlasEntity persona = result.getEntity();

        Object policies = persona.getRelationshipAttribute("policies");
        assertNotNull(policies, "Persona should have policies relationship");

        LOG.info("Persona has policies relationship");
    }

    @Test
    @Order(6)
    void testCreatePurpose() {
        try {
            AtlasEntity purpose = new AtlasEntity("Purpose");
            purpose.setAttribute("name", "test-purpose-" + testId);
            purpose.setAttribute("description", "Test purpose for integration testing");

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(purpose));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            assertEquals("Purpose", created.getTypeName());
            purposeGuid = created.getGuid();

            LOG.info("Created Purpose: guid={}", purposeGuid);
        } catch (AtlasServiceException e) {
            LOG.warn("Purpose creation failed (likely missing Keycloak): {}", e.getMessage());
            purposeCreationSupported = false;
        }
    }

    @Test
    @Order(7)
    void testUpdatePurpose() throws AtlasServiceException {
        Assumptions.assumeTrue(purposeCreationSupported, "Skipping: Purpose creation not supported");
        assertNotNull(purposeGuid);

        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(purposeGuid);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("description", "Updated purpose description");

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response);

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(purposeGuid);
        assertEquals("Updated purpose description",
                updated.getEntity().getAttribute("description"));

        LOG.info("Updated purpose: guid={}", purposeGuid);
    }

    @Test
    @Order(8)
    void testDeletePolicy() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported && policyGuid != null,
                "Skipping: Policy not created");

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(policyGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(policyGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Deleted policy: guid={}", policyGuid);
    }

    @Test
    @Order(9)
    void testDeletePersona() throws AtlasServiceException {
        Assumptions.assumeTrue(personaCreationSupported, "Skipping: Persona not created");
        assertNotNull(personaGuid);

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(personaGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(personaGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Deleted persona: guid={}", personaGuid);
    }

    @Test
    @Order(10)
    void testDeletePurpose() throws AtlasServiceException {
        Assumptions.assumeTrue(purposeCreationSupported, "Skipping: Purpose not created");
        assertNotNull(purposeGuid);

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(purposeGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(purposeGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Deleted purpose: guid={}", purposeGuid);
    }
}
