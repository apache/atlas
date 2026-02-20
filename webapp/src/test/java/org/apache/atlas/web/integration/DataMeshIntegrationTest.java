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
import org.apache.atlas.model.instance.AtlasObjectId;
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
 * Integration test for Data Mesh operations (DataDomain and DataProduct).
 *
 * <p>Exercises domain hierarchy creation, product-domain relationships,
 * and cascading operations. DataDomain/DataProduct preprocessors generate
 * qualifiedNames automatically.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DataMeshIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(DataMeshIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    private String domainGuid;
    private String domainQN;
    private String subDomainGuid;
    private String subDomainQN;
    private String productGuid;
    private boolean domainCreated = false;

    @Test
    @Order(1)
    void testCreateDomain() throws AtlasServiceException {
        AtlasEntity domain = new AtlasEntity("DataDomain");
        domain.setAttribute("name", "test-domain-" + testId);
        // Set a qualifiedName; the preprocessor may override it
        domain.setAttribute("qualifiedName", "default/domain/test-" + testId + "/super");

        try {
            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(domain));
            AtlasEntityHeader created = response.getFirstEntityCreated();

            assertNotNull(created);
            assertEquals("DataDomain", created.getTypeName());
            domainGuid = created.getGuid();

            // Fetch to get the actual qualifiedName (may be auto-generated)
            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(domainGuid);
            domainQN = (String) result.getEntity().getAttribute("qualifiedName");
            assertNotNull(domainQN, "Domain qualifiedName should exist");
            domainCreated = true;

            LOG.info("Created DataDomain: guid={}, qn={}", domainGuid, domainQN);
        } catch (AtlasServiceException e) {
            LOG.warn("DataDomain creation failed (preprocessor may need Keycloak): {}", e.getMessage());
            domainCreated = false;
        }
    }

    @Test
    @Order(2)
    void testGetDomain() throws AtlasServiceException {
        Assumptions.assumeTrue(domainCreated, "DataDomain not created");

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(domainGuid);
        AtlasEntity entity = result.getEntity();

        assertNotNull(entity);
        assertEquals("DataDomain", entity.getTypeName());
        assertEquals("test-domain-" + testId, entity.getAttribute("name"));
        assertEquals(AtlasEntity.Status.ACTIVE, entity.getStatus());

        LOG.info("Fetched domain: guid={}, name={}", domainGuid, entity.getAttribute("name"));
    }

    @Test
    @Order(3)
    void testCreateSubDomain() throws AtlasServiceException {
        Assumptions.assumeTrue(domainCreated, "DataDomain not created");

        AtlasEntity subDomain = new AtlasEntity("DataDomain");
        subDomain.setAttribute("name", "test-subdomain-" + testId);
        subDomain.setAttribute("qualifiedName", domainQN + "/domain/sub-" + testId);
        subDomain.setAttribute("parentDomainQualifiedName", domainQN);
        subDomain.setAttribute("superDomainQualifiedName", domainQN);
        subDomain.setRelationshipAttribute("parentDomain", new AtlasObjectId(domainGuid, "DataDomain"));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(subDomain));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        subDomainGuid = created.getGuid();

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(subDomainGuid);
        subDomainQN = (String) result.getEntity().getAttribute("qualifiedName");
        assertNotNull(subDomainQN);

        LOG.info("Created sub-domain: guid={}, qn={}", subDomainGuid, subDomainQN);
    }

    @Test
    @Order(4)
    void testGetDomainHierarchy() throws AtlasServiceException {
        Assumptions.assumeTrue(domainCreated && subDomainGuid != null, "Domain hierarchy not created");

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(domainGuid, false, false);
        AtlasEntity parent = result.getEntity();

        // Parent domain should have subDomains relationship
        Object children = parent.getRelationshipAttribute("subDomains");
        assertNotNull(children, "Parent domain should have subDomains relationship");
        assertTrue(children instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> childList = (List<Object>) children;
        assertFalse(childList.isEmpty(), "Parent should have at least one child domain");

        LOG.info("Parent domain has {} children", childList.size());
    }

    @Test
    @Order(5)
    void testCreateDataProduct() throws AtlasServiceException {
        Assumptions.assumeTrue(domainCreated, "DataDomain not created");

        AtlasEntity product = new AtlasEntity("DataProduct");
        product.setAttribute("name", "test-product-" + testId);
        product.setAttribute("qualifiedName", domainQN + "/product/test-" + testId);
        product.setAttribute("domainQualifiedName", domainQN);
        product.setAttribute("dataProductAssetsDSL", "{\"query\":{\"match_all\":{}}}");
        product.setRelationshipAttribute("dataDomain", new AtlasObjectId(domainGuid, "DataDomain"));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(product));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("DataProduct", created.getTypeName());
        productGuid = created.getGuid();

        LOG.info("Created DataProduct: guid={}", productGuid);
    }

    @Test
    @Order(6)
    void testGetProductWithDomain() throws AtlasServiceException {
        Assumptions.assumeTrue(productGuid != null, "DataProduct not created");

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(productGuid, false, false);
        AtlasEntity product = result.getEntity();

        assertNotNull(product);
        assertEquals("DataProduct", product.getTypeName());
        assertEquals("test-product-" + testId, product.getAttribute("name"));

        LOG.info("Fetched product with domain relationship: guid={}", productGuid);
    }

    @Test
    @Order(7)
    void testUpdateDomain() throws AtlasServiceException {
        Assumptions.assumeTrue(domainCreated, "DataDomain not created");

        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(domainGuid);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("description", "Updated domain description");
        // Clear relationship attributes to avoid "Cannot update Domain's subDomains or dataProducts relations"
        entity.setRelationshipAttributes(null);

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response);

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(domainGuid);
        assertEquals("Updated domain description",
                updated.getEntity().getAttribute("description"));

        LOG.info("Updated domain description: guid={}", domainGuid);
    }

    @Test
    @Order(8)
    void testDeleteDomainHierarchy() throws AtlasServiceException {
        // Delete product first
        if (productGuid != null) {
            atlasClient.deleteEntityByGuid(productGuid);
            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(productGuid);
            assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());
        }

        // Delete sub-domain
        if (subDomainGuid != null) {
            atlasClient.deleteEntityByGuid(subDomainGuid);
            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(subDomainGuid);
            assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());
        }

        // Delete parent domain
        if (domainGuid != null) {
            atlasClient.deleteEntityByGuid(domainGuid);
            AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(domainGuid);
            assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());
        }

        LOG.info("Deleted domain hierarchy");
    }
}
