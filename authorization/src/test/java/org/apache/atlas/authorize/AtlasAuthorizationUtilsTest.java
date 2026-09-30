/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests for {@link AtlasAuthorizationUtils#scrubEntityHeader(AtlasEntityHeader, org.apache.atlas.type.AtlasTypeRegistry)},
 * the helper used by lineage to redact headers of entities the caller cannot read (ENTITY_READ).
 */
public class AtlasAuthorizationUtilsTest {
    private String originalConf;

    @BeforeMethod
    public void setUp() {
        originalConf = System.getProperty("atlas.conf");

        // src/test/resources contains atlas-application.properties (SIMPLE authorizer) + policy file
        System.setProperty("atlas.conf", "src/test/resources");
    }

    @AfterMethod
    public void tearDown() {
        SecurityContextHolder.clearContext();
        RequestContext.clear();

        if (originalConf != null) {
            System.setProperty("atlas.conf", originalConf);
        }
    }

    @Test
    public void testScrubEntityHeaderRedactsUnauthorizedEntity() {
        // user in an unknown group has no ENTITY_READ grant in the simple-authz policy
        setCurrentUser("unknown-group-user", "UNKNOWN_GROUP");

        AtlasEntityHeader entity = newEntityHeader();

        AtlasAuthorizationUtils.scrubEntityHeader(entity, null);

        // default scrub sets guid to "-1" and clears attributes/classifications
        assertEquals("unauthorized entity guid should be scrubbed", "-1", entity.getGuid());
        assertTrue("unauthorized entity classifications should be cleared",
                entity.getClassifications() == null || entity.getClassifications().isEmpty());
        assertTrue("unauthorized entity attributes should be cleared",
                entity.getAttributes() == null || entity.getAttributes().isEmpty());
    }

    @Test
    public void testScrubEntityHeaderKeepsAuthorizedEntity() {
        // admin (ROLE_ADMIN) is allowed ENTITY_READ, so the header must be left untouched
        setCurrentUser("admin", "ROLE_ADMIN");

        AtlasEntityHeader entity = newEntityHeader();

        AtlasAuthorizationUtils.scrubEntityHeader(entity, null);

        assertEquals("authorized entity guid should be preserved", "guid-1", entity.getGuid());
        assertNotNull("authorized entity classifications should be preserved", entity.getClassifications());
        assertEquals("authorized entity classifications should be preserved", 1, entity.getClassifications().size());
        assertNotNull("authorized entity attributes should be preserved", entity.getAttributes());
        assertEquals("authorized entity attributes should be preserved",
                "db.table@cluster", entity.getAttribute("qualifiedName"));
    }

    @Test
    public void testScrubEntityHeaderHandlesNull() {
        // must be a no-op (no NPE) for a null header
        AtlasAuthorizationUtils.scrubEntityHeader(null, null);
    }

    private AtlasEntityHeader newEntityHeader() {
        AtlasEntityHeader entity = new AtlasEntityHeader("hive_table");

        entity.setGuid("guid-1");
        entity.setAttribute("qualifiedName", "db.table@cluster");
        entity.setClassifications(new ArrayList<>(Collections.singletonList(new AtlasClassification("PII"))));

        return entity;
    }

    private void setCurrentUser(String userName, String group) {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(userName, null,
                        Collections.singletonList(new SimpleGrantedAuthority(group))));
    }
}
