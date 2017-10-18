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

package org.apache.atlas.authorize.simple;

import org.apache.atlas.authorize.AtlasResourceTypes;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for AtlasAuthorizationUtils.
 */
public class AtlasAuthorizationUtilsTest {
    @Test
    public void testGetApi() {
        String contextPath = "/api/atlas/entities";
        assertEquals(AtlasAuthorizationUtils.getApi(contextPath), "entities");

        contextPath = "/api/atlas/entities/111/traits";
        assertEquals(AtlasAuthorizationUtils.getApi(contextPath), "entities");

        contextPath = "/api/atlas/v1/entities";
        assertEquals(AtlasAuthorizationUtils.getApi(contextPath), "entities");

        contextPath = "/api/atlas/v1/entities/111/tags";
        assertEquals(AtlasAuthorizationUtils.getApi(contextPath), "entities");

        // not sure of this use case but the code appears to support url's that don't
        // begin with base url.
        contextPath = "/foo/bar";
        assertEquals(AtlasAuthorizationUtils.getApi(contextPath), "foo");
    }

    @Test
    public void testGetAtlasResourceType() throws Exception {
        String contextPath = "/api/atlas/types";
        Set<AtlasResourceTypes> resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.TYPE));

        contextPath = "/api/atlas/admin/foo";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.OPERATION));

        contextPath = "/api/atlas/graph/foo";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.OPERATION));

        contextPath = "/api/atlas/discovery/search/gremlin";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.OPERATION));

        contextPath = "/api/atlas/entities/111/traits";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));

        contextPath = "/api/atlas/discovery/search";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));

        contextPath = "/api/atlas/entities?type=Column";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));

        contextPath = "/api/atlas/lineage";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));

        contextPath = "/api/atlas/v1/entities/111";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));

        contextPath = "/api/atlas/v1/entities/111/tags/foo";
        resourceTypes = AtlasAuthorizationUtils.getAtlasResourceType(contextPath);
        assertEquals(resourceTypes.size(), 1);
        assertTrue(resourceTypes.contains(AtlasResourceTypes.ENTITY));
    }
}
