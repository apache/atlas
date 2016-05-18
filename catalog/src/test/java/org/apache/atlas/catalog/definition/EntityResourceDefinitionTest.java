/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog.definition;

import org.apache.atlas.catalog.CollectionRequest;
import org.apache.atlas.catalog.InstanceRequest;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.Relation;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for EntityResourceDefinition.
 */
public class EntityResourceDefinitionTest {
    @Test
    public void testGetIdPropertyName() {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        assertEquals(entityDefinition.getIdPropertyName(), "id");
    }

    @Test
    public void testGetTypeName() {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        assertNull(entityDefinition.getTypeName());
    }

    @Test
    public void testResolveHref() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", "111-222-333");
        resourceProps.put("name", "foo");

        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        String href = entityDefinition.resolveHref(resourceProps);
        assertEquals(href, "v1/entities/111-222-333");
    }

    // Because we don't currently support entity creation, this method is basically a no-op.
    @Test
    public void testValidate() throws Exception {
        Request request = new InstanceRequest(Collections.<String, Object>emptyMap());

        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        entityDefinition.validate(request);
    }

    // Because we don't currently support entity creation, no properties are registered
    @Test
    public void testGetPropertyDefinitions() {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        assertTrue(entityDefinition.getPropertyDefinitions().isEmpty());
    }

    @Test
    public void testFilterProperties_Instance() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", "111-222-333");
        resourceProps.put("name", "nameVal");
        resourceProps.put("type", "someType");
        resourceProps.put("foo", "fooVal");
        resourceProps.put("bar", "barVal");
        resourceProps.put("fooBar", "fooBarVal");
        resourceProps.put("other", "otherVal");

        Request request = new InstanceRequest(resourceProps);
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        // no filtering should occur for entity instances
        assertEquals(entityDefinition.filterProperties(request, resourceProps), resourceProps);
    }

    @Test
    public void testFilterProperties_Collection() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", "111-222-333");
        resourceProps.put("name", "nameVal");
        resourceProps.put("type", "someType");
        resourceProps.put("foo", "fooVal");
        resourceProps.put("bar", "barVal");
        resourceProps.put("fooBar", "fooBarVal");
        resourceProps.put("other", "otherVal");

        Request request = new CollectionRequest(resourceProps, "someProperty:someValue");
        request.addAdditionalSelectProperties(Collections.singleton("foo"));
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        // no filtering should occur for entity instances
        Map<String, Object> filteredProps = entityDefinition.filterProperties(request, resourceProps);
        assertEquals(filteredProps.size(), 4);
        // registered collection props
        assertTrue(filteredProps.containsKey("name"));
        assertTrue(filteredProps.containsKey("id"));
        assertTrue(filteredProps.containsKey("type"));

        // added prop
        assertTrue(filteredProps.containsKey("foo"));
    }

    @Test
    public void testGetProjections() {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        Map<String, Projection> projections = entityDefinition.getProjections();
        assertEquals(projections.size(), 3);
        assertTrue(projections.containsKey("tags"));
        assertTrue(projections.containsKey("traits"));
        assertTrue(projections.containsKey("default"));
    }

    @Test
    public void testGetRelations() {
        ResourceDefinition entityDefinition = new EntityResourceDefinition();
        Map<String, Relation> relations = entityDefinition.getRelations();
        assertEquals(relations.size(), 2);
        assertTrue(relations.containsKey("tags"));
        assertTrue(relations.containsKey("traits"));
    }
}
