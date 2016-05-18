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
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.Relation;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for EntityTagResourceDefinition.
 */
public class EntityTagResourceDefinitionTest {
    @Test
    public void testGetIdPropertyName() {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        assertEquals(entityTagDefinition.getIdPropertyName(), "name");
    }

    @Test
    public void testGetTypeName() {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        assertNull(entityTagDefinition.getTypeName());
    }

    @Test
    public void testResolveHref() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("name", "taxonomy1.term1.term11");
        resourceProps.put(EntityTagResourceDefinition.ENTITY_GUID_PROPERTY, "11-22-33");

        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        String href = entityTagDefinition.resolveHref(resourceProps);
        assertEquals(href, "v1/entities/11-22-33/tags/taxonomy1.term1.term11");
    }

    @Test
    public void testValidate() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "taxonomy1.termName");

        Request request = new InstanceRequest(properties);

        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        entityTagDefinition.validate(request);
    }

    @Test(expectedExceptions = InvalidPayloadException.class)
    public void testValidate_missingName() throws Exception {
        Map<String, Object> properties = new HashMap<>();

        Request request = new InstanceRequest(properties);
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        entityTagDefinition.validate(request);
    }

    @Test(expectedExceptions = InvalidPayloadException.class)
    public void testValidate_invalidProperty() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "foo");
        properties.put("description", "desc");

        Request request = new InstanceRequest(properties);

        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        entityTagDefinition.validate(request);
    }

    @Test
    public void testGetPropertyDefinitions() {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        Collection<AttributeDefinition> propertyDefinitions = entityTagDefinition.getPropertyDefinitions();

        assertEquals(propertyDefinitions.size(), 1);
        Set<String> defNames = new HashSet<>();
        for (AttributeDefinition def : propertyDefinitions) {
            defNames.add(def.name);
        }
        assertTrue(defNames.contains("name"));
    }

    @Test
    public void testFilterProperties_Instance() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", "111-222-333");
        resourceProps.put("name", "nameVal");
        resourceProps.put("type", "someType");
        resourceProps.put("foo", "fooVal");
        resourceProps.put("bar", "barVal");
        resourceProps.put("description", "desc");
        resourceProps.put("creation_time", "2016:10:10");
        resourceProps.put("acceptable_use", "something");
        resourceProps.put("available_as_tag", true);
        resourceProps.put("other", "otherVal");

        Request request = new InstanceRequest(resourceProps);
        request.addAdditionalSelectProperties(Collections.singleton("foo"));
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();

        Map<String, Object> filteredProperties = entityTagDefinition.filterProperties(request, resourceProps);
        assertEquals(filteredProperties.size(), 4);
        // registered collection props
        assertTrue(filteredProperties.containsKey("name"));
        assertTrue(filteredProperties.containsKey("description"));
        assertTrue(filteredProperties.containsKey("creation_time"));
        // added prop
        assertTrue(filteredProperties.containsKey("foo"));
    }

    @Test
    public void testFilterProperties_Collection() {
        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", "111-222-333");
        resourceProps.put("name", "nameVal");
        resourceProps.put("type", "someType");
        resourceProps.put("foo", "fooVal");
        resourceProps.put("bar", "barVal");
        resourceProps.put("description", "desc");
        resourceProps.put("creation_time", "2016:10:10");
        resourceProps.put("acceptable_use", "something");
        resourceProps.put("available_as_tag", true);
        resourceProps.put("other", "otherVal");

        Request request = new CollectionRequest(resourceProps, "someProperty:someValue");
        request.addAdditionalSelectProperties(Collections.singleton("foo"));
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();

        Map<String, Object> filteredProps = entityTagDefinition.filterProperties(request, resourceProps);
        assertEquals(filteredProps.size(), 3);
        // registered collection props
        assertTrue(filteredProps.containsKey("name"));
        assertTrue(filteredProps.containsKey("description"));
        // added prop
        assertTrue(filteredProps.containsKey("foo"));
    }

    @Test
    public void testGetProjections() {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        Map<String, Projection> projections = entityTagDefinition.getProjections();
        assertEquals(projections.size(), 1);
        assertTrue(projections.containsKey("terms"));
    }

    @Test
    public void testGetRelations() {
        ResourceDefinition entityTagDefinition = new EntityTagResourceDefinition();
        Map<String, Relation> relations = entityTagDefinition.getRelations();
        assertTrue(relations.isEmpty());
    }
}
