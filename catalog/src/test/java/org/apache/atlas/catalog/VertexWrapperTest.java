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

package org.apache.atlas.catalog;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;
import org.testng.annotations.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.*;

/**
 * Unit tests for VertexWrapper.
 */
public class VertexWrapperTest {
    @Test
    public void testGetVertex() {
        Vertex v = createStrictMock(Vertex.class);
        ResourceDefinition resourceDefinition = createStrictMock(ResourceDefinition.class);

        // just return null for these because they aren't used in this test
        expect(resourceDefinition.getPropertyMapper()).andReturn(null);
        expect(resourceDefinition.getPropertyValueFormatters()).andReturn(null);
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn("testType");
        replay(v, resourceDefinition);

        VertexWrapper vWrapper = new VertexWrapper(v, resourceDefinition);

        assertEquals(vWrapper.getVertex(), v);
        verify(v, resourceDefinition);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetProperty() {
        String testType = "testType";
        String propName = "propName";
        String qualifiedPropName = "Prefix.propName";
        String propValue = "val";
        String formattedValue = "value";
        Vertex v = createStrictMock(Vertex.class);
        PropertyMapper propertyMapper = createStrictMock(PropertyMapper.class);
        PropertyValueFormatter formatter = createStrictMock(PropertyValueFormatter.class);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(propertyMapper.toFullyQualifiedName(propName, testType)).andReturn(qualifiedPropName);
        expect(v.getProperty(qualifiedPropName)).andReturn(propValue);
        expect(formatter.format(propValue)).andReturn((formattedValue));
        replay(v, propertyMapper, formatter);

        VertexWrapper vWrapper = new VertexWrapper(v, propertyMapper, Collections.singletonMap(propName, formatter));
        assertEquals(vWrapper.getProperty(propName), formattedValue);

        // now remove prop
        vWrapper.removeProperty(propName);
        assertNull(vWrapper.getProperty(propName));

        verify(v, propertyMapper, formatter);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetProperty2() {
        String testType = "testType";
        String propName = "propName";
        String qualifiedPropName = "Prefix.propName";
        String propValue = "val";
        String formattedValue = "value";
        Vertex v = createStrictMock(Vertex.class);
        ResourceDefinition resourceDefinition = createStrictMock(ResourceDefinition.class);
        PropertyMapper propertyMapper = createStrictMock(PropertyMapper.class);
        PropertyValueFormatter formatter = createStrictMock(PropertyValueFormatter.class);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(resourceDefinition.getPropertyMapper()).andReturn(propertyMapper);
        expect(resourceDefinition.getPropertyValueFormatters()).andReturn(Collections.singletonMap(propName, formatter));
        expect(propertyMapper.toFullyQualifiedName(propName, testType)).andReturn(qualifiedPropName);
        expect(v.getProperty(qualifiedPropName)).andReturn(propValue);
        expect(formatter.format(propValue)).andReturn((formattedValue));
        replay(v, resourceDefinition, propertyMapper, formatter);

        VertexWrapper vWrapper = new VertexWrapper(v, resourceDefinition);
        assertEquals(vWrapper.getProperty(propName), formattedValue);

        // now remove prop
        vWrapper.removeProperty(propName);
        assertNull(vWrapper.getProperty(propName));

        verify(v, resourceDefinition, propertyMapper, formatter);
    }

    @Test
    public void testGetProperty_removed() {
        String testType = "testType";
        String propName = "propName";
        Vertex v = createStrictMock(Vertex.class);
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        // vertex shouldn't be asked for the removed property
        replay(v);

        VertexWrapper vWrapper = new VertexWrapper(v, null, Collections.<String, PropertyValueFormatter>emptyMap());
        vWrapper.removeProperty(propName);

        assertNull(vWrapper.getProperty(propName));
        verify(v);
    }

    @Test
    public void testGetPropertyKeys() {
        String testType = "testType";
        // vertex returns unordered set
        Set<String> propertyKeys = new HashSet<>();
        propertyKeys.add("foobar");
        propertyKeys.add("Prefix.foo");
        propertyKeys.add("Prefix.bar");

        Vertex v = createStrictMock(Vertex.class);
        PropertyMapper propertyMapper = createMock(PropertyMapper.class);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(v.getPropertyKeys()).andReturn(propertyKeys);
        expect(propertyMapper.toCleanName("Prefix.bar", testType)).andReturn("bar");
        expect(propertyMapper.toCleanName("Prefix.foo", testType)).andReturn("foo");
        expect(propertyMapper.toCleanName("foobar", testType)).andReturn("foobar");

        replay(v, propertyMapper);

        VertexWrapper vWrapper = new VertexWrapper(v, propertyMapper,
                Collections.<String, PropertyValueFormatter>emptyMap());

        Collection<String> resultKeys = vWrapper.getPropertyKeys();
        Iterator<String> propIterator = resultKeys.iterator();
        assertEquals(resultKeys.size(), 3);
        // natural ordering is applied in vertex wrapper
        assertEquals(propIterator.next(), "bar");
        assertEquals(propIterator.next(), "foo");
        assertEquals(propIterator.next(), "foobar");

        verify(v, propertyMapper);
    }

    @Test
    public void testGetPropertyKeys_removed() {
        String testType = "testType";
        Set<String> propertyKeys = new TreeSet<>();
        propertyKeys.add("Prefix.foo");
        propertyKeys.add("Prefix.bar");
        propertyKeys.add("foobar");

        Vertex v = createStrictMock(Vertex.class);
        PropertyMapper propertyMapper = createStrictMock(PropertyMapper.class);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(v.getPropertyKeys()).andReturn(propertyKeys);
        // natural ordering provided by TreeSet
        expect(propertyMapper.toCleanName("Prefix.bar", testType)).andReturn("bar");
        expect(propertyMapper.toCleanName("Prefix.foo", testType)).andReturn("foo");
        expect(propertyMapper.toCleanName("foobar", testType)).andReturn("foobar");

        replay(v, propertyMapper);

        VertexWrapper vWrapper = new VertexWrapper(v, propertyMapper,
                Collections.<String, PropertyValueFormatter>emptyMap());

        // remove props
        vWrapper.removeProperty("foo");
        vWrapper.removeProperty("foobar");

        Collection<String> resultKeys = vWrapper.getPropertyKeys();
        assertEquals(resultKeys.size(), 1);
        assertTrue(resultKeys.contains("bar"));

        verify(v, propertyMapper);
    }

    @Test
    public void testGetPropertyMap() {
        String testType = "testType";
        Set<String> propertyKeys = new HashSet<>();
        propertyKeys.add("Prefix.foo");
        propertyKeys.add("Prefix.bar");
        propertyKeys.add("foobar");

        Vertex v = createMock(Vertex.class);
        PropertyMapper propertyMapper = createMock(PropertyMapper.class);
        PropertyValueFormatter formatter = createMock(PropertyValueFormatter.class);

        Map<String, PropertyValueFormatter> valueFormatters = new HashMap<>();
        valueFormatters.put("foo", formatter);
        valueFormatters.put("bar", formatter);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(v.getPropertyKeys()).andReturn(propertyKeys);
        expect(v.getProperty("Prefix.foo")).andReturn("Prefix.foo:Value");
        expect(v.getProperty("Prefix.bar")).andReturn("Prefix.bar:Value");
        expect(v.getProperty("foobar")).andReturn("foobarValue");

        expect(propertyMapper.toCleanName("Prefix.bar", testType)).andReturn("bar");
        expect(propertyMapper.toCleanName("Prefix.foo", testType)).andReturn("foo");
        expect(propertyMapper.toCleanName("foobar", testType)).andReturn("foobar");

        expect(formatter.format("Prefix.foo:Value")).andReturn("fooValue");
        expect(formatter.format("Prefix.bar:Value")).andReturn("barValue");

        replay(v, propertyMapper, formatter);

        VertexWrapper vWrapper = new VertexWrapper(v, propertyMapper, valueFormatters);
        Map<String, Object> resultMap = vWrapper.getPropertyMap();

        assertEquals(resultMap.size(), 3);
        Iterator<Map.Entry<String, Object>> iter = resultMap.entrySet().iterator();
        Map.Entry<String, Object> entry1 = iter.next();
        assertEquals(entry1.getKey(), "bar");
        assertEquals(entry1.getValue(), "barValue");
        Map.Entry<String, Object> entry2 = iter.next();
        assertEquals(entry2.getKey(), "foo");
        assertEquals(entry2.getValue(), "fooValue");
        Map.Entry<String, Object> entry3 = iter.next();
        assertEquals(entry3.getKey(), "foobar");
        assertEquals(entry3.getValue(), "foobarValue");

        verify(v, propertyMapper, formatter);
    }

    @Test
    public void testGetPropertyMap_removed() {
        String testType = "testType";
        Set<String> propertyKeys = new HashSet<>();
        propertyKeys.add("Prefix.foo");
        propertyKeys.add("Prefix.bar");
        propertyKeys.add("foobar");

        Vertex v = createMock(Vertex.class);
        PropertyMapper propertyMapper = createMock(PropertyMapper.class);
        PropertyValueFormatter formatter = createMock(PropertyValueFormatter.class);

        Map<String, PropertyValueFormatter> valueFormatters = new HashMap<>();
        valueFormatters.put("foo", formatter);
        valueFormatters.put("bar", formatter);

        // mock expectations
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(v.getPropertyKeys()).andReturn(propertyKeys);
        expect(v.getProperty("Prefix.bar")).andReturn("Prefix.bar:Value");
        expect(v.getProperty("foobar")).andReturn("foobarValue");

        expect(propertyMapper.toCleanName("Prefix.bar", testType)).andReturn("bar");
        expect(propertyMapper.toCleanName("Prefix.foo", testType)).andReturn("foo");
        expect(propertyMapper.toCleanName("foobar", testType)).andReturn("foobar");

        expect(formatter.format("Prefix.bar:Value")).andReturn("barValue");

        replay(v, propertyMapper, formatter);

        VertexWrapper vWrapper = new VertexWrapper(v, propertyMapper, valueFormatters);
        //remove "foo" property
        vWrapper.removeProperty("foo");

        Map<String, Object> resultMap = vWrapper.getPropertyMap();
        assertEquals(resultMap.size(), 2);

        Iterator<Map.Entry<String, Object>> iter = resultMap.entrySet().iterator();
        Map.Entry<String, Object> entry1 = iter.next();
        assertEquals(entry1.getKey(), "bar");
        assertEquals(entry1.getValue(), "barValue");
        Map.Entry<String, Object> entry2 = iter.next();
        assertEquals(entry2.getKey(), "foobar");
        assertEquals(entry2.getValue(), "foobarValue");

        verify(v, propertyMapper, formatter);
    }

    @Test
    public void testIsPropertyRemoved() {
        String testType = "testType";
        Vertex v = createMock(Vertex.class);
        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        replay(v);

        VertexWrapper vWrapper = new VertexWrapper(v, null,
                Collections.<String, PropertyValueFormatter>emptyMap());

        vWrapper.removeProperty("foo");
        assertTrue(vWrapper.isPropertyRemoved("foo"));
        assertFalse(vWrapper.isPropertyRemoved("bar"));
    }

    @Test
    public void testSetProperty() {
        String testType = "testType";
        String cleanPropName = "prop1";
        String qualifiedPropName = "test.prop1";
        String propValue = "newValue";
        Vertex v = createStrictMock(Vertex.class);
        PropertyMapper propertyMapper = createStrictMock(PropertyMapper.class);

        expect(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY)).andReturn(testType);
        expect(propertyMapper.toFullyQualifiedName(cleanPropName, testType)).andReturn(qualifiedPropName);
        v.setProperty(qualifiedPropName, propValue);
        replay(v, propertyMapper);

        VertexWrapper vWrapper = new VertexWrapper(
                v, propertyMapper, Collections.<String, PropertyValueFormatter>emptyMap());
        vWrapper.setProperty(cleanPropName, propValue);
        verify(v, propertyMapper);

    }
}
