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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.JanusGraphElement;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasJanusElementTest {
    @Mock
    private AtlasJanusGraph mockGraph;

    @Mock
    private Element mockElement;

    @Mock
    private JanusGraphElement mockJanusGraphElement;

    @Mock
    private Property<Object> mockProperty;

    private AtlasJanusElement<Element> element;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
        element = new AtlasJanusElement<>(mockGraph, mockElement);
    }

    @Test
    public void testConstructor() {
        AtlasJanusElement<Element> testElement = new AtlasJanusElement<>(mockGraph, mockElement);
        assertNotNull(testElement);
        assertEquals(testElement.getWrappedElement(), mockElement);
    }

    @Test
    public void testGetId() {
        Object expectedId = "testId";
        when(mockElement.id()).thenReturn(expectedId);

        Object actualId = element.getId();
        assertEquals(actualId, expectedId);
    }

    @Test
    public void testGetPropertyKeys() {
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("key1");
        expectedKeys.add("key2");

        when(mockElement.keys()).thenReturn(expectedKeys);

        Set<String> actualKeys = element.getPropertyKeys();
        assertEquals(actualKeys, expectedKeys);
    }

    @Test
    public void testGetProperty() {
        String propertyValue = "testValue";
        when(mockElement.property("testKey")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(propertyValue);

        String result = element.getProperty("testKey", String.class);
        assertEquals(result, propertyValue);
    }

    @Test
    public void testGetPropertyNotPresent() {
        when(mockElement.property("nonExistentKey")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(false);

        String result = element.getProperty("nonExistentKey", String.class);
        assertNull(result);
    }

    @Test
    public void testGetPropertyNullValue() {
        when(mockElement.property("nullKey")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(null);

        String result = element.getProperty("nullKey", String.class);
        assertNull(result);
    }

    @Test
    public void testGetPropertyAtlasEdge() {
        String edgeId = "edgeId123";
        AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> mockEdge = mock(AtlasEdge.class);

        when(mockElement.property("edgeProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(edgeId);
        when(mockGraph.getEdge(edgeId)).thenReturn(mockEdge);

        AtlasEdge result = element.getProperty("edgeProperty", AtlasEdge.class);
        assertEquals(result, mockEdge);
    }

    @Test
    public void testGetPropertyAtlasVertex() {
        String vertexId = "vertexId123";
        AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> mockVertex = mock(AtlasVertex.class);

        when(mockElement.property("vertexProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(vertexId);
        when(mockGraph.getVertex(vertexId)).thenReturn(mockVertex);

        AtlasVertex result = element.getProperty("vertexProperty", AtlasVertex.class);
        assertEquals(result, mockVertex);
    }

    @Test
    public void testGetPropertyValues() {
        String propertyValue = "testValue";
        when(mockElement.property("testKey")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(propertyValue);

        Collection<String> result = element.getPropertyValues("testKey", String.class);
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertTrue(result.contains(propertyValue));
    }

    @Test
    public void testGetListProperty() {
        List<String> expectedList = Arrays.asList("item1", "item2", "item3");
        when(mockElement.property("listProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(expectedList);

        List<String> result = element.getListProperty("listProperty");
        assertEquals(result, expectedList);
    }

    @Test
    public void testGetListPropertyWithElementType() {
        List<String> stringList = Arrays.asList("id1", "id2", "id3");
        when(mockElement.property("listProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(stringList);

        List<String> result = element.getListProperty("listProperty", String.class);
        assertEquals(result, stringList);
    }

    @Test
    public void testGetListPropertyWithAtlasEdgeType() {
        List<String> edgeIds = Arrays.asList("edge1", "edge2");
        List<AtlasEdge<?, ?>> mockEdges = new ArrayList<>();
        AtlasEdge<?, ?> mockEdge1 = mock(AtlasEdge.class);
        AtlasEdge<?, ?> mockEdge2 = mock(AtlasEdge.class);
        mockEdges.add(mockEdge1);
        mockEdges.add(mockEdge2);

        when(mockElement.property("edgeListProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(edgeIds);
        when(mockGraph.getEdge("edge1")).thenReturn(ArgumentMatchers.any());
        when(mockGraph.getEdge("edge2")).thenReturn(ArgumentMatchers.any());

        List<AtlasEdge> result = element.getListProperty("edgeListProperty", AtlasEdge.class);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetListPropertyWithAtlasVertexType() {
        List<String> vertexIds = Arrays.asList("vertex1", "vertex2");
        List<AtlasVertex<?, ?>> mockVertices = new ArrayList<>();
        AtlasVertex<?, ?> mockVertex1 = mock(AtlasVertex.class);
        AtlasVertex<?, ?> mockVertex2 = mock(AtlasVertex.class);
        mockVertices.add(mockVertex1);
        mockVertices.add(mockVertex2);

        when(mockElement.property("vertexListProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(vertexIds);
        when(mockGraph.getVertex("vertex1")).thenReturn(ArgumentMatchers.any());
        when(mockGraph.getVertex("vertex2")).thenReturn(ArgumentMatchers.any());

        List<AtlasVertex> result = element.getListProperty("vertexListProperty", AtlasVertex.class);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetListPropertyEmpty() {
        List<String> emptyList = Collections.emptyList();
        when(mockElement.property("emptyListProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(emptyList);

        List<String> result = element.getListProperty("emptyListProperty", String.class);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetListPropertyNull() {
        when(mockElement.property("nullListProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(null);

        List<String> result = element.getListProperty("nullListProperty", String.class);
        assertNull(result);
    }

    @Test
    public void testSetListProperty() {
        List<String> testList = Arrays.asList("item1", "item2");

        element.setListProperty("testList", testList);
    }

    @Test
    public void testSetPropertyFromElementsIds() {
        List<AtlasElement> elements = new ArrayList<>();
        AtlasElement element1 = mock(AtlasElement.class);
        AtlasElement element2 = mock(AtlasElement.class);

        when(element1.getId()).thenReturn("id1");
        when(element2.getId()).thenReturn("id2");

        elements.add(element1);
        elements.add(element2);

        element.setPropertyFromElementsIds("elementIds", elements);
    }

    @Test
    public void testSetPropertyFromElementId() {
        AtlasElement atlasElement = mock(AtlasElement.class);
        when(atlasElement.getId()).thenReturn("elementId");

        element.setPropertyFromElementId("elementId", atlasElement);
    }

    @Test
    public void testRemoveProperty() {
        Property<Object> property1 = mock(Property.class);
        Property<Object> property2 = mock(Property.class);

        Iterator<Property<Object>> propertyIterator = Arrays.asList(property1, property2).iterator();
        when(mockElement.properties("testProperty")).thenReturn((Iterator) propertyIterator);

        element.removeProperty("testProperty");
    }

    @Test
    public void testRemovePropertyValue() {
        Property<Object> property1 = mock(Property.class);
        Property<Object> property2 = mock(Property.class);

        when(property1.value()).thenReturn("value1");
        when(property2.value()).thenReturn("value2");

        Iterator<Property<Object>> propertyIterator = Arrays.asList(property1, property2).iterator();
        when(mockElement.properties("testProperty")).thenReturn((Iterator) propertyIterator);

        element.removePropertyValue("testProperty", "value1");
    }

    @Test
    public void testRemoveAllPropertyValue() {
        Property<Object> property1 = mock(Property.class);
        Property<Object> property2 = mock(Property.class);
        Property<Object> property3 = mock(Property.class);

        when(property1.value()).thenReturn("value1");
        when(property2.value()).thenReturn("value1");
        when(property3.value()).thenReturn("value2");

        Iterator<Property<Object>> propertyIterator = Arrays.asList(property1, property2, property3).iterator();
        when(mockElement.properties("testProperty")).thenReturn((Iterator) propertyIterator);

        element.removeAllPropertyValue("testProperty", "value1");
    }

    @Test
    public void testSetProperty() {
        element.setProperty("testProperty", "testValue");
    }

    @Test
    public void testSetPropertyWithNullValue() {
        String existingValue = "existingValue";
        when(mockElement.property("testProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(existingValue);
        when(mockElement.properties("testProperty")).thenReturn((Iterator) Collections.singletonList(mockProperty).iterator());

        // Should remove property when setting to null
        element.setProperty("testProperty", null);
        verify(mockProperty, times(1)).remove();
    }

    @Test
    public void testSetPropertyWithNullValueNoExisting() {
        when(mockElement.property("testProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(false);

        element.setProperty("testProperty", null);
    }

    @Test
    public void testExists() {
        element = new AtlasJanusElement<>(mockGraph, mockJanusGraphElement);

        when(mockJanusGraphElement.isRemoved()).thenReturn(false);

        boolean result = element.exists();
        assertTrue(result);
    }

    @Test
    public void testExistsRemoved() {
        element = new AtlasJanusElement<>(mockGraph, mockJanusGraphElement);

        when(mockJanusGraphElement.isRemoved()).thenReturn(true);

        boolean result = element.exists();
        assertFalse(result);
    }

    @Test
    public void testExistsWithException() {
        element = new AtlasJanusElement<>(mockGraph, mockJanusGraphElement);

        when(mockJanusGraphElement.isRemoved()).thenThrow(new IllegalStateException("Element removed"));

        boolean result = element.exists();
        assertFalse(result);
    }

    @Test
    public void testSetJsonProperty() {
        Object testValue = "testJsonValue";

        element.setJsonProperty("jsonProperty", testValue);
    }

    @Test
    public void testGetJsonProperty() {
        String jsonValue = "jsonValue";
        when(mockElement.property("jsonProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(jsonValue);

        String result = element.getJsonProperty("jsonProperty");
        assertEquals(result, jsonValue);
    }

    @Test
    public void testGetIdForDisplay() {
        Object id = "displayId";
        when(mockElement.id()).thenReturn(id);

        String result = element.getIdForDisplay();
        assertEquals(result, "displayId");
    }

    @Test
    public void testIsIdAssigned() {
        boolean result = element.isIdAssigned();
        assertTrue(result); // Should always return true
    }

    @Test
    public void testGetWrappedElement() {
        Element wrappedElement = element.getWrappedElement();
        assertEquals(wrappedElement, mockElement);
    }

    @Test
    public void testHashCode() {
        int hashCode1 = element.hashCode();
        int hashCode2 = element.hashCode();

        assertEquals(hashCode1, hashCode2); // Same object should have same hash code

        // Test with different element
        Element anotherElement = mock(Element.class);
        AtlasJanusElement<Element> anotherAtlasElement = new AtlasJanusElement<>(mockGraph, anotherElement);

        // Hash codes may or may not be different, but should be consistent
        int anotherHashCode = anotherAtlasElement.hashCode();
        assertEquals(anotherHashCode, anotherAtlasElement.hashCode());
    }

    @Test
    public void testEquals() {
        // Test equality with itself
        assertTrue(element.equals(element));

        // Test equality with null
        assertFalse(element.equals(null));

        // Test equality with different type
        assertFalse(element.equals("not an AtlasJanusElement"));

        // Test equality with same wrapped element
        AtlasJanusElement<Element> sameElement = new AtlasJanusElement<>(mockGraph, mockElement);
        assertTrue(element.equals(sameElement));

        // Test equality with different wrapped element
        Element differentElement = mock(Element.class);
        AtlasJanusElement<Element> differentAtlasElement = new AtlasJanusElement<>(mockGraph, differentElement);
        assertFalse(element.equals(differentAtlasElement));
    }

    @Test
    public void testEqualsWithMockedElements() {
        // Create elements with different IDs for testing
        Element element1 = mock(Element.class);
        Element element2 = mock(Element.class);

        when(element1.id()).thenReturn("id1");
        when(element2.id()).thenReturn("id2");

        AtlasJanusElement<Element> atlasElement1 = new AtlasJanusElement<>(mockGraph, element1);
        AtlasJanusElement<Element> atlasElement2 = new AtlasJanusElement<>(mockGraph, element2);
        AtlasJanusElement<Element> atlasElement1Copy = new AtlasJanusElement<>(mockGraph, element1);

        assertTrue(atlasElement1.equals(atlasElement1Copy));
        assertFalse(atlasElement1.equals(atlasElement2));
    }

    @Test
    public void testRemovePropertyValueWithNullObjects() {
        Property<Object> property1 = mock(Property.class);
        Property<Object> property2 = mock(Property.class);

        when(property1.value()).thenReturn(null);
        when(property2.value()).thenReturn("value2");

        Iterator<Property<Object>> propertyIterator = Arrays.asList(property1, property2).iterator();
        when(mockElement.properties("testProperty")).thenReturn((Iterator) propertyIterator);

        // Should not throw exception when removing null value
        element.removePropertyValue("testProperty", null);
    }

    @Test
    public void testRemoveAllPropertyValueWithNullObjects() {
        Property<Object> property1 = mock(Property.class);
        Property<Object> property2 = mock(Property.class);
        Property<Object> property3 = mock(Property.class);

        when(property1.value()).thenReturn(null);
        when(property2.value()).thenReturn(null);
        when(property3.value()).thenReturn("value3");

        Iterator<Property<Object>> propertyIterator = Arrays.asList(property1, property2, property3).iterator();
        when(mockElement.properties("testProperty")).thenReturn((Iterator) propertyIterator);

        // Should not throw exception when removing all null values
        element.removeAllPropertyValue("testProperty", null);
    }

    @Test
    public void testGetPropertyWithComplexId() {
        Object complexId = new Object() {
            @Override
            public String toString() {
                return "complexId123";
            }
        };

        AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> mockVertex = mock(AtlasVertex.class);

        when(mockElement.property("complexProperty")).thenReturn(mockProperty);
        when(mockProperty.isPresent()).thenReturn(true);
        when(mockProperty.value()).thenReturn(complexId.toString());
        when(mockGraph.getVertex("complexId123")).thenReturn(mockVertex);

        AtlasVertex result = element.getProperty("complexProperty", AtlasVertex.class);
        assertEquals(result, mockVertex);
    }
}
