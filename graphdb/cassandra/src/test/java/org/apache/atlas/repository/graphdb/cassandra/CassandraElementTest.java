package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasElement;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraElement (abstract base class) via CassandraVertex.
 * Covers: property CRUD, type conversions, state management, JSON serialization.
 */
public class CassandraElementTest {

    private CassandraGraph mockGraph;
    private CassandraVertex element; // concrete subclass to test abstract CassandraElement

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== Constructor Tests ========================

    @Test
    public void testNewElementHasIsNewTrue() {
        element = new CassandraVertex("v1", mockGraph);
        assertTrue(element.isNew());
        assertFalse(element.isDirty());
        assertFalse(element.isDeleted());
    }

    @Test
    public void testElementWithPropertiesHasIsNewFalse() {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "test");
        element = new CassandraVertex("v1", props, mockGraph);
        assertFalse(element.isNew());
        assertFalse(element.isDirty());
    }

    @Test
    public void testElementWithNullPropertiesInitializesEmptyMap() {
        element = new CassandraVertex("v1", (Map<String, Object>) null, mockGraph);
        assertNotNull(element.getProperties());
        assertTrue(element.getProperties().isEmpty());
    }

    @Test
    public void testElementCopiesPropertiesMap() {
        Map<String, Object> props = new HashMap<>();
        props.put("key", "value");
        element = new CassandraVertex("v1", props, mockGraph);
        props.put("key2", "value2"); // mutate original
        assertNull(element.getProperty("key2", String.class)); // should not be affected
    }

    // ======================== ID Tests ========================

    @Test
    public void testGetId() {
        element = new CassandraVertex("vertex-123", mockGraph);
        assertEquals(element.getId(), "vertex-123");
    }

    @Test
    public void testGetIdString() {
        element = new CassandraVertex("vertex-123", mockGraph);
        assertEquals(element.getIdString(), "vertex-123");
    }

    @Test
    public void testIsIdAssigned() {
        element = new CassandraVertex("v1", mockGraph);
        assertTrue(element.isIdAssigned());
    }

    @Test
    public void testGetIdForDisplay() {
        element = new CassandraVertex("display-id", mockGraph);
        assertEquals(element.getIdForDisplay(), "display-id");
    }

    // ======================== Property CRUD Tests ========================

    @Test
    public void testSetAndGetProperty() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        assertEquals(element.getProperty("name", String.class), "Atlas");
    }

    @Test
    public void testGetPropertyReturnsNullForMissing() {
        element = new CassandraVertex("v1", mockGraph);
        assertNull(element.getProperty("nonexistent", String.class));
    }

    @Test
    public void testSetPropertyNullRemovesIt() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.setProperty("name", null);
        assertNull(element.getProperty("name", String.class));
    }

    @Test
    public void testRemoveProperty() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.removeProperty("name");
        assertNull(element.getProperty("name", String.class));
    }

    @Test
    public void testGetPropertyKeys() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("a", 1);
        element.setProperty("b", 2);
        element.setProperty("c", 3);
        Collection<? extends String> keys = element.getPropertyKeys();
        assertEquals(keys.size(), 3);
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void testGetPropertyKeysIsUnmodifiable() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("key", "val");
        Collection<? extends String> keys = element.getPropertyKeys();
        try {
            ((Collection<String>) keys).add("newKey");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    // ======================== Type Conversion Tests ========================

    @Test
    public void testGetPropertyAsString() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("count", 42);
        assertEquals(element.getProperty("count", String.class), "42");
    }

    @Test
    public void testGetPropertyAsLong() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("count", 42);
        assertEquals(element.getProperty("count", Long.class), Long.valueOf(42));
    }

    @Test
    public void testGetPropertyAsLongFromString() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("count", "123");
        assertEquals(element.getProperty("count", Long.class), Long.valueOf(123));
    }

    @Test
    public void testGetPropertyAsInteger() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("count", 42L);
        assertEquals(element.getProperty("count", Integer.class), Integer.valueOf(42));
    }

    @Test
    public void testGetPropertyAsBoolean() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("active", "true");
        assertEquals(element.getProperty("active", Boolean.class), Boolean.TRUE);
    }

    @Test
    public void testGetPropertyAsDouble() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("score", 3.14f);
        Double result = element.getProperty("score", Double.class);
        assertNotNull(result);
        assertEquals(result, 3.14, 0.01);
    }

    @Test
    public void testGetPropertyAsFloat() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("score", 3.14);
        Float result = element.getProperty("score", Float.class);
        assertNotNull(result);
        assertEquals(result, 3.14f, 0.01f);
    }

    @Test
    public void testGetPropertyAsShort() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("num", 42);
        assertEquals(element.getProperty("num", Short.class), Short.valueOf((short) 42));
    }

    @Test
    public void testGetPropertyAsByte() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("num", 7);
        assertEquals(element.getProperty("num", Byte.class), Byte.valueOf((byte) 7));
    }

    @Test
    public void testGetPropertyDirectTypeMatch() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        assertEquals(element.getProperty("name", String.class), "Atlas");
    }

    // ======================== Collection Property Tests ========================

    @Test
    public void testGetPropertyValuesFromCollection() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> tags = Arrays.asList("a", "b", "c");
        element.setProperty("tags", tags);
        Collection<String> values = element.getPropertyValues("tags", String.class);
        assertEquals(values.size(), 3);
    }

    @Test
    public void testGetPropertyValuesSingleValue() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        Collection<String> values = element.getPropertyValues("name", String.class);
        assertEquals(values.size(), 1);
        assertTrue(values.contains("Atlas"));
    }

    @Test
    public void testGetPropertyValuesNullReturnsEmpty() {
        element = new CassandraVertex("v1", mockGraph);
        Collection<String> values = element.getPropertyValues("missing", String.class);
        assertTrue(values.isEmpty());
    }

    @Test
    public void testSetListProperty() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> list = Arrays.asList("x", "y", "z");
        element.setListProperty("items", list);
        List<String> result = element.getListProperty("items");
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), "x");
    }

    @Test
    public void testSetListPropertyNullRemoves() {
        element = new CassandraVertex("v1", mockGraph);
        element.setListProperty("items", Arrays.asList("a"));
        element.setListProperty("items", null);
        assertNull(element.getListProperty("items"));
    }

    @Test
    public void testGetMultiValuedProperty() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> list = new ArrayList<>(Arrays.asList("a", "b"));
        element.setProperty("tags", list);
        List<String> result = element.getMultiValuedProperty("tags", String.class);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetMultiValuedPropertySingleValue() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("tag", "solo");
        List<String> result = element.getMultiValuedProperty("tag", String.class);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0), "solo");
    }

    @Test
    public void testGetMultiValuedPropertyNullReturnsEmpty() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> result = element.getMultiValuedProperty("missing", String.class);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetMultiValuedSetProperty() {
        element = new CassandraVertex("v1", mockGraph);
        Set<String> set = new LinkedHashSet<>(Arrays.asList("a", "b"));
        element.setProperty("tags", set);
        Set<String> result = element.getMultiValuedSetProperty("tags", String.class);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("a"));
    }

    @Test
    public void testGetMultiValuedSetPropertyFromList() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> list = Arrays.asList("a", "b", "a"); // duplicate
        element.setProperty("tags", list);
        Set<String> result = element.getMultiValuedSetProperty("tags", String.class);
        assertEquals(result.size(), 2); // deduplicated
    }

    @Test
    public void testGetMultiValuedSetPropertySingleValue() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("tag", "solo");
        Set<String> result = element.getMultiValuedSetProperty("tag", String.class);
        assertEquals(result.size(), 1);
        assertTrue(result.contains("solo"));
    }

    // ======================== Remove Property Value Tests ========================

    @Test
    public void testRemovePropertyValueFromCollection() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> list = new ArrayList<>(Arrays.asList("a", "b", "c"));
        element.setProperty("tags", list);
        element.removePropertyValue("tags", "b");
        List<String> result = element.getMultiValuedProperty("tags", String.class);
        assertEquals(result.size(), 2);
        assertFalse(result.contains("b"));
    }

    @Test
    public void testRemovePropertyValueSingleMatch() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.removePropertyValue("name", "Atlas");
        assertNull(element.getProperty("name", String.class));
    }

    @Test
    public void testRemovePropertyValueNoMatch() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.removePropertyValue("name", "Other");
        assertEquals(element.getProperty("name", String.class), "Atlas");
    }

    @Test
    public void testRemoveAllPropertyValue() {
        element = new CassandraVertex("v1", mockGraph);
        List<String> list = new ArrayList<>(Arrays.asList("a", "b", "a", "c"));
        element.setProperty("tags", list);
        element.removeAllPropertyValue("tags", "a");
        List<String> result = element.getMultiValuedProperty("tags", String.class);
        assertFalse(result.contains("a"));
    }

    // ======================== Element ID Property Tests ========================

    @Test
    public void testSetPropertyFromElementId() {
        element = new CassandraVertex("v1", mockGraph);
        CassandraVertex other = new CassandraVertex("v2", mockGraph);
        element.setPropertyFromElementId("ref", other);
        assertEquals(element.getProperty("ref", String.class), "v2");
    }

    @Test
    public void testSetPropertyFromElementIdNull() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("ref", "v2");
        element.setPropertyFromElementId("ref", null);
        assertNull(element.getProperty("ref", String.class));
    }

    @Test
    public void testSetPropertyFromElementsIds() {
        element = new CassandraVertex("v1", mockGraph);
        List<AtlasElement> elements = new ArrayList<>();
        elements.add(new CassandraVertex("v2", mockGraph));
        elements.add(new CassandraVertex("v3", mockGraph));
        element.setPropertyFromElementsIds("refs", elements);
        List<String> result = element.getListProperty("refs");
        assertEquals(result.size(), 2);
        assertEquals(result.get(0), "v2");
        assertEquals(result.get(1), "v3");
    }

    @Test
    public void testSetPropertyFromElementsIdsEmptyList() {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("refs", Arrays.asList("v2"));
        element.setPropertyFromElementsIds("refs", Collections.emptyList());
        assertNull(element.getProperty("refs", String.class));
    }

    // ======================== JSON Property Tests ========================

    @Test
    public void testSetAndGetJsonProperty() {
        element = new CassandraVertex("v1", mockGraph);
        Map<String, String> data = new HashMap<>();
        data.put("key", "value");
        element.setJsonProperty("config", data);
        Object result = element.getJsonProperty("config");
        assertNotNull(result);
    }

    @Test
    public void testSetJsonPropertyNull() {
        element = new CassandraVertex("v1", mockGraph);
        element.setJsonProperty("config", "something");
        element.setJsonProperty("config", null);
        assertNull(element.getJsonProperty("config"));
    }

    // ======================== toJson Tests ========================

    @Test
    public void testToJsonAllProperties() throws Exception {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.setProperty("version", 2);
        JSONObject json = element.toJson(null);
        assertEquals(json.getString("id"), "v1");
        assertEquals(json.getString("name"), "Atlas");
        assertEquals(json.getInt("version"), 2);
    }

    @Test
    public void testToJsonFilteredProperties() throws Exception {
        element = new CassandraVertex("v1", mockGraph);
        element.setProperty("name", "Atlas");
        element.setProperty("secret", "hidden");
        Set<String> keys = new HashSet<>(Collections.singletonList("name"));
        JSONObject json = element.toJson(keys);
        assertEquals(json.getString("id"), "v1");
        assertEquals(json.getString("name"), "Atlas");
        assertFalse(json.has("secret"));
    }

    // ======================== State Management Tests ========================

    @Test
    public void testExistsReturnsTrueByDefault() {
        element = new CassandraVertex("v1", mockGraph);
        assertTrue(element.exists());
    }

    @Test
    public void testExistsReturnsFalseAfterDelete() {
        element = new CassandraVertex("v1", mockGraph);
        element.markDeleted();
        assertFalse(element.exists());
    }

    @Test
    public void testMarkDirty() {
        Map<String, Object> props = new HashMap<>();
        element = new CassandraVertex("v1", props, mockGraph);
        assertFalse(element.isDirty());
        element.setProperty("key", "val");
        assertTrue(element.isDirty());
    }

    @Test
    public void testMarkDirtyNotifiesGraphForExistingVertex() {
        Map<String, Object> props = new HashMap<>();
        element = new CassandraVertex("v1", props, mockGraph);
        assertFalse(element.isNew()); // loaded from DB
        element.setProperty("key", "val");
        verify(mockGraph).notifyVertexDirty(element);
    }

    @Test
    public void testMarkDirtyDoesNotNotifyGraphForNewVertex() {
        element = new CassandraVertex("v1", mockGraph);
        assertTrue(element.isNew());
        element.setProperty("key", "val");
        verify(mockGraph, never()).notifyVertexDirty(any());
    }

    @Test
    public void testMarkDeleted() {
        element = new CassandraVertex("v1", mockGraph);
        element.markDeleted();
        assertTrue(element.isDeleted());
        assertTrue(element.isDirty());
    }

    @Test
    public void testMarkPersisted() {
        element = new CassandraVertex("v1", mockGraph);
        assertTrue(element.isNew());
        element.setProperty("x", "y");
        assertTrue(element.isDirty());
        element.markPersisted();
        assertFalse(element.isNew());
        assertFalse(element.isDirty());
    }

    // ======================== GetWrappedElement Test ========================

    @Test
    public void testGetWrappedElement() {
        element = new CassandraVertex("v1", mockGraph);
        Object wrapped = element.getWrappedElement();
        assertSame(wrapped, element);
    }

    // ======================== Equals and HashCode Tests ========================

    @Test
    public void testEqualsById() {
        CassandraVertex v1a = new CassandraVertex("v1", mockGraph);
        CassandraVertex v1b = new CassandraVertex("v1", mockGraph);
        assertEquals(v1a, v1b);
    }

    @Test
    public void testNotEqualsDifferentId() {
        CassandraVertex v1 = new CassandraVertex("v1", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);
        assertNotEquals(v1, v2);
    }

    @Test
    public void testHashCodeConsistent() {
        CassandraVertex v1a = new CassandraVertex("v1", mockGraph);
        CassandraVertex v1b = new CassandraVertex("v1", mockGraph);
        assertEquals(v1a.hashCode(), v1b.hashCode());
    }

    @Test
    public void testNotEqualsNull() {
        element = new CassandraVertex("v1", mockGraph);
        assertFalse(element.equals(null));
    }

    @Test
    public void testNotEqualsDifferentType() {
        element = new CassandraVertex("v1", mockGraph);
        CassandraEdge edge = new CassandraEdge("v1", "out", "in", "label", mockGraph);
        assertNotEquals(element, edge);
    }
}
