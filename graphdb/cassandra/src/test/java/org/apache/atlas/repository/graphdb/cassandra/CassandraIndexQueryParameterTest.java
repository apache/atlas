package org.apache.atlas.repository.graphdb.cassandra;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests for CassandraIndexQueryParameter.
 * Covers: constructor, getter methods, null handling.
 */
public class CassandraIndexQueryParameterTest {

    @Test
    public void testConstructorAndGetters() {
        CassandraIndexQueryParameter param = new CassandraIndexQueryParameter("boost", "2.0");
        assertEquals(param.getParameterName(), "boost");
        assertEquals(param.getParameterValue(), "2.0");
    }

    @Test
    public void testNullValues() {
        CassandraIndexQueryParameter param = new CassandraIndexQueryParameter(null, null);
        assertNull(param.getParameterName());
        assertNull(param.getParameterValue());
    }

    @Test
    public void testEmptyValues() {
        CassandraIndexQueryParameter param = new CassandraIndexQueryParameter("", "");
        assertEquals(param.getParameterName(), "");
        assertEquals(param.getParameterValue(), "");
    }

    @Test
    public void testSpecialCharacters() {
        CassandraIndexQueryParameter param = new CassandraIndexQueryParameter("key.nested", "value with spaces & symbols!");
        assertEquals(param.getParameterName(), "key.nested");
        assertEquals(param.getParameterValue(), "value with spaces & symbols!");
    }
}
