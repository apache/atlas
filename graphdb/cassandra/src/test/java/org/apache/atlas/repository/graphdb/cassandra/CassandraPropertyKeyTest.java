package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests for CassandraPropertyKey.
 */
public class CassandraPropertyKeyTest {

    @Test
    public void testConstructorAndGetters() {
        CassandraPropertyKey key = new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        assertEquals(key.getName(), "__guid");
        assertEquals(key.getPropertyClass(), String.class);
        assertEquals(key.getCardinality(), AtlasCardinality.SINGLE);
    }

    @Test
    public void testSingleCardinality() {
        CassandraPropertyKey key = new CassandraPropertyKey("name", String.class, AtlasCardinality.SINGLE);
        assertEquals(key.getCardinality(), AtlasCardinality.SINGLE);
    }

    @Test
    public void testSetCardinality() {
        CassandraPropertyKey key = new CassandraPropertyKey("tags", String.class, AtlasCardinality.SET);
        assertEquals(key.getCardinality(), AtlasCardinality.SET);
    }

    @Test
    public void testListCardinality() {
        CassandraPropertyKey key = new CassandraPropertyKey("meanings", String.class, AtlasCardinality.LIST);
        assertEquals(key.getCardinality(), AtlasCardinality.LIST);
    }

    @Test
    public void testDifferentPropertyClasses() {
        CassandraPropertyKey stringKey = new CassandraPropertyKey("name", String.class, AtlasCardinality.SINGLE);
        CassandraPropertyKey longKey = new CassandraPropertyKey("count", Long.class, AtlasCardinality.SINGLE);
        CassandraPropertyKey boolKey = new CassandraPropertyKey("active", Boolean.class, AtlasCardinality.SINGLE);

        assertEquals(stringKey.getPropertyClass(), String.class);
        assertEquals(longKey.getPropertyClass(), Long.class);
        assertEquals(boolKey.getPropertyClass(), Boolean.class);
    }
}
