package org.apache.atlas.repository.graphdb.cassandra;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests for CassandraEdgeLabel.
 */
public class CassandraEdgeLabelTest {

    @Test
    public void testConstructorAndGetName() {
        CassandraEdgeLabel label = new CassandraEdgeLabel("__Process.inputs");
        assertEquals(label.getName(), "__Process.inputs");
    }

    @Test
    public void testSimpleLabel() {
        CassandraEdgeLabel label = new CassandraEdgeLabel("knows");
        assertEquals(label.getName(), "knows");
    }

    @Test
    public void testEdgeLabelWithSpecialChars() {
        CassandraEdgeLabel label = new CassandraEdgeLabel("__AtlasGlossaryTerm.anchor");
        assertEquals(label.getName(), "__AtlasGlossaryTerm.anchor");
    }
}
