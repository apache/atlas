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

package org.apache.atlas.repository.migration;

import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.*;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

public class MigrationBaseAsserts {
    protected static final String ASSERT_NAME_PROPERTY = "Asset.name";
    private final String TYPE_NAME_PROPERTY = "__typeName";
    private final String R_GUID_PROPERTY_NAME = "_r__guid";

    protected AtlasGraph graph;

    protected MigrationBaseAsserts(AtlasGraph graph) {
        this.graph = graph;
    }

    protected void assertHiveVertices(int dbCount, int tableCount, int columnCount) {
        int i = 0;

        Iterator<AtlasVertex> results = getVertices("hive_db", null);
        for (Iterator<AtlasVertex> it = results; it.hasNext(); i++) {
            assertNotNull(it.next());
        }
        assertEquals(i, dbCount);

        i = 0;
        results = getVertices("hive_table", null);
        for (Iterator<AtlasVertex> it = results; it.hasNext(); i++) {
            assertNotNull(it.next());
        }
        assertEquals(i, tableCount);

        i = 0;
        results = getVertices("hive_column", null);
        for (Iterator<AtlasVertex> it = results; it.hasNext(); i++) {
            assertNotNull(it.next());
        }

        assertTrue(i > 0);
        assertEquals(i, columnCount);
    }

    protected Iterator<AtlasVertex> getVertices(String typeName, String name) {
        AtlasGraphQuery query = graph.query().has(TYPE_NAME_PROPERTY, typeName);

        if(!StringUtils.isEmpty(name)) {
            query = query.has(ASSERT_NAME_PROPERTY, name);
        }

        return query.vertices().iterator();
    }

    protected AtlasVertex getVertex(String typeName, String name) {
        Iterator<AtlasVertex> iterator = getVertices(typeName, name);

        return iterator.hasNext() ? iterator.next() : null;
    }

    protected void assertEdges(String typeName, String assetName, AtlasEdgeDirection edgeDirection, int startIdx, int expectedItems, String edgeTypeName) {
        assertEdges(getVertex(typeName, assetName).getEdges(edgeDirection).iterator(),startIdx, expectedItems, edgeTypeName);
    }

    protected void assertEdges(Iterator<AtlasEdge> results, int startIdx, int expectedItems, String edgeTypeName) {
        int count = 0;
        AtlasEdge e = null;
        for (Iterator<AtlasEdge> it = results; it.hasNext() && count < startIdx; count++) {
            e = it.next();
        }

        assertNotNull(GraphHelper.getProperty(e, R_GUID_PROPERTY_NAME));
        assertNotNull(GraphHelper.getProperty(e, "tagPropagation"));

        if(StringUtils.isNotEmpty(edgeTypeName)) {
            assertEquals(GraphHelper.getProperty(e, TYPE_NAME_PROPERTY), edgeTypeName, edgeTypeName);
        }

        assertEquals(count, expectedItems, String.format("%s", edgeTypeName));
    }

    protected void assertTypeAttribute(String typeName, int expectedSize, String name, String guid, String propertyName) {
        AtlasVertex v         = getVertex(typeName, name);
        String     guidActual = GraphHelper.getGuid(v);
        List list       = (List) GraphHelper.getProperty(v, propertyName);

        assertEquals(guidActual, guid);
        assertNotNull(list);
        assertEquals(list.size(), expectedSize);
    }

    protected void assertTypeCountNameGuid(String typeName, int expectedItems, String name, String guid) {
        Iterator<AtlasVertex> results = getVertices(typeName, name);

        int count = 0;
        for (Iterator<AtlasVertex> it = results; it.hasNext(); ) {
            AtlasVertex v = it.next();

            assertEquals(GraphHelper.getTypeName(v), typeName);

            if(StringUtils.isNotEmpty(guid)) {
                assertEquals(GraphHelper.getGuid(v), guid, name);
            }

            if(StringUtils.isNotEmpty(name)) {
                assertEquals(GraphHelper.getProperty(v, ASSERT_NAME_PROPERTY), name, name);
            }

            count++;
        }

        assertEquals(count, expectedItems, String.format("%s:%s", typeName, name));
    }

    protected void assertMigrationStatus(int expectedTotalCount) {
        AtlasVertex v = getVertex("__MigrationStatus", "");
        assertEquals((long) GraphHelper.getProperty(v, "currentIndex"), expectedTotalCount);
    }
}
