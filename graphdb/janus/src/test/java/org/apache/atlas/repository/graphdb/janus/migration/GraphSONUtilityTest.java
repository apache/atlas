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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.repository.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.ATTRIBUTE_INDEX_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_KEY_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_GUID;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_VERTEX_PROPAGATE_KEY;
import static org.apache.atlas.repository.Constants.EDGE_ID_IN_IMPORT_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_ID_IN_IMPORT_KEY;
import static org.testng.Assert.*;

public class GraphSONUtilityTest extends BaseUtils {

    private final String HIVE_TABLE_NAME_PROPERTY = "Asset.name";
    private final String HIVE_TABLE_NAME = "test_table_view";
    private final String HIVE_TABLE_COLUMNS_RELATIONSHIP = "hive_table.columns";
    private final String HIVE_TABLE_COLUMNS_MAP_RELATIONSHIP = "hive_table.columnsMap";
    private final String HIVE_TABLE_COLUMNS_PARAMETERS_MAP = "hive_table.parameters";
    private final String HIVE_TABLE_COLUMNS_PARAMETERS_MAP_KEY = "transient_lastDdlTime";
    private final String HIVE_TABLE_COLUMNS_PARAMETERS_MAP_VALUE_KEY = String.format("%s.%s", HIVE_TABLE_COLUMNS_PARAMETERS_MAP, HIVE_TABLE_COLUMNS_PARAMETERS_MAP_KEY);
    private final String HIVE_TABLE_TYPE = "hive_table";
    
    @Test
    public void idFetch() {
        JsonNode node = getCol1();
        final int EXPECTED_ID = 98336;
        Object o = GraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokensTP2._ID));

        assertNotNull(o);
        assertEquals((int) o, EXPECTED_ID);
    }

    @Test
    public void verifyReadProperties() {
        JsonNode node = getCol1();
        Map<String, Object> props = GraphSONUtility.readProperties(node);

        assertEquals(props.get("__superTypeNames").getClass(), ArrayList.class);
        assertEquals(props.get("Asset.name").getClass(), String.class);
        assertEquals(props.get("hive_column.position").getClass(), Integer.class);
        assertEquals(props.get("__timestamp").getClass(), Long.class);

        assertNotNull(props);
    }

    @Test
    public void dataNodeReadAndVertexAddedToGraph() throws IOException {
        JsonNode entityNode = getCol1();
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        Map<String, Object> map = gu.vertexFromJson(tg, entityNode);

        assertNull(map);
        assertEquals((long) tg.traversal().V().count().next(), 1L);

        Vertex v = tg.vertices().next();
        assertTrue(v.property(VERTEX_ID_IN_IMPORT_KEY).isPresent());
    }

    @Test
    public void typeNodeReadAndVertexNotAddedToGraph() throws IOException {
        JsonNode entityNode = getDbType();
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        gu.vertexFromJson(tg, entityNode);

        Assert.assertEquals((long) tg.traversal().V().count().next(), 0L);
    }

    @Test
    public void updateNonPrimitiveArrayProperty() throws IOException {
        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getNonPrimitiveArray());

        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);
        addVertexToGraph(tg, gu, getTableV());

        Vertex v = tg.traversal().V().next();
        assertNotNull(v);
        assertTrue(v.property(HIVE_TABLE_COLUMNS_RELATIONSHIP).isPresent());

        Map<String, String> list = (Map<String, String>) v.property(HIVE_TABLE_COLUMNS_RELATIONSHIP).value();
        assertEquals(list.size(), 2);
    }


    @Test
    public void updatePrimitiveMapProperty() {
        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getPostProcessMapPrimitive());

        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);
        addVertexToGraph(tg, gu, getTableV());

        Vertex v = tg.traversal().V().next();
        assertNotNull(v);
        assertTrue(v.property(HIVE_TABLE_COLUMNS_PARAMETERS_MAP).isPresent());
        assertEquals(((Map) v.property(HIVE_TABLE_COLUMNS_PARAMETERS_MAP).value()).size(), 1);
        assertEquals(((Map) v.property(HIVE_TABLE_COLUMNS_PARAMETERS_MAP).value()).get(HIVE_TABLE_COLUMNS_PARAMETERS_MAP_KEY), "1522693834");
        assertFalse(v.property(HIVE_TABLE_COLUMNS_PARAMETERS_MAP_VALUE_KEY).isPresent());
    }

    @Test
    public void edgeReadAndAddedToGraph() {
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        Map<String, Object> m = null;

        addVertexToGraph(tg, gu, getDBV(), getTableV());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdge());

        Assert.assertEquals((long) tg.traversal().V().count().next(), 2L);
        Assert.assertEquals((long) tg.traversal().E().count().next(), 1L);

        Edge e = tg.edges().next();
        assertTrue(e.property(EDGE_ID_IN_IMPORT_KEY).isPresent());
    }

    @Test
    public void edgeReadAndArrayIndexAdded() throws IOException {
        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getPostProcessMap());
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);
        Map<String, Object> m = null;

        addVertexToGraph(tg, gu, getDBV(), getTableV(), getCol1(), getCol2());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdgeCol(), getEdgeCol2());

        Iterator<Edge> edges = tg.edges();
        while(edges.hasNext()) {
            Edge e = edges.next();
            String arraySpecificLabel = StringUtils.remove(e.label(), Constants.INTERNAL_PROPERTY_KEY_PREFIX);
            if(arraySpecificLabel.equals(HIVE_TABLE_COLUMNS_RELATIONSHIP)) {
                assertTrue(e.property(ATTRIBUTE_INDEX_PROPERTY_KEY).isPresent());
            }
            assertTrue(e.property(EDGE_ID_IN_IMPORT_KEY).isPresent());
        }

        Iterator<Vertex> vertices = tg.vertices();
        while(vertices.hasNext()) {
            Vertex v = vertices.next();
            if(v.property(HIVE_TABLE_NAME_PROPERTY).isPresent()) {
                if(v.property(HIVE_TABLE_NAME_PROPERTY).value().toString().equals(HIVE_TABLE_NAME)) {
                    assertTrue(v.property(HIVE_TABLE_COLUMNS_RELATIONSHIP).isPresent());
                }
            }
        }
    }

    @Test
    public void nonPrimitiveMap_Removed() throws IOException {
        Set<String> actualKeys = new HashSet<String>() {{
            add("col3");
            add("col4");
        }};

        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getPostProcessMap());
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);

        addVertexToGraph(tg, gu, getDBV(), getTableV(), getCol1(), getCol2());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdgeCol3(), getEdgeCol4());

        Iterator<Vertex> vertices = tg.vertices();
        while(vertices.hasNext()) {
            Vertex v = vertices.next();
            if(!v.property(HIVE_TABLE_COLUMNS_MAP_RELATIONSHIP).isPresent()) continue;

            fail("Non-primitive map should be removed during vertex creation.");
        }

        Iterator<Edge> edges = tg.edges();
        while(edges.hasNext()) {
            Edge e = edges.next();
            String mapSpecificLabel = StringUtils.remove(e.label(), Constants.INTERNAL_PROPERTY_KEY_PREFIX);
            assertEquals(mapSpecificLabel, HIVE_TABLE_COLUMNS_MAP_RELATIONSHIP);
            assertTrue(e.property(ATTRIBUTE_KEY_PROPERTY_KEY).isPresent());

            assertTrue(actualKeys.contains((String) e.property(ATTRIBUTE_KEY_PROPERTY_KEY).value()));
        }
    }

    @Test
    public void tagAssociated_NewAttributesAdded() throws IOException {

        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getPostProcessMap());
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);

        addVertexToGraph(tg, gu, getTagV(), getDBV(), getTableV(), getCol3());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdgeTag());

        Iterator<Vertex> vertices = tg.vertices();
        while(vertices.hasNext()) {
            Vertex v = vertices.next();
            if(v.id().toString() != "16752") continue;

            assertTrue(v.property(CLASSIFICATION_ENTITY_GUID).isPresent());
            assertTrue(v.property(CLASSIFICATION_VERTEX_PROPAGATE_KEY).isPresent());
            assertEquals(v.property(CLASSIFICATION_VERTEX_PROPAGATE_KEY).values(), "NONE");
        }

        Iterator<Edge> edges = tg.edges();
        while(edges.hasNext()) {
            Edge e = edges.next();
            assertTrue(e.property(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY).isPresent());
            assertEquals(e.property(STATE_PROPERTY_KEY).value(), "ACTIVE");
            assertTrue(e.property(Constants.RELATIONSHIP_GUID_PROPERTY_KEY).isPresent());
        }
    }

    @Test
    public void processEdge_PropagateSetTo_NONE() throws IOException {
        ElementProcessors elementProcessors = new ElementProcessors(new HashMap<>(), getPostProcessMap());
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);

        addVertexToGraph(tg, gu, getTagV(), getDBV(), getTableV(), getCol3());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdgeTag());

        Iterator<Vertex> vertices = tg.vertices();
        while(vertices.hasNext()) {
            Vertex v = vertices.next();
            if(v.id().toString() != "16752") continue;

            assertTrue(v.property(CLASSIFICATION_ENTITY_GUID).isPresent());
            assertTrue(v.property(CLASSIFICATION_VERTEX_PROPAGATE_KEY).isPresent());
            assertEquals(v.property(CLASSIFICATION_VERTEX_PROPAGATE_KEY).values(), "NONE");
        }

        Iterator<Edge> edges = tg.edges();
        while(edges.hasNext()) {
            Edge e = edges.next();
            assertTrue(e.property(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY).isPresent());
            assertEquals(e.property(STATE_PROPERTY_KEY).value(), "ACTIVE");
            assertTrue(e.property(Constants.RELATIONSHIP_GUID_PROPERTY_KEY).isPresent());
        }
    }

    @Test
    public void processEdge_PropagateSetTo_ONE_TO_TWO() throws IOException {
        Map<String, RelationshipCacheGenerator.TypeInfo> typeCache = new HashMap<String, RelationshipCacheGenerator.TypeInfo>() {{
            put("__Process.inputs", new RelationshipCacheGenerator.TypeInfo("dataset_process_inputs", AtlasRelationshipDef.PropagateTags.TWO_TO_ONE));
        }};

        ElementProcessors elementProcessors = new ElementProcessors(typeCache, getPostProcessMap());
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(elementProcessors);

        addVertexToGraph(tg, gu, getDBV(), getTableV(), getProcessV());
        addEdgeToGraph(tg, gu, new MappedElementCache(), getEdgeProcess());

        Iterator<Edge> edges = tg.edges();
        while(edges.hasNext()) {
            Edge e = edges.next();
            assertTrue(e.property(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY).isPresent());
            assertEquals(e.property(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY).value().toString(), "TWO_TO_ONE");
            assertEquals(e.property(STATE_PROPERTY_KEY).value(), "ACTIVE");
            assertTrue(e.property(Constants.RELATIONSHIP_GUID_PROPERTY_KEY).isPresent());
        }
    }

    private Map<String, Map<String, List<String>>> getPostProcessMap() {
        Map<String, Map<String, List<String>>> map = new HashMap<>();
        map.put(HIVE_TABLE_TYPE, new HashMap<>());

        map.get(HIVE_TABLE_TYPE).put("ARRAY", new ArrayList<>());
        map.get(HIVE_TABLE_TYPE).put("MAP", new ArrayList<>());

        map.get(HIVE_TABLE_TYPE).get("ARRAY").add(HIVE_TABLE_COLUMNS_RELATIONSHIP);
        map.get(HIVE_TABLE_TYPE).get("MAP").add(HIVE_TABLE_COLUMNS_MAP_RELATIONSHIP);

        return map;
    }

    private Map<String, Map<String, List<String>>> getPostProcessMapPrimitive() {
        Map<String, Map<String, List<String>>> map = new HashMap<>();
        map.put(HIVE_TABLE_TYPE, new HashMap<>());
        map.get(HIVE_TABLE_TYPE).put("MAP_PRIMITIVE", new ArrayList<>());
        map.get(HIVE_TABLE_TYPE).get("MAP_PRIMITIVE").add(HIVE_TABLE_COLUMNS_PARAMETERS_MAP);

        return map;
    }

    private Map<String, Map<String, List<String>>> getNonPrimitiveArray() {
        Map<String, Map<String, List<String>>> map = new HashMap<>();
        map.put(HIVE_TABLE_TYPE, new HashMap<>());
        map.get(HIVE_TABLE_TYPE).put("ARRAY", new ArrayList<>());
        map.get(HIVE_TABLE_TYPE).get("ARRAY").add(HIVE_TABLE_COLUMNS_RELATIONSHIP);

        return map;
    }
}
