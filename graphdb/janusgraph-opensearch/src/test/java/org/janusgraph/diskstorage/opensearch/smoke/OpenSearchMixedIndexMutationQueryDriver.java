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
package org.janusgraph.diskstorage.opensearch.smoke;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.opensearch.rest.OpenSearchBulkWireFormatVerifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * C4 validation: mixed-index document mutations and basic query execution against OpenSearch 2.x
 * through the JanusGraph → OpenSearchIndex path (no Atlas discovery APIs).
 *
 * <pre>
 *   mvn -pl graphdb/janusgraph-opensearch test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.janusgraph.diskstorage.opensearch.smoke.OpenSearchMixedIndexMutationQueryDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true
 * </pre>
 */
public final class OpenSearchMixedIndexMutationQueryDriver {

    public static final String GRAPH_INDEX_NAME   = "c4janus";
    public static final String MIXED_INDEX_NAME   = "c4mixed";
    public static final String BACKING_INDEX_NAME = "search";
    public static final String PHYSICAL_INDEX     = GRAPH_INDEX_NAME + "_" + MIXED_INDEX_NAME.toLowerCase();

    /** Small batch size forces scroll when querying more results than this. */
    private static final int SCROLL_BATCH_SIZE = 5;

    private static final String SCROLL_GROUP = "c4scroll";
    private static final int    SCROLL_DOC_COUNT = 18;

    private OpenSearchMixedIndexMutationQueryDriver() {
    }

    public static void execute() throws Exception {
        OpenSearchSmokeSupport.verifyOpenSearchReachable();
        OpenSearchSmokeSupport.registerOpenSearchBackend();
        OpenSearchBulkWireFormatVerifier.verify(
                OpenSearchSmokeSupport.getOpenSearchHost(), OpenSearchSmokeSupport.getOpenSearchPort());
        OpenSearchSmokeSupport.deletePhysicalIndex(PHYSICAL_INDEX);

        org.janusgraph.diskstorage.configuration.ReadConfiguration config =
                OpenSearchSmokeSupport.buildConfiguration(GRAPH_INDEX_NAME, BACKING_INDEX_NAME, SCROLL_BATCH_SIZE);

        try (JanusGraph graph = JanusGraphFactory.open(config)) {
            Schema schema = createSchema(graph);
            runInsertTests(graph, schema);
            runUpdateTests(graph, schema);
            runDeleteTests(graph, schema);
            runCardinalityTests(graph, schema);
            runBasicQueryTests(graph, schema);
            runPaginationAndScrollTests(graph, schema);
            runMutationQueryConsistencyTests(graph, schema);
        }
    }

    public static void main(String[] args) throws Exception {
        execute();

        System.out.println();
        System.out.println("C4 mixed-index mutation and query driver finished successfully.");
        System.out.println("Inspect OpenSearch with:");
        System.out.println("  curl -s http://" + OpenSearchSmokeSupport.getOpenSearchHost() + ":"
                + OpenSearchSmokeSupport.getOpenSearchPort() + "/" + PHYSICAL_INDEX + "/_count");
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    private static Schema createSchema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey textField = mgmt.makePropertyKey("textField").dataType(String.class).make();
        PropertyKey textStringField = mgmt.makePropertyKey("textStringField").dataType(String.class).make();
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        PropertyKey longField = mgmt.makePropertyKey("longField").dataType(Long.class).make();
        PropertyKey active = mgmt.makePropertyKey("active").dataType(Boolean.class).make();
        PropertyKey birthDate = mgmt.makePropertyKey("birthDate").dataType(Date.class).make();
        PropertyKey created = mgmt.makePropertyKey("created").dataType(Instant.class).make();
        PropertyKey batchGroup = mgmt.makePropertyKey("batchGroup").dataType(String.class).make();
        PropertyKey listTags = mgmt.makePropertyKey("listTags").dataType(String.class)
                .cardinality(Cardinality.LIST).make();
        PropertyKey setTags = mgmt.makePropertyKey("setTags").dataType(String.class)
                .cardinality(Cardinality.SET).make();

        mgmt.buildIndex(MIXED_INDEX_NAME, Vertex.class)
                .addKey(name, Mapping.STRING.asParameter())
                .addKey(textField, Mapping.TEXT.asParameter())
                .addKey(textStringField, Mapping.TEXTSTRING.asParameter())
                .addKey(age)
                .addKey(longField)
                .addKey(active)
                .addKey(birthDate)
                .addKey(created)
                .addKey(batchGroup, Mapping.STRING.asParameter())
                .addKey(listTags, Mapping.STRING.asParameter())
                .addKey(setTags, Mapping.STRING.asParameter())
                .buildMixedIndex(BACKING_INDEX_NAME);

        mgmt.commit();
        System.out.println("[OK] C4 schema committed (mixed index: " + MIXED_INDEX_NAME + ")");
        return new Schema(name, textField, textStringField, age, longField, active, birthDate, created,
                batchGroup, listTags, setTags);
    }

    // -------------------------------------------------------------------------
    // C4.1 Insert
    // -------------------------------------------------------------------------

    private static void runInsertTests(JanusGraph graph, Schema s) {
        JanusGraphVertex alice = graph.addVertex();
        alice.property(s.name.name(), "Alice");
        alice.property(s.textField.name(), "hello atlas world");
        alice.property(s.textStringField.name(), "exactValue");
        alice.property(s.age.name(), 30);
        alice.property(s.longField.name(), 9_000_000_000L);
        alice.property(s.active.name(), true);
        alice.property(s.birthDate.name(), new Date(631152000000L)); // 1990-01-01
        alice.property(s.created.name(), Instant.parse("2020-01-01T00:00:00Z"));
        alice.property(s.batchGroup.name(), "c4insert");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:Alice", 1);
        System.out.println("[OK] C4.1 single document insert indexed (all field types)");

        List<JanusGraphVertex> bulk = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JanusGraphVertex v = graph.addVertex();
            v.property(s.name.name(), "Bulk" + i);
            v.property(s.age.name(), 20 + i);
            v.property(s.batchGroup.name(), "c4bulk");
            bulk.add(v);
        }
        graph.tx().commit();
        assertVertexCount(graph, "v.batchGroup:c4bulk", 5);
        System.out.println("[OK] C4.1 bulk insert indexed 5 documents");
    }

    // -------------------------------------------------------------------------
    // C4.2 Update
    // -------------------------------------------------------------------------

    private static void runUpdateTests(JanusGraph graph, Schema s) {
        JanusGraphVertex bob = graph.addVertex();
        bob.property(s.name.name(), "Bob");
        bob.property(s.age.name(), 25);
        bob.property(s.batchGroup.name(), "c4update");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:Bob AND v.age:25", 1);

        bob.property(s.age.name(), 35);
        graph.tx().commit();
        assertVertexCount(graph, "v.name:Bob AND v.age:35", 1);
        assertVertexCount(graph, "v.name:Bob AND v.age:25", 0);
        System.out.println("[OK] C4.2 property update visible via indexQuery (retry_on_conflict wired at bulk layer)");
    }

    // -------------------------------------------------------------------------
    // C4.3 Delete
    // -------------------------------------------------------------------------

    private static void runDeleteTests(JanusGraph graph, Schema s) {
        JanusGraphVertex doomed = graph.addVertex();
        doomed.property(s.name.name(), "ToDelete");
        doomed.property(s.batchGroup.name(), "c4delete");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:ToDelete", 1);

        doomed.remove();
        graph.tx().commit();
        assertVertexCount(graph, "v.name:ToDelete", 0);
        System.out.println("[OK] C4.3 single delete removes document from index");

        List<JanusGraphVertex> bulkDelete = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            JanusGraphVertex v = graph.addVertex();
            v.property(s.name.name(), "BulkDel" + i);
            v.property(s.batchGroup.name(), "c4bulkdelete");
            bulkDelete.add(v);
        }
        graph.tx().commit();
        assertVertexCount(graph, "v.batchGroup:c4bulkdelete", 3);

        for (JanusGraphVertex v : bulkDelete) {
            v.remove();
        }
        graph.tx().commit();
        assertVertexCount(graph, "v.batchGroup:c4bulkdelete", 0);
        System.out.println("[OK] C4.3 bulk delete removes multiple documents");
    }

    // -------------------------------------------------------------------------
    // C4.4 Cardinality / stored-script paths
    // -------------------------------------------------------------------------

    private static void runCardinalityTests(JanusGraph graph, Schema s) {
        JanusGraphVertex single = graph.addVertex();
        single.property(s.name.name(), "SingleCard");
        single.property(s.batchGroup.name(), "c4card");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:SingleCard", 1);
        System.out.println("[OK] C4.4 SINGLE cardinality indexed");

        JanusGraphVertex listVertex = graph.addVertex();
        listVertex.property(s.name.name(), "ListCard");
        listVertex.property(s.batchGroup.name(), "c4card");
        listVertex.property(s.listTags.name(), "alpha");
        listVertex.property(s.listTags.name(), "beta");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:ListCard AND v.listTags:alpha", 1);
        listVertex.property(s.listTags.name(), "gamma");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:ListCard AND v.listTags:gamma", 1);
        System.out.println("[OK] C4.4 LIST cardinality insert and stored-script add");

        JanusGraphVertex setVertex = graph.addVertex();
        setVertex.property(s.name.name(), "SetCard");
        setVertex.property(s.batchGroup.name(), "c4card");
        setVertex.property(s.setTags.name(), "red");
        setVertex.property(s.setTags.name(), "blue");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:SetCard AND v.setTags:blue", 1);
        setVertex.property(s.setTags.name(), "green");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:SetCard AND v.setTags:green", 1);
        System.out.println("[OK] C4.4 SET cardinality insert and stored-script add");
    }

    // -------------------------------------------------------------------------
    // C4.5 Basic queries (OpenSearchIndex.query via graph.indexQuery)
    // -------------------------------------------------------------------------

    private static void runBasicQueryTests(JanusGraph graph, Schema s) {
        seedQueryFixtures(graph, s);
        final String qfix = "v.batchGroup:queryfix";

        assertVertexCount(graph, "v.name:QueryAlice", 1);
        System.out.println("[OK] C4.5 equality query");

        assertVertexCount(graph, qfix + " AND v.textField:(atlas search)", 1);
        System.out.println("[OK] C4.5 text query");

        assertVertexCount(graph, qfix + " AND v.age:30", 1);
        assertVertexCount(graph, qfix + " AND v.age:>20", 2);
        assertVertexCount(graph, qfix + " AND v.age:<40", 2);
        assertVertexCount(graph, qfix + " AND v.age:>=30", 1);
        assertVertexCount(graph, qfix + " AND v.age:<=30", 2);
        System.out.println("[OK] C4.5 numeric range queries (>, <, >=, <=, ==)");

        assertVertexCount(graph, qfix + " AND -v.age:30", 1);
        System.out.println("[OK] C4.5 not-equal query");

        assertVertexCount(graph, qfix + " AND _exists_:v.longField", 1);
        System.out.println("[OK] C4.5 exists query");

        assertVertexCount(graph, qfix + " AND v.age:[25 TO 35]", 2);
        assertVertexCount(graph, qfix + " AND v.active:true", 1);
        assertVertexCount(graph, "v.name:QueryAlice OR v.name:QueryBob", 2);
        System.out.println("[OK] C4.5 compound AND/OR queries");

        assertVertexCount(graph, qfix + " AND v.textStringField__STRING:dualValue", 1);
        System.out.println("[OK] C4.5 TEXTSTRING dual-mapping query");

        List<String> ordered = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:queryfix")
                .orderBy(s.age.name(), Order.asc)
                .limit(10)
                .vertexStream()
                .map(r -> (String) r.getElement().value(s.name.name()))
                .collect(Collectors.toList());
        if (!ordered.get(0).equals("QueryBob") || !ordered.get(1).equals("QueryAlice")) {
            throw new IllegalStateException("Unexpected order: " + ordered);
        }
        System.out.println("[OK] C4.5 ordering");

        long limited = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:queryfix")
                .limit(1)
                .vertexStream()
                .count();
        if (limited != 1) {
            throw new IllegalStateException("Expected limit 1 but got " + limited);
        }
        System.out.println("[OK] C4.5 limit");
    }

    private static void seedQueryFixtures(JanusGraph graph, Schema s) {
        JanusGraphVertex alice = graph.addVertex();
        alice.property(s.name.name(), "QueryAlice");
        alice.property(s.textField.name(), "atlas search engine");
        alice.property(s.textStringField.name(), "dualValue");
        alice.property(s.age.name(), 30);
        alice.property(s.longField.name(), 42L);
        alice.property(s.active.name(), true);
        alice.property(s.batchGroup.name(), "queryfix");

        JanusGraphVertex bob = graph.addVertex();
        bob.property(s.name.name(), "QueryBob");
        bob.property(s.textField.name(), "other content");
        bob.property(s.age.name(), 25);
        bob.property(s.active.name(), false);
        bob.property(s.batchGroup.name(), "queryfix");

        graph.tx().commit();
    }

    // -------------------------------------------------------------------------
    // C4.6 Pagination / scroll
    // -------------------------------------------------------------------------

    private static void runPaginationAndScrollTests(JanusGraph graph, Schema s) throws Exception {
        for (int i = 0; i < SCROLL_DOC_COUNT; i++) {
            JanusGraphVertex v = graph.addVertex();
            v.property(s.name.name(), SCROLL_GROUP + i);
            v.property(s.age.name(), i);
            v.property(s.batchGroup.name(), SCROLL_GROUP);
        }
        graph.tx().commit();

        long total = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:" + SCROLL_GROUP).vertexTotals();
        if (total != SCROLL_DOC_COUNT) {
            throw new IllegalStateException("Expected " + SCROLL_DOC_COUNT + " scroll docs but vertexTotals=" + total);
        }

        List<String> allViaScroll = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:" + SCROLL_GROUP)
                .limit(SCROLL_DOC_COUNT)
                .vertexStream()
                .map(r -> (String) r.getElement().value(s.name.name()))
                .collect(Collectors.toList());
        if (allViaScroll.size() != SCROLL_DOC_COUNT) {
            throw new IllegalStateException("Scroll query returned " + allViaScroll.size() + " docs, expected "
                    + SCROLL_DOC_COUNT);
        }
        Set<String> unique = new HashSet<>(allViaScroll);
        if (unique.size() != SCROLL_DOC_COUNT) {
            throw new IllegalStateException("Scroll query returned duplicate document IDs");
        }
        System.out.println("[OK] C4.6 scroll retrieved all " + SCROLL_DOC_COUNT + " documents without duplicates");

        List<String> page = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:" + SCROLL_GROUP)
                .offset(5)
                .limit(5)
                .vertexStream()
                .map(r -> (String) r.getElement().value(s.name.name()))
                .collect(Collectors.toList());
        if (page.size() != 5) {
            throw new IllegalStateException("Offset/limit page expected 5 results but got " + page.size());
        }
        System.out.println("[OK] C4.6 offset/limit pagination");

        String scrollStats = OpenSearchSmokeSupport.httpGet("/_nodes/stats/indices/search?filter_path=nodes.*.indices.search.open_contexts");
        if (scrollStats.contains("\"open_contexts\":") && !scrollStats.contains("\"open_contexts\":0")) {
            System.out.println("[WARN] C4.6 OpenSearch still reports open scroll contexts: " + scrollStats.trim());
        } else {
            System.out.println("[OK] C4.6 scroll contexts cleaned up (open_contexts=0 or unavailable)");
        }
    }

    // -------------------------------------------------------------------------
    // C4.7 Mutation / query consistency
    // -------------------------------------------------------------------------

    private static void runMutationQueryConsistencyTests(JanusGraph graph, Schema s) {
        JanusGraphVertex keep = graph.addVertex();
        keep.property(s.name.name(), "KeepMe");
        keep.property(s.age.name(), 10);
        keep.property(s.batchGroup.name(), "c4consistency");

        JanusGraphVertex target = graph.addVertex();
        target.property(s.name.name(), "Target");
        target.property(s.age.name(), 50);
        target.property(s.batchGroup.name(), "c4consistency");
        graph.tx().commit();

        assertVertexCount(graph, "v.name:Target", 1);
        assertVertexCount(graph, "v.batchGroup:c4consistency", 2);

        target.property(s.age.name(), 55);
        graph.tx().commit();
        assertVertexCount(graph, "v.name:Target AND v.age:55", 1);
        assertVertexCount(graph, "v.name:Target AND v.age:50", 0);
        assertVertexCount(graph, "v.name:KeepMe", 1);

        target.remove();
        graph.tx().commit();
        assertVertexCount(graph, "v.name:Target", 0);
        assertVertexCount(graph, "v.name:KeepMe", 1);
        System.out.println("[OK] C4.7 insert → query → update → query → delete → query consistency");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void assertVertexCount(JanusGraph graph, String query, long expected) {
        Long actual = graph.indexQuery(MIXED_INDEX_NAME, query).vertexTotals();
        if (actual == null || actual != expected) {
            throw new IllegalStateException("Query [" + query + "] expected " + expected + " vertices but got " + actual);
        }
    }

    private static final class Schema {
        final PropertyKey name;
        final PropertyKey textField;
        final PropertyKey textStringField;
        final PropertyKey age;
        final PropertyKey longField;
        final PropertyKey active;
        final PropertyKey birthDate;
        final PropertyKey created;
        final PropertyKey batchGroup;
        final PropertyKey listTags;
        final PropertyKey setTags;

        Schema(PropertyKey name, PropertyKey textField, PropertyKey textStringField, PropertyKey age,
               PropertyKey longField, PropertyKey active, PropertyKey birthDate, PropertyKey created,
               PropertyKey batchGroup, PropertyKey listTags, PropertyKey setTags) {
            this.name = name;
            this.textField = textField;
            this.textStringField = textStringField;
            this.age = age;
            this.longField = longField;
            this.active = active;
            this.birthDate = birthDate;
            this.created = created;
            this.batchGroup = batchGroup;
            this.listTags = listTags;
            this.setTags = setTags;
        }
    }
}
