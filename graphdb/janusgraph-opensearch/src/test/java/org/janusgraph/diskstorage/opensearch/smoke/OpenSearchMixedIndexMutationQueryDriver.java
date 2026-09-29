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
import java.util.stream.Stream;

/**
 * Validates mixed-index document mutations and basic query execution against a real OpenSearch server
 * through the JanusGraph → OpenSearchIndex path (no Atlas discovery APIs). The server version is configurable
 * (system property {@code opensearch.docker.version} / {@code opensearch.docker.image}) — the dedicated
 * {@code opensearch-it} CI job validates OpenSearch 3.7 and 3.8. OpenSearch 2.x is NOT currently part of the CI
 * matrix; this driver does not depend on any 3.x-only API, but has not been verified against 2.x in CI.
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
        System.out.println("Mixed-index mutation and query driver finished successfully.");
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
        System.out.println("[OK] Schema committed (mixed index: " + MIXED_INDEX_NAME + ")");
        return new Schema(name, textField, textStringField, age, longField, active, birthDate, created,
                batchGroup, listTags, setTags);
    }

    // -------------------------------------------------------------------------
    // Insert
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
        System.out.println("[OK] Single document insert indexed (all field types)");

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
        System.out.println("[OK] Bulk insert indexed 5 documents");
    }

    // -------------------------------------------------------------------------
    // Update
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
        System.out.println("[OK] Property update visible via indexQuery (retry_on_conflict wired at bulk layer)");
    }

    // -------------------------------------------------------------------------
    // Delete
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
        System.out.println("[OK] Single delete removes document from index");

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
        System.out.println("[OK] Bulk delete removes multiple documents");
    }

    // -------------------------------------------------------------------------
    // Cardinality / stored-script paths
    // -------------------------------------------------------------------------

    private static void runCardinalityTests(JanusGraph graph, Schema s) {
        JanusGraphVertex single = graph.addVertex();
        single.property(s.name.name(), "SingleCard");
        single.property(s.batchGroup.name(), "c4card");
        graph.tx().commit();
        assertVertexCount(graph, "v.name:SingleCard", 1);
        System.out.println("[OK] SINGLE cardinality indexed");

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
        System.out.println("[OK] LIST cardinality insert and stored-script add");

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
        System.out.println("[OK] SET cardinality insert and stored-script add");
    }

    // -------------------------------------------------------------------------
    // Basic queries (OpenSearchIndex.query via graph.indexQuery)
    // -------------------------------------------------------------------------

    private static void runBasicQueryTests(JanusGraph graph, Schema s) {
        seedQueryFixtures(graph, s);
        final String qfix = "v.batchGroup:queryfix";

        assertVertexCount(graph, "v.name:QueryAlice", 1);
        System.out.println("[OK] Equality query");

        assertVertexCount(graph, qfix + " AND v.textField:(atlas search)", 1);
        System.out.println("[OK] Text query");

        assertVertexCount(graph, qfix + " AND v.age:30", 1);
        assertVertexCount(graph, qfix + " AND v.age:>20", 2);
        assertVertexCount(graph, qfix + " AND v.age:<40", 2);
        assertVertexCount(graph, qfix + " AND v.age:>=30", 1);
        assertVertexCount(graph, qfix + " AND v.age:<=30", 2);
        System.out.println("[OK] Numeric range queries (>, <, >=, <=, ==)");

        assertVertexCount(graph, qfix + " AND -v.age:30", 1);
        System.out.println("[OK] Not-equal query");

        assertVertexCount(graph, qfix + " AND _exists_:v.longField", 1);
        System.out.println("[OK] Exists query");

        assertVertexCount(graph, qfix + " AND v.age:[25 TO 35]", 2);
        assertVertexCount(graph, qfix + " AND v.active:true", 1);
        assertVertexCount(graph, "v.name:QueryAlice OR v.name:QueryBob", 2);
        System.out.println("[OK] Compound AND/OR queries");

        assertVertexCount(graph, qfix + " AND v.textStringField__STRING:dualValue", 1);
        System.out.println("[OK] TEXTSTRING dual-mapping query");

        List<String> ordered = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:queryfix")
                .orderBy(s.age.name(), Order.asc)
                .limit(10)
                .vertexStream()
                .map(r -> (String) r.getElement().value(s.name.name()))
                .collect(Collectors.toList());
        if (!ordered.get(0).equals("QueryBob") || !ordered.get(1).equals("QueryAlice")) {
            throw new IllegalStateException("Unexpected order: " + ordered);
        }
        System.out.println("[OK] Ordering");

        long limited = graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:queryfix")
                .limit(1)
                .vertexStream()
                .count();
        if (limited != 1) {
            throw new IllegalStateException("Expected limit 1 but got " + limited);
        }
        System.out.println("[OK] Limit");
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
    // Pagination / scroll
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
        System.out.println("[OK] Scroll retrieved all " + SCROLL_DOC_COUNT + " documents without duplicates");

        // offset(5).limit(5) against SCROLL_BATCH_SIZE=5 forces a scroll (window size 5) that fetches exactly
        // offset+limit=10 of the 18 total docs (page 1 skipped, page 2 taken) — i.e. it is NOT naturally exhausted
        // (8 docs remain unfetched server-side). Must be explicitly closed, same as the early-terminated query
        // below, or its OpenSearch scroll context is left open until the scroll-keep-alive TTL expires.
        List<String> page = collectAndClose(
                graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:" + SCROLL_GROUP)
                        .offset(5)
                        .limit(5)
                        .vertexStream()
                        .map(r -> (String) r.getElement().value(s.name.name())));
        if (page.size() != 5) {
            throw new IllegalStateException("Offset/limit page expected 5 results but got " + page.size());
        }
        System.out.println("[OK] Offset/limit pagination");

        // Early termination: request fewer documents (10) than exist (18) but enough to force scroll usage
        // (limit >= SCROLL_BATCH_SIZE), then explicitly close the stream before the scroll is naturally exhausted.
        // This is the exact scenario that used to leak an OpenSearch scroll context on the RawQuery path
        // (graph.indexQuery(indexName, queryString) — used throughout this driver) before the onClose(scroll::close)
        // wiring fix; it is intentionally NOT collected to completion so the fix's hard-assertion below is meaningful.
        final int earlyLimit = SCROLL_BATCH_SIZE * 2;
        List<String> earlyTerminated = collectAndClose(
                graph.indexQuery(MIXED_INDEX_NAME, "v.batchGroup:" + SCROLL_GROUP)
                        .limit(earlyLimit)
                        .vertexStream()
                        .map(r -> (String) r.getElement().value(s.name.name())));
        if (earlyTerminated.size() != earlyLimit) {
            throw new IllegalStateException("Early-terminated scroll query expected " + earlyLimit
                    + " results but got " + earlyTerminated.size());
        }
        System.out.println("[OK] Early-terminated scroll query (limit " + earlyLimit + " of " + SCROLL_DOC_COUNT
                + ") returned the expected subset");

        String scrollStats = OpenSearchSmokeSupport.httpGet("/_nodes/stats/indices/search?filter_path=nodes.*.indices.search.open_contexts");
        if (scrollStats.contains("\"open_contexts\":") && !scrollStats.contains("\"open_contexts\":0")) {
            throw new IllegalStateException("OpenSearch still reports open scroll contexts after all scroll "
                    + "queries (including the early-terminated one) were closed — scroll context leak: "
                    + scrollStats.trim());
        }
        System.out.println("[OK] Scroll contexts cleaned up (open_contexts=0 or unavailable)");
    }

    /**
     * Collects a {@code Stream} and always closes it, releasing any {@code onClose} handler attached to its
     * source (e.g. an OpenSearch scroll context) — including when the stream is not fully exhausted (a limit()
     * short-circuits collection). Generic so it works regardless of the concrete JanusGraph result-stream type.
     */
    private static <T> List<T> collectAndClose(Stream<T> stream) {
        try (Stream<T> closeable = stream) {
            return closeable.collect(Collectors.toList());
        }
    }

    // -------------------------------------------------------------------------
    // Mutation / query consistency
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
        System.out.println("[OK] Insert → query → update → query → delete → query consistency");
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
