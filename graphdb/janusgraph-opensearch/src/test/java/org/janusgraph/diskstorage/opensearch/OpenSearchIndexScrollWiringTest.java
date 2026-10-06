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
package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.indexing.RawQuery;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Unit tests for {@link OpenSearchIndex#withScrollCleanup(Iterator, Stream)} — the shared wiring used by every
 * {@code query(...)} overload that may back its {@code Stream} with an {@link OpenSearchScroll}. This is the exact
 * seam that was previously missing from the {@code query(RawQuery, ...)} path (used by Atlas' native/raw
 * {@code graph.indexQuery(indexName, queryString)} calls), which allowed the server-side scroll context to leak on
 * early termination. Docker-free.
 */
public class OpenSearchIndexScrollWiringTest {

    private static final int BATCH_SIZE = 2;

    @Test
    public void wiresCloseToScrollWhenIteratorIsScroll() {
        RecordingClient client = new RecordingClient();
        // Full initial batch => not finished => scroll not deleted yet on construction.
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-1", BATCH_SIZE), BATCH_SIZE);

        Stream<RawQuery.Result<String>> stream = OpenSearchIndex.withScrollCleanup(scroll, streamOf(scroll));

        assertEquals(client.deleteScrollCalls, 0, "scroll must not be released before the stream is closed");

        stream.close();

        assertEquals(client.deleteScrollCalls, 1, "closing the wrapped stream must release the scroll context");
        assertEquals(client.lastDeletedScrollId, "scroll-1");
    }

    @Test
    public void closingBeforeLimitConsumesStreamStillReleasesScroll() {
        // Mirrors query(RawQuery, ...) / query(IndexQuery, ...): a caller applies .limit(n) with n smaller than the
        // batch and then closes the stream (e.g. via try-with-resources) without exhausting it.
        RecordingClient client = new RecordingClient();
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-2", BATCH_SIZE), BATCH_SIZE);

        try (Stream<RawQuery.Result<String>> limited =
                     OpenSearchIndex.withScrollCleanup(scroll, streamOf(scroll)).limit(1)) {
            List<RawQuery.Result<String>> collected = new ArrayList<>();
            limited.forEach(collected::add);
            assertEquals(collected.size(), 1);
        }

        assertEquals(client.deleteScrollCalls, 1,
                "early termination behind a limit() must still release the scroll context on stream close");
    }

    @Test
    public void isNoOpForNonScrollIterator() {
        // Single-page (non-scroll) result path: withScrollCleanup must not alter behavior or attempt any deletion.
        RecordingClient client = new RecordingClient();
        Iterator<RawQuery.Result<String>> plainIterator =
                Collections.singletonList(new RawQuery.Result<>("doc-0", 1.0f)).iterator();
        Stream<RawQuery.Result<String>> plainStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(plainIterator, Spliterator.ORDERED), false);

        Stream<RawQuery.Result<String>> wrapped = OpenSearchIndex.withScrollCleanup(plainIterator, plainStream);
        List<RawQuery.Result<String>> collected = new ArrayList<>();
        wrapped.forEach(collected::add);
        wrapped.close();

        assertEquals(collected.size(), 1);
        assertEquals(client.deleteScrollCalls, 0, "non-scroll path must never call deleteScroll");
        assertFalse(client.searchCalled, "non-scroll path must never touch the OpenSearch client at all");
    }

    private static Stream<RawQuery.Result<String>> streamOf(Iterator<RawQuery.Result<String>> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    private static OpenSearchResponse response(String scrollId, int hits) {
        OpenSearchResponse response = new OpenSearchResponse();
        List<RawQuery.Result<String>> results = new ArrayList<>();
        for (int i = 0; i < hits; i++) {
            results.add(new RawQuery.Result<>(scrollId + "-doc-" + i, 1.0f));
        }
        response.setResults(results);
        response.setScrollId(scrollId);
        return response;
    }

    /** Minimal hand-written {@link OpenSearchClient} fake; tracks scroll delete/search interactions only. */
    private static final class RecordingClient implements OpenSearchClient {
        boolean searchCalled;
        int     deleteScrollCalls;
        String  lastDeletedScrollId;

        @Override
        public OpenSearchResponse search(String scrollId) throws IOException {
            searchCalled = true;
            return response(scrollId, 0);
        }

        @Override
        public void deleteScroll(String scrollId) throws IOException {
            deleteScrollCalls++;
            lastDeletedScrollId = scrollId;
        }

        // ---- unused interface methods ----
        @Override public OpenSearchMajorVersion getMajorVersion() { return null; }
        @Override public void clusterHealthRequest(String timeout) { }
        @Override public boolean indexExists(String indexName) { return false; }
        @Override public boolean isIndex(String indexName) { return false; }
        @Override public boolean isAlias(String aliasName) { return false; }
        @Override public void createStoredScript(String scriptName, Map<String, Object> script) { }
        @Override public org.janusgraph.diskstorage.opensearch.script.OSScriptResponse getStoredScript(String scriptName) { return null; }
        @Override public void createIndex(String indexName, Map<String, Object> settings) { }
        @Override public void updateIndexSettings(String indexName, Map<String, Object> settings) { }
        @Override public void updateClusterSettings(Map<String, Object> settings) { }
        @Override public Map getIndexSettings(String indexName) { return null; }
        @Override public void createMapping(String indexName, String typeName, Map<String, Object> mapping) { }
        @Override public org.janusgraph.diskstorage.opensearch.mapping.IndexMapping getMapping(String indexName, String typeName) { return null; }
        @Override public void deleteIndex(String indexName) { }
        @Override public void clearStore(String indexName, String storeName) { }
        @Override public void bulkRequest(List<OpenSearchMutation> requests, String ingestPipeline) { }
        @Override public long countTotal(String indexName, Map<String, Object> requestData) { return 0; }
        @Override public Number min(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public Number max(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public double avg(String indexName, Map<String, Object> requestData, String fieldName) { return 0; }
        @Override public Number sum(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public OpenSearchResponse search(String indexName, Map<String, Object> request, boolean useScroll) { return null; }
        @Override public void addAlias(String alias, String index) { }
        @Override public void close() { }
    }
}
