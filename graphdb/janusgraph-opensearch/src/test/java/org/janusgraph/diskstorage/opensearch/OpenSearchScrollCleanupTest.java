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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Verifies the scroll-context cleanup behavior of {@link OpenSearchScroll}: the server-side scroll
 * context must be released on normal exhaustion, on early termination via {@link OpenSearchScroll#close()}, and on
 * a fetch exception — and cleanup must be best-effort and idempotent.
 */
public class OpenSearchScrollCleanupTest {

    private static final int BATCH_SIZE = 2;

    @Test
    public void deletesScrollOnNormalExhaustion() {
        // Initial batch smaller than batchSize => finished => scroll deleted eagerly in the constructor.
        RecordingClient client = new RecordingClient();
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-1", 1), BATCH_SIZE);

        assertTrue(scroll.hasNext());
        scroll.next();
        assertEquals(client.deleteScrollCalls, 1);

        // Idempotent: closing an already-exhausted scroll does not re-delete.
        scroll.close();
        assertEquals(client.deleteScrollCalls, 1);
    }

    @Test
    public void deletesScrollOnEarlyTerminationViaClose() {
        // Full initial batch => not finished => scroll NOT deleted yet; simulates a consumer stopping early (limit).
        RecordingClient client = new RecordingClient();
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-2", BATCH_SIZE), BATCH_SIZE);

        assertEquals(client.deleteScrollCalls, 0, "not finished yet, so no delete on construction");

        scroll.close();

        assertEquals(client.deleteScrollCalls, 1, "close() must release the scroll context");
        assertEquals(client.lastDeletedScrollId, "scroll-2");
    }

    @Test
    public void deletesScrollOnFetchExceptionAndSurfacesOriginalError() {
        RecordingClient client = new RecordingClient();
        client.failNextSearch = true;
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-3", BATCH_SIZE), BATCH_SIZE);

        // Drain the initial batch so hasNext() must fetch the next page, which fails.
        scroll.next();
        scroll.next();

        try {
            scroll.hasNext();
            fail("expected UncheckedIOException from the failing fetch");
        } catch (UncheckedIOException expected) {
            assertEquals(client.deleteScrollCalls, 1, "scroll must be released even when the next fetch fails");
        }
    }

    @Test
    public void cleanupFailureDoesNotMaskFlow() {
        RecordingClient client = new RecordingClient();
        client.failDeleteScroll = true;
        OpenSearchScroll scroll = new OpenSearchScroll(client, response("scroll-4", BATCH_SIZE), BATCH_SIZE);

        // A failing deleteScroll must be swallowed (best-effort) — close() must not throw.
        scroll.close();

        assertEquals(client.deleteScrollCalls, 1);
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

    /** Records scroll interactions; can be configured to fail the next search or the deleteScroll call. */
    private static final class RecordingClient implements OpenSearchClient {
        boolean failNextSearch;
        boolean failDeleteScroll;
        int     deleteScrollCalls;
        String  lastDeletedScrollId;

        @Override
        public OpenSearchResponse search(String scrollId) throws IOException {
            if (failNextSearch) {
                throw new IOException("scroll fetch failed");
            }
            // Return an empty, finished page.
            return response(scrollId, 0);
        }

        @Override
        public void deleteScroll(String scrollId) throws IOException {
            deleteScrollCalls++;
            lastDeletedScrollId = scrollId;
            if (failDeleteScroll) {
                throw new IOException("delete scroll failed");
            }
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
