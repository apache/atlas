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

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for {@link OpenSearchIndex#createIndexIdempotent(OpenSearchClient, String, Map)} — the HA / concurrent
 * index-creation safety fix. Two Atlas nodes starting concurrently may both attempt to create the same index; the
 * loser must treat an already-existing index as success without masking genuine creation failures.
 */
public class OpenSearchIndexIdempotentCreateTest {

    private static final String INDEX = "janusgraph_vertex_index";
    private static final Map<String, Object> SETTINGS = Collections.emptyMap();

    @Test
    public void createSucceedsWhenIndexIsAbsent() throws IOException {
        FakeClient client = new FakeClient();
        client.existsAfterCreate = true;

        boolean created = OpenSearchIndex.createIndexIdempotent(client, INDEX, SETTINGS);

        assertTrue(created, "create should report success when it created the index");
        assertEquals(client.createCalls, 1);
    }

    @Test
    public void concurrentAlreadyExistsIsTreatedAsSuccess() throws IOException {
        // Simulate the race loser: createIndex fails (resource_already_exists), but the index exists afterwards
        // because another node created it. This must NOT be surfaced as an error.
        FakeClient client = new FakeClient();
        client.failCreate = true;
        client.existsAfterCreate = true;

        boolean created = OpenSearchIndex.createIndexIdempotent(client, INDEX, SETTINGS);

        assertFalse(created, "create should report it did not create the index (another node did)");
        assertEquals(client.createCalls, 1);
        assertEquals(client.existsCalls, 1, "should re-check existence exactly once after a failed create");
    }

    @Test
    public void genuineFailureIsRethrownWhenIndexStillAbsent() {
        // createIndex fails and the index still does not exist afterwards: this is a real error and must propagate.
        FakeClient client = new FakeClient();
        client.failCreate = true;
        client.existsAfterCreate = false;

        try {
            OpenSearchIndex.createIndexIdempotent(client, INDEX, SETTINGS);
            fail("expected the original IOException to be rethrown");
        } catch (IOException expected) {
            assertEquals(expected.getMessage(), "resource_already_exists_exception");
        }
    }

    /** Minimal hand-written {@link OpenSearchClient} for the create/exists interactions under test. */
    private static final class FakeClient implements OpenSearchClient {
        boolean failCreate;
        boolean existsAfterCreate;
        int     createCalls;
        int     existsCalls;

        @Override
        public void createIndex(String indexName, Map<String, Object> settings) throws IOException {
            createCalls++;
            if (failCreate) {
                throw new IOException("resource_already_exists_exception");
            }
        }

        @Override
        public boolean indexExists(String indexName) {
            existsCalls++;
            return existsAfterCreate;
        }

        // ---- unused interface methods ----
        @Override public OpenSearchMajorVersion getMajorVersion() { return null; }
        @Override public void clusterHealthRequest(String timeout) { }
        @Override public boolean isIndex(String indexName) { return false; }
        @Override public boolean isAlias(String aliasName) { return false; }
        @Override public void createStoredScript(String scriptName, Map<String, Object> script) { }
        @Override public org.janusgraph.diskstorage.opensearch.script.OSScriptResponse getStoredScript(String scriptName) { return null; }
        @Override public void updateIndexSettings(String indexName, Map<String, Object> settings) { }
        @Override public void updateClusterSettings(Map<String, Object> settings) { }
        @Override public Map getIndexSettings(String indexName) { return null; }
        @Override public void createMapping(String indexName, String typeName, Map<String, Object> mapping) { }
        @Override public org.janusgraph.diskstorage.opensearch.mapping.IndexMapping getMapping(String indexName, String typeName) { return null; }
        @Override public void deleteIndex(String indexName) { }
        @Override public void clearStore(String indexName, String storeName) { }
        @Override public void bulkRequest(java.util.List<OpenSearchMutation> requests, String ingestPipeline) { }
        @Override public long countTotal(String indexName, Map<String, Object> requestData) { return 0; }
        @Override public Number min(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public Number max(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public double avg(String indexName, Map<String, Object> requestData, String fieldName) { return 0; }
        @Override public Number sum(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) { return null; }
        @Override public OpenSearchResponse search(String indexName, Map<String, Object> request, boolean useScroll) { return null; }
        @Override public OpenSearchResponse search(String scrollId) { return null; }
        @Override public void deleteScroll(String scrollId) { }
        @Override public void addAlias(String alias, String index) { }
        @Override public void close() { }
    }
}
