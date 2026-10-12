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
package org.apache.atlas.semantic;

import org.apache.atlas.utils.AtlasJson;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_EMBEDDING_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class OpenSearchSemanticStoreTest {
    @Test
    public void parseStoredEmbedding() {
        String json = "{"
                + "\"hits\": {"
                + "  \"hits\": [{"
                + "    \"_id\": \"doc-abc\","
                + "    \"_source\": {"
                + "      \"" + SEMANTIC_EMBEDDING_FIELD + "\": [0.1, 0.2]"
                + "    }"
                + "  }]"
                + "}"
                + "}";

        assertEquals(OpenSearchSemanticStore.parseStoredEmbedding(json).size(), 2);
    }

    @Test
    public void parseStoredEmbeddingReturnsNullWhenMissing() {
        assertNull(OpenSearchSemanticStore.parseStoredEmbedding("{\"hits\":{\"hits\":[]}}"));
        assertNull(OpenSearchSemanticStore.parseStoredEmbedding("{\"hits\":{\"hits\":[{\"_source\":{}}]}}"));
    }

    @Test
    public void buildUpdateEmbeddingByQueryBodyUsesGuidPhraseAndScript() {
        Map<String, Object> body = OpenSearchSemanticStore.buildUpdateEmbeddingByQueryBody("guid-1",
                List.of(0.1, 0.2));
        assertEquals(AtlasJson.toJson(body.get("query")), "{\"match_phrase\":{\"__guid\":\"guid-1\"}}");
        assertTrue(body.containsKey("script"));

        String serialized = AtlasJson.toJson(body);
        assertTrue(serialized.contains(SEMANTIC_EMBEDDING_FIELD));
        assertTrue(serialized.contains("params.embedding"));
    }

    @Test
    public void parseUpdateByQueryUpdatedCount() {
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryUpdatedCount("{\"updated\":1}"), 1);
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryUpdatedCount("{\"updated\":0}"), 0);
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryUpdatedCount("{}"), -1);
    }

    @Test
    public void parseUpdateByQueryVersionConflicts() {
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryVersionConflicts("{\"version_conflicts\":1}"), 1);
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryVersionConflicts("{\"updated\":0}"), 0);
        assertEquals(OpenSearchSemanticStore.parseUpdateByQueryVersionConflicts("{}"), 0);
    }

    @Test
    public void entityScanBodyPagesInPointInTimeAfterLastSortValue() {
        String missing = AtlasJson.toJson(OpenSearchSemanticStore.buildEntityScanBody(true, 50, "pit-1", Collections.singletonList(42)));
        assertTrue(missing.contains("\"exists\":{\"field\":\"" + SEMANTIC_EMBEDDING_FIELD + "\"}"));
        assertTrue(missing.contains("\"sort\":[{\"_shard_doc\":\"asc\"}]"));
        assertEquals(OpenSearchSemanticStore.buildEntityScanBody(true, 50, "pit-1", null).get("pit"), Map.of("id", "pit-1", "keep_alive", "10m"));
        assertTrue(missing.contains("\"search_after\":[42]"));
        assertTrue(missing.contains("\"size\":50"));

        String all = AtlasJson.toJson(OpenSearchSemanticStore.buildEntityScanBody(false, 50, "pit-1", null));
        assertFalse(all.contains(SEMANTIC_EMBEDDING_FIELD));
        assertFalse(all.contains("search_after"));
    }

    @Test
    public void vectorFilterCombinesTypesStateAndExcludedGuids() {
        String json = AtlasJson.toJson(OpenSearchSemanticStore.buildVectorFilter(
                new VectorSearchFilter(Collections.singleton("hive_table"), Collections.singleton("guid-1"), true)));

        assertTrue(json.contains("\"filter\":[{\"terms\":{\"__typeName.keyword\":[\"hive_table\"]}},{\"term\":{\"__state.keyword\":\"ACTIVE\"}}]"));
        assertTrue(json.contains("\"must_not\":[{\"match_phrase\":{\"__guid\":\"guid-1\"}}]"));
        assertNull(OpenSearchSemanticStore.buildVectorFilter(new VectorSearchFilter(Collections.emptySet(), Collections.emptySet(), false)));
    }

    @Test
    public void vectorFilterAddsTheEntityFilterClause() {
        Map<String, Object> entityFilterClause = Collections.singletonMap("term", Collections.singletonMap("Asset•__s_owner", "alice"));

        String json = AtlasJson.toJson(OpenSearchSemanticStore.buildVectorFilter(
                new VectorSearchFilter(Collections.emptySet(), Collections.emptySet(), false, entityFilterClause)));

        assertEquals(json, "{\"bool\":{\"filter\":[{\"term\":{\"Asset•__s_owner\":\"alice\"}}]}}");
    }

    @Test
    public void parseSearchHitsReadsGuidFromSourceAndScore() {
        String json = "{\"hits\":{\"hits\":["
                + "{\"_id\":\"doc-1\",\"_score\":0.91,\"_source\":{\"__guid\":\"g1\"}},"
                + "{\"_id\":\"doc-2\",\"_source\":{\"__guid\":\"g2\"}},"
                + "{\"_id\":\"doc-3\",\"_score\":0.5,\"_source\":{}}"
                + "]}}";

        List<VectorSearchHit> hits = OpenSearchSemanticStore.parseSearchHits(json);

        assertEquals(hits, Arrays.asList(new VectorSearchHit("g1", 0.91), new VectorSearchHit("g2", 0.0)));
    }

    @Test
    public void parseSearchHitsReturnsEmptyWithoutHits() {
        assertTrue(OpenSearchSemanticStore.parseSearchHits("{}").isEmpty());
        assertTrue(OpenSearchSemanticStore.parseSearchHits("{\"hits\":{\"total\":{\"value\":0}}}").isEmpty());
    }

    @Test
    public void isModelDeployedReadsTheModelState() {
        assertTrue(OpenSearchSemanticStore.isModelDeployed("{\"name\":\"m\",\"model_state\":\"DEPLOYED\"}"));
        assertTrue(OpenSearchSemanticStore.isModelDeployed("{\"model_state\":\"PARTIALLY_DEPLOYED\"}"));
        assertFalse(OpenSearchSemanticStore.isModelDeployed("{\"model_state\":\"UNDEPLOYED\"}"));
        assertFalse(OpenSearchSemanticStore.isModelDeployed("{\"model_state\":\"DEPLOY_FAILED\"}"));
        assertFalse(OpenSearchSemanticStore.isModelDeployed("{}"));
    }

    @Test
    public void parseEntityScanPageReturnsGuidsLastSortAndPitId() throws Exception {
        String json = "{\"pit_id\":\"pit-2\",\"_shards\":{\"total\":3,\"failed\":0},\"hits\":{\"hits\":["
                + "{\"_id\":\"a\",\"_source\":{\"__guid\":\"g1\"},\"sort\":[7]},"
                + "{\"_id\":\"b\",\"_source\":{\"__guid\":\"g2\"},\"sort\":[9]}"
                + "]}}";

        OpenSearchSemanticStore.EntityScanPage page = OpenSearchSemanticStore.parseEntityScanPage(json);
        assertEquals(page.guids, Arrays.asList("g1", "g2"));
        assertEquals(page.hitCount, 2);
        assertEquals(page.lastSort, Collections.singletonList(9));
        assertEquals(page.pitId, "pit-2");

        OpenSearchSemanticStore.EntityScanPage empty = OpenSearchSemanticStore.parseEntityScanPage("{\"hits\":{\"hits\":[]}}");
        assertTrue(empty.guids.isEmpty());
        assertEquals(empty.hitCount, 0);
    }

    @Test
    public void parseEntityScanPageFailsRetryablyOnShardFailure() {
        try {
            OpenSearchSemanticStore.parseEntityScanPage("{\"_shards\":{\"total\":3,\"failed\":1},\"hits\":{\"hits\":[]}}");
            fail("expected SemanticSearchException");
        } catch (SemanticSearchException e) {
            assertTrue(e.isRetryable());
        }
    }

    @Test
    public void parsePitId() throws Exception {
        assertEquals(OpenSearchSemanticStore.parsePitId("{\"pit_id\":\"p1\",\"creation_time\":1}"), "p1");
        try {
            OpenSearchSemanticStore.parsePitId("{}");
            fail("expected SemanticSearchException");
        } catch (SemanticSearchException expected) {
        }
    }
}
