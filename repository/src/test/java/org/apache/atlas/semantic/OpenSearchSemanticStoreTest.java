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
    public void buildUpdateEmbeddingByQueryBodyUsesGuidTermAndScript() {
        Map<String, Object> body = OpenSearchSemanticStore.buildUpdateEmbeddingByQueryBody("guid-1",
                List.of(0.1, 0.2));
        assertTrue(body.containsKey("query"));
        assertTrue(body.containsKey("script"));

        String serialized = AtlasJson.toJson(body);
        assertTrue(serialized.contains("guid-1"));
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
    public void entityScanBodyFiltersMissingEmbeddingsAndPagesByGuid() {
        String missing = AtlasJson.toJson(OpenSearchSemanticStore.buildEntityScanBody(true, 50, "guid-50"));
        assertTrue(missing.contains("\"exists\":{\"field\":\"" + SEMANTIC_EMBEDDING_FIELD + "\"}"));
        assertTrue(missing.contains("\"search_after\":[\"guid-50\"]"));
        assertTrue(missing.contains("__guid.keyword"));

        String all = AtlasJson.toJson(OpenSearchSemanticStore.buildEntityScanBody(false, 50, null));
        assertFalse(all.contains(SEMANTIC_EMBEDDING_FIELD));
        assertFalse(all.contains("search_after"));
    }

    @Test
    public void vectorFilterCombinesTypesAndExcludedGuids() {
        String json = AtlasJson.toJson(OpenSearchSemanticStore.buildVectorFilter(
                new VectorSearchFilter(Collections.singleton("hive_table"), Collections.singleton("guid-1"))));

        assertTrue(json.contains("\"filter\":[{\"terms\":{\"__typeName.keyword\":[\"hive_table\"]}}]"));
        assertTrue(json.contains("\"must_not\":[{\"terms\":{\"__guid.keyword\":[\"guid-1\"]}}]"));
        assertNull(OpenSearchSemanticStore.buildVectorFilter(new VectorSearchFilter(Collections.emptySet(), Collections.emptySet())));
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
    public void parseEntityScanPageReturnsGuidsAndLastSortValue() {
        String json = "{\"hits\":{\"hits\":["
                + "{\"_id\":\"a\",\"_source\":{\"__guid\":\"g1\"},\"sort\":[\"g1\"]},"
                + "{\"_id\":\"b\",\"_source\":{\"__guid\":\"g2\"},\"sort\":[\"g2\"]}"
                + "]}}";

        OpenSearchSemanticStore.EntityScanPage page = OpenSearchSemanticStore.parseEntityScanPage(json);
        assertEquals(page.guids, Arrays.asList("g1", "g2"));
        assertEquals(page.hitCount, 2);
        assertEquals(page.lastSortValue, "g2");

        OpenSearchSemanticStore.EntityScanPage empty = OpenSearchSemanticStore.parseEntityScanPage("{\"hits\":{\"hits\":[]}}");
        assertTrue(empty.guids.isEmpty());
        assertNull(empty.lastSortValue);
    }
}
