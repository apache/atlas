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

import java.util.List;
import java.util.Map;

import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_EMBEDDING_FIELD;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_INGEST_PIPELINE_NAME;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_TEXT_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class OpenSearchSemanticStoreTest {
    @Test
    public void parseEmbeddingDimensionFromMappingResponse() {
        String json = "{"
                + "\"janusgraph_vertex_index\": {"
                + "  \"mappings\": {"
                + "    \"properties\": {"
                + "      \"" + SEMANTIC_EMBEDDING_FIELD + "\": {"
                + "        \"type\": \"knn_vector\","
                + "        \"dimension\": 384"
                + "      }"
                + "    }"
                + "  }"
                + "}"
                + "}";

        Map<String, Object> mapping = AtlasJson.fromJson(json, Map.class);
        assertEquals(OpenSearchSemanticStore.parseEmbeddingDimensionFromMapping(mapping, "janusgraph_vertex_index"),
                Integer.valueOf(384));
    }

    @Test
    public void parseEmbeddingDimensionReturnsNullWhenMissing() {
        assertNull(OpenSearchSemanticStore.parseEmbeddingDimensionFromMapping(null, "janusgraph_vertex_index"));
    }

    @Test
    public void parseOpenSearchDistributionFromVersion() {
        Map<String, Object> version = AtlasJson.fromJson("{\"distribution\":\"opensearch\"}", Map.class);
        assertEquals(OpenSearchSemanticStore.parseOpenSearchDistribution(version), "opensearch");
    }

    @Test
    public void parseVertexIndexDocumentFromSearchResponse() {
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

        OpenSearchSemanticStore.VertexIndexDocument document =
                OpenSearchSemanticStore.parseVertexIndexDocumentFromSearchResponse(json);
        assertEquals(document.getDocumentId(), "doc-abc");
        assertEquals(((List<?>) document.getSource().get(SEMANTIC_EMBEDDING_FIELD)).size(), 2);
    }

    @Test
    public void parseVertexIndexDocumentReturnsNullWhenMissing() {
        assertNull(OpenSearchSemanticStore.parseVertexIndexDocumentFromSearchResponse("{\"hits\":{\"hits\":[]}}"));
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
    public void buildIngestPipelineBodyUsesVertexIndexFieldNames() {
        Map<String, Object> body = OpenSearchSemanticStore.buildIngestPipelineBody("model-123");
        assertTrue(body.containsKey("processors"));
        assertEquals(((List<?>) body.get("processors")).size(), 2);

        String serialized = AtlasJson.toJson(body);
        assertTrue(serialized.contains(SEMANTIC_TEXT_FIELD));
        assertTrue(serialized.contains(SEMANTIC_EMBEDDING_FIELD));
        assertTrue(serialized.contains(SEMANTIC_INGEST_PIPELINE_NAME) || serialized.contains("model-123"));
    }
}
