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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.utils.AtlasJson;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_EMBEDDING_FIELD;
import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_TEXT_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class SemanticIndexSetupTest {
    @Test
    public void validateConfigurationFailsWhenSemanticSearchIsDisabled() throws Exception {
        String enabledConf = AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName();

        ApplicationProperties.get().setProperty(enabledConf, false);
        try {
            SemanticSearchException e = expectThrows(SemanticSearchException.class, SemanticIndexSetup::validateConfiguration);
            assertTrue(e.getMessage().contains(enabledConf), e.getMessage());
        } finally {
            ApplicationProperties.get().clearProperty(enabledConf);
        }
    }

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
        assertEquals(SemanticIndexSetup.parseEmbeddingDimensionFromMapping(mapping, "janusgraph_vertex_index"),
                Integer.valueOf(384));
    }

    @Test
    public void parseEmbeddingDimensionReturnsNullWhenMissing() {
        assertNull(SemanticIndexSetup.parseEmbeddingDimensionFromMapping(null, "janusgraph_vertex_index"));
    }

    @Test
    public void selectModelIdUsesTheOldestOfTheDeployedModels() throws Exception {
        String response = "{\"hits\":{\"hits\":["
                + "{\"_id\":\"old\",\"_source\":{\"model_config\":{\"embedding_dimension\":384}}},"
                + "{\"_id\":\"new\",\"_source\":{\"model_config\":{\"embedding_dimension\":384}}},"
                + "{\"_id\":\"remote\",\"_source\":{}}]}}";

        assertEquals(SemanticIndexSetup.selectModelId(response, "m", 384), "old");
    }

    @Test
    public void selectModelIdFailsWithoutDeployedModel() {
        SemanticSearchException e = expectThrows(SemanticSearchException.class,
                () -> SemanticIndexSetup.selectModelId("{\"hits\":{\"hits\":[]}}", "m", 384));
        assertTrue(e.getMessage().contains("'m'"), e.getMessage());
    }

    @Test
    public void selectModelIdFailsWhenAModelHasAnotherDimension() {
        String response = "{\"hits\":{\"hits\":["
                + "{\"_id\":\"a\",\"_source\":{\"model_config\":{\"embedding_dimension\":384}}},"
                + "{\"_id\":\"b\",\"_source\":{\"model_config\":{\"embedding_dimension\":768}}}]}}";

        SemanticSearchException e = expectThrows(SemanticSearchException.class, () -> SemanticIndexSetup.selectModelId(response, "m", 384));
        assertTrue(e.getMessage().contains("b (768)"), e.getMessage());
    }

    @Test
    public void parseOpenSearchDistributionFromVersion() {
        Map<String, Object> version = AtlasJson.fromJson("{\"distribution\":\"opensearch\"}", Map.class);
        assertEquals(SemanticIndexSetup.parseOpenSearchDistribution(version), "opensearch");
    }

    @Test
    public void parseKnnEnabledFromNestedSettings() {
        String json = "{"
                + "\"janusgraph_vertex_index\": {"
                + "  \"settings\": {"
                + "    \"index\": {"
                + "      \"knn\": \"true\""
                + "    }"
                + "  }"
                + "}"
                + "}";

        assertTrue(SemanticIndexSetup.parseKnnEnabledFromSettingsResponse(AtlasJson.fromJson(json, Map.class), "janusgraph_vertex_index"));
    }

    @Test
    public void parseKnnEnabledReturnsFalseWhenMissing() {
        assertFalse(SemanticIndexSetup.parseKnnEnabledFromSettingsResponse(
                AtlasJson.fromJson("{\"janusgraph_vertex_index\":{\"settings\":{}}}", Map.class), "janusgraph_vertex_index"));
    }

    @Test
    public void parseIgnoresOtherIndexes() {
        String knnOn = "{\"other_index\":{\"settings\":{\"index\":{\"knn\":\"true\"}},"
                + "\"mappings\":{\"properties\":{\"" + SEMANTIC_EMBEDDING_FIELD + "\":{\"type\":\"knn_vector\",\"dimension\":384}}}}}";

        assertFalse(SemanticIndexSetup.parseKnnEnabledFromSettingsResponse(AtlasJson.fromJson(knnOn, Map.class), "janusgraph_vertex_index"));
        assertNull(SemanticIndexSetup.parseEmbeddingDimensionFromMapping(AtlasJson.fromJson(knnOn, Map.class), "janusgraph_vertex_index"));
    }

    @Test
    public void buildIngestPipelineBodyUsesVertexIndexFieldNames() {
        Map<String, Object> body = SemanticIndexSetup.buildIngestPipelineBody("model-123");
        assertTrue(body.containsKey("processors"));
        assertEquals(((List<?>) body.get("processors")).size(), 2);

        String serialized = AtlasJson.toJson(body);
        assertTrue(serialized.contains(SEMANTIC_TEXT_FIELD));
        assertTrue(serialized.contains(SEMANTIC_EMBEDDING_FIELD));
        assertTrue(serialized.contains("\"model_id\":\"model-123\""));
    }
}
