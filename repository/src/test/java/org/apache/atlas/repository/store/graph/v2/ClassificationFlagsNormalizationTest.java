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
package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for MS-594: data inconsistency in Cassandra tags where
 * {@code restrictPropagationThroughHierarchy} is sometimes missing.
 *
 * <h3>Root cause</h3>
 * {@code AtlasClassification} uses {@code @JsonSerialize(include=NON_NULL)}, so
 * any propagation flag left as {@code null} is <b>omitted</b> from the JSON stored
 * in Cassandra's {@code tag_meta_json} column.
 *
 * <p>The {@code addClassifications} path correctly defaults all null flags before
 * calling {@code putDirectTag}, but {@code updateClassificationsV2} does NOT —
 * it calls {@code putDirectTag} at line 5270 with the raw request classification
 * before the null-handling code at lines 5354-5361.</p>
 *
 * <h3>Impact</h3>
 * When a client sends an update without explicitly setting all propagation flags,
 * the stored tag loses the unset flags entirely. On read, the DAO defaults them
 * to {@code false}, which may differ from the actual intended value.
 *
 * @see EntityGraphMapper#updateClassificationsV2
 * @see EntityGraphMapper#addClassifications
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassificationFlagsNormalizationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    void setUp() throws Exception {
        // EntityGraphMapper's static initializers need ApplicationProperties
        ApplicationProperties.set(new PropertiesConfiguration());
    }

    // =========================================================================
    //  Demonstrate the problem: null flags are omitted from serialized JSON
    // =========================================================================

    @Test
    void nullRestrictPropagationThroughHierarchy_isOmittedFromJson() throws Exception {
        // Simulate a classification update request where the client doesn't set
        // restrictPropagationThroughHierarchy (leaves it null)
        AtlasClassification classification = new AtlasClassification("TestTag");
        classification.setEntityGuid("test-guid-123");
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(true);
        classification.setRestrictPropagationThroughLineage(false);
        // restrictPropagationThroughHierarchy is NOT set — remains null

        String json = AtlasType.toJson(classification);
        JsonNode node = MAPPER.readTree(json);

        // With NON_NULL serialization, null fields are omitted
        assertFalse(node.has("restrictPropagationThroughHierarchy"),
                "Null restrictPropagationThroughHierarchy should be omitted from JSON (NON_NULL serialization). " +
                "This is what causes the data inconsistency in Cassandra.");
    }

    @Test
    void nullRestrictPropagationThroughLineage_isOmittedFromJson() throws Exception {
        AtlasClassification classification = new AtlasClassification("TestTag");
        classification.setEntityGuid("test-guid-123");
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(true);
        // restrictPropagationThroughLineage NOT set
        classification.setRestrictPropagationThroughHierarchy(false);

        String json = AtlasType.toJson(classification);
        JsonNode node = MAPPER.readTree(json);

        assertFalse(node.has("restrictPropagationThroughLineage"),
                "Null restrictPropagationThroughLineage should be omitted from JSON.");
    }

    @Test
    void nullPropagate_isOmittedFromJson() throws Exception {
        AtlasClassification classification = new AtlasClassification("TestTag");
        classification.setEntityGuid("test-guid-123");
        // propagate NOT set
        classification.setRemovePropagationsOnEntityDelete(true);
        classification.setRestrictPropagationThroughLineage(false);
        classification.setRestrictPropagationThroughHierarchy(false);

        String json = AtlasType.toJson(classification);
        JsonNode node = MAPPER.readTree(json);

        assertFalse(node.has("propagate"),
                "Null propagate should be omitted from JSON.");
    }

    // =========================================================================
    //  Test the normalization fix: after normalizing, all flags are present
    // =========================================================================

    @Test
    void normalizeClassificationForUpdate_setsDefaultsForNullFlags() throws Exception {
        // Simulate the update scenario: current classification in Cassandra has all flags set,
        // but the update request has some flags null (client didn't send them)
        AtlasClassification currentInCassandra = new AtlasClassification("TestTag");
        currentInCassandra.setEntityGuid("test-guid-123");
        currentInCassandra.setPropagate(true);
        currentInCassandra.setRemovePropagationsOnEntityDelete(true);
        currentInCassandra.setRestrictPropagationThroughLineage(true);
        currentInCassandra.setRestrictPropagationThroughHierarchy(false);

        // Update request: only sets propagate, leaves restrict flags null
        AtlasClassification updateRequest = new AtlasClassification("TestTag");
        updateRequest.setEntityGuid("test-guid-123");
        updateRequest.setPropagate(false);
        updateRequest.setRemovePropagationsOnEntityDelete(true);
        // restrictPropagationThroughLineage is null
        // restrictPropagationThroughHierarchy is null

        // Apply normalization (the fix)
        EntityGraphMapper.normalizeClassificationFlagsForUpdate(updateRequest, currentInCassandra);

        // After normalization, null flags should be filled from current classification
        assertNotNull(updateRequest.getRestrictPropagationThroughLineage(),
                "restrictPropagationThroughLineage should not be null after normalization");
        assertNotNull(updateRequest.getRestrictPropagationThroughHierarchy(),
                "restrictPropagationThroughHierarchy should not be null after normalization");
        assertEquals(true, updateRequest.getRestrictPropagationThroughLineage(),
                "restrictPropagationThroughLineage should be preserved from current classification");
        assertEquals(false, updateRequest.getRestrictPropagationThroughHierarchy(),
                "restrictPropagationThroughHierarchy should be preserved from current classification");

        // Verify serialized JSON now includes all flags
        String json = AtlasType.toJson(updateRequest);
        JsonNode node = MAPPER.readTree(json);
        assertTrue(node.has("restrictPropagationThroughLineage"),
                "Serialized JSON must include restrictPropagationThroughLineage");
        assertTrue(node.has("restrictPropagationThroughHierarchy"),
                "Serialized JSON must include restrictPropagationThroughHierarchy");
        assertTrue(node.has("propagate"),
                "Serialized JSON must include propagate");
        assertTrue(node.has("removePropagationsOnEntityDelete"),
                "Serialized JSON must include removePropagationsOnEntityDelete");
    }

    @Test
    void normalizeClassificationForUpdate_currentAlsoNull_usesDefaults() throws Exception {
        // Edge case: both the update request AND the current classification have null flags
        // (from tags stored before the fix)
        AtlasClassification currentInCassandra = new AtlasClassification("TestTag");
        currentInCassandra.setEntityGuid("test-guid-123");
        currentInCassandra.setPropagate(true);
        currentInCassandra.setRemovePropagationsOnEntityDelete(true);
        // restrictPropagationThroughLineage is null (stored before fix)
        // restrictPropagationThroughHierarchy is null (stored before fix)

        AtlasClassification updateRequest = new AtlasClassification("TestTag");
        updateRequest.setEntityGuid("test-guid-123");
        updateRequest.setPropagate(true);
        // All restrict flags null

        // Apply normalization
        EntityGraphMapper.normalizeClassificationFlagsForUpdate(updateRequest, currentInCassandra);

        // Should fall back to defaults (false) when both are null
        assertEquals(false, updateRequest.getRestrictPropagationThroughLineage(),
                "Should default to false when both current and update are null");
        assertEquals(false, updateRequest.getRestrictPropagationThroughHierarchy(),
                "Should default to false when both current and update are null");
        assertNotNull(updateRequest.isPropagate());
        assertNotNull(updateRequest.getRemovePropagationsOnEntityDelete());
    }

    @Test
    void normalizeClassificationForUpdate_explicitValues_notOverwritten() throws Exception {
        // When the client explicitly sets flags, they should NOT be overwritten
        AtlasClassification currentInCassandra = new AtlasClassification("TestTag");
        currentInCassandra.setEntityGuid("test-guid-123");
        currentInCassandra.setPropagate(false);
        currentInCassandra.setRemovePropagationsOnEntityDelete(true);
        currentInCassandra.setRestrictPropagationThroughLineage(false);
        currentInCassandra.setRestrictPropagationThroughHierarchy(false);

        AtlasClassification updateRequest = new AtlasClassification("TestTag");
        updateRequest.setEntityGuid("test-guid-123");
        updateRequest.setPropagate(true);
        updateRequest.setRemovePropagationsOnEntityDelete(false);
        updateRequest.setRestrictPropagationThroughLineage(true);
        updateRequest.setRestrictPropagationThroughHierarchy(false);

        // Apply normalization
        EntityGraphMapper.normalizeClassificationFlagsForUpdate(updateRequest, currentInCassandra);

        // Explicit values should be preserved, not overwritten
        assertEquals(true, updateRequest.isPropagate());
        assertEquals(false, updateRequest.getRemovePropagationsOnEntityDelete());
        assertEquals(true, updateRequest.getRestrictPropagationThroughLineage());
        assertEquals(false, updateRequest.getRestrictPropagationThroughHierarchy());
    }
}
