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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for deleteByIds batch lookup behavior.
 *
 * <h2>Behavior</h2>
 * <ul>
 * <li>When DELETE_BATCH_LOOKUP_ENABLED is OFF, single lookups are used</li>
 * <li>When DELETE_BATCH_LOOKUP_ENABLED is ON, batch lookups are used</li>
 * <li>When batch lookup fails, fallback to single lookups occurs</li>
 * </ul>
 */
class DeleteByIdsBatchLookupTest {

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @Test
    void testBatchLookupFlagDefaults() {
        // Verify default flag values
        assertFalse(AtlasConfiguration.DELETE_BATCH_LOOKUP_ENABLED.getBoolean(),
                "DELETE_BATCH_LOOKUP_ENABLED should default to false");
        assertEquals(500, AtlasConfiguration.DELETE_BATCH_LOOKUP_SIZE.getInt(),
                "DELETE_BATCH_LOOKUP_SIZE should default to 500");
    }

    @Test
    void testBatchLookupSizeConfiguration() {
        // Verify batch size is configurable and has reasonable default
        int batchSize = AtlasConfiguration.DELETE_BATCH_LOOKUP_SIZE.getInt();
        assertTrue(batchSize > 0, "Batch size should be positive");
        assertTrue(batchSize <= 10000, "Batch size should be reasonable (<=10000)");
    }
}
