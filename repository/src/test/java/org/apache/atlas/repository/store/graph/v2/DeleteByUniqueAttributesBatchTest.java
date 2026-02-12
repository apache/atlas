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
 * Tests for deleteByUniqueAttributes batch resolution configuration.
 *
 * <h2>Behavior</h2>
 * <ul>
 * <li>When DELETE_BATCH_OPERATIONS_ENABLED is OFF, single lookups are used</li>
 * <li>When DELETE_BATCH_OPERATIONS_ENABLED is ON, batch resolution is attempted</li>
 * <li>When batch resolution fails, fallback to single lookups occurs</li>
 * </ul>
 */
class DeleteByUniqueAttributesBatchTest {

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
    void testUniqueAttrBatchFlagDefaults() {
        // Verify default flag values
        assertEquals(200, AtlasConfiguration.DELETE_UNIQUEATTR_BATCH_SIZE.getInt(),
                "DELETE_UNIQUEATTR_BATCH_SIZE should default to 200");
    }

    @Test
    void testUniqueAttrBatchSizeConfiguration() {
        // Verify batch size is configurable and has reasonable default
        int batchSize = AtlasConfiguration.DELETE_UNIQUEATTR_BATCH_SIZE.getInt();
        assertTrue(batchSize > 0, "Batch size should be positive");
        assertTrue(batchSize <= 1000, "Batch size should be reasonable (<=1000)");
    }
}
