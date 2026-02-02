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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Early Exit for No-Lineage Entities in removeHasLineageOnDelete().
 *
 * <h2>Early Exit Rules (when DELETE_HASLINEAGE_EARLYEXIT_ENABLED is ON)</h2>
 * <ul>
 * <li>Non-Process/DataSet: Skip (existing behavior)</li>
 * <li>Process/DataSet with no lineage edges: Skip edge iteration entirely</li>
 * <li>Process/DataSet with edges but 0 active: Skip resetHasLineageOnInputOutputDelete</li>
 * <li>Process/DataSet with active edges: Process normally</li>
 * </ul>
 */
class HasLineageEarlyExitTest {

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
    void testEarlyExitFlagDefaults() {
        // Verify default flag value is OFF (safe default)
        assertFalse(AtlasConfiguration.DELETE_HASLINEAGE_EARLYEXIT_ENABLED.getBoolean(),
                "DELETE_HASLINEAGE_EARLYEXIT_ENABLED should default to false for safe rollout");
    }
}
