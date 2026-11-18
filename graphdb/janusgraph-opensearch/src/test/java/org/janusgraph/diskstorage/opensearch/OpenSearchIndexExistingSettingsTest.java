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

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class OpenSearchIndexExistingSettingsTest {
    @Test
    public void existingIndexSettingsLeaveOutKnn() {
        Map<String, Object> configured = new HashMap<>();
        configured.put("knn", true);
        configured.put("index.knn", true);
        configured.put("mapping.total_fields.limit", 3000);

        Map<String, Object> expected = new HashMap<>();
        expected.put("index.mapping.total_fields.limit", 3000);

        assertEquals(OpenSearchIndex.existingIndexSettings(configured), expected);
    }

    @Test
    public void existingIndexSettingsAreEmptyWithOnlyKnn() {
        Map<String, Object> configured = new HashMap<>();
        configured.put("knn", true);

        assertTrue(OpenSearchIndex.existingIndexSettings(configured).isEmpty());
    }
}
