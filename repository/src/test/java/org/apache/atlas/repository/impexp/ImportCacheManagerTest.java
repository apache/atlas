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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ImportCacheManagerTest {
    // scenario 4: a non-terminal (in-flight) import is pinned and must survive size pressure, while
    // terminal (already-durable) entries are evicted normally.
    @Test
    public void testInFlightRequestSurvivesSizePressure() {
        ImportCacheManager<String, AtlasAsyncImportRequest> cache = new ImportCacheManager<>(AsyncImportService::isTerminal);

        AtlasAsyncImportRequest inFlight = request("import-processing", ImportStatus.PROCESSING);

        cache.put(inFlight.getImportId(), inFlight);

        // push well past the max size with terminal (evictable) entries
        for (int i = 0; i < 20; i++) {
            AtlasAsyncImportRequest terminal = request("import-done-" + i, ImportStatus.SUCCESSFUL);

            cache.put(terminal.getImportId(), terminal);
        }

        // the pinned in-flight import is still there
        assertNotNull(cache.get("import-processing"));

        // the oldest terminal entries were evicted under pressure
        assertNull(cache.get("import-done-0"));
    }

    @Test
    public void testInvalidateStillRemovesPinnedEntry() {
        ImportCacheManager<String, AtlasAsyncImportRequest> cache = new ImportCacheManager<>(AsyncImportService::isTerminal);

        AtlasAsyncImportRequest inFlight = request("import-processing", ImportStatus.PROCESSING);

        cache.put(inFlight.getImportId(), inFlight);
        cache.invalidate(inFlight.getImportId());

        assertNull(cache.get("import-processing"));
    }

    private AtlasAsyncImportRequest request(String importId, ImportStatus status) {
        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest();

        request.setImportId(importId);
        request.setStatus(status);

        return request;
    }
}
