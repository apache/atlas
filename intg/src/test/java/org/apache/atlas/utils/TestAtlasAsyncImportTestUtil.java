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

package org.apache.atlas.utils;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.commons.configuration.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.atlas.AtlasConfiguration.ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION;
import static org.apache.atlas.utils.AtlasAsyncImportTestUtil.OPTION_KEY_MIN_ASYNC_IMPORT_COMPLETION_TIME;
import static org.testng.Assert.assertTrue;

public class TestAtlasAsyncImportTestUtil {
    private Configuration conf;

    @BeforeClass
    public void setup() throws Exception {
        conf = ApplicationProperties.get();
    }

    @Test
    public void testInterceptWaitsForRemainingTimeWhenOverrideEnabled() {
        // Given
        conf.setProperty(ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION.getPropertyName(), true);
        AtlasImportRequest importRequest = new AtlasImportRequest();
        importRequest.setOption(OPTION_KEY_MIN_ASYNC_IMPORT_COMPLETION_TIME, "3000");
        AtlasImportResult importResult = new AtlasImportResult();
        importResult.setRequest(importRequest);

        AtlasAsyncImportRequest asyncRequest = new AtlasAsyncImportRequest(importResult);
        long now = System.currentTimeMillis();
        asyncRequest.setReceivedAt(now);
        asyncRequest.setCompletedAt(now + 1000);

        long before = System.currentTimeMillis();
        // When
        AtlasAsyncImportTestUtil.intercept(asyncRequest);
        long after = System.currentTimeMillis();

        // Then
        long actualWait = after - before;
        assertTrue(actualWait >= 1900, "Should have waited ~2000ms");
    }

    @Test
    public void testInterceptSkipsSleepWhenDurationAlreadyMet() {
        // Given
        conf.setProperty(ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION.getPropertyName(), true);
        AtlasImportRequest importRequest = new AtlasImportRequest();
        importRequest.setOption(OPTION_KEY_MIN_ASYNC_IMPORT_COMPLETION_TIME, "3000");
        AtlasImportResult importResult = new AtlasImportResult();
        importResult.setRequest(importRequest);

        AtlasAsyncImportRequest asyncRequest = new AtlasAsyncImportRequest();
        long now = System.currentTimeMillis();
        asyncRequest.setReceivedAt(now);
        asyncRequest.setCompletedAt(now + 4000);

        long before = System.currentTimeMillis();
        // When
        AtlasAsyncImportTestUtil.intercept(asyncRequest);
        long after = System.currentTimeMillis();

        // Then
        assertTrue(after - before < 200, "Should not sleep if already exceeded");
    }

    @Test
    public void testInterceptSkipsSleepWheOverrideIsDisabled() {
        // Given
        conf.setProperty(ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION.getPropertyName(), false);

        AtlasImportRequest importRequest = new AtlasImportRequest();
        importRequest.setOption(OPTION_KEY_MIN_ASYNC_IMPORT_COMPLETION_TIME, "3000");
        AtlasImportResult importResult = new AtlasImportResult();
        importResult.setRequest(importRequest);

        AtlasAsyncImportRequest asyncRequest = new AtlasAsyncImportRequest(importResult);

        long before = System.currentTimeMillis();
        AtlasAsyncImportTestUtil.intercept(asyncRequest);
        long after = System.currentTimeMillis();

        // Then
        // Ensure that we did not actually sleep (i.e., took less than 200ms)
        assertTrue((after - before) < 200, "intercept() should not sleep when override is disabled");
    }
}
