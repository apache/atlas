package org.apache.atlas.utils;

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

import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;

import static org.apache.atlas.AtlasConfiguration.ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION;

public class AtlasAsyncImportTestUtil {
    public static final String OPTION_KEY_ASYNC_IMPORT_MIN_DURATION_IN_MS = "asyncImportMinDurationInMs";

    private AtlasAsyncImportTestUtil() {
        // to block instantiation
    }

    public static long intercept(AtlasAsyncImportRequest asyncImportRequest) {
        if (ATLAS_ASYNC_IMPORT_MIN_DURATION_OVERRIDE_TEST_AUTOMATION.getBoolean()) {
            AtlasImportResult importResult = asyncImportRequest.getImportResult();

            if (importResult != null) {
                AtlasImportRequest importRequest = importResult.getRequest();

                if (importRequest != null) {
                    long minImportDurationInMs = Long.parseLong(importRequest.getOptions().getOrDefault(OPTION_KEY_ASYNC_IMPORT_MIN_DURATION_IN_MS, "0"));
                    long waitTimeInMs          = minImportDurationInMs - (asyncImportRequest.getCompletedTime() - asyncImportRequest.getReceivedTime());

                    if (waitTimeInMs > 0) {
                        try {
                            Thread.sleep(waitTimeInMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    return waitTimeInMs;
                }
            }
        }

        return -1;
    }
}
