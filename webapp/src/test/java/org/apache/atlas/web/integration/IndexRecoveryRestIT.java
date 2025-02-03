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
package org.apache.atlas.web.integration;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasServiceException;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Map;

import static org.apache.atlas.repository.Constants.INDEX_RECOVERY_PREFIX;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class IndexRecoveryRestIT extends BaseResourceIT {
    @Test
    public void startIndexRecovery() throws Exception {
        Map<String, String> indexRecoveryDataBefore = atlasClientV2.getIndexRecoveryData();

        try {
            atlasClientV2.startIndexRecovery(null);
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), AtlasErrorCode.BAD_REQUEST.getHttpCode().getStatusCode());
        }

        long now = System.currentTimeMillis();

        atlasClientV2.startIndexRecovery(Instant.ofEpochMilli(now));

        Map<String, String> indexRecoveryDataAfter = atlasClientV2.getIndexRecoveryData();

        String customTimeKey = StringUtils.removeStart(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, INDEX_RECOVERY_PREFIX);

        assertNotEquals(indexRecoveryDataBefore.get(customTimeKey), indexRecoveryDataAfter.get(customTimeKey));
        assertEquals(Instant.ofEpochMilli(now).toString(), indexRecoveryDataAfter.get(customTimeKey));
    }
}
