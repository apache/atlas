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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the shared static-client lifecycle guard: closing an OpenSearch index instance clears the
 * shared static reference only when it still points to that instance, so a short-lived (e.g. bulk-loading) index
 * closing cannot wipe a different graph's live client reference.
 */
public class AtlasOpenSearchIndexInstanceTest {
    @AfterMethod
    public void resetInstance() {
        AtlasOpenSearchIndex.setInstanceForTests(null);
    }

    @Test
    public void clearOnlyWhenStaticStillPointsToClosingInstance() {
        AtlasOpenSearchIndex primary = mock(AtlasOpenSearchIndex.class);
        AtlasOpenSearchIndex bulk    = mock(AtlasOpenSearchIndex.class);

        // Primary graph owns the static reference.
        AtlasOpenSearchIndex.setInstanceForTests(primary);

        // A different (bulk) instance closing must NOT clear the primary's reference.
        assertFalse(AtlasOpenSearchIndex.clearInstanceIfCurrent(bulk));
        assertSame(AtlasOpenSearchIndex.getInstanceForTests(), primary);

        // The owning instance closing clears the reference.
        assertTrue(AtlasOpenSearchIndex.clearInstanceIfCurrent(primary));
        assertNull(AtlasOpenSearchIndex.getInstanceForTests());
    }

    @Test
    public void clearIsIdempotentWhenAlreadyNull() {
        AtlasOpenSearchIndex.setInstanceForTests(null);
        AtlasOpenSearchIndex some = mock(AtlasOpenSearchIndex.class);

        assertFalse(AtlasOpenSearchIndex.clearInstanceIfCurrent(some));
        assertNull(AtlasOpenSearchIndex.getInstanceForTests());
    }
}
