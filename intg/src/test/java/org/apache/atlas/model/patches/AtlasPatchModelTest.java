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
package org.apache.atlas.model.patches;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class AtlasPatchModelTest {
    @Test
    public void equalsAndHashCode_includeAppliedMetadataFields() {
        AtlasPatch first = new AtlasPatch();
        first.setId("p1");
        first.setDescription("desc");
        first.setType("type");
        first.setAction("action");
        first.setUpdatedBy("updater");
        first.setCreatedBy("creator");
        first.setAppliedBy("node-a");
        first.setCreatedTime(1L);
        first.setUpdatedTime(2L);
        first.setAppliedAt(3L);
        first.setStatus(AtlasPatch.PatchStatus.IN_PROGRESS);

        AtlasPatch second = new AtlasPatch();
        second.setId("p1");
        second.setDescription("desc");
        second.setType("type");
        second.setAction("action");
        second.setUpdatedBy("updater");
        second.setCreatedBy("creator");
        second.setAppliedBy("node-a");
        second.setCreatedTime(1L);
        second.setUpdatedTime(2L);
        second.setAppliedAt(3L);
        second.setStatus(AtlasPatch.PatchStatus.IN_PROGRESS);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());

        second.setAppliedBy("node-b");
        assertNotEquals(first, second);
    }

    @Test
    public void patchStatus_includesNewStates() {
        assertEquals(AtlasPatch.PatchStatus.valueOf("NOT_APPLIED"), AtlasPatch.PatchStatus.NOT_APPLIED);
        assertEquals(AtlasPatch.PatchStatus.valueOf("IN_PROGRESS"), AtlasPatch.PatchStatus.IN_PROGRESS);
    }

    @Test
    public void toString_containsAppliedByAndAppliedAt() {
        AtlasPatch patch = new AtlasPatch();
        patch.setAppliedBy("worker-1");
        patch.setAppliedAt(42L);

        String value = patch.toString();
        assertTrue(value.contains("appliedBy='worker-1'"));
        assertTrue(value.contains("appliedAt=42"));
    }
}
