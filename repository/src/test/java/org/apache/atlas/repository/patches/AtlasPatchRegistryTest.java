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

package org.apache.atlas.repository.patches;

import org.apache.atlas.TestModules;
import org.apache.atlas.model.patches.AtlasPatch;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.tasks.GraphClaim;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer.TYPEDEF_PATCH_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasPatchRegistryTest {
    private static final long ONE_HOUR = 3600000L;

    @Inject
    private AtlasGraph graph;

    @Test
    public void noPatchesRegistered() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);

        assertPatches(registry, 0);
    }

    @Test(dependsOnMethods = "noPatchesRegistered")
    public void registerPatch() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);

        registry.register("1", "test patch", TYPEDEF_PATCH_TYPE, "apply", AtlasPatch.PatchStatus.UNKNOWN);

        assertPatches(registry, 1);
    }

    @Test(dependsOnMethods = "registerPatch")
    public void updateStatusForPatch() {
        final AtlasPatch.PatchStatus expectedStatus = AtlasPatch.PatchStatus.APPLIED;
        String                       patchId        = "1";

        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);

        registry.updateStatus(patchId, expectedStatus);

        AtlasPatch.AtlasPatches patches = assertPatches(registry, 1);

        assertEquals(patches.getPatches().get(0).getId(), patchId);
        assertEquals(patches.getPatches().get(0).getStatus(), expectedStatus);
        assertNotNull(patches.getPatches().get(0).getAppliedBy());
        assertTrue(patches.getPatches().get(0).getAppliedAt() > 0L);
    }

    @Test(dependsOnMethods = "updateStatusForPatch")
    public void notAppliedStatusShouldBeRunnable() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        registry.updateStatus("1", AtlasPatch.PatchStatus.NOT_APPLIED);

        assertTrue(registry.isApplicable("1", null, 0));
    }

    @Test(dependsOnMethods = "notAppliedStatusShouldBeRunnable")
    public void onlyOneNodeGetsAPatch() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        String             patchId  = "contended-patch";

        assertTrue(registry.tryClaimPatchExecution(patchId, "node-1", ONE_HOUR));
        assertFalse(registry.tryClaimPatchExecution(patchId, "node-2", ONE_HOUR),
                "a patch already being applied by another node must not be handed out again");
    }

    /**
     * The reason the previous rule - reclaim anything claimed before I started - was wrong: nodes do
     * not start at the same moment, so the later one declared every patch the earlier one was applying
     * abandoned, and both applied it.
     */
    @Test(dependsOnMethods = "notAppliedStatusShouldBeRunnable")
    public void aPeerStillWorkingIsLeftAlone() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        String             patchId  = "long-running-patch";

        assertTrue(registry.tryClaimPatchExecution(patchId, "node-1", ONE_HOUR));

        registry.recoverStaleInProgressClaims("node-2");

        assertEquals(registry.getStatus(patchId), AtlasPatch.PatchStatus.IN_PROGRESS,
                "a patch whose claim is still live is being worked on, however long ago it started");
        assertFalse(registry.tryClaimPatchExecution(patchId, "node-2", ONE_HOUR));
    }

    @Test(dependsOnMethods = "notAppliedStatusShouldBeRunnable")
    public void aPatchIsTakenOverOnceTheClaimLapses() throws InterruptedException {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        String             patchId  = "abandoned-patch";

        assertTrue(registry.tryClaimPatchExecution(patchId, "node-1", 1L));

        Thread.sleep(5L);

        registry.recoverStaleInProgressClaims("node-2");

        assertEquals(registry.getStatus(patchId), AtlasPatch.PatchStatus.FAILED,
                "a patch left behind by a node that never came back is reported as failed, so it is retried");
        assertTrue(registry.tryClaimPatchExecution(patchId, "node-2", ONE_HOUR));
    }

    /**
     * A handler disabled by configuration returns without recording anything.  The patch has to come
     * out of IN_PROGRESS: peers read that state as work to recover, and the patch itself would never
     * be attempted again by the node that holds it.
     */
    @Test(dependsOnMethods = "notAppliedStatusShouldBeRunnable")
    public void aPatchNobodyRanIsLeftFreeToRunLater() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        String             patchId  = "declined-patch";

        assertTrue(registry.tryClaimPatchExecution(patchId, registry.getNodeId(), ONE_HOUR));

        registry.releaseUnfinishedClaim(patchId);

        assertEquals(registry.getStatus(patchId), AtlasPatch.PatchStatus.UNKNOWN);
        assertNull(GraphClaim.holderOf(graph, Constants.CLAIM_PATCH_PREFIX + patchId),
                "the claim must go with the status, or nothing can ever pick the patch up");
        assertTrue(registry.tryClaimPatchExecution(patchId, "node-2", ONE_HOUR));
    }

    @Test(dependsOnMethods = "notAppliedStatusShouldBeRunnable")
    public void finishingAPatchGivesTheClaimBack() {
        AtlasPatchRegistry registry = new AtlasPatchRegistry(graph);
        String             patchId  = "finished-patch";

        assertTrue(registry.tryClaimPatchExecution(patchId, registry.getNodeId(), ONE_HOUR));

        registry.updateStatus(patchId, AtlasPatch.PatchStatus.APPLIED);

        assertNull(GraphClaim.holderOf(graph, Constants.CLAIM_PATCH_PREFIX + patchId));
    }

    private AtlasPatch.AtlasPatches assertPatches(AtlasPatchRegistry registry, int i) {
        AtlasPatch.AtlasPatches patches = registry.getAllPatches();

        assertNotNull(patches);
        assertEquals(patches.getPatches().size(), i);

        return patches;
    }
}
