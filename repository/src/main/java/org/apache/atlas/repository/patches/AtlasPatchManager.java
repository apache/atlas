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

package org.apache.atlas.repository.patches;

import org.apache.atlas.model.patches.AtlasPatch;
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.SKIPPED;

@Component
public class AtlasPatchManager {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchManager.class);

    private final PatchContext context;

    @Inject
    public AtlasPatchManager(AtlasGraph atlasGraph, AtlasTypeRegistry typeRegistry, GraphBackedSearchIndexer indexer) {
        this.context = new PatchContext(atlasGraph, typeRegistry, indexer);
    }

    public AtlasPatch.AtlasPatches getAllPatches() {
        return context.getPatchRegistry().getAllPatches();
    }

    public void applyAll() {
        final AtlasPatchHandler handlers[] = {
                new UniqueAttributePatch(context)
        };

        try {
            for (AtlasPatchHandler handler : handlers) {
                PatchStatus patchStatus = handler.getStatusFromRegistry();

                if (patchStatus == APPLIED || patchStatus == SKIPPED) {
                    LOG.info("Ignoring java handler: {}; status: {}", handler.getPatchId(), patchStatus);
                } else {
                    LOG.info("Applying java handler: {}; status: {}", handler.getPatchId(), patchStatus);

                    handler.apply();
                }
            }
        }
        catch (Exception ex) {
            LOG.error("Error applying patches.", ex);
        }
    }
}
