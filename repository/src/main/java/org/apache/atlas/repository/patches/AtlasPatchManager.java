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

import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.patches.AtlasPatch.AtlasPatches;
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.tasks.GraphClaimable;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.SKIPPED;

@Component
public class AtlasPatchManager {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchManager.class);

    private final List<AtlasPatchHandler>  handlers = new ArrayList<>();
    private final AtlasGraph               atlasGraph;
    private final AtlasTypeRegistry        typeRegistry;
    private final GraphBackedSearchIndexer indexer;
    private final EntityGraphMapper        entityGraphMapper;
    private       PatchContext             context;
    private final Object                   initLock = new Object();

    @Inject
    public AtlasPatchManager(AtlasGraph atlasGraph, AtlasTypeRegistry typeRegistry, GraphBackedSearchIndexer indexer, EntityGraphMapper entityGraphMapper) {
        this.atlasGraph        = atlasGraph;
        this.typeRegistry      = typeRegistry;
        this.indexer           = indexer;
        this.entityGraphMapper = entityGraphMapper;
    }

    public AtlasPatches getAllPatches() {
        initIfNeeded();
        return context.getPatchRegistry().getAllPatches();
    }

    public void applyAll() {
        applyInternal(true);
    }

    public void recoverFailedOrInProgress() {
        applyInternal(false);
    }

    private void applyInternal(boolean includeNotApplied) {
        LOG.info("==> AtlasPatchManager.applyAll()");

        initIfNeeded();
        String nodeId = buildPatchNodeId();
        long processStartMs = System.currentTimeMillis();
        AtlasPatchRegistry registry = context.getPatchRegistry();
        List<AtlasPatchHandler> failedHandlers = new ArrayList<>();

        try {
            for (AtlasPatchHandler handler : handlers) {
                PatchStatus patchStatus = handler.getStatusFromRegistry();
                if (patchStatus == PatchStatus.FAILED) {
                    failedHandlers.add(handler);
                    continue;
                }

                applyHandler(handler, patchStatus, registry, nodeId, processStartMs, includeNotApplied);
            }

            if (!failedHandlers.isEmpty()) {
                failedHandlers.sort(Comparator.comparing(AtlasPatchHandler::getPatchId));
                for (AtlasPatchHandler handler : failedHandlers) {
                    PatchStatus patchStatus = handler.getStatusFromRegistry();
                    applyHandler(handler, patchStatus, registry, nodeId, processStartMs, includeNotApplied);
                }
            }
        } catch (Exception ex) {
            LOG.error("Error applying patches.", ex);
        } finally {
            // After all the patches are applied, we are clearing the request created at time of applying all the patches.
            RequestContext.clear();
        }

        LOG.info("<== AtlasPatchManager.applyAll()");
    }

    private void applyHandler(AtlasPatchHandler handler, PatchStatus patchStatus, AtlasPatchRegistry registry,
            String nodeId, long processStartMs, boolean includeNotApplied) throws AtlasBaseException {
        if (patchStatus == APPLIED || patchStatus == SKIPPED) {
            LOG.info("Ignoring java handler: {}; status: {}", handler.getPatchId(), patchStatus);
            return;
        }

        if (!includeNotApplied && !registry.isRecoveryApplicable(handler.getPatchId())) {
            LOG.info("Ignoring non-recovery handler: {}; status: {}", handler.getPatchId(), patchStatus);
            return;
        }

        if (registry.findByPatchId(handler.getPatchId()) == null) {
            registry.register(handler.getPatchId(), handler.getPatchId(),
                    AtlasPatchHandler.JAVA_PATCH_TYPE, "apply", PatchStatus.UNKNOWN);
        }

        GraphClaimable<Boolean> claimAction = new GraphClaimable<Boolean>() {
            @Override
            public Boolean tryClaim() {
                return registry.tryClaimPatchExecution(handler.getPatchId(), nodeId, processStartMs);
            }

            @Override
            public void recoverStaleClaims() {
                registry.recoverStaleInProgressClaims(nodeId, processStartMs);
            }
        };

        claimAction.recoverStaleClaims();
        if (!claimAction.tryClaim()) {
            LOG.info("Skipping java handler: {}; node={}; claim not acquired", handler.getPatchId(), nodeId);
            return;
        }

        LOG.info("Applying java handler: {}; node={}; status={}", handler.getPatchId(), nodeId, patchStatus);

        try {
            handler.apply();
        } catch (Exception ex) {
            LOG.error("Error applying patch {}. Marking FAILED.", handler.getPatchId(), ex);
            handler.setStatus(PatchStatus.FAILED);
        }
    }

    private String buildPatchNodeId() {
        String runMode  = AtlasRunMode.current().name();
        String hostName = System.getenv("HOSTNAME");
        String jvmId    = ManagementFactory.getRuntimeMXBean().getName();

        if (StringUtils.isBlank(hostName)) {
            hostName = "unknown-host";
        }

        return runMode + "@" + hostName + "#" + jvmId;
    }

    public void addPatchHandler(AtlasPatchHandler patchHandler) {
        handlers.add(patchHandler);
    }

    public PatchContext getContext() {
        return this.context;
    }

    private void init() {
        LOG.info("==> AtlasPatchManager.init()");

        this.context = new PatchContext(atlasGraph, typeRegistry, indexer, entityGraphMapper);
        this.handlers.clear();

        // register all java patches here
        handlers.add(new UniqueAttributePatch(context));
        handlers.add(new ClassificationTextPatch(context));
        handlers.add(new FreeTextRequestHandlerPatch(context));
        handlers.add(new SuggestionsRequestHandlerPatch(context));
        handlers.add(new IndexConsistencyPatch(context));
        handlers.add(new ReIndexPatch(context));
        handlers.add(new ProcessNamePatch(context));
        handlers.add(new UpdateCompositeIndexStatusPatch(context));
        handlers.add(new RelationshipTypeNamePatch(context));
        handlers.add(new ProcessImpalaNamePatch(context));
        handlers.add(new ReplaceHugeSparkProcessAttributesPatch(context));

        LOG.info("<== AtlasPatchManager.init()");
    }

    private void initIfNeeded() {
        if (context != null) {
            return;
        }

        synchronized (initLock) {
            if (context == null) {
                init();
            }
        }
    }
}
