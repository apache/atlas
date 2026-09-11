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

import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
@Order(3)
public class AtlasPatchService implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchService.class);

    private final AtlasPatchManager patchManager;

    @Inject
    public AtlasPatchService(AtlasPatchManager patchManager) {
        this.patchManager  = patchManager;
    }

    @Override
    public void start() throws AtlasException {
        // activation is handled exclusively by instanceIsActive()
    }

    @Override
    public void stop() {
        LOG.info("AtlasPatchService.stop(): stopped");
    }

    @Override
    public void instanceIsActive() {
        LOG.info("==> AtlasPatchService.instanceIsActive()");

        // MONOLITHIC/INITIALIZER apply full patch set.
        // Other RUN_MODEs execute only failed/stale recovery via shared CAS.
        if (!AtlasRunMode.current().runsInitialization()) {
            LOG.info("AtlasPatchService.instanceIsActive(): RUN_MODE={} — running patch recovery-only pass",
                    AtlasRunMode.current());
            startRecoveryOnly();
            return;
        }

        startInternal();

        LOG.info("<== AtlasPatchService.instanceIsActive()");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.ATLAS_PATCH_SERVICE.getOrder();
    }

    void startInternal() {
        try {
            LOG.info("AtlasPatchService: applying patches...");

            patchManager.applyAll();
        } catch (Exception ex) {
            LOG.error("AtlasPatchService: failed in applying patches", ex);
        }
    }

    void startRecoveryOnly() {
        try {
            LOG.info("AtlasPatchService: running patch recovery-only pass...");
            patchManager.recoverFailedOrInProgress();
        } catch (Exception ex) {
            LOG.error("AtlasPatchService: recovery-only pass failed", ex);
        }
    }
}
