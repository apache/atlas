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

import javafx.application.Application;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
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
        this.patchManager = patchManager;
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("PatchService: Started.");

        startInternal(ApplicationProperties.get());
    }

    void startInternal(Configuration configuration) {
        if (!HAConfiguration.isHAEnabled(configuration)) {
            instanceIsActive();
        }
    }

    @Override
    public void stop() {
        LOG.info("PatchService: Stopped.");
    }

    @Override
    public void instanceIsActive() {
        try {
            LOG.info("PatchService: Applying patches...");
            patchManager.applyAll();
        }
        catch (Exception ex) {
            LOG.error("PatchService: Applying patches: Failed!", ex);
        }
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive: No action for now.");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.ATLAS_PATCH_SERVICE.getOrder();
    }
}
