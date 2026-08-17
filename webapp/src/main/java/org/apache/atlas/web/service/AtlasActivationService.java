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
package org.apache.atlas.web.service;

import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.server.common.service.EmbeddedServer;
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.atlas.service.Service;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Activates all Atlas subsystems on startup.
 *
 * <p>Since Atlas runs in active-active peer mode (no ZooKeeper), every node
 * transitions directly from STARTING → BECOMING_ACTIVE → ACTIVE.  There is no
 * leader election, no follower state, and no Curator dependency.
 *
 * <p>{@link #start()} sorts all {@link ActiveStateChangeHandler}s by their
 * {@code HandlerOrder}, calls {@code instanceIsActive()} on each in sequence,
 * then marks the node ACTIVE.
 *
 * <h3>SERVICE_TYPE=INITIALIZATION</h3>
 * After all handlers complete the process calls {@code System.exit(0)} so the
 * JVM terminates cleanly.  Designed for a Kubernetes init-container that
 * prepares the store once before the actual server pods start.
 */
@Component
public class AtlasActivationService implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasActivationService.class);

    private final ServiceState                   serviceState;
    private final AtlasMetricsUtil               metricsUtil;
    private final AtlasAuditService              auditService;
    private       Set<ActiveStateChangeHandler>  activeStateChangeHandlerProviders;
    private       List<ActiveStateChangeHandler> activeStateChangeHandlers;

    @Inject
    AtlasActivationService(Set<ActiveStateChangeHandler> activeStateChangeHandlerProviders,
                           ServiceState serviceState,
                           AtlasMetricsUtil metricsUtil,
                           AtlasAuditService auditService) {
        this.activeStateChangeHandlerProviders = activeStateChangeHandlerProviders;
        this.activeStateChangeHandlers         = new ArrayList<>();
        this.serviceState                      = serviceState;
        this.metricsUtil                       = metricsUtil;
        this.auditService                      = auditService;
    }

    /**
     * Activates this node as a peer. All {@link ActiveStateChangeHandler}s receive
     * {@code instanceIsActive()} in strict {@code HandlerOrder} sequence.
     *
     * <p>When {@code SERVICE_TYPE=INITIALIZATION} the JVM exits with code 0 after
     * all handlers complete.
     */
    @Override
    public void start() throws AtlasException {
        AtlasRunMode mode = AtlasRunMode.current();

        LOG.info("AtlasActivationService.start(): activating (RUN_MODE={})", mode);

        metricsUtil.onServerStart();

        if (activeStateChangeHandlers.isEmpty()) {
            activeStateChangeHandlers.addAll(activeStateChangeHandlerProviders);
            activeStateChangeHandlers.sort(Comparator.comparingInt(ActiveStateChangeHandler::getHandlerOrder));
            LOG.info("AtlasActivationService: handlers (ordered): {}", activeStateChangeHandlers);
        }

        serviceState.becomingActive();

        try {
            for (ActiveStateChangeHandler handler : activeStateChangeHandlers) {
                handler.instanceIsActive();
            }

            metricsUtil.onServerActivation();
            serviceState.setActive();

            LOG.info("AtlasActivationService: node is now ACTIVE (RUN_MODE={})", mode);

            auditActivation();
        } catch (Exception e) {
            LOG.error("AtlasActivationService: exception during activation", e);
        } finally {
            RequestContext.clear();
        }

        if (mode.exitsAfterInit()) {
            LOG.info("AtlasActivationService: RUN_MODE=INITIALIZER — initialization complete, exiting");
            exitAfterInitialization();
        }
    }

    @Override
    public void stop() {
        LOG.info("AtlasActivationService.stop()");
    }

    private void auditActivation() {
        try {
            Date date = new Date();
            auditService.add(AtlasAuditEntry.AuditOperation.SERVER_START, EmbeddedServer.SERVER_START_TIME, date, null, null, 0);
            auditService.add(AtlasAuditEntry.AuditOperation.SERVER_STATE_ACTIVE, date, date, null, null, 0);
        } catch (AtlasBaseException e) {
            LOG.error("AtlasActivationService: failed to record activation audit entry", e);
        } finally {
            RequestContext.clear();
        }
    }

    protected void exitAfterInitialization() {
        System.exit(0);
    }
}
