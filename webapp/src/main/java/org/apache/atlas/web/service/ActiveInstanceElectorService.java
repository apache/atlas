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

package org.apache.atlas.web.service;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.AtlasServerIdSelector;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A service that implements leader election to determine whether this Atlas server is Active.
 *
 * The service implements leader election through <a href="http://curator.apache.org/">Curator</a>'s
 * {@link LeaderLatch} recipe. The service also implements {@link LeaderLatchListener} to get
 * notified of changes to leadership state. Upon becoming leader, this instance is treated as the
 * active Atlas instance and calls {@link ActiveStateChangeHandler}s to activate them. Conversely,
 * on being removed from leadership, this instance is treated as a passive instance and calls
 * {@link ActiveStateChangeHandler}s to deactivate them.
 */
@Singleton
public class ActiveInstanceElectorService implements Service, LeaderLatchListener {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveInstanceElectorService.class);

    private final Configuration configuration;
    private final ServiceState serviceState;
    private final ActiveInstanceState activeInstanceState;
    private Collection<Provider<ActiveStateChangeHandler>> activeStateChangeHandlerProviders;
    private Collection<ActiveStateChangeHandler> activeStateChangeHandlers;
    private CuratorFactory curatorFactory;
    private LeaderLatch leaderLatch;
    private String serverId;

    /**
     * Create a new instance of {@link ActiveInstanceElectorService}
     * @param activeStateChangeHandlerProviders The list of registered {@link ActiveStateChangeHandler}s that
     *                                          must be called back on state changes.
     * @throws AtlasException
     */
    @Inject
    public ActiveInstanceElectorService(
            Collection<Provider<ActiveStateChangeHandler>> activeStateChangeHandlerProviders,
            CuratorFactory curatorFactory, ActiveInstanceState activeInstanceState,
            ServiceState serviceState)
            throws AtlasException {
        this(ApplicationProperties.get(), activeStateChangeHandlerProviders,
                curatorFactory, activeInstanceState, serviceState);
    }

    ActiveInstanceElectorService(Configuration configuration,
                                 Collection<Provider<ActiveStateChangeHandler>> activeStateChangeHandlerProviders,
                                 CuratorFactory curatorFactory, ActiveInstanceState activeInstanceState,
                                 ServiceState serviceState) {
        this.configuration = configuration;
        this.activeStateChangeHandlerProviders = activeStateChangeHandlerProviders;
        this.activeStateChangeHandlers = new ArrayList<>();
        this.curatorFactory = curatorFactory;
        this.activeInstanceState = activeInstanceState;
        this.serviceState = serviceState;
    }

    /**
     * Join leader election on starting up.
     *
     * If Atlas High Availability configuration is disabled, this operation is a no-op.
     * @throws AtlasException
     */
    @Override
    public void start() throws AtlasException {
        if (!HAConfiguration.isHAEnabled(configuration)) {
            LOG.info("HA is not enabled, no need to start leader election service");
            return;
        }
        cacheActiveStateChangeHandlers();
        serverId = AtlasServerIdSelector.selectServerId(configuration);
        joinElection();
    }

    private void joinElection() {
        LOG.info("Starting leader election for {}", serverId);
        leaderLatch = curatorFactory.leaderLatchInstance(serverId);
        leaderLatch.addListener(this);
        try {
            leaderLatch.start();
            LOG.info("Leader latch started for {}.", serverId);
        } catch (Exception e) {
            LOG.info("Exception while starting leader latch for {}.", serverId, e);
        }
    }

    /**
     * Leave leader election process and clean up resources on shutting down.
     *
     * If Atlas High Availability configuration is disabled, this operation is a no-op.
     * @throws AtlasException
     */
    @Override
    public void stop() {
        if (!HAConfiguration.isHAEnabled(configuration)) {
            LOG.info("HA is not enabled, no need to stop leader election service");
            return;
        }
        try {
            leaderLatch.close();
            curatorFactory.close();
        } catch (IOException e) {
            LOG.error("Error closing leader latch", e);
        }
    }

    /**
     * Call all registered {@link ActiveStateChangeHandler}s on being elected active.
     *
     * In addition, shared state information about this instance becoming active is updated
     * using {@link ActiveInstanceState}.
     */
    @Override
    public void isLeader() {
        LOG.warn("Server instance with server id {} is elected as leader", serverId);
        serviceState.becomingActive();
        try {
            for (ActiveStateChangeHandler handler : activeStateChangeHandlers) {
                handler.instanceIsActive();
            }
            activeInstanceState.update(serverId);
            serviceState.setActive();
        } catch (Exception e) {
            LOG.error("Got exception while activating", e);
            notLeader();
            rejoinElection();
        }
    }

    private void cacheActiveStateChangeHandlers() {
        if (activeStateChangeHandlers.size()==0) {
            for (Provider<ActiveStateChangeHandler> provider : activeStateChangeHandlerProviders) {
                ActiveStateChangeHandler handler = provider.get();
                activeStateChangeHandlers.add(handler);
            }
        }
    }

    private void rejoinElection() {
        try {
            leaderLatch.close();
            joinElection();
        } catch (IOException e) {
            LOG.error("Error rejoining election", e);
        }
    }

    /**
     * Call all registered {@link ActiveStateChangeHandler}s on becoming passive instance.
     */
    @Override
    public void notLeader() {
        LOG.warn("Server instance with server id {} is removed as leader", serverId);
        serviceState.becomingPassive();
        for (ActiveStateChangeHandler handler: activeStateChangeHandlers) {
            try {
                handler.instanceIsPassive();
            } catch (AtlasException e) {
                LOG.error("Error while reacting to passive state.", e);
            }
        }
        serviceState.setPassive();
    }
}
