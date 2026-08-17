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

package org.apache.atlas.server.common.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;

/**
 * Tracks the lifecycle state of this Atlas node.
 *
 * <p>In active-active peer mode the only runtime states are:
 * <ul>
 *   <li>{@link ServiceStateValue#BECOMING_ACTIVE} — node is starting up, not yet ready</li>
 *   <li>{@link ServiceStateValue#ACTIVE}          — node is fully active and serving requests</li>
 *   <li>{@link ServiceStateValue#MIGRATING}       — node is running a data migration</li>
 * </ul>
 *
 * <p>There are no leader, follower, or passive states.
 * Transitions are directed by {@code AtlasActivationService}.
 */
@Singleton
@Component
public class ServiceState implements ServiceStateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceState.class);

    private volatile ServiceStateValue state;

    public ServiceState() throws AtlasException {
        this(ApplicationProperties.get());
    }

    @Inject
    public ServiceState(Configuration configuration) {
        if (!StringUtils.isEmpty(configuration.getString(ATLAS_MIGRATION_MODE_FILENAME, ""))) {
            state = ServiceStateValue.MIGRATING;
            LOG.info("ServiceState: migration mode detected — initial state is MIGRATING");
        } else {
            state = ServiceStateValue.BECOMING_ACTIVE;
            LOG.info("ServiceState: initial state is BECOMING_ACTIVE");
        }
    }

    public ServiceStateValue getState() {
        return state;
    }

    public void becomingActive() {
        LOG.info("ServiceState: transitioning to BECOMING_ACTIVE from {}", state);
        state = ServiceStateValue.BECOMING_ACTIVE;
    }

    public void setActive() {
        LOG.info("ServiceState: transitioning to ACTIVE from {}", state);
        state = ServiceStateValue.ACTIVE;
    }

    public void setMigration() {
        LOG.info("ServiceState: transitioning to MIGRATING from {}", state);
        state = ServiceStateValue.MIGRATING;
    }

    @Override
    public boolean isActive() {
        return state == ServiceStateValue.ACTIVE;
    }

    @Override
    public boolean isInstanceInTransition() {
        return state == ServiceStateValue.BECOMING_ACTIVE;
    }

    @Override
    public boolean isInstanceInMigration() {
        return state == ServiceStateValue.MIGRATING;
    }

    @Override
    public String getStateName() {
        return state.toString();
    }

    public enum ServiceStateValue {
        /** Node is starting up — activation handlers are being called. */
        BECOMING_ACTIVE,
        /** Node is fully active and serving requests. */
        ACTIVE,
        /** Node is running a data migration. */
        MIGRATING
    }
}
