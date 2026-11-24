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

import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;

/**
 * A class that maintains the state of this instance.
 *
 * The states are maintained at a granular level, including in-transition states. The transitions are
 * directed by {@link ActiveInstanceElectorService}.
 */
@Singleton
@Component
public class ServiceState implements ServiceStateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceState.class);

    public enum ServiceStateValue {
        ACTIVE,
        PASSIVE,
        BECOMING_ACTIVE,
        BECOMING_PASSIVE,
        MIGRATING
    }

    private Configuration configuration;
    private volatile ServiceStateValue state;
    private final HighAvailability highAvailability;

    @Inject
    public ServiceState(Configuration configuration, HighAvailability highAvailability) {
        this.configuration    = configuration;
        this.highAvailability = highAvailability;

        state = !highAvailability.isHAEnabled(configuration) ? ServiceStateValue.ACTIVE : ServiceStateValue.PASSIVE;

        if (!StringUtils.isEmpty(configuration.getString(ATLAS_MIGRATION_MODE_FILENAME, ""))) {
            state = ServiceStateValue.MIGRATING;
        }
    }

    public ServiceStateValue getState() {
        return state;
    }

    public void becomingActive() {
        LOG.warn("Instance becoming active from {}", state);
        setState(ServiceStateValue.BECOMING_ACTIVE);
    }

    public void setActive() {
        LOG.warn("Instance is active from {}", state);
        setState(ServiceStateValue.ACTIVE);
    }

    public void becomingPassive() {
        LOG.warn("Instance becoming passive from {}", state);
        setState(ServiceStateValue.BECOMING_PASSIVE);
    }

    public void setPassive() {
        LOG.warn("Instance is passive from {}", state);
        setState(ServiceStateValue.PASSIVE);
    }

    @Override
    public boolean isInstanceInTransition() {
        ServiceStateValue state = getState();
        return state == ServiceStateValue.BECOMING_ACTIVE
                || state == ServiceStateValue.BECOMING_PASSIVE;
    }

    public void setMigration() {
        LOG.warn("Instance in {}", state);
        setState(ServiceStateValue.MIGRATING);
    }

    @Override
    public boolean isInstanceInMigration() {
        return getState() == ServiceStateValue.MIGRATING;
    }

    @Override
    public boolean isActive() {
        return getState() == ServiceStateValue.ACTIVE;
    }

    @Override
    public String getStateName() {
        return getState().toString();
    }

    private void setState(ServiceStateValue newState) {
        checkState(highAvailability.isHAEnabled(configuration),
                "Cannot change state as requested, as HA is not enabled for this instance.");

        state = newState;
    }
}
