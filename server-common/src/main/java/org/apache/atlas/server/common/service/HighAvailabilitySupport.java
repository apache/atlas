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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.atlas.server.common.service;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.AtlasServerIdSelector;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * Interface to abstract High Availability (HA) configuration retrieval.
 * Enables shared services in 'server-common' to operate without direct
 * dependencies on application-specific configuration classes.
 */
public interface HighAvailabilitySupport {

    /**
     * Determines if HA mode is active based on the provided configuration.
     */
    boolean isHAEnabled(Configuration configuration);

    /**
     * Resolves the unique ID for the current server instance.
     * @throws AtlasException if the server ID cannot be resolved.
     */
    String selectServerId(Configuration configuration) throws AtlasException;

    /**
     * Retrieves the network address bound to a specific server ID.
     */
    String getBoundAddressForId(Configuration configuration, String serverId);

    /**
     * Extracts ZooKeeper connection and properties required.
     */
    HighAvailabilityProperties getZookeeperProperties(Configuration configuration);

    /**
     * {@link HAConfiguration}-backed implementation for unit tests and call sites
     * that need a default without Spring (same contract as the webapp
     * {@code @Component} adapter).
     */
    final class AtlasConfigurationDefaults implements HighAvailabilitySupport {
        @Override
        public boolean isHAEnabled(Configuration configuration) {
            return HAConfiguration.isHAEnabled(configuration);
        }

        @Override
        public String selectServerId(Configuration configuration) throws AtlasException {
            return AtlasServerIdSelector.selectServerId(configuration);
        }

        @Override
        public String getBoundAddressForId(Configuration configuration, String serverId) {
            return HAConfiguration.getBoundAddressForId(configuration, serverId);
        }

        @Override
        public HighAvailabilityProperties getZookeeperProperties(Configuration configuration) {
            HAConfiguration.ZookeeperProperties p = HAConfiguration.getZookeeperProperties(configuration);

            return new HighAvailabilityProperties(
                    p.getConnectString(),
                    p.getZkRoot(),
                    p.getRetriesSleepTimeMillis(),
                    p.getNumRetries(),
                    p.getSessionTimeout(),
                    p.getAcl(),
                    p.getAuth());
        }
    }
}
