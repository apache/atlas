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
package org.apache.atlas.server.common.service;

import org.apache.atlas.AtlasException;
import org.apache.commons.configuration2.Configuration;

/**
 * Interface to abstract High Availability (HA) configuration retrieval.
 * Enables shared services in 'server-common' to operate without direct
 * dependencies on application-specific configuration classes.
 */
public interface HighAvailability {

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
}
