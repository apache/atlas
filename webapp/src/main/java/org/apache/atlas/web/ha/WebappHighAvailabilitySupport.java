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
package org.apache.atlas.web.ha;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.AtlasServerIdSelector;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.server.common.service.HighAvailabilityProperties;
import org.apache.atlas.server.common.service.HighAvailabilitySupport;
import org.apache.commons.configuration.Configuration;
import org.springframework.stereotype.Component;

/**
 * Webapp implementation of {@link HighAvailabilitySupport} using {@link HAConfiguration}.
 */
@Component
public class WebappHighAvailabilitySupport implements HighAvailabilitySupport {
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
        HAConfiguration.ZookeeperProperties props = HAConfiguration.getZookeeperProperties(configuration);

        return new HighAvailabilityProperties(
                props.getConnectString(),
                props.getZkRoot(),
                props.getRetriesSleepTimeMillis(),
                props.getNumRetries(),
                props.getSessionTimeout(),
                props.getAcl(),
                props.getAuth());
    }
}
