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
package org.apache.atlas.notification.rest.web.security;

import org.apache.atlas.notification.rest.web.service.ActiveInstanceState;
import org.apache.atlas.notification.rest.web.service.ServiceState;
import org.apache.atlas.server.common.filters.spi.ActiveInstanceStateProvider;
import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtlasSecurityStateProviderConfig {
    @Bean
    public ActiveInstanceStateProvider activeInstanceStateProvider(ActiveInstanceState activeInstanceState) {
        return activeInstanceState::getActiveServerAddress;
    }

    @Bean
    public ServiceStateProvider serviceStateProvider(ServiceState serviceState) {
        return new ServiceStateProvider() {
            @Override
            public boolean isActive() {
                return serviceState.getState() == ServiceState.ServiceStateValue.ACTIVE;
            }

            @Override
            public boolean isInstanceInTransition() {
                return serviceState.isInstanceInTransition();
            }

            @Override
            public boolean isInstanceInMigration() {
                return serviceState.isInstanceInMigration();
            }

            @Override
            public String getStateName() {
                return serviceState.getState().toString();
            }
        };
    }
}
