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
package org.apache.atlas.web.security;

import org.apache.atlas.server.common.filters.ActiveServerFilter;
import org.apache.atlas.server.common.filters.AtlasAuthenticationFilter;
import org.apache.atlas.server.common.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.server.common.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.server.common.filters.spi.ActiveInstanceStateProvider;
import org.apache.atlas.server.common.filters.spi.AtlasAuthenticationProviderBridge;
import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtlasSecurityFilterBeansConfig {
    @Bean
    public AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge(AtlasAuthenticationProvider authenticationProvider) {
        return new AtlasAuthenticationProviderBridge() {
            @Override
            public java.util.List<org.springframework.security.core.GrantedAuthority> getAuthoritiesFromUGI(String userName) {
                return AtlasAuthenticationProvider.getAuthoritiesFromUGI(userName);
            }

            @Override
            public void setSsoEnabled(boolean enabled) {
                authenticationProvider.setSsoEnabled(enabled);
            }

            @Override
            public org.springframework.security.core.Authentication authenticate(org.springframework.security.core.Authentication authentication) {
                return authenticationProvider.authenticate(authentication);
            }
        };
    }

    @Bean
    public ActiveServerFilter commonActiveServerFilter(ActiveInstanceStateProvider activeInstanceStateProvider, ServiceStateProvider serviceStateProvider) {
        return new ActiveServerFilter(activeInstanceStateProvider, serviceStateProvider);
    }

    @Bean
    public AtlasAuthenticationFilter commonAtlasAuthenticationFilter(AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge) {
        return new AtlasAuthenticationFilter(atlasAuthenticationProviderBridge);
    }

    @Bean
    public AtlasKnoxSSOAuthenticationFilter commonAtlasKnoxSSOAuthenticationFilter(AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge) {
        return new AtlasKnoxSSOAuthenticationFilter(atlasAuthenticationProviderBridge);
    }

    @Bean
    public AtlasCSRFPreventionFilter commonAtlasCSRFPreventionFilter() {
        return new AtlasCSRFPreventionFilter();
    }
}
