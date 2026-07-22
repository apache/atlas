/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.security;

import org.apache.atlas.server.common.filters.spi.AtlasAuthenticationProviderBridge;
import org.apache.atlas.server.common.security.AtlasAuthenticationProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.List;

/**
 * Loads {@code test-spring-security.xml} for tests (e.g. {@link FileAuthenticationTest}) and
 * registers {@link AtlasAuthenticationProviderBridge} for Knox/ Kerberos filters declared in XML.
 * XML alone cannot express the bridge; production uses {@link AtlasSecurityConfig} instead.
 * <p>
 * Profile-gated so classpath scanning for embedded-server / full webapp tests does not import
 * {@code test-spring-security.xml} alongside {@link AtlasSecurityConfig} (that overlap creates a
 * circular dependency between the Knox SSO filter and {@code atlasAuthenticationProviderBridge}).
 */
@Configuration
@Profile("testSpringSecurityBridge")
@ImportResource("classpath:test-spring-security.xml")
public class TestSpringSecurityBridgeConfig {
    @Bean
    public AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge(AtlasAuthenticationProvider authenticationProvider) {
        return new AtlasAuthenticationProviderBridge() {
            @Override
            public List<GrantedAuthority> getAuthoritiesFromUGI(String userName) {
                return AtlasAuthenticationProvider.getAuthoritiesFromUGI(userName);
            }

            @Override
            public void setSsoEnabled(boolean enabled) {
                authenticationProvider.setSsoEnabled(enabled);
            }

            @Override
            public Authentication authenticate(Authentication authentication) {
                return authenticationProvider.authenticate(authentication);
            }
        };
    }
}
