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
package org.apache.atlas.web.security;

import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;

@Component
public class AtlasAuthenticationProvider extends
        AtlasAbstractAuthenticationProvider {
    private static final Logger LOG = LoggerFactory
            .getLogger(AtlasAuthenticationProvider.class);

    private String atlasAuthenticationMethod = "UNKNOWN";

    enum AUTH_METHOD {
        FILE, LDAP, AD
    };

    @Autowired
    AtlasLdapAuthenticationProvider ldapAuthenticationProvider;

    @Autowired
    AtlasFileAuthenticationProvider fileAuthenticationProvider;

    @Autowired
    AtlasADAuthenticationProvider adAuthenticationProvider;

    @PostConstruct
    void setAuthenticationMethod() {
        try {
            Configuration configuration = ApplicationProperties.get();
            this.atlasAuthenticationMethod = configuration.getString(
                    "atlas.login.method", "UNKNOWN");
        } catch (Exception e) {
            LOG.error(
                    "Error while getting atlas.login.method application properties",
                    e);
        }
    }

    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {

        if (atlasAuthenticationMethod.equalsIgnoreCase(AUTH_METHOD.FILE.name())) {
            authentication = fileAuthenticationProvider
                    .authenticate(authentication);
        } else if (atlasAuthenticationMethod.equalsIgnoreCase(AUTH_METHOD.LDAP
                .name())) {
            authentication = ldapAuthenticationProvider
                    .authenticate(authentication);
        } else if (atlasAuthenticationMethod.equalsIgnoreCase(AUTH_METHOD.AD
                .name())) {
            authentication = adAuthenticationProvider
                    .authenticate(authentication);
        } else {
            LOG.error("Invalid authentication method :"
                    + atlasAuthenticationMethod);
        }

        if (authentication != null && authentication.isAuthenticated()) {
            return authentication;
        } else {
            LOG.error("Authentication failed.");
            throw new AtlasAuthenticationException("Authentication failed.");
        }
    }

  
}
