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

import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.atlas.util.PropertiesUtil;
import org.apache.atlas.web.model.User;
import org.apache.log4j.Logger;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;
import org.springframework.stereotype.Component;

@Component
public class AtlasLdapAuthenticationProvider extends
        AtlasAbstractAuthenticationProvider {
    private static Logger LOG = Logger
            .getLogger(AtlasLdapAuthenticationProvider.class);

    private String ldapURL;
    private String ldapUserDNPattern;
    private String ldapGroupSearchBase;
    private String ldapGroupSearchFilter;
    private String ldapGroupRoleAttribute;
    private String ldapBindDN;
    private String ldapBindPassword;
    private String ldapDefaultRole;
    private String ldapUserSearchFilter;
    private String ldapReferral;
    private String ldapBase;

    @PostConstruct
    public void setup() {
        setLdapProperties();
    }

    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        try {
            return getLdapBindAuthentication(authentication);
        } catch (Exception e) {
            throw new AtlasAuthenticationException(e.getMessage(), e.getCause());
        }
    }

    private Authentication getLdapBindAuthentication(
            Authentication authentication) throws Exception {
        try {
            String userName = authentication.getName();
            String userPassword = "";
            if (authentication.getCredentials() != null) {
                userPassword = authentication.getCredentials().toString();
            }

            LdapContextSource ldapContextSource = getLdapContextSource();

            DefaultLdapAuthoritiesPopulator defaultLdapAuthoritiesPopulator = getDefaultLdapAuthoritiesPopulator(ldapContextSource);

            if (ldapUserSearchFilter == null
                    || ldapUserSearchFilter.trim().isEmpty()) {
                ldapUserSearchFilter = "(uid={0})";
            }

            FilterBasedLdapUserSearch userSearch = new FilterBasedLdapUserSearch(
                    ldapBase, ldapUserSearchFilter, ldapContextSource);
            userSearch.setSearchSubtree(true);

            BindAuthenticator bindAuthenticator = getBindAuthenticator(
                    userSearch, ldapContextSource);

            LdapAuthenticationProvider ldapAuthenticationProvider = new LdapAuthenticationProvider(
                    bindAuthenticator, defaultLdapAuthoritiesPopulator);

            if (userName != null && userPassword != null
                    && !userName.trim().isEmpty()
                    && !userPassword.trim().isEmpty()) {
                final List<GrantedAuthority> grantedAuths = getAuthorities(userName);
                final UserDetails principal = new User(userName, userPassword,
                        grantedAuths);
                final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(
                        principal, userPassword, grantedAuths);
                authentication = ldapAuthenticationProvider.authenticate(finalAuthentication);
                return authentication;
            } else {
                throw new AtlasAuthenticationException(
                        "LDAP Authentication::userName or userPassword is null or empty for userName "
                                + userName);
            }
        } catch (Exception e) {
            LOG.error("LDAP Authentication Failed:", e);
            throw new AtlasAuthenticationException(
                    "LDAP Authentication Failed", e);
        }
    }

    private void setLdapProperties() {
        ldapURL = PropertiesUtil.getProperty("atlas.ldap.url", ldapURL);
        ldapUserDNPattern = PropertiesUtil.getProperty(
                "atlas.ldap.user.dnpattern", ldapUserDNPattern);
        ldapGroupSearchBase = PropertiesUtil.getProperty(
                "atlas.ldap.group.searchbase", ldapGroupSearchBase);
        ldapGroupSearchFilter = PropertiesUtil.getProperty(
                "atlas.ldap.group.searchfilter", ldapGroupSearchFilter);
        ldapGroupRoleAttribute = PropertiesUtil.getProperty(
                "atlas.ldap.group.roleattribute", ldapGroupRoleAttribute);
        ldapBindDN = PropertiesUtil.getProperty("atlas.ldap.bind.dn",
                ldapBindDN);
        ldapBindPassword = PropertiesUtil.getProperty(
                "atlas.ldap.bind.password", ldapBindDN);
        ldapDefaultRole = PropertiesUtil.getProperty("atlas.ldap.default.role",
                ldapDefaultRole);
        ldapUserSearchFilter = PropertiesUtil.getProperty(
                "atlas.ldap.user.searchfilter", ldapUserSearchFilter);
        ldapReferral = PropertiesUtil.getProperty("atlas.ldap.referral",
                ldapReferral);
        ldapBase = PropertiesUtil.getProperty("atlas.ldap.base.dn", ldapBase);
    }

    private LdapContextSource getLdapContextSource() throws Exception {
        LdapContextSource ldapContextSource = new DefaultSpringSecurityContextSource(
                ldapURL);
        ldapContextSource.setUserDn(ldapBindDN);
        ldapContextSource.setPassword(ldapBindPassword);
        ldapContextSource.setReferral(ldapReferral);
        ldapContextSource.setCacheEnvironmentProperties(false);
        ldapContextSource.setAnonymousReadOnly(false);
        ldapContextSource.setPooled(true);
        ldapContextSource.afterPropertiesSet();
        return ldapContextSource;
    }

    private DefaultLdapAuthoritiesPopulator getDefaultLdapAuthoritiesPopulator(
            LdapContextSource ldapContextSource) {
        DefaultLdapAuthoritiesPopulator defaultLdapAuthoritiesPopulator = new DefaultLdapAuthoritiesPopulator(
                ldapContextSource, ldapGroupSearchBase);
        defaultLdapAuthoritiesPopulator
                .setGroupRoleAttribute(ldapGroupRoleAttribute);
        defaultLdapAuthoritiesPopulator
                .setGroupSearchFilter(ldapGroupSearchFilter);
        defaultLdapAuthoritiesPopulator.setIgnorePartialResultException(true);
        return defaultLdapAuthoritiesPopulator;
    }

    private BindAuthenticator getBindAuthenticator(
            FilterBasedLdapUserSearch userSearch,
            LdapContextSource ldapContextSource) throws Exception {
        BindAuthenticator bindAuthenticator = new BindAuthenticator(
                ldapContextSource);
        bindAuthenticator.setUserSearch(userSearch);
        String[] userDnPatterns = new String[] { ldapUserDNPattern };
        bindAuthenticator.setUserDnPatterns(userDnPatterns);
        bindAuthenticator.afterPropertiesSet();
        return bindAuthenticator;
    }
}
