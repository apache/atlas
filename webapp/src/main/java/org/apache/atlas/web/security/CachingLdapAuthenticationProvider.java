package org.apache.atlas.web.security;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.LdapAuthenticator;
import org.springframework.security.ldap.userdetails.LdapAuthoritiesPopulator;

public class CachingLdapAuthenticationProvider extends
        LdapAuthenticationProvider {

    private static Logger LOG = LoggerFactory.getLogger(AtlasLdapAuthenticationProvider.class);
    private long timeToRefreshAuthentication = TimeUnit.MINUTES.toMillis(30);

    public CachingLdapAuthenticationProvider(LdapAuthenticator authenticator) {
        super(authenticator);
    }

    public CachingLdapAuthenticationProvider(LdapAuthenticator authenticator,
                                             LdapAuthoritiesPopulator authoritiesPopulator) {
        super(authenticator, authoritiesPopulator);
    }

    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        String userName = authentication.getName();
        Authentication result = AtlasLdapAuthenticationProvider.cachedAuthentications.get(userName);
        Date lastRefresh = AtlasLdapAuthenticationProvider.cachedWhen.get(userName);
        if (result != null
                && (System.currentTimeMillis() - lastRefresh.getTime()) < getTimeToRefreshAuthentication()) {
            LOG.debug("Cache hit for " + userName + " last refresh "
                    + lastRefresh);
            return result;
        }

        LOG.debug((lastRefresh == null ? "Initial authentication for "
                + userName : "Refresh authentication for " + userName));

        result = super.authenticate(authentication);
        AtlasLdapAuthenticationProvider.cachedAuthentications.put(userName, result);
        AtlasLdapAuthenticationProvider.cachedWhen.put(userName, new Date());
        return result;
    }

    public void setTimeToRefreshAuthentication(long timeToRefreshAuthentication) {
        this.timeToRefreshAuthentication = timeToRefreshAuthentication;
    }

    public long getTimeToRefreshAuthentication() {
        return timeToRefreshAuthentication;
    }

}