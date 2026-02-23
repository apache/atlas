package org.apache.atlas.web.security;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.web.filters.ActiveServerFilter;
import org.apache.atlas.web.filters.AtlasAuthenticationFilter;
import org.apache.atlas.web.filters.AtlasAuthenticationEntryPoint;
import org.apache.atlas.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.web.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.web.filters.AtlasXSSPreventionFilter;
import org.apache.atlas.web.filters.StaleTransactionCleanupFilter;
import org.apache.commons.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.web.authentication.session.NullAuthenticatedSessionStrategy;
import org.springframework.security.web.authentication.session.RegisterSessionAuthenticationStrategy;
import org.springframework.security.web.authentication.session.SessionAuthenticationStrategy;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AtlasSecurityConfigTest {
    @Mock
    private AtlasKnoxSSOAuthenticationFilter ssoAuthenticationFilter;
    @Mock
    private AtlasCSRFPreventionFilter atlasCSRFPreventionFilter;
    @Mock
    private AtlasAuthenticationFilter atlasAuthenticationFilter;
    @Mock
    private AtlasAuthenticationProvider authenticationProvider;
    @Mock
    private AtlasAuthenticationSuccessHandler successHandler;
    @Mock
    private AtlasAuthenticationFailureHandler failureHandler;
    @Mock
    private AtlasAuthenticationEntryPoint atlasAuthenticationEntryPoint;
    @Mock
    private AtlasXSSPreventionFilter atlasXSSPreventionFilter;
    @Mock
    private Configuration configuration;
    @Mock
    private StaleTransactionCleanupFilter staleTransactionCleanupFilter;
    @Mock
    private ActiveServerFilter activeServerFilter;

    @Test
    public void sessionAuthenticationStrategyUsesNullStrategyWhenKeycloakIsStateless() {
        AtlasSecurityConfig securityConfig = createSecurityConfig(true, true);

        SessionAuthenticationStrategy strategy = securityConfig.sessionAuthenticationStrategy();

        assertInstanceOf(NullAuthenticatedSessionStrategy.class, strategy);
    }

    @Test
    public void sessionAuthenticationStrategyUsesRegisterStrategyWhenKeycloakIsStateful() {
        AtlasSecurityConfig securityConfig = createSecurityConfig(true, false);

        SessionAuthenticationStrategy strategy = securityConfig.sessionAuthenticationStrategy();

        assertInstanceOf(RegisterSessionAuthenticationStrategy.class, strategy);
    }

    @Test
    public void sessionAuthenticationStrategyUsesRegisterStrategyWhenKeycloakIsDisabled() {
        AtlasSecurityConfig securityConfig = createSecurityConfig(false, true);

        SessionAuthenticationStrategy strategy = securityConfig.sessionAuthenticationStrategy();

        assertInstanceOf(RegisterSessionAuthenticationStrategy.class, strategy);
    }

    private AtlasSecurityConfig createSecurityConfig(boolean keycloakEnabled, boolean keycloakStatelessEnabled) {
        when(configuration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false))
                .thenReturn(keycloakEnabled);
        when(configuration.getBoolean(
                AtlasConfiguration.KEYCLOAK_STATELESS_SESSION_ENABLED.getPropertyName(),
                AtlasConfiguration.KEYCLOAK_STATELESS_SESSION_ENABLED.getBoolean()))
                .thenReturn(keycloakStatelessEnabled);

        return new AtlasSecurityConfig(ssoAuthenticationFilter,
                atlasCSRFPreventionFilter,
                atlasAuthenticationFilter,
                authenticationProvider,
                successHandler,
                failureHandler,
                atlasAuthenticationEntryPoint,
                atlasXSSPreventionFilter,
                configuration,
                staleTransactionCleanupFilter,
                activeServerFilter);
    }
}
