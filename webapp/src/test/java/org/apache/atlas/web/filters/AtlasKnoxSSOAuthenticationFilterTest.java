package org.apache.atlas.web.filters;

import org.apache.atlas.web.security.AtlasAuthenticationProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AtlasKnoxSSOAuthenticationFilterTest {
    @Mock
    private AtlasAuthenticationProvider authenticationProvider;
    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;
    @Mock
    private FilterChain filterChain;

    @BeforeEach
    public void setUp() {
        SecurityContextHolder.clearContext();
    }

    @AfterEach
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void doFilterDoesNotCreateSessionForLocalLoginBypassCheck() throws Exception {
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, null);
        ReflectionTestUtils.setField(filter, "ssoEnabled", true);

        when(request.getSession(false)).thenReturn(null);
        when(request.getRequestURI()).thenReturn("/api/atlas/v2/search/basic");

        filter.doFilter(request, response, filterChain);

        verify(request).getSession(false);
        verify(request, never()).getSession();
        verify(filterChain).doFilter(request, response);
    }
}
