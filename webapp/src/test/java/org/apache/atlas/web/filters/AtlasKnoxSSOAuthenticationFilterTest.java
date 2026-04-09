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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.atlas.web.filters;

import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.web.security.AtlasAuthenticationProvider;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.cert.CertificateException;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasKnoxSSOAuthenticationFilterTest {
    @Mock
    private HttpServletRequest servletRequest;

    @Mock
    private HttpServletResponse servletResponse;

    @Mock
    private FilterChain filterChain;

    @Mock
    private AtlasAuthenticationProvider authenticationProvider;

    @Mock
    private Configuration configuration;

    @Mock
    private SignedJWT signedJWT;

    @Mock
    private JWTClaimsSet jwtClaimsSet;

    @Mock
    private HttpSession httpSession;

    @Mock
    private SecurityContext securityContext;

    @Mock
    private Authentication existingAuth;

    @Mock
    private RSAPublicKey rsaPublicKey;

    @Mock
    private JWSVerifier jwsVerifier;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void tearDown() {
        // Reset SecurityContext after each test
        SecurityContextHolder.clearContext();
    }

    // Helper method to use reflection to set private fields
    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    // Helper method to get private fields using reflection
    private Object getPrivateField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    // Helper method to call private methods using reflection
    private Object callPrivateMethod(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    @Test
    public void testDoFilterSSODisabled() throws IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class)) {
            // Mock configuration to disable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(false);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

            filter.doFilter(servletRequest, servletResponse, filterChain);

            verify(filterChain).doFilter(servletRequest, servletResponse);
        }
    }

    @Test
    public void testDoFilterLocalLogin() throws IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class)) {
            // Mock configuration to enable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL))
                    .thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY))
                    .thenReturn("mockPublicKey");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT))
                    .thenReturn(new String[] {"Mozilla"});

            // Mock session with locallogin attribute
            when(servletRequest.getSession()).thenReturn(httpSession);
            when(httpSession.getAttribute("locallogin")).thenReturn("true");

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

            filter.doFilter(servletRequest, servletResponse, filterChain);

            verify(servletRequest).setAttribute("ssoEnabled", false);
            verify(filterChain).doFilter(servletRequest, servletResponse);
            verifyNoInteractions(authenticationProvider);
        }
    }

    @Test
    public void testDoFilterAlreadyAuthenticated() throws IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class);
                MockedStatic<SecurityContextHolder> mockedSecurityHolder = mockStatic(SecurityContextHolder.class)) {
            // Mock configuration to enable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY)).thenReturn("mockPublicKey");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(new String[] {"Mozilla"});

            // Mock existing authentication (not SSO authentication)
            mockedSecurityHolder.when(SecurityContextHolder::getContext).thenReturn(securityContext);
            when(securityContext.getAuthentication()).thenReturn(existingAuth);
            when(existingAuth.isAuthenticated()).thenReturn(true);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

            filter.doFilter(servletRequest, servletResponse, filterChain);

            verify(filterChain).doFilter(servletRequest, servletResponse);
            verifyNoInteractions(authenticationProvider);
        }
    }

    @Test
    public void testDoFilterInvalidJWT() throws IOException, ServletException, ParseException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class);
                MockedStatic<SignedJWT> mockedSignedJWT = mockStatic(SignedJWT.class);
                MockedStatic<SecurityContextHolder> mockedSecurityHolder = mockStatic(SecurityContextHolder.class)) {
            // Mock configuration to enable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY)).thenReturn("mockPublicKey");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(new String[] {"Mozilla"});
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME, AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME_DEFAULT)).thenReturn("hadoop-jwt");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM, AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT)).thenReturn("originalUrl");

            // Mock request with invalid JWT cookie
            when(servletRequest.getCookies()).thenReturn(new Cookie[] {new Cookie("hadoop-jwt", "invalid.jwt.token")});
            when(servletRequest.getHeader("User-Agent")).thenReturn("Mozilla/5.0");
            when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost/atlas"));
            when(servletRequest.getScheme()).thenReturn("http");
            when(servletRequest.getServerName()).thenReturn("localhost");
            when(servletRequest.getServerPort()).thenReturn(8080);
            when(servletRequest.getContextPath()).thenReturn("/atlas");
            when(servletRequest.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));

            // Mock JWT parsing failure
            mockedSignedJWT.when(() -> SignedJWT.parse("invalid.jwt.token")).thenThrow(new ParseException("Invalid JWT", 0));

            // Mock no existing authentication
            mockedSecurityHolder.when(SecurityContextHolder::getContext).thenReturn(securityContext);
            when(securityContext.getAuthentication()).thenReturn(null);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
            filter.doFilter(servletRequest, servletResponse, filterChain);

            verify(servletResponse).sendRedirect(contains("https://knox.sso?originalUrl="));
            verifyNoInteractions(authenticationProvider);
        }
    }

    @Test
    public void testDoFilterXMLHttpRequestNoJWT() throws IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class);
                MockedStatic<SecurityContextHolder> mockedSecurityHolder = mockStatic(SecurityContextHolder.class)) {
            // Mock configuration to enable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY)).thenReturn("mockPublicKey");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(new String[] {"Mozilla"});
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME, AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME_DEFAULT)).thenReturn("hadoop-jwt");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM, AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT)).thenReturn("originalUrl");

            // Mock request with no JWT and XMLHttpRequest header
            when(servletRequest.getCookies()).thenReturn(null);
            when(servletRequest.getHeader("User-Agent")).thenReturn("Mozilla/5.0");
            when(servletRequest.getHeader("X-Requested-With")).thenReturn("XMLHttpRequest");
            when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost/atlas"));
            when(servletRequest.getScheme()).thenReturn("http");
            when(servletRequest.getServerName()).thenReturn("localhost");
            when(servletRequest.getServerPort()).thenReturn(8080);
            when(servletRequest.getContextPath()).thenReturn("/atlas");
            when(servletRequest.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));

            // Mock no existing authentication
            mockedSecurityHolder.when(SecurityContextHolder::getContext).thenReturn(securityContext);
            when(securityContext.getAuthentication()).thenReturn(null);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
            filter.doFilter(servletRequest, servletResponse, filterChain);

            verify(servletResponse).setContentType("application/json");
            verify(servletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            verify(servletResponse).sendError(eq(HttpServletResponse.SC_UNAUTHORIZED), contains("knoxssoredirectURL"));
        }
    }

    @Test
    public void testLoadJwtPropertiesNoProviderUrl() {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class)) {
            // Mock configuration with no provider URL
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn(null);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
            SSOAuthenticationProperties properties = filter.loadJwtProperties();

            assertNull(properties);
        }
    }

    @Test
    public void testLoadJwtPropertiesValid() throws CertificateException, IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class)) {
            // Mock configuration with valid properties
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY)).thenReturn("mockPublicKey");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME, AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME_DEFAULT)).thenReturn("custom-jwt");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM, AtlasKnoxSSOAuthenticationFilter.JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT)).thenReturn("customUrlParam");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(new String[] {"Chrome", "Firefox"});

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
            SSOAuthenticationProperties properties = filter.loadJwtProperties();

            assertEquals("https://knox.sso", properties.getAuthenticationProviderUrl());
            assertEquals("custom-jwt", properties.getCookieName());
            assertEquals("customUrlParam", properties.getOriginalUrlQueryParam());
            assertTrue(Arrays.asList(properties.getUserAgentList()).contains("Chrome"));
        }
    }

    @Test
    public void testGetJWTFromCookie() {
        when(servletRequest.getCookies()).thenReturn(new Cookie[] {new Cookie("hadoop-jwt", "test.jwt.token")});

        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
        String jwt = filter.getJWTFromCookie(servletRequest);

        assertEquals("test.jwt.token", jwt);
    }

    @Test
    public void testGetJWTFromCookieNoCookies() {
        when(servletRequest.getCookies()).thenReturn(null);

        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
        String jwt = filter.getJWTFromCookie(servletRequest);

        assertNull(jwt);
    }

    @Test
    public void testValidateTokenValid() throws Exception {
        when(signedJWT.getState()).thenReturn(JWSObject.State.SIGNED);
        when(signedJWT.getSignature()).thenReturn(mock(com.nimbusds.jose.util.Base64URL.class));
        when(signedJWT.verify(any())).thenReturn(true);
        when(signedJWT.getJWTClaimsSet()).thenReturn(jwtClaimsSet);
        when(jwtClaimsSet.getExpirationTime()).thenReturn(new Date(System.currentTimeMillis() + 10000));

        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setPublicKey(rsaPublicKey);
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        // Use reflection to set verifier field
        setPrivateField(filter, "verifier", jwsVerifier);
        when(jwsVerifier.verify(any(), any(), any())).thenReturn(true);

        boolean valid = filter.validateToken(signedJWT);

        assertTrue(valid);
    }

    @Test
    public void testValidateTokenInvalidSignature() throws Exception {
        when(signedJWT.getState()).thenReturn(JWSObject.State.SIGNED);
        when(signedJWT.getSignature()).thenReturn(mock(com.nimbusds.jose.util.Base64URL.class));
        when(signedJWT.verify(any())).thenReturn(false);

        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setPublicKey(rsaPublicKey);
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        // Use reflection to set verifier field
        setPrivateField(filter, "verifier", jwsVerifier);

        boolean valid = filter.validateToken(signedJWT);

        assertFalse(valid);
    }

    @Test
    public void testValidateTokenExpired() throws Exception {
        when(signedJWT.getState()).thenReturn(JWSObject.State.SIGNED);
        when(signedJWT.getSignature()).thenReturn(mock(com.nimbusds.jose.util.Base64URL.class));
        when(signedJWT.verify(any())).thenReturn(true);
        when(signedJWT.getJWTClaimsSet()).thenReturn(jwtClaimsSet);
        when(jwtClaimsSet.getExpirationTime()).thenReturn(new Date(System.currentTimeMillis() - 10000));

        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setPublicKey(rsaPublicKey);
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        // Use reflection to set verifier field
        setPrivateField(filter, "verifier", jwsVerifier);

        boolean valid = filter.validateToken(signedJWT);

        assertFalse(valid);
    }

    @Test
    public void testIsWebUserAgentUsingReflection() throws Exception {
        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setUserAgentList(new String[] {"Mozilla", "Chrome"});
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        // Use reflection to call private isWebUserAgent method
        Boolean result = (Boolean) callPrivateMethod(filter, "isWebUserAgent", new Class[] {String.class}, "Mozilla/5.0");
        assertTrue(result);

        result = (Boolean) callPrivateMethod(filter, "isWebUserAgent", new Class[] {String.class}, "Postman");
        assertFalse(result);
    }

    @Test
    public void testConstructLoginURLNonXML() throws Exception {
        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setAuthenticationProviderUrl("https://knox.sso");
        properties.setOriginalUrlQueryParam("customUrlParam");
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost/atlas"));
        when(servletRequest.getQueryString()).thenReturn("param=value");
        when(servletRequest.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));

        String url = filter.constructLoginURL(servletRequest, false);
        assertEquals("https://knox.sso?customUrlParam=http://localhost/atlas?param=value", url);
    }

    @Test
    public void testConstructLoginURLXMLWithReferer() throws Exception {
        SSOAuthenticationProperties properties = new SSOAuthenticationProperties();
        properties.setAuthenticationProviderUrl("https://knox.sso");
        properties.setOriginalUrlQueryParam("customUrlParam");
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider, properties);

        when(servletRequest.getHeader("referer")).thenReturn("http://localhost/referer");
        when(servletRequest.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));

        String url = filter.constructLoginURL(servletRequest, true);
        assertEquals("https://knox.sso?customUrlParam=http://localhost/referer", url);
    }

    @Test
    public void testParseXForwardHeaderUsingReflection() throws Exception {
        when(servletRequest.getHeaderNames()).thenReturn(Collections.enumeration(Arrays.asList("x-forwarded-proto", "x-forwarded-host", "x-forwarded-context")));
        when(servletRequest.getHeaders("x-forwarded-proto")).thenReturn(Collections.enumeration(Collections.singletonList("https")));
        when(servletRequest.getHeaders("x-forwarded-host")).thenReturn(Collections.enumeration(Collections.singletonList("proxy.host")));
        when(servletRequest.getHeaders("x-forwarded-context")).thenReturn(Collections.enumeration(Collections.singletonList("/context")));

        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

        // Use reflection to call private parseXForwardHeader method
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) callPrivateMethod(filter, "parseXForwardHeader",
                new Class[] {HttpServletRequest.class}, servletRequest);

        assertNotNull(headers);
        assertEquals("https", headers.get("x-forwarded-proto"));
        assertEquals("proxy.host", headers.get("x-forwarded-host"));
        assertEquals("/context", headers.get("x-forwarded-context"));
    }

    @Test
    public void testConstructForwardableURLUsingReflection() throws Exception {
        Map<String, String> xFwdHeaderMap = new HashMap<>();
        xFwdHeaderMap.put("x-forwarded-proto", "https");
        xFwdHeaderMap.put("x-forwarded-host", "proxy.host");
        xFwdHeaderMap.put("x-forwarded-context", "/context");

        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

        // Use reflection to call private constructForwardableURL method
        String url = (String) callPrivateMethod(filter, "constructForwardableURL",
                new Class[] {Map.class, String.class}, xFwdHeaderMap, "/test");

        assertEquals("https://proxy.host/context/atlas/test", url);
    }

    @Test
    public void testSafeAppend() throws Exception {
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
        StringBuilder sb = new StringBuilder();

        // Use reflection to call private safeAppend method
        callPrivateMethod(filter, "safeAppend", new Class[] {StringBuilder.class, String[].class}, sb, new String[] {"test", "example"});

        assertEquals("testexample", sb.toString());
    }

    @Test
    public void testInit() throws ServletException {
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
        FilterConfig filterConfig = mock(FilterConfig.class);

        // This should not throw any exception
        filter.init(filterConfig);
    }

    @Test
    public void testDestroy() {
        AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);

        // This should not throw any exception
        filter.destroy();
    }

    @Test
    public void testParseRSAPublicKey() throws Exception {
        String pemKey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuGbXWiK3dQTyCbX5xdE4yCuYp9eiCO34+0zMACLKcmRhL2xNkAC2+2QQKJm5Dk1pj2DFRQ9xEJJmAT1rPnKl3N0mGUfX9mVLNl5GZ5FqzQTc6B5TtQK9B2g3QJ9H5BsYnfRhvJlRdVLKj0JZ3b2Ldy4j8r6Wq7v2J8j5Lfc2S5Z1";

        try {
            RSAPublicKey publicKey = AtlasKnoxSSOAuthenticationFilter.parseRSAPublicKey(pemKey);
            assertNotNull(publicKey);
        } catch (Exception e) {
            // Expected since we're using a mock PEM key
            assertTrue(e instanceof ServletException || e instanceof CertificateException);
        }
    }

    @Test
    public void testDoFilterNonWebUserAgent() throws IOException, ServletException, AtlasException {
        try (MockedStatic<ApplicationProperties> mockedStatic = mockStatic(ApplicationProperties.class);
                MockedStatic<SecurityContextHolder> mockedSecurityHolder = mockStatic(SecurityContextHolder.class)) {
            // Mock configuration to enable SSO
            mockedStatic.when(ApplicationProperties::get).thenReturn(configuration);
            when(configuration.getBoolean("atlas.sso.knox.enabled", false)).thenReturn(true);
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL)).thenReturn("https://knox.sso");
            when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY)).thenReturn("mockPublicKey");
            when(configuration.getStringArray(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(new String[] {"Mozilla"});

            // Mock request with non-web user agent
            when(servletRequest.getCookies()).thenReturn(null);
            when(servletRequest.getHeader("User-Agent")).thenReturn("curl/7.64.1");

            // Mock no existing authentication
            mockedSecurityHolder.when(SecurityContextHolder::getContext).thenReturn(securityContext);
            when(securityContext.getAuthentication()).thenReturn(null);

            AtlasKnoxSSOAuthenticationFilter filter = new AtlasKnoxSSOAuthenticationFilter(authenticationProvider);
            filter.doFilter(servletRequest, servletResponse, filterChain);

            // Should continue with filter chain for non-web user agents
            verify(filterChain).doFilter(servletRequest, servletResponse);
            verifyNoInteractions(authenticationProvider);
        }
    }
}
