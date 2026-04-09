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

package org.apache.atlas.web.filters;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerException;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasAuthenticationFilterTest {
    private static final String AUTH_COOKIE = AuthenticatedURL.AUTH_COOKIE;
    private static final String TIMEOUT_ACTION = "timeout";

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockFilterChain;

    @Mock
    private FilterConfig mockFilterConfig;

    @Mock
    private ServletContext mockServletContext;

    @Mock
    private AuthenticationHandler mockAuthHandler;

    @Mock
    private AuthenticationToken mockAuthToken;

    @Mock
    private Signer mockSigner;

    @Mock
    private SignerSecretProvider mockSecretProvider;

    @Mock
    private HttpSession mockSession;

    @Mock
    private SecurityContext mockSecurityContext;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private UserGroupInformation mockUgi;

    @Mock
    private AtlasResponseRequestWrapper mockResponseWrapper;

    private AtlasAuthenticationFilter filter;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        when(mockFilterConfig.getServletContext()).thenReturn(mockServletContext);
        when(mockRequest.getSession()).thenReturn(mockSession);
        when(mockRequest.getCookies()).thenReturn(null);
        SecurityContextHolder.setContext(mockSecurityContext);

        filter = spy(new AtlasAuthenticationFilter());
    }

    @AfterMethod
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testConstructor() throws ServletException {
        try (MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class);
                MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAuthUtil.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(true);
            mockedAppProps.when(ApplicationProperties::get).thenThrow(new AtlasException("test"));

            AtlasAuthenticationFilter newFilter = new AtlasAuthenticationFilter();
            assertNotNull(newFilter);
        }
    }

    @Test
    public void testInitKerberosDisabled() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<AtlasConfiguration> mockedAtlasConfig = mockStatic(AtlasConfiguration.class)) {
            // Use a real PropertiesConfiguration instead of a mock
            PropertiesConfiguration realConf = new PropertiesConfiguration();
            realConf.addProperty("atlas.authentication.method.kerberos", "false");
            realConf.addProperty("atlas.authentication.method.kerberos.token.validity", "3600");
            realConf.addProperty("atlas.http.authentication.signature.secret", "secret");
            realConf.addProperty("atlas.proxyusers", "user1");
            realConf.addProperty("atlas.authentication.method.kerberos.name.rules", "RULES");
            realConf.addProperty("atlas.authentication.method.kerberos.keytab", "/keytab");
            realConf.addProperty("atlas.authentication.method.kerberos.principal", "principal/_HOST@REALM");

            mockedAppProps.when(ApplicationProperties::get).thenReturn(realConf);

            // sessionTimeout is normally initialized during init, force it to -1 for this case
            ReflectionTestUtils.setField(filter, "sessionTimeout", -1);

            // Inject our real config into the filter
            ReflectionTestUtils.setField(filter, "configuration", realConf);

            // Call init() – should not throw NPE now
            filter.init(mockFilterConfig);

            // Verify expected state
            assertEquals(-1, ReflectionTestUtils.getField(filter, "sessionTimeout"));
            assertNull(ReflectionTestUtils.getField(filter, "logoutHandler"));
        }
    }

    @Test(expectedExceptions = ServletException.class)
    public void testInitInvalidTokenValidity() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            Configuration mockConf = mock(Configuration.class);
            mockedAppProps.when(ApplicationProperties::get).thenReturn(mockConf);

            when(mockConf.getString("atlas.authentication.method.kerberos.token.validity")).thenReturn("invalid");

            filter.init(mockFilterConfig);
        }
    }

    @Test
    public void testInitializeSecretProviderNew() throws Exception {
        try (MockedStatic<AuthenticationFilter> mockedAuthFilter = mockStatic(AuthenticationFilter.class)) {
            when(mockServletContext.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(null);
            when(mockFilterConfig.getServletContext()).thenReturn(mockServletContext);
            when(mockFilterConfig.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());

            mockedAuthFilter.when(() -> AuthenticationFilter.constructSecretProvider(any(), any(), eq(false))).thenReturn(mockSecretProvider);

            filter.initializeSecretProvider(mockFilterConfig);

            assertEquals(mockSecretProvider, ReflectionTestUtils.getField(filter, "secretProvider"));
            assertTrue((Boolean) ReflectionTestUtils.getField(filter, "isInitializedByTomcat"));
        }
    }

    @Test
    public void testDestroyInitializedByTomcat() {
        ReflectionTestUtils.setField(filter, "secretProvider", mockSecretProvider);
        ReflectionTestUtils.setField(filter, "isInitializedByTomcat", true);

        filter.destroy();

        verify(mockSecretProvider).destroy();
    }

    @Test
    public void testDestroyNotInitializedByTomcat() {
        ReflectionTestUtils.setField(filter, "secretProvider", mockSecretProvider);
        ReflectionTestUtils.setField(filter, "isInitializedByTomcat", false);

        filter.destroy();

        verify(mockSecretProvider, never()).destroy();
    }

    @Test
    public void testGetConfigurationKerberosEnabled() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<SecurityUtil> mockedSecurityUtil = mockStatic(SecurityUtil.class);
                MockedStatic<InetAddress> mockedInet = mockStatic(InetAddress.class);
                MockedStatic<AtlasConfiguration> mockedAtlasConfig = mockStatic(AtlasConfiguration.class)) {
            Configuration mockConf = new MapConfiguration(new HashMap<>());
            mockedAppProps.when(ApplicationProperties::get).thenReturn(mockConf);

            mockConf.setProperty("atlas.authentication.method.kerberos", "true");
            mockConf.setProperty("atlas.authentication.method.kerberos.name.rules", "RULES");
            mockConf.setProperty("atlas.authentication.method.kerberos.keytab", "/keytab");
            mockConf.setProperty("atlas.authentication.method.kerberos.principal", "principal/_HOST@REALM");
            mockConf.setProperty(SecurityProperties.BIND_ADDRESS, "localhost");
            mockConf.setProperty("atlas.authentication.method.kerberos.support.keytab.browser.login", true);
            mockConf.setProperty("atlas.authentication.method.trustedproxy", true);
            mockConf.setProperty("atlas.proxyusers", new String[] {"user1"});
            mockConf.setProperty("atlas.csrf.browser-useragents-regex", "Mozilla.*");

            // Set the sessionTimeout field directly instead of mocking the enum method
            ReflectionTestUtils.setField(filter, "sessionTimeout", 3600);

            InetAddress mockAddr = mock(InetAddress.class);
            mockedInet.when(InetAddress::getLocalHost).thenReturn(mockAddr);
            when(mockAddr.getHostName()).thenReturn("localhost");

            mockedSecurityUtil.when(() -> SecurityUtil.getServerPrincipal("principal/_HOST@REALM", "localhost")).thenReturn("resolved@REALM");

            Vector<String> initParams = new Vector<>(Collections.singletonList("param1"));
            when(mockFilterConfig.getInitParameterNames()).thenReturn(initParams.elements());
            when(mockFilterConfig.getInitParameter("param1")).thenReturn("value1");

            Properties props = filter.getConfiguration("", mockFilterConfig);

            assertEquals("kerberos", props.getProperty(AuthenticationFilter.AUTH_TYPE));
            assertEquals("RULES", props.getProperty("kerberos.name.rules"));
            assertEquals("/keytab", props.getProperty("kerberos.keytab"));
            assertEquals("resolved@REALM", props.getProperty(KerberosAuthenticationHandler.PRINCIPAL));
            assertEquals("value1", props.getProperty("param1"));
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testGetConfigurationPrincipalResolutionFailure() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<SecurityUtil> mockedSecurityUtil = mockStatic(SecurityUtil.class)) {
            Configuration mockConf = mock(Configuration.class);
            mockedAppProps.when(ApplicationProperties::get).thenReturn(mockConf);

            when(mockConf.getString("atlas.authentication.method.kerberos")).thenReturn("true");
            when(mockConf.getString("atlas.authentication.method.kerberos.principal")).thenReturn("invalid");

            mockedSecurityUtil.when(() -> SecurityUtil.getServerPrincipal(anyString(), anyString())).thenThrow(new IOException("fail"));

            filter.getConfiguration("", mockFilterConfig);
        }
    }

    @Test(expectedExceptions = ServletException.class)
    public void testGetConfigurationApplicationPropertiesException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(ApplicationProperties::get).thenThrow(new AtlasException("fail"));

            filter.getConfiguration("", mockFilterConfig);
        }
    }

    @Test
    public void testGetTokenValid() throws Exception {
        ReflectionTestUtils.setField(filter, "signer", mockSigner);

        Cookie cookie = new Cookie(AUTH_COOKIE, "signedToken");
        when(mockRequest.getCookies()).thenReturn(new Cookie[] {cookie});
        when(mockSigner.verifyAndExtract("signedToken")).thenReturn("u=user&t=kerberos&e=" + (System.currentTimeMillis() + 10000) + "&s=signature&p=user");

        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.getType()).thenReturn("kerberos");

        Method getTokenMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("getToken", HttpServletRequest.class);
        getTokenMethod.setAccessible(true);
        AuthenticationToken token = (AuthenticationToken) getTokenMethod.invoke(filter, mockRequest);

        assertNotNull(token);
        assertEquals("user", token.getUserName());
        assertEquals("kerberos", token.getType());
    }

    @Test(expectedExceptions = InvocationTargetException.class)
    public void testGetTokenInvalidSignature() throws Exception {
        ReflectionTestUtils.setField(filter, "signer", mockSigner);

        Cookie cookie = new Cookie(AUTH_COOKIE, "invalid");
        when(mockRequest.getCookies()).thenReturn(new Cookie[] {cookie});
        when(mockSigner.verifyAndExtract("invalid")).thenThrow(new SignerException("invalid"));

        Method getTokenMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("getToken", HttpServletRequest.class);
        getTokenMethod.setAccessible(true);
        getTokenMethod.invoke(filter, mockRequest);
    }

    @Test(expectedExceptions = InvocationTargetException.class)
    public void testGetTokenExpired() throws Exception {
        ReflectionTestUtils.setField(filter, "signer", mockSigner);

        Cookie cookie = new Cookie(AUTH_COOKIE, "signedToken");
        when(mockRequest.getCookies()).thenReturn(new Cookie[] {cookie});
        when(mockSigner.verifyAndExtract("signedToken")).thenReturn("u=user&t=kerberos&e=1&s=signature&p=user");

        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.getType()).thenReturn("kerberos");

        Method getTokenMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("getToken", HttpServletRequest.class);
        getTokenMethod.setAccessible(true);
        getTokenMethod.invoke(filter, mockRequest);
    }

    @Test
    public void testDoFilterExistingAuth() throws IOException, ServletException {
        when(mockSecurityContext.getAuthentication()).thenReturn(mockAuthentication);

        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    }

    @Test
    public void testDoFilterBasicAuth() throws IOException, ServletException {
        when(mockSecurityContext.getAuthentication()).thenReturn(null);
        when(mockRequest.getHeader("Authorization")).thenReturn("Basic test");

        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    }

    @Test
    public void testDoFilterKerberos() throws IOException, ServletException, NoSuchMethodException {
        ReflectionTestUtils.setField(filter, "isKerberos", true);
        when(mockSecurityContext.getAuthentication()).thenReturn(null);
        when(mockRequest.getHeader("Authorization")).thenReturn(null);

        Method doKerberosAuthMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
        doKerberosAuthMethod.setAccessible(true);

        filter.doFilter(mockRequest, mockResponse, mockFilterChain);
    }

    @Test
    public void testDoFilterTimeoutAction() throws IOException, ServletException {
        ReflectionTestUtils.setField(filter, "supportTrustedProxy", true);
        ReflectionTestUtils.setField(filter, "sessionTimeout", 3600);
        ReflectionTestUtils.setField(filter, "logoutHandler", mock(SecurityContextLogoutHandler.class));

        when(mockSecurityContext.getAuthentication()).thenReturn(mockAuthentication);
        when(mockRequest.getParameter("action")).thenReturn(TIMEOUT_ACTION);
        when(mockRequest.getParameter("doAs")).thenReturn("user");
        when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://test/api"));
        when(mockRequest.getRequestURI()).thenReturn("/api");
        when(mockRequest.getHeader("x-forwarded-for")).thenReturn(null);
        when(mockRequest.getServerName()).thenReturn("test");
        when(mockRequest.getServerPort()).thenReturn(80);
        when(mockRequest.getScheme()).thenReturn("http");

        try (MockedStatic<RestUtil> mockedRestUtil = mockStatic(RestUtil.class)) {
            mockedRestUtil.when(() -> RestUtil.constructForwardableURL(any())).thenReturn("http://test/api");
            mockedRestUtil.when(() -> RestUtil.constructRedirectURL(any(), anyString(), anyString(), anyString())).thenReturn("http://test/logout");

            filter.doFilter(mockRequest, mockResponse, mockFilterChain);

            verify(mockResponse).sendRedirect("http://test/logout");
        }
    }

    @Test
    public void testDoFilterNPE() throws IOException, ServletException {
        when(mockSecurityContext.getAuthentication()).thenReturn(null);
        when(mockRequest.getHeader("Authorization")).thenReturn(null);
        ReflectionTestUtils.setField(filter, "isKerberos", false);

        doThrow(new NullPointerException()).when(mockFilterChain).doFilter(any(), any());

        filter.doFilter(mockRequest, mockResponse, mockFilterChain);

        verify(mockResponse).sendError(eq(400), anyString());
    }

    @Test
    public void testDoKerberosAuthProxyUser() throws Exception {
        ReflectionTestUtils.setField(filter, "signer", mockSigner);
        ReflectionTestUtils.setField(filter, "supportTrustedProxy", true);

        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.managementOperation(any(), any(), any())).thenReturn(true);
        when(mockAuthHandler.getType()).thenReturn("kerberos");

        when(mockAuthToken.getUserName()).thenReturn("proxy");
        when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");

        // Prevent NPE from FilterChain
        when(mockRequest.getMethod()).thenReturn("GET");   // ✅ important
        doAnswer(invocation -> null).when(mockFilterChain).doFilter(any(), any());
        doNothing().when(mockRequest).setAttribute(anyString(), any());

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class);
                MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<ProxyUsers> mockedProxyUsers = mockStatic(ProxyUsers.class)) {
            mockedServlets.when(() -> Servlets.getDoAsUser(mockRequest)).thenReturn("user");
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser("proxy")).thenReturn(mockUgi);
            mockedUgi.when(() -> UserGroupInformation.createProxyUser("user", mockUgi)).thenReturn(mockUgi);

            Cookie cookie = new Cookie(AUTH_COOKIE, "signedToken");
            when(mockRequest.getCookies()).thenReturn(new Cookie[] {cookie});
            when(mockSigner.verifyAndExtract("signedToken"))
                    .thenReturn("u=proxy&t=kerberos&e=" + (System.currentTimeMillis() + 10000) + "&s=signature&p=proxy");

            when(mockAuthHandler.authenticate(any(), any())).thenReturn(mockAuthToken);
            when(mockAuthToken.getExpires()).thenReturn(System.currentTimeMillis() + 10000);
            when(mockAuthToken.isExpired()).thenReturn(false);
            when(mockAuthToken.getType()).thenReturn("kerberos");
            when(mockAuthToken.toString())
                    .thenReturn("u=proxy&t=kerberos&e=" + (System.currentTimeMillis() + 10000) + "&s=signature&p=proxy");

            Method doKerberosAuthMethod = AtlasAuthenticationFilter.class
                    .getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
            doKerberosAuthMethod.setAccessible(true);

            doKerberosAuthMethod.invoke(filter, mockRequest, mockResponse, mockFilterChain);

            mockedProxyUsers.verify(() -> ProxyUsers.authorize(mockUgi, "127.0.0.1"));
        }
    }

    @Test
    public void testDoKerberosAuthProxyUserIgnore() throws Exception {
        ReflectionTestUtils.setField(filter, "atlasProxyUsers", new HashSet<>(Collections.singletonList("proxy")));

        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.managementOperation(any(), any(), any())).thenReturn(true);

        when(mockAuthToken.getUserName()).thenReturn("proxy");
        when(mockRequest.getRemoteUser()).thenReturn("proxy");

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedServlets.when(() -> Servlets.getDoAsUser(mockRequest)).thenReturn(null);

            // Mock the getToken method by setting up the filter to return the token
            ReflectionTestUtils.setField(filter, "signer", mockSigner);
            Cookie cookie = new Cookie(AUTH_COOKIE, "signedToken");
            when(mockRequest.getCookies()).thenReturn(new Cookie[] {cookie});
            when(mockSigner.verifyAndExtract("signedToken")).thenReturn("u=proxy&t=kerberos&e=" + (System.currentTimeMillis() + 10000) + "&s=signature&p=proxy");

            // Set up the authHandler to return the mock token
            when(mockAuthHandler.authenticate(any(), any())).thenReturn(mockAuthToken);
            when(mockAuthToken.getExpires()).thenReturn(System.currentTimeMillis() + 10000);
            when(mockAuthToken.isExpired()).thenReturn(false);
            when(mockAuthToken.getType()).thenReturn("kerberos");

            Method doKerberosAuthMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
            doKerberosAuthMethod.setAccessible(true);

            doKerberosAuthMethod.invoke(filter, mockRequest, mockResponse, mockFilterChain);

            verify(mockResponse).setHeader("WWW-Authenticate", "");
            verify(mockResponse).setStatus(401);
        }
    }

    @Test
    public void testDoKerberosAuthAuthenticationException() throws Exception {
        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.managementOperation(any(), any(), any())).thenThrow(new AuthenticationException("fail"));

        Method doKerberosAuthMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
        doKerberosAuthMethod.setAccessible(true);

        doKerberosAuthMethod.invoke(filter, mockRequest, mockResponse, mockFilterChain);

        verify(mockResponse).sendError(403, "fail");
    }

    @Test
    public void testDoKerberosAuthUnauthorizedNonBrowserNoSession() throws Exception {
        ReflectionTestUtils.setField(filter, "supportKeyTabBrowserLogin", false);

        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.managementOperation(any(), any(), any())).thenReturn(false);

        when(mockRequest.getHeader("User-Agent")).thenReturn("curl");
        when(mockResponse.getHeaderNames()).thenReturn(Collections.singletonList("Other-Header"));

        Method doKerberosAuthMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
        doKerberosAuthMethod.setAccessible(true);

        doKerberosAuthMethod.invoke(filter, mockRequest, mockResponse, mockFilterChain);
    }

    @Test
    public void testDoKerberosAuthUnauthorizedWithSessionCookie() throws Exception {
        ReflectionTestUtils.setField(filter, "supportKeyTabBrowserLogin", false);

        // Set the authHandler field directly using reflection
        ReflectionTestUtils.setField(filter, "authHandler", mockAuthHandler);
        when(mockAuthHandler.managementOperation(any(), any(), any())).thenReturn(false);

        when(mockRequest.getHeader("User-Agent")).thenReturn("curl");
        when(mockResponse.getHeaderNames()).thenReturn(Collections.singletonList("Set-Cookie"));
        when(mockResponse.getHeader("Set-Cookie")).thenReturn("ATLASSESSIONID=123");

        Method doKerberosAuthMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("doKerberosAuth", ServletRequest.class, ServletResponse.class, FilterChain.class);
        doKerberosAuthMethod.setAccessible(true);

        doKerberosAuthMethod.invoke(filter, mockRequest, mockResponse, mockFilterChain);

        verify(mockFilterChain, never()).doFilter(mockRequest, mockResponse);
    }

    @Test
    public void testKerberosFilterChainWrapperDoFilterOptions() throws IOException, ServletException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        // Create the inner class instance using reflection
        Class<?> wrapperClass = AtlasAuthenticationFilter.class.getDeclaredClasses()[0]; // First inner class
        Constructor<?> constructor = wrapperClass.getDeclaredConstructor(AtlasAuthenticationFilter.class, ServletRequest.class, ServletResponse.class, FilterChain.class);
        constructor.setAccessible(true);
        FilterChain wrapper = (FilterChain) constructor.newInstance(filter, mockRequest, mockResponse, mockFilterChain);

        when(mockRequest.getMethod()).thenReturn("OPTIONS");

        wrapper.doFilter(mockRequest, mockResponse);

        // Verifies OPTIONS handled by optionsServlet
        verify(mockFilterChain, never()).doFilter(mockRequest, mockResponse);
    }

    @Test
    public void testKerberosFilterChainWrapperDoFilterAuthSetup() throws IOException, ServletException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        // Create the inner class instance using reflection
        Class<?> wrapperClass = AtlasAuthenticationFilter.class.getDeclaredClasses()[0]; // First inner class
        Constructor<?> constructor = wrapperClass.getDeclaredConstructor(AtlasAuthenticationFilter.class, ServletRequest.class, ServletResponse.class, FilterChain.class);
        constructor.setAccessible(true);
        FilterChain wrapper = (FilterChain) constructor.newInstance(filter, mockRequest, mockResponse, mockFilterChain);

        when(mockRequest.getMethod()).thenReturn("GET");
        when(mockRequest.getRemoteUser()).thenReturn("user");
        when(mockSecurityContext.getAuthentication()).thenReturn(null);
        ReflectionTestUtils.setField(filter, "sessionTimeout", 3600);

        try (MockedStatic<AtlasAbstractAuthenticationProvider> mockedProvider = mockStatic(AtlasAbstractAuthenticationProvider.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedServlets.when(() -> Servlets.getRequestURI(mockRequest)).thenReturn("/api");
            List<GrantedAuthority> authorities = Collections.singletonList(mock(GrantedAuthority.class));
            mockedProvider.when(() -> AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("user")).thenReturn(authorities);

            wrapper.doFilter(mockRequest, mockResponse);

            verify(mockSecurityContext).setAuthentication(any(Authentication.class));
            verify(mockSession).setMaxInactiveInterval(3600);
            verify(mockFilterChain).doFilter(mockRequest, mockResponse);
        }
    }

    @Test
    public void testKerberosFilterChainWrapperDoFilterProxyUser() throws IOException, ServletException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, InstantiationException {
        // Create the inner class instance using reflection
        Class<?> wrapperClass = AtlasAuthenticationFilter.class.getDeclaredClasses()[0]; // First inner class
        Constructor<?> constructor = wrapperClass.getDeclaredConstructor(AtlasAuthenticationFilter.class, ServletRequest.class, ServletResponse.class, FilterChain.class);
        constructor.setAccessible(true);
        FilterChain wrapper = (FilterChain) constructor.newInstance(filter, mockRequest, mockResponse, mockFilterChain);

        when(mockRequest.getMethod()).thenReturn("GET");
        when(mockRequest.getAttribute("proxyUser")).thenReturn("proxyUser");
        when(mockSecurityContext.getAuthentication()).thenReturn(null);
        ReflectionTestUtils.setField(filter, "sessionTimeout", 3600);

        try (MockedStatic<AtlasAbstractAuthenticationProvider> mockedProvider = mockStatic(AtlasAbstractAuthenticationProvider.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedServlets.when(() -> Servlets.getRequestURI(mockRequest)).thenReturn("/api");
            List<GrantedAuthority> authorities = Collections.singletonList(mock(GrantedAuthority.class));
            mockedProvider.when(() -> AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("proxyUser")).thenReturn(authorities);

            wrapper.doFilter(mockRequest, mockResponse);

            verify(mockSecurityContext).setAuthentication(any(Authentication.class));
            verify(mockSession).setMaxInactiveInterval(3600);
            verify(mockFilterChain).doFilter(mockRequest, mockResponse);
        }
    }

    @Test
    public void testKerberosFilterChainWrapperDoFilterExistingAuth() throws IOException, ServletException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        // Create the inner class instance using reflection
        Class<?> wrapperClass = AtlasAuthenticationFilter.class.getDeclaredClasses()[0]; // First inner class
        Constructor<?> constructor = wrapperClass.getDeclaredConstructor(AtlasAuthenticationFilter.class, ServletRequest.class, ServletResponse.class, FilterChain.class);
        constructor.setAccessible(true);
        FilterChain wrapper = (FilterChain) constructor.newInstance(filter, mockRequest, mockResponse, mockFilterChain);

        when(mockRequest.getMethod()).thenReturn("GET");
        when(mockRequest.getRemoteUser()).thenReturn("user");
        when(mockSecurityContext.getAuthentication()).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedServlets.when(() -> Servlets.getRequestURI(mockRequest)).thenReturn("/api");

            wrapper.doFilter(mockRequest, mockResponse);

            verify(mockSecurityContext, never()).setAuthentication(any());
            verify(mockFilterChain).doFilter(mockRequest, mockResponse);
        }
    }

    @Test
    public void testParseBrowserUserAgents() throws Exception {
        filter.parseBrowserUserAgents("Mozilla.*,Chrome.*");

        Set<Pattern> agents = (Set<Pattern>) ReflectionTestUtils.getField(filter, "browserUserAgents");
        assertEquals(2, agents.size());
    }

    @Test
    public void testIsBrowserTrue() throws Exception {
        filter.parseBrowserUserAgents("Mozilla.*");

        boolean result = filter.isBrowser("Mozilla/5.0");
        assertTrue(result);
    }

    @Test
    public void testIsBrowserFalse() throws Exception {
        filter.parseBrowserUserAgents("Mozilla.*");

        boolean result = filter.isBrowser("curl");
        assertFalse(result);
    }

    @Test
    public void testIsBrowserNull() throws Exception {
        filter.parseBrowserUserAgents("Mozilla.*");

        boolean result = filter.isBrowser(null);
        assertFalse(result);
    }

    @Test
    public void testCreateAtlasAuthCookie() throws Exception {
        Method createCookieMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("createAtlasAuthCookie", HttpServletResponse.class, String.class, String.class, String.class, long.class, boolean.class);
        createCookieMethod.setAccessible(true);

        createCookieMethod.invoke(filter, mockResponse, "token", "domain", "/path", System.currentTimeMillis() + 1000, true);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockResponse).addHeader(eq("Set-Cookie"), captor.capture());
        String cookie = captor.getValue();
        assertTrue(cookie.contains("hadoop.auth=\"token\""));
        assertTrue(cookie.contains("Domain=domain"));
        assertTrue(cookie.contains("Path=/path"));
        assertTrue(cookie.contains("Secure"));
        assertTrue(cookie.contains("HttpOnly"));
    }

    @Test
    public void testCreateAtlasAuthCookieEmptyToken() throws Exception {
        Method createCookieMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("createAtlasAuthCookie", HttpServletResponse.class, String.class, String.class, String.class, long.class, boolean.class);
        createCookieMethod.setAccessible(true);

        createCookieMethod.invoke(filter, mockResponse, "", null, null, -1, false);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockResponse).addHeader(eq("Set-Cookie"), captor.capture());
        String cookie = captor.getValue();
        assertTrue(cookie.startsWith("hadoop.auth=;"));
        assertFalse(cookie.contains("Expires"));
    }

    @Test
    public void testReadUserFromCookie() throws Exception {
        when(mockResponse.containsHeader("Set-Cookie")).thenReturn(true);
        when(mockResponse.getHeaders("Set-Cookie")).thenReturn(Collections.singletonList("hadoop.auth=u=user&t=type&e=1000&s=sig; Path=/"));

        Method readMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("readUserFromCookie", HttpServletResponse.class);
        readMethod.setAccessible(true);
        String user = (String) readMethod.invoke(null, mockResponse);

        assertEquals("user", user);
    }

    @Test
    public void testReadUserFromCookieNoUser() throws Exception {
        when(mockResponse.containsHeader("Set-Cookie")).thenReturn(true);
        when(mockResponse.getHeaders("Set-Cookie")).thenReturn(Collections.singletonList("hadoop.auth=t=type; Path=/"));

        Method readMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("readUserFromCookie", HttpServletResponse.class);
        readMethod.setAccessible(true);
        String user = (String) readMethod.invoke(null, mockResponse);

        assertNull(user);
    }

    @Test
    public void testReadUserFromCookieNoCookie() throws Exception {
        when(mockResponse.containsHeader("Set-Cookie")).thenReturn(false);

        Method readMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("readUserFromCookie", HttpServletResponse.class);
        readMethod.setAccessible(true);
        String user = (String) readMethod.invoke(null, mockResponse);

        assertNull(user);
    }

    @Test
    public void testGetProxyuserConfiguration() throws Exception {
        Configuration mockConf = new MapConfiguration(new HashMap<>());
        mockConf.setProperty("atlas.proxyuser.user1.groups", "group1");
        ReflectionTestUtils.setField(filter, "configuration", mockConf);

        Method getProxyMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("getProxyuserConfiguration");
        getProxyMethod.setAccessible(true);
        org.apache.hadoop.conf.Configuration hadoopConf = (org.apache.hadoop.conf.Configuration) getProxyMethod.invoke(filter);

        assertEquals("group1", hadoopConf.get("atlas.proxyuser.user1.groups"));
    }

    @Test
    public void testGetProxyuserConfigurationEmpty() throws Exception {
        Configuration mockConf = new MapConfiguration(new HashMap<>());
        ReflectionTestUtils.setField(filter, "configuration", mockConf);

        Method getProxyMethod = AtlasAuthenticationFilter.class.getDeclaredMethod("getProxyuserConfiguration");
        getProxyMethod.setAccessible(true);
        org.apache.hadoop.conf.Configuration hadoopConf = (org.apache.hadoop.conf.Configuration) getProxyMethod.invoke(filter);

        assertTrue(hadoopConf.getFinalParameters().isEmpty());
    }
}
