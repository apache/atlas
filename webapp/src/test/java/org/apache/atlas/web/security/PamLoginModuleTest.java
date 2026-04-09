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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.web.security;

import org.jvnet.libpam.PAM;
import org.jvnet.libpam.UnixUser;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PamLoginModuleTest {
    @Mock
    private Subject mockSubject;

    @Mock
    private CallbackHandler mockCallbackHandler;

    @Mock
    private PAM mockPam;

    @Mock
    private UnixUser mockUnixUser;

    private PamLoginModule pamLoginModule;
    private Map<String, Object> options;
    private Map<String, Object> sharedState;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        pamLoginModule = new PamLoginModule();
        options = new HashMap<>();
        sharedState = new HashMap<>();
    }

    @Test
    public void testConstructor() throws Exception {
        // Create new instance to test constructor
        PamLoginModule newModule = new PamLoginModule();

        // Verify initial state using reflection
        assertFalse(getPrivateField(newModule, "authSucceeded", Boolean.class));
        assertNull(getPrivateField(newModule, "pam"));
        assertNull(getPrivateField(newModule, "subject"));
        assertNull(getPrivateField(newModule, "callbackHandler"));
        assertNull(getPrivateField(newModule, "options"));
        assertNull(getPrivateField(newModule, "username"));
        assertNull(getPrivateField(newModule, "password"));
        assertNull(getPrivateField(newModule, "principal"));
    }

    @Test
    public void testInitialize() throws Exception {
        // Setup
        options.put("testKey", "testValue");

        // Execute
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Verify using reflection
        assertEquals(getPrivateField(pamLoginModule, "subject"), mockSubject);
        assertEquals(getPrivateField(pamLoginModule, "callbackHandler"), mockCallbackHandler);

        @SuppressWarnings("unchecked")
        Map<String, Object> actualOptions = getPrivateField(pamLoginModule, "options");
        assertNotNull(actualOptions);
        assertEquals(actualOptions.get("testKey"), "testValue");

        // Verify it's a copy, not the same reference
        assertNotSame(actualOptions, options);
    }

    @Test
    public void testLogin_Success() throws Exception {
        // Setup
        options.put(PamLoginModule.SERVICE_KEY, "test-service");
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Mock callback handling
        doAnswer(invocation -> {
            Callback[] callbacks = invocation.getArgument(0);
            NameCallback nameCallback = (NameCallback) callbacks[0];
            PasswordCallback passwordCallback = (PasswordCallback) callbacks[1];

            nameCallback.setName("testuser");
            passwordCallback.setPassword("testpass".toCharArray());

            return null;
        }).when(mockCallbackHandler).handle(any(Callback[].class));

        try (MockedConstruction<PAM> mockedPam = mockConstruction(PAM.class, (mock, context) -> {
            when(mock.authenticate(anyString(), anyString())).thenReturn(mockUnixUser);
        })) {
            // Execute
            boolean result = pamLoginModule.login();

            // Verify
            assertTrue(result);
            assertTrue(getPrivateField(pamLoginModule, "authSucceeded", Boolean.class));
            assertEquals(getPrivateField(pamLoginModule, "username"), "testuser");
            assertEquals(getPrivateField(pamLoginModule, "password"), "testpass");
            assertNotNull(getPrivateField(pamLoginModule, "principal"));

            // Verify PAM was created with correct service
            assertEquals(mockedPam.constructed().size(), 1);
            PAM constructedPam = mockedPam.constructed().get(0);
            verify(constructedPam).authenticate("testuser", "testpass");
        }
    }

    @Test(expectedExceptions = LoginException.class, expectedExceptionsMessageRegExp = "Error: PAM service was not defined")
    public void testLogin_NoService() throws Exception {
        // Setup - no service key in options
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Execute - should throw LoginException
        pamLoginModule.login();
    }

    @Test(expectedExceptions = LoginException.class, expectedExceptionsMessageRegExp = "Error: no CallbackHandler available.*")
    public void testLogin_NoCallbackHandler() throws Exception {
        // Setup
        options.put(PamLoginModule.SERVICE_KEY, "test-service");
        pamLoginModule.initialize(mockSubject, null, sharedState, options); // null callback handler

        try (MockedConstruction<PAM> mockedPam = mockConstruction(PAM.class)) {
            // Execute - should throw LoginException
            pamLoginModule.login();
        }
    }

    @Test
    public void testCommit_Success() throws Exception {
        // Setup successful authentication first
        setupSuccessfulAuthentication();

        Set<Principal> principals = new HashSet<>();
        when(mockSubject.getPrincipals()).thenReturn(principals);
        when(mockSubject.isReadOnly()).thenReturn(false);

        // Execute
        boolean result = pamLoginModule.commit();

        // Verify
        assertTrue(result);
        assertEquals(principals.size(), 1);
        assertTrue(principals.iterator().next() instanceof PamPrincipal);
    }

    @Test
    public void testCommit_AuthNotSucceeded() throws Exception {
        // Setup - no authentication
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Execute
        boolean result = pamLoginModule.commit();

        // Verify
        assertFalse(result);
    }

    @Test(expectedExceptions = LoginException.class, expectedExceptionsMessageRegExp = "Subject is read-only")
    public void testCommit_ReadOnlySubject() throws Exception {
        // Setup successful authentication first
        setupSuccessfulAuthentication();

        when(mockSubject.isReadOnly()).thenReturn(true);

        // Execute - should throw LoginException
        pamLoginModule.commit();

        // Verify cleanup was called
        assertFalse(getPrivateField(pamLoginModule, "authSucceeded", Boolean.class));
    }

    @Test
    public void testCommit_PrincipalAlreadyExists() throws Exception {
        // Setup successful authentication first
        setupSuccessfulAuthentication();

        Set<Principal> principals = new HashSet<>();
        PamPrincipal existingPrincipal = getPrivateField(pamLoginModule, "principal");
        principals.add(existingPrincipal); // Add the same principal

        when(mockSubject.getPrincipals()).thenReturn(principals);
        when(mockSubject.isReadOnly()).thenReturn(false);

        // Execute
        boolean result = pamLoginModule.commit();

        // Verify
        assertTrue(result);
        assertEquals(principals.size(), 1); // Should still be 1, not added again
    }

    @Test
    public void testAbort_AuthSucceeded() throws Exception {
        // Setup successful authentication first
        setupSuccessfulAuthentication();

        // Execute
        boolean result = pamLoginModule.abort();

        // Verify
        assertTrue(result);

        // Verify cleanup was performed
        assertFalse(getPrivateField(pamLoginModule, "authSucceeded", Boolean.class));
        assertNull(getPrivateField(pamLoginModule, "username"));
        assertNull(getPrivateField(pamLoginModule, "password"));
        assertNull(getPrivateField(pamLoginModule, "principal"));
    }

    @Test
    public void testAbort_AuthNotSucceeded() throws Exception {
        // Setup - no authentication
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Execute
        boolean result = pamLoginModule.abort();

        // Verify
        assertFalse(result);
    }

    @Test
    public void testLogout_Success() throws Exception {
        // Setup successful authentication and commit
        setupSuccessfulAuthentication();

        Set<Principal> principals = new HashSet<>();
        PamPrincipal principal = getPrivateField(pamLoginModule, "principal");
        principals.add(principal);

        when(mockSubject.getPrincipals()).thenReturn(principals);
        when(mockSubject.isReadOnly()).thenReturn(false);

        // Execute
        boolean result = pamLoginModule.logout();

        // Verify
        assertTrue(result);
        assertTrue(principals.isEmpty()); // Principal should be removed

        // Verify cleanup was performed
        assertFalse(getPrivateField(pamLoginModule, "authSucceeded", Boolean.class));
        assertNull(getPrivateField(pamLoginModule, "username"));
        assertNull(getPrivateField(pamLoginModule, "password"));
        assertNull(getPrivateField(pamLoginModule, "principal"));
    }

    @Test(expectedExceptions = LoginException.class, expectedExceptionsMessageRegExp = "Subject is read-only")
    public void testLogout_ReadOnlySubject() throws Exception {
        // Setup successful authentication first
        setupSuccessfulAuthentication();

        when(mockSubject.isReadOnly()).thenReturn(true);

        // Execute - should throw LoginException
        pamLoginModule.logout();

        // Verify cleanup was called
        assertFalse(getPrivateField(pamLoginModule, "authSucceeded", Boolean.class));
    }

    @Test
    public void testServiceKeyConstant() {
        // Verify the SERVICE_KEY constant
        assertEquals(PamLoginModule.SERVICE_KEY, "service");
    }

    // Helper methods

    private void setupSuccessfulAuthentication() throws Exception {
        options.put(PamLoginModule.SERVICE_KEY, "test-service");
        pamLoginModule.initialize(mockSubject, mockCallbackHandler, sharedState, options);

        // Mock callback handling
        doAnswer(invocation -> {
            Callback[] callbacks = invocation.getArgument(0);
            NameCallback nameCallback = (NameCallback) callbacks[0];
            PasswordCallback passwordCallback = (PasswordCallback) callbacks[1];

            nameCallback.setName("testuser");
            passwordCallback.setPassword("testpass".toCharArray());

            return null;
        }).when(mockCallbackHandler).handle(any(Callback[].class));

        try (MockedConstruction<PAM> mockedPam = mockConstruction(PAM.class, (mock, context) -> {
            when(mock.authenticate(anyString(), anyString())).thenReturn(mockUnixUser);

            // Set the mock PAM instance in the pamLoginModule
            setPrivateField(pamLoginModule, "pam", mock);
        })) {
            pamLoginModule.login();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getPrivateField(Object target, String fieldName, Class<T> type) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private <T> T getPrivateField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
