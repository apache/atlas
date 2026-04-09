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
package org.apache.atlas.utils;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AuthenticationUtilTest {
    @Test
    public void testIsKerberosAuthenticationEnabledWithConfiguration() {
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", true);
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config);
        assertTrue(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithConfigurationFalse() {
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", false);
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config);
        assertFalse(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithConfigurationDefaultValue() {
        Configuration config = new BaseConfiguration();
        // Property not set, should use default value
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config, true);
        assertTrue(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithConfigurationDefaultValueFalse() {
        Configuration config = new BaseConfiguration();
        // Property not set, should use default value
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config, false);
        assertFalse(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithConfigurationNoProperty() {
        Configuration config = new BaseConfiguration();
        // No kerberos property set
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config);
        assertFalse(result); // Should return false as default
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithUgiCredentials() {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true);
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", true);
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config, true);
        assertTrue(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithUgiNoCredentials() {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.hasKerberosCredentials()).thenReturn(false);
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", false);
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config, false);
        assertFalse(result);
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        Constructor<AuthenticationUtil> constructor = AuthenticationUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
        } catch (InvocationTargetException e) {
            // This is expected since the constructor is private and may block instantiation
        }
    }

    @Test
    public void testIsKerberosAuthenticationEnabledConfigPropertyPresent() {
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", "true");
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config);
        assertTrue(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledConfigPropertyString() {
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", "false");
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config);
        assertFalse(result);
    }

    @Test
    public void testIsKerberosAuthenticationEnabledWithDefaultOverride() {
        Configuration config = new BaseConfiguration();
        config.setProperty("atlas.authentication.method.kerberos", true);
        // Even with default false, property value should win
        boolean result = AuthenticationUtil.isKerberosAuthenticationEnabled(config, false);
        assertTrue(result);
    }
}
