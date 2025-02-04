/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.listeners;

import org.apache.atlas.web.security.BaseSecurityTest;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class LoginProcessorIT extends BaseSecurityTest {
    protected static final String kerberosRule = "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";

    @Test
    public void testDefaultSimpleLogin() throws Exception {
        LoginProcessor processor = new LoginProcessor() {
            @Override
            protected org.apache.commons.configuration.Configuration getApplicationConfiguration() {
                return new PropertiesConfiguration();
            }
        };

        processor.login();

        assertNotNull(UserGroupInformation.getCurrentUser());
        assertFalse(UserGroupInformation.isLoginKeytabBased());
        assertFalse(UserGroupInformation.isSecurityEnabled());
    }

    @Test
    public void testKerberosLogin() throws Exception {
        final File keytab = setupKDCAndPrincipals();

        LoginProcessor processor = new LoginProcessor() {
            @Override
            protected Configuration getHadoopConfiguration() {
                Configuration config = new Configuration(false);

                config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
                config.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
                config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL, kerberosRule);

                return config;
            }

            @Override
            protected org.apache.commons.configuration.Configuration getApplicationConfiguration() {
                PropertiesConfiguration config = new PropertiesConfiguration();

                config.setProperty("atlas.authentication.method", "kerberos");
                config.setProperty("atlas.authentication.principal", "dgi@EXAMPLE.COM");
                config.setProperty("atlas.authentication.keytab", keytab.getAbsolutePath());
                return config;
            }

            @Override
            protected boolean isHadoopCluster() {
                return true;
            }
        };
        processor.login();

        assertTrue(UserGroupInformation.getLoginUser().getShortUserName().endsWith("dgi"));
        assertNotNull(UserGroupInformation.getCurrentUser());
        assertTrue(UserGroupInformation.isLoginKeytabBased());
        assertTrue(UserGroupInformation.isSecurityEnabled());

        kdc.stop();
    }

    private File setupKDCAndPrincipals() throws Exception {
        // set up the KDC
        File kdcWorkDir = startKDC();

        assertNotNull(kdc.getRealm());

        return createKeytab(kdc, kdcWorkDir, "dgi", "dgi.keytab");
    }
}
