/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.security;

import java.io.File;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.web.TestUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import static org.mockito.Mockito.when;

public class FileAuthenticationTest {

    private static ApplicationContext applicationContext = null;
    private static AtlasAuthenticationProvider authProvider = null;
    private String originalConf;
    private static final Logger LOG = LoggerFactory
            .getLogger(FileAuthenticationTest.class);
    @Mock
    Authentication authentication;

    @BeforeMethod
    public void setup1() {
        MockitoAnnotations.initMocks(this);
    }

    @BeforeClass
    public void setup() throws Exception {

        String persistDir = TestUtils.getTempDirectory();

        setupUserCredential(persistDir);

        setUpAltasApplicationProperties(persistDir);
        
        originalConf = System.getProperty("atlas.conf");
        System.setProperty("atlas.conf", persistDir);

        applicationContext = new ClassPathXmlApplicationContext(
                "spring-security.xml");
        authProvider = applicationContext
                .getBean(org.apache.atlas.web.security.AtlasAuthenticationProvider.class);

    }

    private void setUpAltasApplicationProperties(String persistDir) throws Exception{
        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty("atlas.login.method", "FILE");
        configuration.setProperty("atlas.login.credentials.file", persistDir
                + "/users-credentials");

        TestUtils.writeConfiguration(configuration, persistDir + File.separator
                + ApplicationProperties.APPLICATION_PROPERTIES);
        
    }
    
    private void setupUserCredential(String tmpDir) throws Exception {

        StringBuilder credentialFileStr = new StringBuilder(1024);
        credentialFileStr.append("admin=admin123\n");
        credentialFileStr.append("user=user123\n");
        credentialFileStr.append("test=test123\n");
        File credentialFile = new File(tmpDir, "users-credentials");
        FileUtils.write(credentialFile, credentialFileStr.toString());
    }

    @Test
    public void testValidUserLogin() {

        when(authentication.getName()).thenReturn("admin");
        when(authentication.getCredentials()).thenReturn("admin123");

        Authentication auth = authProvider.authenticate(authentication);
        LOG.debug(" " + auth);

        Assert.assertTrue(auth.isAuthenticated());
    }

    @Test
    public void testInValidPasswordLogin() {

        when(authentication.getName()).thenReturn("admin");
        when(authentication.getCredentials()).thenReturn("wrongpassword");

       try {
            Authentication auth = authProvider.authenticate(authentication);
            LOG.debug(" " + auth);
        } catch (BadCredentialsException bcExp) {
            Assert.assertEquals("Wrong password", bcExp.getMessage());
        }
    }

    @Test
    public void testInValidUsernameLogin() {
   
        when(authentication.getName()).thenReturn("wrongUserName");
        when(authentication.getCredentials()).thenReturn("wrongpassword");
      try {
            Authentication auth = authProvider.authenticate(authentication);
            LOG.debug(" " + auth);
        } catch (UsernameNotFoundException uExp) {
            Assert.assertTrue(uExp.getMessage().contains("Username not found."));
        }
    }

    @AfterClass
    public void tearDown() throws Exception {

        if (originalConf != null) {
            System.setProperty("atlas.conf", originalConf);
        }
        applicationContext = null;
        authProvider = null;
    }
}
