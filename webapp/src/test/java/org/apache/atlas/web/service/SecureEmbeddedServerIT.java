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

package org.apache.atlas.web.service;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.atlas.security.SecurityProperties.CERT_STORES_CREDENTIAL_PROVIDER_PATH;

public class SecureEmbeddedServerIT extends SecureEmbeddedServerITBase {
    @Test
    public void testServerConfiguredUsingCredentialProvider() throws Exception {
        // setup the configuration
        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH, providerUrl);
        // setup the credential provider
        setupCredentials();

        SecureEmbeddedServer secureEmbeddedServer = null;
        try {
            String appPath = System.getProperty("user.dir") + getWarPath();

            secureEmbeddedServer = new SecureEmbeddedServer(21443, appPath) {
                @Override
                protected PropertiesConfiguration getConfiguration() {
                    return configuration;
                }
            };
            secureEmbeddedServer.server.start();

            URL url = new URL("https://localhost:21443/");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            // test to see whether server is up and root page can be served
            Assert.assertEquals(connection.getResponseCode(), 200);
        } catch(Throwable e) {
            Assert.fail("War deploy failed", e);
        } finally {
            secureEmbeddedServer.server.stop();
        }

    }
}
