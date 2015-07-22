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
package org.apache.atlas.web.filters;

import org.apache.atlas.web.security.BaseSecurityTest;
import org.apache.atlas.web.service.EmbeddedServer;
import org.apache.commons.configuration.ConfigurationException;
import org.eclipse.jetty.server.Server;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 *
 */
public class MetadataAuthenticationSimpleFilterIT extends BaseSecurityTest {

    class TestEmbeddedServer extends EmbeddedServer {
        public TestEmbeddedServer(int port, String path) throws IOException {
            super(port, path);
        }

        Server getServer() {
            return server;
        }
    }

    @Test(enabled = false)
    public void testSimpleLogin() throws Exception {
        String originalConf = System.getProperty("metadata.conf");
        System.setProperty("metadata.conf", System.getProperty("user.dir"));
        generateSimpleLoginConfiguration();

        TestEmbeddedServer server = new TestEmbeddedServer(23001, "webapp/target/apache-atlas");

        try {
            startEmbeddedServer(server.getServer());

            URL url = new URL("http://localhost:23001");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            try {
                Assert.assertEquals(connection.getResponseCode(), 403);
            } catch (Exception e) {
                e.printStackTrace();
            }

            url = new URL("http://localhost:23001/?user.name=testuser");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            Assert.assertEquals(connection.getResponseCode(), 200);
        } finally {
            server.getServer().stop();
            if (originalConf != null) {
                System.setProperty("metadata.conf", originalConf);
            } else {
                System.clearProperty("metadata.conf");
            }
        }


    }

    protected void generateSimpleLoginConfiguration() throws IOException, ConfigurationException {
        Properties config = new Properties();
        config.setProperty("atlas.http.authentication.enabled", "true");
        config.setProperty("atlas.http.authentication.type", "simple");

        generateTestProperties(config);
    }

}
