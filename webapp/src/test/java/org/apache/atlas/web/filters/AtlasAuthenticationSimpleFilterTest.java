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

import org.apache.atlas.RequestContext;
import org.apache.atlas.web.security.BaseSecurityTest;
import org.apache.atlas.web.service.EmbeddedServer;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.Server;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class AtlasAuthenticationSimpleFilterTest extends BaseSecurityTest {
    public static final String TESTUSER = "testuser";

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
        String originalConf = System.getProperty("atlas.conf");
        System.setProperty("atlas.conf", System.getProperty("user.dir"));
        generateSimpleLoginConfiguration();

        TestEmbeddedServer server = new TestEmbeddedServer(23001, "webapp/target/apache-atlas");

        try {
            startEmbeddedServer(server.getServer());

            URL url = new URL("http://localhost:23001");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            assertEquals(connection.getResponseCode(), Response.Status.BAD_REQUEST.getStatusCode());

            url = new URL("http://localhost:23001/?user.name=testuser");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            assertEquals(connection.getResponseCode(), Response.Status.OK.getStatusCode());
            assertEquals(RequestContext.get().getUser(), TESTUSER);
        } finally {
            server.getServer().stop();
            if (originalConf != null) {
                System.setProperty("atlas.conf", originalConf);
            } else {
                System.clearProperty("atlas.conf");
            }
        }
    }

    protected String generateSimpleLoginConfiguration() throws Exception {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty("atlas.http.authentication.enabled", "true");
        config.setProperty("atlas.http.authentication.type", "simple");
        return writeConfiguration(config);
    }
}
