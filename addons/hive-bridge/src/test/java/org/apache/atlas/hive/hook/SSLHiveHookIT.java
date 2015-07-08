/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.web.service.SecureEmbeddedServer;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLHostnameVerifier;
import org.codehaus.jettison.json.JSONArray;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import static org.apache.atlas.security.SecurityProperties.CERT_STORES_CREDENTIAL_PROVIDER_PATH;
import static org.apache.atlas.security.SecurityProperties.KEYSTORE_FILE_KEY;
import static org.apache.atlas.security.SecurityProperties.KEYSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.SERVER_CERT_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_FILE_KEY;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_PASSWORD_KEY;

public class SSLHiveHookIT {
    private static final String DGI_URL = "https://localhost:21443/";
    private Driver driver;
    private AtlasClient dgiCLient;
    private SessionState ss;
    private Path jksPath;
    private String providerUrl;
    private TestSecureEmbeddedServer secureEmbeddedServer;

    class TestSecureEmbeddedServer extends SecureEmbeddedServer {

        public TestSecureEmbeddedServer(int port, String path) throws IOException {
            super(port, path);
        }

        public Server getServer() {
            return server;
        }

        @Override
        public PropertiesConfiguration getConfiguration() {
            return super.getConfiguration();
        }
    }

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        HiveConf conf = getHiveConf();
        driver = new Driver(conf);
        ss = new SessionState(conf, System.getProperty("user.name"));
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);

        jksPath = new Path(Files.createTempDirectory("tempproviders").toString(), "test.jks");
        providerUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

        String persistDir = null;
        URL resource = SSLHiveHookIT.class.getResource("/");
        if (resource != null) {
            persistDir = resource.toURI().getPath();
        }
        // delete prior ssl-client.xml file
        resource = SSLHiveHookIT.class.getResource("/" + SecurityProperties.SSL_CLIENT_PROPERTIES);
        if (resource != null) {
            File sslClientFile = new File(persistDir, SecurityProperties.SSL_CLIENT_PROPERTIES);
            if (sslClientFile != null && sslClientFile.exists()) {
                sslClientFile.delete();
            }
        }
        setupCredentials();

        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(TLS_ENABLED, true);
        configuration.setProperty(TRUSTSTORE_FILE_KEY, "../../webapp/target/atlas.keystore");
        configuration.setProperty(KEYSTORE_FILE_KEY, "../../webapp/target/atlas.keystore");
        configuration.setProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH, providerUrl);
        configuration.setProperty(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY,
                SSLHostnameVerifier.DEFAULT_AND_LOCALHOST.toString());

        configuration.save(new FileWriter(persistDir + File.separator + "client.properties"));

        dgiCLient = new AtlasClient(DGI_URL) {
            @Override
            protected PropertiesConfiguration getClientProperties() throws AtlasException {
                return configuration;
            }
        };

        secureEmbeddedServer = new TestSecureEmbeddedServer(21443, "webapp/target/apache-atlas") {
            @Override
            public PropertiesConfiguration getConfiguration() {
                return configuration;
            }
        };
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setWar(System.getProperty("user.dir") + getWarPath());
        secureEmbeddedServer.getServer().setHandler(webapp);

        secureEmbeddedServer.getServer().start();

    }

    @AfterClass
    public void tearDown() throws Exception {
        if (secureEmbeddedServer != null) {
            secureEmbeddedServer.getServer().stop();
        }
    }

    protected void setupCredentials() throws Exception {
        Configuration conf = new Configuration(false);

        File file = new File(jksPath.toUri().getPath());
        file.delete();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerUrl);

        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);

        // create new aliases
        try {

            char[] storepass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(KEYSTORE_PASSWORD_KEY, storepass);

            char[] trustpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(TRUSTSTORE_PASSWORD_KEY, trustpass);

            char[] trustpass2 = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry("ssl.client.truststore.password", trustpass2);

            char[] certpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(SERVER_CERT_PASSWORD_KEY, certpass);

            // write out so that it can be found in checks
            provider.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    protected String getWarPath() {
        return String.format("/../../webapp/target/atlas-webapp-%s",
                System.getProperty("project.version"));
    }

    private HiveConf getHiveConf() {
        return HiveHookIT.createHiveConf(DGI_URL);
    }

    private void runCommand(String cmd) throws Exception {
        ss.setCommandType(null);
        driver.run(cmd);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create database " + dbName);

        assertDatabaseIsRegistered(dbName);
    }

    private void assertDatabaseIsRegistered(String dbName) throws Exception {
        assertInstanceIsRegistered(HiveDataTypes.HIVE_DB.getName(), "name", dbName);
    }

    private void assertInstanceIsRegistered(String typeName, String colName, String colValue) throws Exception {
        JSONArray results = dgiCLient.rawSearch(typeName, colName, colValue);
        Assert.assertEquals(results.length(), 1);
    }
}
