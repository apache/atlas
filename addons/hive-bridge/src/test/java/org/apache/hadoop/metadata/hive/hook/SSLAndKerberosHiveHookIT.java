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

package org.apache.hadoop.metadata.hive.hook;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.PropertiesUtil;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.security.SecurityProperties;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLHostnameVerifier;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.jetty.webapp.WebAppContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.metadata.security.SecurityProperties.*;

public class SSLAndKerberosHiveHookIT extends BaseSSLAndKerberosTest {
    public static final String TEST_USER_JAAS_SECTION = "TestUser";
    public static final String TESTUSER = "testuser";
    public static final String TESTPASS = "testpass";

    private static final String DGI_URL = "https://localhost:21443/";
    private Driver driver;
    private MetadataServiceClient dgiCLient;
    private SessionState ss;
    private TestSecureEmbeddedServer secureEmbeddedServer;
    private Subject subject;
    private String originalConf;

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
        URL resource = SSLAndKerberosHiveHookIT.class.getResource("/");
        if (resource != null) {
            persistDir = resource.toURI().getPath();
        }
        // delete prior ssl-client.xml file
        resource = SSLAndKerberosHiveHookIT.class.getResource("/" + SecurityProperties.SSL_CLIENT_PROPERTIES);
        if (resource != null) {
            File sslClientFile = new File(persistDir, SecurityProperties.SSL_CLIENT_PROPERTIES);
            if (sslClientFile != null && sslClientFile.exists()) {
                sslClientFile.delete();
            }
        }
        setupKDCAndPrincipals();
        setupCredentials();

        // client will actually only leverage subset of these properties
        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(TLS_ENABLED, true);
        configuration.setProperty(TRUSTSTORE_FILE_KEY, "../../webapp/target/metadata.keystore");
        configuration.setProperty(KEYSTORE_FILE_KEY, "../../webapp/target/metadata.keystore");
        configuration.setProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH, providerUrl);
        configuration.setProperty("metadata.http.authentication.type", "kerberos");
        configuration.setProperty(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, SSLHostnameVerifier.DEFAULT_AND_LOCALHOST.toString());

        configuration.save(new FileWriter(persistDir + File.separator + "client.properties"));

        String confLocation = System.getProperty("metadata.conf");
        URL url;
        if (confLocation == null) {
            url = PropertiesUtil.class.getResource("/application.properties");
        } else {
            url = new File(confLocation, "application.properties").toURI().toURL();
        }
        configuration.load(url);
        configuration.setProperty(TLS_ENABLED, true);
        configuration.setProperty("metadata.http.authentication.enabled", "true");
        configuration.setProperty("metadata.http.authentication.kerberos.principal", "HTTP/localhost@" + kdc.getRealm());
        configuration.setProperty("metadata.http.authentication.kerberos.keytab", httpKeytabFile.getAbsolutePath());
        configuration.setProperty("metadata.http.authentication.kerberos.name.rules",
                "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT");

        configuration.save(new FileWriter(persistDir + File.separator + "application.properties"));

        dgiCLient = new MetadataServiceClient(DGI_URL) {
            @Override
            protected PropertiesConfiguration getClientProperties() throws MetadataException {
                return configuration;
            }
        };

        secureEmbeddedServer = new TestSecureEmbeddedServer(21443, "webapp/target/metadata-governance") {
            @Override
            public PropertiesConfiguration getConfiguration() {
                return configuration;
            }
        };
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setWar(System.getProperty("user.dir") + getWarPath());
        secureEmbeddedServer.getServer().setHandler(webapp);

        // save original setting
        originalConf = System.getProperty("metadata.conf");
        System.setProperty("metadata.conf", persistDir);
        secureEmbeddedServer.getServer().start();

        subject = loginTestUser();
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (secureEmbeddedServer != null) {
            secureEmbeddedServer.getServer().stop();
        }

        if (kdc != null) {
            kdc.stop();
        }

        if (originalConf != null) {
            System.setProperty("metadata.conf", originalConf);
        }
    }

    protected Subject loginTestUser() throws LoginException, IOException {
        LoginContext lc = new LoginContext(TEST_USER_JAAS_SECTION, new CallbackHandler() {

            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (int i = 0; i < callbacks.length; i++) {
                    if (callbacks[i] instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback) callbacks[i];
                        passwordCallback.setPassword(TESTPASS.toCharArray());
                    }
                    if (callbacks[i] instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback) callbacks[i];
                        nameCallback.setName(TESTUSER);
                    }
                }
            }
        });
        // attempt authentication
        lc.login();
        return lc.getSubject();
    }

    private void runCommand(final String cmd) throws Exception {
        ss.setCommandType(null);
        Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                driver.run(cmd);

                return null;
            }
        });
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create database " + dbName);

        assertDatabaseIsRegistered(dbName);
    }

    @Test
    public void testCreateTable() throws Exception {
        String dbName = "db" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create database " + dbName);

        String tableName = "table" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create table " + dbName + "." + tableName + "(id int, name string)");
        assertTableIsRegistered(tableName);

        tableName = "table" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create table " + tableName + "(id int, name string)");
        assertTableIsRegistered(tableName);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered("default");
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = "table" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create table " + tableName + "(id int, name string)");

        String newTableName = "table" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        String query = "create table " + newTableName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(newTableName);
        assertInstanceIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(), "queryText", query);
    }

    private void assertTableIsRegistered(String tableName) throws Exception {
        assertInstanceIsRegistered(HiveDataTypes.HIVE_TABLE.getName(), "name", tableName);
    }

    private void assertDatabaseIsRegistered(String dbName) throws Exception {
        assertInstanceIsRegistered(HiveDataTypes.HIVE_DB.getName(), "name", dbName);
    }

    private void assertInstanceIsRegistered(final String typeName, final String colName, final String colValue) throws Exception {
        Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                JSONArray results = dgiCLient.rawSearch(typeName, colName, colValue);
                Assert.assertEquals(results.length(), 1);

                return null;
            }
        });
    }
}
