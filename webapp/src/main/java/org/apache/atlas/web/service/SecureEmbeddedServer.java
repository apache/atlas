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

package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.atlas.security.SecurityProperties.ATLAS_SSL_EXCLUDE_CIPHER_SUITES;
import static org.apache.atlas.security.SecurityProperties.CERT_STORES_CREDENTIAL_PROVIDER_PATH;
import static org.apache.atlas.security.SecurityProperties.CLIENT_AUTH_KEY;
import static org.apache.atlas.security.SecurityProperties.DEFATULT_TRUSTORE_FILE_LOCATION;
import static org.apache.atlas.security.SecurityProperties.DEFAULT_CIPHER_SUITES;
import static org.apache.atlas.security.SecurityProperties.DEFAULT_KEYSTORE_FILE_LOCATION;
import static org.apache.atlas.security.SecurityProperties.KEYSTORE_FILE_KEY;
import static org.apache.atlas.security.SecurityProperties.KEYSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.SERVER_CERT_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_FILE_KEY;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.ATLAS_SSL_EXCLUDE_PROTOCOLS;
import static org.apache.atlas.security.SecurityProperties.DEFAULT_EXCLUDE_PROTOCOLS;


/**
 * This is a jetty server which requires client auth via certificates.
 */
public class SecureEmbeddedServer extends EmbeddedServer {

    private static final Logger LOG = LoggerFactory.getLogger(SecureEmbeddedServer.class);

    public SecureEmbeddedServer(String host, int port, String path) throws IOException {
        super(host, port, path);
    }

    @Override
    protected Connector getConnector(String host, int port) throws IOException {
        org.apache.commons.configuration.Configuration config = getConfiguration();

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(config.getString(KEYSTORE_FILE_KEY,
                System.getProperty(KEYSTORE_FILE_KEY, DEFAULT_KEYSTORE_FILE_LOCATION)));
        sslContextFactory.setKeyStorePassword(getPassword(config, KEYSTORE_PASSWORD_KEY));
        sslContextFactory.setKeyManagerPassword(getPassword(config, SERVER_CERT_PASSWORD_KEY));
        sslContextFactory.setTrustStorePath(config.getString(TRUSTSTORE_FILE_KEY,
                System.getProperty(TRUSTSTORE_FILE_KEY, DEFATULT_TRUSTORE_FILE_LOCATION)));
        sslContextFactory.setTrustStorePassword(getPassword(config, TRUSTSTORE_PASSWORD_KEY));
        sslContextFactory.setWantClientAuth(config.getBoolean(CLIENT_AUTH_KEY, Boolean.getBoolean(CLIENT_AUTH_KEY)));

        List<Object> cipherList = config.getList(ATLAS_SSL_EXCLUDE_CIPHER_SUITES, DEFAULT_CIPHER_SUITES);
        sslContextFactory.setExcludeCipherSuites(cipherList.toArray(new String[cipherList.size()]));
        sslContextFactory.setRenegotiationAllowed(false);

        String[] excludedProtocols = config.containsKey(ATLAS_SSL_EXCLUDE_PROTOCOLS) ?
                config.getStringArray(ATLAS_SSL_EXCLUDE_PROTOCOLS) : DEFAULT_EXCLUDE_PROTOCOLS;
        if (excludedProtocols != null && excludedProtocols.length > 0) {
            sslContextFactory.addExcludeProtocols(excludedProtocols);
        }

        // SSL HTTP Configuration
        // HTTP Configuration
        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        final int bufferSize = AtlasConfiguration.WEBSERVER_REQUEST_BUFFER_SIZE.getInt();
        http_config.setSecurePort(port);
        http_config.setRequestHeaderSize(bufferSize);
        http_config.setResponseHeaderSize(bufferSize);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);

        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());

        // SSL Connector
        ServerConnector sslConnector = new ServerConnector(server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(https_config));
        sslConnector.setPort(port);
        server.addConnector(sslConnector);

        return sslConnector;
    }

    /**
     * Retrieves a password from a configured credential provider or prompts for the password and stores it in the
     * configured credential provider.
     * @param config application configuration
     * @param key the key/alias for the password.
     * @return the password.
     * @throws IOException
     */
    private String getPassword(org.apache.commons.configuration.Configuration config, String key) throws IOException {

        String password;

        String provider = config.getString(CERT_STORES_CREDENTIAL_PROVIDER_PATH);
        if (provider != null) {
            LOG.info("Attempting to retrieve password from configured credential provider path");
            Configuration c = new Configuration();
            c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider);
            CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(c).get(0);
            CredentialProvider.CredentialEntry entry = credentialProvider.getCredentialEntry(key);
            if (entry == null) {
                throw new IOException(String.format("No credential entry found for %s. "
                        + "Please create an entry in the configured credential provider", key));
            } else {
                password = String.valueOf(entry.getCredential());
            }

        } else {
            throw new IOException("No credential provider path configured for storage of certificate store passwords");
        }

        return password;
    }

    /**
     * Returns the application configuration.
     * @return
     */
    protected org.apache.commons.configuration.Configuration getConfiguration() {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException("Unable to load configuration: " + ApplicationProperties.APPLICATION_PROPERTIES);
        }
    }
}
