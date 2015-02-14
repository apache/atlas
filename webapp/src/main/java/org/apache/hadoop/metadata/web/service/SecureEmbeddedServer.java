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

package org.apache.hadoop.metadata.web.service;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.security.SslSocketConnector;

/**
 * This is a jetty server which requires client auth via certificates.
 */
public class SecureEmbeddedServer extends EmbeddedServer {

    public SecureEmbeddedServer(int port, String path) {
        super(port, path);
    }

    protected Connector getConnector(int port) {
        PropertiesConfiguration config = getConfiguration();

        SslSocketConnector connector = new SslSocketConnector();
        connector.setPort(port);
        connector.setHost("0.0.0.0");
        connector.setKeystore(config.getString("keystore.file",
                System.getProperty("keystore.file", "conf/metadata.keystore")));
        connector.setKeyPassword(config.getString("keystore.password",
                System.getProperty("keystore.password", "metadata-passwd")));
        connector.setTruststore(config.getString("truststore.file",
                System.getProperty("truststore.file", "conf/metadata.keystore")));
        connector.setTrustPassword(config.getString("truststore.password",
                System.getProperty("truststore.password", "metadata-passwd")));
        connector.setPassword(config.getString("password",
                System.getProperty("password", "metadata-passwd")));
        connector.setWantClientAuth(true);
        return connector;
    }

    private PropertiesConfiguration getConfiguration() {
        try {
            return new PropertiesConfiguration("application.properties");
        } catch (ConfigurationException e) {
            throw new RuntimeException("Unable to load configuration: application.properties");
        }
    }
}