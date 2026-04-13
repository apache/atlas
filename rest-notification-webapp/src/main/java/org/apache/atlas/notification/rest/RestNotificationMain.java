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

package org.apache.atlas.notification.rest;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.notification.rest.web.service.EmbeddedServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Iterator;

public class RestNotificationMain {
    private static final Logger LOG                       = LoggerFactory.getLogger(RestNotificationMain.class);
    private static final String APP_PATH                  = "app";
    private static final String APP_PORT                  = "port";
    private static final String REST_NOTIFICATION_HOME    = "atlas.rest.notification.home";
    private static final String REST_NOTIFICATION_DATA    = "atlas.rest.notification.data";
    private static final String REST_NOTIFICATION_LOG_DIR = "atlas.rest.notification.log.dir";
    private static final String REST_SERVER_HTTPS_PORT    = "atlas.rest.server.https.port";
    private static final String REST_SERVER_HTTP_PORT     = "atlas.rest.server.http.port";

    private static EmbeddedServer server;

    /**
     * Prevent users from constructing this.
     */
    private RestNotificationMain() {
    }

    protected static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option opt;

        opt = new Option(APP_PATH, true, "Application Path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(APP_PORT, true, "Application Port");
        opt.setRequired(false);
        options.addOption(opt);

        return new DefaultParser().parse(options, args);
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        PropertiesConfiguration buildConfiguration = new PropertiesConfiguration("rest-notification-buildinfo.properties");
        String appPath = "rest-notification-webapp/target/rest-notification-webapp" /*\\+ getProjectVersion(buildConfiguration)*/;

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        setApplicationHome();
        Configuration configuration = ApplicationProperties.get();
        final String enableTLSFlag = configuration.getString(SecurityProperties.TLS_ENABLED);
        final String appHost = configuration.getString(SecurityProperties.BIND_ADDRESS, EmbeddedServer.REST_DEFAULT_BIND_ADDRESS);

        if (!isLocalAddress(InetAddress.getByName(appHost))) {
            String msg =
                    "Failed to start Rest Notification server. Address " + appHost
                            + " does not belong to this host. Correct configuration parameter: "
                            + SecurityProperties.BIND_ADDRESS;
            LOG.error(msg);
            throw new IOException(msg);
        }

        final int appPort = getApplicationPort(cmd, enableTLSFlag, configuration);
        System.setProperty(AtlasConstants.SYSTEM_PROPERTY_APP_PORT, String.valueOf(appPort));
        final boolean enableTLS = isTLSEnabled(enableTLSFlag, appPort);
        configuration.setProperty(SecurityProperties.TLS_ENABLED, String.valueOf(enableTLS));

        showStartupInfo(buildConfiguration, enableTLS, appPort);

        server = EmbeddedServer.newServer(appHost, appPort, appPath, enableTLS);
        installLogBridge();

        server.start();
    }

    public static String getProjectVersion(PropertiesConfiguration buildConfiguration) {
        return buildConfiguration.getString("project.version");
    }

    private static void setApplicationHome() {
        if (System.getProperty(REST_NOTIFICATION_HOME) == null) {
            System.setProperty(REST_NOTIFICATION_HOME, "target");
        }
        if (System.getProperty(REST_NOTIFICATION_DATA) == null) {
            System.setProperty(REST_NOTIFICATION_DATA, "target/data");
        }
        if (System.getProperty(REST_NOTIFICATION_LOG_DIR) == null) {
            System.setProperty(REST_NOTIFICATION_LOG_DIR, "target/logs");
        }
    }

    private static boolean isLocalAddress(InetAddress addr) {
        // Check if the address is any local or loop back
        boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

        // Check if the address is defined on any interface
        if (!local) {
            try {
                local = NetworkInterface.getByInetAddress(addr) != null;
            } catch (SocketException e) {
                local = false;
            }
        }
        return local;
    }


    static int getApplicationPort(CommandLine cmd, String enableTLSFlag, Configuration configuration) {
        String optionValue = cmd.hasOption(APP_PORT) ? cmd.getOptionValue(APP_PORT) : null;

        final int appPort;

        if (StringUtils.isNotEmpty(optionValue)) {
            appPort = Integer.valueOf(optionValue);
        } else {
            // default : atlas.enableTLS is true
            appPort = getPortValue(configuration, enableTLSFlag);
        }

        return appPort;
    }


    private static int getPortValue(Configuration configuration, String enableTLSFlag) {
        int appPort;

        assert configuration != null;
        appPort = StringUtils.isEmpty(enableTLSFlag) || enableTLSFlag.equals("true") ?
                configuration.getInt(REST_SERVER_HTTPS_PORT, 41443) :
                configuration.getInt(REST_SERVER_HTTP_PORT, 41000);
        return appPort;
    }

    private static boolean isTLSEnabled(String enableTLSFlag, int appPort) {
        return Boolean.valueOf(StringUtils.isEmpty(enableTLSFlag) ?
                System.getProperty(org.apache.atlas.security.SecurityProperties.TLS_ENABLED, (appPort % 1000) == 443 ? "true" : "false") : enableTLSFlag);
    }


    private static void showStartupInfo(PropertiesConfiguration buildConfiguration, boolean enableTLS, int appPort) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("\n############################################");
        buffer.append("############################################");
        buffer.append("\n                               Rest Notification Server (STARTUP)");
        buffer.append("\n");
        try {
            final Iterator<String> keys = buildConfiguration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                buffer.append('\n').append('\t').append(key).
                        append(":\t").append(buildConfiguration.getProperty(key));
            }
        } catch (Throwable e) {
            buffer.append("*** Unable to get build info ***");
        }
        buffer.append("\n############################################");
        buffer.append("############################################");
        LOG.info(buffer.toString());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", enableTLS, appPort);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    private static void installLogBridge() {
        // Optionally remove existing handlers attached to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();  // (since SLF4J 1.6.5)

        // add SLF4JBridgeHandler to j.u.l's root logger, should be done once during
        // the initialization phase of your application
        SLF4JBridgeHandler.install();
    }

}
