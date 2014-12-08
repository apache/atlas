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

package org.apache.hadoop.metadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metadata.web.service.EmbeddedServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver for running Metadata as a standalone server with embedded jetty server.
 */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";

    /**
     * Prevent users from constructing this.
     */
    private Main() {
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option opt;

        opt = new Option(APP_PATH, true, "Application Path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(APP_PORT, true, "Application Port");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        String projectVersion = getProjectVersion();
        // String appPath = "webapp/target/metadata-webapp-" + projectVersion;
        String appPath = "webapp/target/metadata-governance";

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        PropertiesConfiguration configuration = new PropertiesConfiguration("application.properties");
        final String enableTLSFlag = configuration.getString("metadata.enableTLS");
        final int appPort = getApplicationPort(cmd, enableTLSFlag);
        final boolean enableTLS = isTLSEnabled(enableTLSFlag, appPort);
        configuration.setProperty("metadata.enableTLS", String.valueOf(enableTLS));

        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", enableTLS, appPort);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        EmbeddedServer server = EmbeddedServer.newServer(appPort, appPath, enableTLS);
        server.start();
    }

    private static String getProjectVersion() throws ConfigurationException {
        PropertiesConfiguration configuration =
                new PropertiesConfiguration("metadata-buildinfo.properties");
        return configuration.getString("project.version");
    }

    private static int getApplicationPort(CommandLine cmd, String enableTLSFlag) {
        final int appPort;
        if (cmd.hasOption(APP_PORT)) {
            appPort = Integer.valueOf(cmd.getOptionValue(APP_PORT));
        } else {
            // default : metadata.enableTLS is true
            appPort = StringUtils.isEmpty(enableTLSFlag)
                    || enableTLSFlag.equals("true") ? 15443 : 15000;
        }

        return appPort;
    }

    private static boolean isTLSEnabled(String enableTLSFlag, int appPort) {
        return Boolean.valueOf(StringUtils.isEmpty(enableTLSFlag)
                ? System.getProperty("metadata.enableTLS", (appPort % 1000) == 443 ? "true" : "false")
                : enableTLSFlag);
    }
}
