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

package org.apache.atlas;

import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;


/**
 * An application that allows users to run admin commands against an Atlas server.
 *
 * The application uses {@link AtlasClient} to send REST requests to the Atlas server. The details of connections
 * and other configuration is specified in the Atlas properties file.
 * Exit status of the application will be as follows:
 * <li>0: successful execution</li>
 * <li>1: error in options used for the application</li>
 * <li>-1/255: application error</li>
 */
public class AtlasAdminClient {

    private static final Option STATUS = new Option("status", false, "Get the status of an atlas instance");
    private static final Options OPTIONS = new Options();

    private static final int INVALID_OPTIONS_STATUS = 1;
    private static final int PROGRAM_ERROR_STATUS = -1;

    static {
        OPTIONS.addOption(STATUS);
    }

    public static void main(String[] args) throws AtlasException, ParseException {
        AtlasAdminClient atlasAdminClient = new AtlasAdminClient();
        int result = atlasAdminClient.run(args);
        System.exit(result);
    }

    private int run(String[] args) throws AtlasException {
        CommandLine commandLine = parseCommandLineOptions(args);
        Configuration configuration = ApplicationProperties.get();
        String atlasServerUri = configuration.getString(
                AtlasConstants.ATLAS_REST_ADDRESS_KEY, AtlasConstants.DEFAULT_ATLAS_REST_ADDRESS);

        AtlasClient atlasClient = null;
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
            atlasClient = new AtlasClient(new String[]{atlasServerUri}, basicAuthUsernamePassword);
        } else {
            atlasClient = new AtlasClient(atlasServerUri, null, null);
        }
        return handleCommand(commandLine, atlasServerUri, atlasClient);
    }

    private int handleCommand(CommandLine commandLine, String atlasServerUri, AtlasClient atlasClient) {
        int cmdStatus = PROGRAM_ERROR_STATUS;
        if (commandLine.hasOption(STATUS.getOpt())) {
            try {
                System.out.println(atlasClient.getAdminStatus());
                cmdStatus = 0;
            } catch (AtlasServiceException e) {
                System.err.println("Could not retrieve status of the server at " + atlasServerUri);
                printStandardHttpErrorDetails(e);
            }
        } else {
            System.err.println("Unsupported option. Refer to usage for valid options.");
            printUsage(INVALID_OPTIONS_STATUS);
        }
        return cmdStatus;
    }

    private void printStandardHttpErrorDetails(AtlasServiceException e) {
        System.err.println("Error details: ");
        System.err.println("HTTP Status: " + e.getStatus().getStatusCode() + ","
                + e.getStatus().getReasonPhrase());
        System.err.println("Exception message: " + e.getMessage());
    }

    private CommandLine parseCommandLineOptions(String[] args) {
        if (args.length == 0) {
            printUsage(INVALID_OPTIONS_STATUS);
        }
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(OPTIONS, args);
        } catch (ParseException e) {
            System.err.println("Could not parse command line options.");
            printUsage(INVALID_OPTIONS_STATUS);
        }
        return commandLine;
    }

    private void printUsage(int statusCode) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("atlas_admin.py", OPTIONS);
        System.exit(statusCode);
    }

}
