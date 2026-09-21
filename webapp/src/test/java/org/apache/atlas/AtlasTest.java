/*
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AtlasTest {

    @Test
    public void testParseArgsWithNoArguments() throws Exception {
        CommandLine commandLine = Atlas.parseArgs(new String[0]);

        assertTrue(commandLine.getOptions().length == 0);
    }

    @Test
    public void testParseArgsWithApplicationPath() throws Exception {
        CommandLine commandLine =
                Atlas.parseArgs(new String[]{"--app", "/opt/atlas/webapp"});

        assertTrue(commandLine.hasOption("app"));
        assertEquals("/opt/atlas/webapp", commandLine.getOptionValue("app"));
    }

    @Test
    public void testParseArgsWithApplicationPort() throws Exception {
        CommandLine commandLine =
                Atlas.parseArgs(new String[]{"--port", "21443"});

        assertTrue(commandLine.hasOption("port"));
        assertEquals("21443", commandLine.getOptionValue("port"));
    }

    @Test
    public void testGetApplicationPortUsesCommandLinePortWhenProvided()
            throws Exception {
        CommandLine commandLine =
                Atlas.parseArgs(new String[]{"--port", "9999"});

        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(Atlas.ATLAS_SERVER_HTTPS_PORT, 21443);

        int port = Atlas.getApplicationPort(commandLine, "true", configuration);

        assertEquals(9999, port);
    }

    @Test
    public void testGetApplicationPortUsesHttpsPortWhenTlsEnabled()
            throws Exception {
        CommandLine commandLine = Atlas.parseArgs(new String[0]);

        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(Atlas.ATLAS_SERVER_HTTPS_PORT, 21443);

        int port = Atlas.getApplicationPort(commandLine, "true", configuration);

        assertEquals(21443, port);
    }

    @Test
    public void testGetApplicationPortUsesHttpPortWhenTlsDisabled()
            throws Exception {
        CommandLine commandLine = Atlas.parseArgs(new String[0]);

        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(Atlas.ATLAS_SERVER_HTTP_PORT, 21000);

        int port = Atlas.getApplicationPort(commandLine, "false", configuration);

        assertEquals(21000, port);
    }

    @Test
    public void testGetApplicationPortUsesHttpsPortWhenTlsSettingIsEmpty()
            throws Exception {
        CommandLine commandLine = Atlas.parseArgs(new String[0]);

        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(Atlas.ATLAS_SERVER_HTTPS_PORT, 21443);

        int port = Atlas.getApplicationPort(commandLine, "", configuration);

        assertEquals(21443, port);
    }

    @Test
    public void testGetProjectVersion() {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty("project.version", "3.0.0-SNAPSHOT");

        assertEquals(
                "3.0.0-SNAPSHOT",
                Atlas.getProjectVersion(configuration));
    }
}
