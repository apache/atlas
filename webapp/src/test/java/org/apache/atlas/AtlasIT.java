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
package org.apache.atlas;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class AtlasIT {
    @Test
    public void testPortSelection() throws Exception {
        PropertiesConfiguration config = new PropertiesConfiguration();

        // test ports via config
        config.setProperty(Atlas.ATLAS_SERVER_HTTP_PORT, 21001);
        config.setProperty(Atlas.ATLAS_SERVER_HTTPS_PORT, 22443);

        int port = Atlas.getApplicationPort(Atlas.parseArgs(new String[] {}), "false", config);

        assertEquals(port, 21001, "wrong http port");

        port = Atlas.getApplicationPort(Atlas.parseArgs(new String[] {}), "true", config);

        assertEquals(port, 22443, "wrong https port");

        // test defaults
        port = Atlas.getApplicationPort(Atlas.parseArgs(new String[] {}), "false", new PropertiesConfiguration());

        assertEquals(port, 21000, "wrong http port");

        port = Atlas.getApplicationPort(Atlas.parseArgs(new String[] {}), "true", new PropertiesConfiguration());

        assertEquals(port, 21443, "wrong https port");

        // test command line override
        CommandLine commandLine = Atlas.parseArgs(new String[] {"--port", "22000"});

        port = Atlas.getApplicationPort(commandLine, "true", config);

        assertEquals(port, 22000, "wrong https port");

        port = Atlas.getApplicationPort(commandLine, "false", config);

        assertEquals(port, 22000, "wrong https port");
    }
}
