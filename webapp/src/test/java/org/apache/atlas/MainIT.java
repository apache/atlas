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
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class MainIT {

  @Test
  public void testPortSelection () throws Exception {
    PropertiesConfiguration config = new PropertiesConfiguration();
    // test ports via config
    config.setProperty(Main.ATLAS_SERVER_HTTP_PORT, 21001);
    config.setProperty(Main.ATLAS_SERVER_HTTPS_PORT, 22443);
    int port = Main.getApplicationPort(Main.parseArgs(new String[] {}), "false",
                                       config );
    Assert.assertEquals(21001, port, "wrong http port");
    port = Main.getApplicationPort(Main.parseArgs(new String[] {}), "true",
                                   config );
    Assert.assertEquals(22443, port, "wrong https port");
    // test defaults
    port = Main.getApplicationPort(Main.parseArgs(new String[] {}), "false",
                                   new PropertiesConfiguration() );
    Assert.assertEquals(21000, port, "wrong http port");
    port = Main.getApplicationPort(Main.parseArgs(new String[] {}), "true",
                                   new PropertiesConfiguration() );
    Assert.assertEquals(21443, port, "wrong https port");
    // test command line override
    CommandLine commandLine = Main.parseArgs(new String[] {"--port", "22000"});
    port = Main.getApplicationPort(commandLine, "true", config);
    Assert.assertEquals(22000, port, "wrong https port");
    port = Main.getApplicationPort(commandLine, "false", config);
    Assert.assertEquals(22000, port, "wrong https port");
  }
}
