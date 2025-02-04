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

package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.AtlasClient;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Integration test for Admin jersey resource.
 */
public class AdminJerseyResourceIT extends BaseResourceIT {
    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testGetVersion() throws Exception {
        ObjectNode response = atlasClientV1.callAPIWithBodyAndParams(AtlasClient.API_V1.VERSION, null, (String[]) null);

        assertNotNull(response);

        PropertiesConfiguration buildConfiguration = new PropertiesConfiguration("atlas-buildinfo.properties");

        assertEquals(response.get("Version").asText(), buildConfiguration.getString("build.version"));
        assertEquals(response.get("Name").asText(), buildConfiguration.getString("project.name"));
        assertEquals(response.get("Description").asText(), buildConfiguration.getString("project.description"));
    }
}
