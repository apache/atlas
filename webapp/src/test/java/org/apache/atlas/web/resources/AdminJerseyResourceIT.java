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

package org.apache.atlas.web.resources;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

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
        WebResource resource = service.path("api/atlas/admin/version");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        PropertiesConfiguration buildConfiguration = new PropertiesConfiguration("atlas-buildinfo.properties");


        JSONObject response = new JSONObject(responseAsString);
        Assert.assertEquals(response.get("Version"), buildConfiguration.getString("build.version"));
        Assert.assertEquals(response.get("Name"), buildConfiguration.getString("project.name"));
        Assert.assertEquals(response.get("Description"), buildConfiguration.getString("project.description"));
    }
}
