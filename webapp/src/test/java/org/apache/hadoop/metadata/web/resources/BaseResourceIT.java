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

package org.apache.hadoop.metadata.web.resources;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

public abstract class BaseResourceIT {

    protected TypeSystem typeSystem;
    protected WebResource service;
    protected MetadataServiceClient serviceClient;

    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        String baseUrl = "http://localhost:21000/";

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
        serviceClient = new MetadataServiceClient(baseUrl);
    }

    protected void sumbitType(String typesAsJSON, String type) throws Exception {
        WebResource resource = service
                .path("api/metadata/types/submit")
                .path(type);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertEquals(response.get("typeName"), type);
        Assert.assertNotNull(response.get("types"));
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));
    }
}
