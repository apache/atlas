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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Integration tests for Rexster Graph Jersey Resource.
 */
@Test
public class RexsterGraphJerseyResourceIT extends BaseResourceIT {

    @Test (enabled = false)
    public void testGetVertex() throws Exception {
        // todo: add a vertex before fetching it

        WebResource resource = service
                .path("api/metadata/graph/vertices")
                .path("0");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        Assert.assertNotNull(response);
    }

    public void testGetVertexWithInvalidId() throws Exception {
        WebResource resource = service
                .path("api/metadata/graph/vertices/blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    public void testGetVertexProperties() throws Exception {

    }

    public void testGetVertices() throws Exception {

    }

    public void testGetVertexEdges() throws Exception {

    }

    public void testGetEdge() throws Exception {

    }
}
