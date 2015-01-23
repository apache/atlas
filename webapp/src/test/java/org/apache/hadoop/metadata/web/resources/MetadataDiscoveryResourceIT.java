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

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


public class MetadataDiscoveryResourceIT extends BaseResourceIT {
	
    @Test
    public void testUriExists() throws Exception {
        WebResource resource = service
                .path("api/metadata/discovery/search/fulltext")
                .queryParam("depth", "0").queryParam("text","foo").queryParam("property","Name");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertNotEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        
    }
    
    @Test  (dependsOnMethods = "testUriExists", enabled = false)
    public void testSearchForNonExistentText() throws Exception {
        WebResource resource = service
                .path("api/metadata/discovery/search/fulltext")
                .queryParam("depth", "0").queryParam("text","foo").queryParam("property","Name");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        //TODO - Assure zero vertices and edges.
        Assert.assertEquals(true,true);
        
    }
    
    @Test  (dependsOnMethods = "testUriExists", enabled = false)
    public void testSearchForTextNoDepth() throws Exception {
        WebResource resource = service
                .path("api/metadata/discovery/search/fulltext")
                .queryParam("depth", "0").queryParam("text","kid").queryParam("property","Name");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        //TODO - Get count of expected vertices and Edges
        Assert.assertEquals(true, true);
        
    }
    
    @Test  (dependsOnMethods = "testUriExists", enabled = false)
    public void testSearchTextWithDepth() throws Exception {
        WebResource resource = service
                .path("api/metadata/discovery/search/fulltext")
                .queryParam("depth", "4").queryParam("text","Grandad").queryParam("property","Name");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        //TODO - Get count of expected vertices and Edges
        Assert.assertEquals(true, true);
        
    }
    
    

}