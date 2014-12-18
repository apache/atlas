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
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final String ENTITY_NAME = "clicks-table";
    private static final String ENTITY_TYPE = "hive-table";
    private static final String DATABASE_NAME = "ads";
    private static final String TABLE_NAME = "clicks-table";

    @Test
    public void testSubmitEntity() {
        String entityStream = getTestEntityJSON();

        WebResource resource = service
                .path("api/metadata/entities/submit")
                .path(ENTITY_TYPE);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, entityStream);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        Assert.assertNotNull(response);

        try {
            Assert.assertNotNull(UUID.fromString(response));
        } catch (IllegalArgumentException e) {
            Assert.fail("Response is not a guid, " + response);
        }
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path(ENTITY_TYPE)
                .path(ENTITY_NAME);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    private static String getTestEntityJSON() {
        Map<String, String> props = new HashMap<>();
        props.put("entityName", ENTITY_NAME);
        props.put("entityType", ENTITY_TYPE);
        props.put("database", DATABASE_NAME);
        props.put("table", TABLE_NAME);
        return JSONValue.toJSONString(props);
    }

    @Test
    public void testGetInvalidEntityDefinition() {
        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path(ENTITY_TYPE)
                .path("blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/")
                .path(ENTITY_TYPE)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    @Test (enabled = false) // todo: enable this later
    public void testGetEntityListForBadEntityType() {
        ClientResponse clientResponse = service
                .path("api/metadata/entities/list/blah")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }
}
