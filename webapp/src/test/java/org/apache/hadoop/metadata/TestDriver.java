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

package org.apache.hadoop.metadata;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.json.simple.JSONValue;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.util.HashMap;
import java.util.Map;

public class TestDriver {

    public static void main(String[] args) throws Exception {
        String baseUrl = "http://localhost:15000/";

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        WebResource service = client.resource(UriBuilder.fromUri(baseUrl).build());

/*
        ClientResponse clientResponse = service.path("api/metadata/entities/list/blah")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
*/


//        String filePath = "/tmp/metadata/sampleentity.json";
//        InputStream entityStream = getServletInputStream(filePath);

        final String entityName = "clicks-table";
        final String entityType = "hive-table";

        submitEntity(service, entityName, entityType);

        WebResource resource = service
                .path("api/metadata/entities/definition")
                .path(entityType)
                .path(entityName);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }

    private static void submitEntity(WebResource service, String entityName, String entityType) {
        Map<String, String> props = new HashMap<>();
        props.put("entityName", entityName);
        props.put("entityType", entityType);
        props.put("database", "foo");
        props.put("blah", "blah");
        String entityStream = JSONValue.toJSONString(props);


        WebResource resource = service
                .path("api/metadata/entities/submit")
                .path(entityType);

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.POST, ClientResponse.class, entityStream);
        String response = clientResponse.getEntity(String.class);
        System.out.println("response = " + response);
    }
}
