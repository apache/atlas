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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

/**
 * Base class for integration tests.
 * Sets up the web resource and has helper methods to create type and entity.
 */
public abstract class BaseResourceIT {

    protected WebResource service;
    protected AtlasClient serviceClient;
    public static String baseUrl = "http://localhost:21000/";

    @BeforeClass
    public void setUp() throws Exception {

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
        serviceClient = new AtlasClient(baseUrl);
    }

    protected void createType(TypesDef typesDef) throws Exception {
        HierarchicalTypeDefinition<ClassType> sampleType = typesDef.classTypesAsJavaList().get(0);
        if (serviceClient.getType(sampleType.typeName) == null ) {
            String typesAsJSON = TypesSerialization.toJson(typesDef);
            createType(typesAsJSON);
        }
    }

    protected void createType(String typesAsJSON) throws Exception {
        WebResource resource = service
                .path("api/atlas/types");

        ClientResponse clientResponse = resource
                .accept(Servlets.JSON_MEDIA_TYPE)
                .type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("types"));
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
    }

    protected Id createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        System.out.println("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        JSONObject jsonObject = serviceClient.createEntity(entityJSON);
        String guid = jsonObject.getString(AtlasClient.GUID);
        System.out.println("created instance for type " + typeName + ", guid: " + guid);

        // return the reference to created instance with guid
        return new Id(guid, 0, referenceable.getTypeName());
    }
}
