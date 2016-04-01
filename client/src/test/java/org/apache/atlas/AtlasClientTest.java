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

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class AtlasClientTest {

    @Test
    public void shouldVerifyServerIsReady() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);

        WebResource.Builder builder = setupBuilder(AtlasClient.API.VERSION, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("{\"Version\":\"version-rrelease\",\"Name\":\"apache-atlas\"," +
                "\"Description\":\"Metadata Management and Data Governance Platform over Hadoop\"}");
        when(builder.method(AtlasClient.API.VERSION.getMethod(), ClientResponse.class, null)).thenReturn(response);

        assertTrue(atlasClient.isServerReady());
    }

    private WebResource.Builder setupBuilder(AtlasClient.API api, WebResource webResource) {
        WebResource adminVersionResource = mock(WebResource.class);
        when(webResource.path(api.getPath())).thenReturn(adminVersionResource);
        WebResource.Builder builder = mock(WebResource.Builder.class);
        when(adminVersionResource.accept(AtlasClient.JSON_MEDIA_TYPE)).thenReturn(builder);
        when(builder.type(AtlasClient.JSON_MEDIA_TYPE)).thenReturn(builder);
        return builder;
    }

    @Test
    public void shouldReturnFalseIfServerIsNotReady() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);
        WebResource.Builder builder = setupBuilder(AtlasClient.API.VERSION, webResource);
        when(builder.method(AtlasClient.API.VERSION.getMethod(), ClientResponse.class, null)).thenThrow(
                new ClientHandlerException());
        assertFalse(atlasClient.isServerReady());
    }

    @Test
    public void shouldReturnFalseIfServiceIsUnavailable() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);
        WebResource.Builder builder = setupBuilder(AtlasClient.API.VERSION, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        when(response.getClientResponseStatus()).thenReturn(ClientResponse.Status.SERVICE_UNAVAILABLE);

        when(builder.method(AtlasClient.API.VERSION.getMethod(), ClientResponse.class, null)).thenReturn(response);

        assertFalse(atlasClient.isServerReady());
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void shouldThrowErrorIfAnyResponseOtherThanServiceUnavailable() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);
        WebResource.Builder builder = setupBuilder(AtlasClient.API.VERSION, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getClientResponseStatus()).thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR);

        when(builder.method(AtlasClient.API.VERSION.getMethod(), ClientResponse.class, null)).thenReturn(response);

        atlasClient.isServerReady();
        fail("Should throw exception");
    }
    
    @Test
    public void shouldGetAdminStatus() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);

        WebResource.Builder builder = setupBuilder(AtlasClient.API.STATUS, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("{\"Status\":\"Active\"}");
        when(builder.method(AtlasClient.API.STATUS.getMethod(), ClientResponse.class, null)).thenReturn(response);

        String status = atlasClient.getAdminStatus();
        assertEquals(status, "Active");
    }

    @Test(expectedExceptions = AtlasServiceException.class)
    public void shouldReturnStatusAsUnknownOnException() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);

        WebResource.Builder builder = setupBuilder(AtlasClient.API.STATUS, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getClientResponseStatus()).thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR);
        when(builder.method(AtlasClient.API.STATUS.getMethod(), ClientResponse.class, null)).thenReturn(response);

        String status = atlasClient.getAdminStatus();
        fail("Should fail with AtlasServiceException");
    }

    @Test
    public void shouldReturnStatusAsUnknownIfJSONIsInvalid() throws AtlasServiceException {
        WebResource webResource = mock(WebResource.class);
        AtlasClient atlasClient = new AtlasClient(webResource);

        WebResource.Builder builder = setupBuilder(AtlasClient.API.STATUS, webResource);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("{\"status\":\"Active\"}");
        when(builder.method(AtlasClient.API.STATUS.getMethod(), ClientResponse.class, null)).thenReturn(response);

        String status = atlasClient.getAdminStatus();
        assertEquals(status, AtlasClient.UNKNOWN_STATUS);
    }
}
