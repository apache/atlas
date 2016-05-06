/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.resources.EntityResource;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class LocalAtlasClientTest {
    @Mock
    private EntityResource entityResource;

    @Mock
    private ServiceState serviceState;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEntity() throws Exception {
        Response response = mock(Response.class);
        when(entityResource.submit(any(HttpServletRequest.class))).thenReturn(response);
        final String guid = random();
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(AtlasClient.GUID, new JSONArray(Arrays.asList(guid)));
        }});

        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, entityResource);
        List<String> results = atlasClient.createEntity(new Referenceable(random()));
        assertEquals(results.size(), 1);
        assertEquals(results.get(0), guid);
    }

    @Test
    public void testException() throws Exception {
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, entityResource);

        Response response = mock(Response.class);
        when(entityResource.submit(any(HttpServletRequest.class))).thenThrow(new WebApplicationException(response));
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put("stackTrace", "stackTrace");
        }});
        when(response.getStatus()).thenReturn(Response.Status.BAD_REQUEST.getStatusCode());
        try {
            atlasClient.createEntity(new Referenceable(random()));
            fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST);
        }

        when(entityResource.updateByUniqueAttribute(anyString(), anyString(), anyString(),
                any(HttpServletRequest.class))).thenThrow(new WebApplicationException(response));
        when(response.getStatus()).thenReturn(Response.Status.NOT_FOUND.getStatusCode());
        try {
            atlasClient.updateEntity(random(), random(), random(), new Referenceable(random()));
            fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.NOT_FOUND);
        }

    }

    @Test
    public void testIsServerReady() throws Exception {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, entityResource);
        assertTrue(atlasClient.isServerReady());

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.BECOMING_ACTIVE);
        assertFalse(atlasClient.isServerReady());
    }

    @Test
    public void testUpdateEntity() throws Exception {
        final String guid = random();
        Response response = mock(Response.class);
        when(entityResource.updateByUniqueAttribute(anyString(), anyString(), anyString(),
                any(HttpServletRequest.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(AtlasClient.GUID, guid);
        }});

        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, entityResource);
        String actualId = atlasClient.updateEntity(random(), random(), random(), new Referenceable(random()));
        assertEquals(actualId, guid);
    }

    @Test
    public void testDeleteEntity() throws Exception {
        final String guid = random();
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(AtlasClient.GUID, new JSONArray(Arrays.asList(guid)));
        }});

        when(entityResource.deleteEntities(anyListOf(String.class), anyString(), anyString(), anyString())).thenReturn(response);
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, entityResource);
        List<String> results = atlasClient.deleteEntity(random(), random(), random());
        assertEquals(results.size(), 1);
        assertEquals(results.get(0), guid);
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }
}
