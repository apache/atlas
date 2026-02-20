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

package org.apache.atlas.notification;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.notification.rest.RestNotification;
import org.apache.commons.configuration.Configuration;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.atlas.kafka.KafkaNotification.ATLAS_HOOK_TOPIC;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RestNotificationTest {
    private NotificationInterface notifier;
    private Configuration         conf;

    @Mock
    private WebResource service;

    @Mock
    private WebResource.Builder resourceBuilderMock;

    @BeforeClass
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        conf = ApplicationProperties.get();

        conf.setProperty(AtlasConfiguration.NOTIFICATION_HOOK_REST_ENABLED.getPropertyName(), true);
        conf.setProperty(NotificationProvider.CONF_ATLAS_HOOK_SPOOL_ENABLED, false);

        notifier = NotificationProvider.get();
    }

    @Test
    public void testNotificationProvider() {
        assertEquals(notifier.getClass(), RestNotification.class);
    }

    @Test
    public void testPostNotificationToTopic() {
        AtlasClientV2       client   = new AtlasClientV2(service, conf);
        AtlasBaseClient.API api      = client.formatPathWithParameter(AtlasClientV2.API_V2.POST_NOTIFICATIONS_TO_TOPIC, ATLAS_HOOK_TOPIC);
        WebResource.Builder builder  = setupBuilder(api, service);
        ClientResponse      response = mock(ClientResponse.class);

        when(response.getStatus()).thenReturn(Response.Status.NO_CONTENT.getStatusCode());
        when(builder.method(anyString(), ArgumentMatchers.<Class<ClientResponse>>any(), anyList())).thenReturn(response);

        ((RestNotification) notifier).atlasClientV2 = client;

        try {
            ((RestNotification) notifier).sendInternal(NotificationInterface.NotificationType.HOOK, new ArrayList<>(Collections.singletonList("Dummy")));
        } catch (NotificationException e) {
            fail("Failed with Exception");
        }
    }

    @Test
    public void testNotificationException() {
        AtlasClientV2       client   = new AtlasClientV2(service, conf);
        AtlasBaseClient.API api      = client.formatPathWithParameter(AtlasClientV2.API_V2.POST_NOTIFICATIONS_TO_TOPIC, ATLAS_HOOK_TOPIC);
        WebResource.Builder builder  = setupBuilder(api, service);
        ClientResponse      response = mock(ClientResponse.class);

        when(response.getStatus()).thenReturn(AtlasErrorCode.NOTIFICATION_EXCEPTION.getHttpCode().getStatusCode());
        when(response.getEntity(String.class)).thenReturn(AtlasErrorCode.NOTIFICATION_EXCEPTION.getErrorCode());
        when(builder.method(anyString(), ArgumentMatchers.<Class<ClientResponse>>any(), anyList())).thenReturn(response);

        ((RestNotification) notifier).atlasClientV2 = client;

        try {
            ((RestNotification) notifier).sendInternal(NotificationInterface.NotificationType.HOOK, new ArrayList<>(Collections.singletonList("Dummy")));
        } catch (NotificationException e) {
            assertTrue(e.getMessage().contains(AtlasErrorCode.NOTIFICATION_EXCEPTION.getErrorCode()));
        }
    }

    private WebResource.Builder setupBuilder(AtlasClientV2.API api, WebResource webResource) {
        when(webResource.path(api.getPath())).thenReturn(service);
        when(webResource.path(api.getNormalizedPath())).thenReturn(service);

        return getBuilder(service);
    }

    private WebResource.Builder getBuilder(WebResource resourceObject) {
        when(resourceObject.getRequestBuilder()).thenReturn(resourceBuilderMock);
        when(resourceObject.path(anyString())).thenReturn(resourceObject);
        when(resourceBuilderMock.accept(MediaType.APPLICATION_JSON)).thenReturn(resourceBuilderMock);
        when(resourceBuilderMock.type(MediaType.MULTIPART_FORM_DATA)).thenReturn(resourceBuilderMock);
        when(resourceBuilderMock.type(MediaType.APPLICATION_JSON + "; charset=UTF-8")).thenReturn(resourceBuilderMock);

        return resourceBuilderMock;
    }
}
