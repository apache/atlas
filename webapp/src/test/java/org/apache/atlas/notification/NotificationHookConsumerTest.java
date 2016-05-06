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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.LocalAtlasClient;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class NotificationHookConsumerTest {

    @Mock
    private NotificationInterface notificationInterface;

    @Mock
    private LocalAtlasClient atlasClient;

    @Mock
    private Configuration configuration;

    @Mock
    private ExecutorService executorService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConsumerCanProceedIfServerIsReady() throws InterruptedException, AtlasServiceException {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenReturn(true);

        assertTrue(hookConsumer.serverAvailable(timer));

        verifyZeroInteractions(timer);
    }

    @Test
    public void testConsumerWaitsNTimesIfServerIsNotReadyNTimes() throws AtlasServiceException, InterruptedException {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenReturn(false, false, false, true);

        assertTrue(hookConsumer.serverAvailable(timer));

        verify(timer, times(3)).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
    }

    @Test
    public void testConsumerProceedsWithFalseIfInterrupted() throws AtlasServiceException, InterruptedException {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        doThrow(new InterruptedException()).when(timer).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
        when(atlasClient.isServerReady()).thenReturn(false);

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumerProceedsWithFalseOnAtlasServiceException() throws AtlasServiceException {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenThrow(new AtlasServiceException(AtlasClient.API.VERSION,
                new Exception()));

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumersStartedIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        notificationHookConsumer.startInternal(configuration, executorService);
        verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK, 1);
        verify(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreNotStartedIfHAIsEnabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        notificationHookConsumer.startInternal(configuration, executorService);
        verifyZeroInteractions(notificationInterface);
    }

    @Test
    public void testConsumersAreStartedWhenInstanceBecomesActive() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsActive();
        verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK, 1);
        verify(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreStoppedWhenInstanceBecomesPassive() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasClient);
        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsPassive();
        verify(notificationInterface).close();
        verify(executorService).shutdownNow();
    }
}
