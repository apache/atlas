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
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class NotificationHookConsumerTest {

    @Test
    public void testConsumerCanProceedIfServerIsReady() throws InterruptedException, AtlasServiceException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer();
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(atlasClient, mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenReturn(true);

        assertTrue(hookConsumer.serverAvailable(timer));

        verifyZeroInteractions(timer);
    }

    @Test
    public void testConsumerWaitsNTimesIfServerIsNotReadyNTimes() throws AtlasServiceException, InterruptedException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer();
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(atlasClient, mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenReturn(false, false, false, true);

        assertTrue(hookConsumer.serverAvailable(timer));

        verify(timer, times(3)).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
    }

    @Test
    public void testConsumerProceedsWithFalseIfInterrupted() throws AtlasServiceException, InterruptedException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer();
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(atlasClient, mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        doThrow(new InterruptedException()).when(timer).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
        when(atlasClient.isServerReady()).thenReturn(false);

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumerProceedsWithFalseOnAtlasServiceException() throws AtlasServiceException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer();
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(atlasClient, mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(atlasClient.isServerReady()).thenThrow(new AtlasServiceException(AtlasClient.API.VERSION,
                new Exception()));

        assertFalse(hookConsumer.serverAvailable(timer));
    }
}
