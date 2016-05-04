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

package org.apache.atlas.hook;

import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.hook.HookNotification;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;


public class AtlasHookTest {
    @Test
    public void testnotifyEntities() throws Exception{
        List<HookNotification.HookNotificationMessage> hookNotificationMessages = new ArrayList<>();
        NotificationInterface notifInterface = mock(NotificationInterface.class);
        doThrow(new NotificationException(new Exception())).when(notifInterface)
                .send(NotificationInterface.NotificationType.HOOK, hookNotificationMessages);
        AtlasHook.notifInterface = notifInterface;
        AtlasHook.notifyEntities(hookNotificationMessages, 2);
        System.out.println("AtlasHook.notifyEntities() returns successfully");
    }
}
