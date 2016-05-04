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

package org.apache.atlas.notification.hook;

import com.google.gson.JsonDeserializer;
import org.apache.atlas.notification.AbstractMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.NotificationInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

/**
 * Hook notification message deserializer.
 */
public class HookMessageDeserializer extends AbstractMessageDeserializer<HookNotification.HookNotificationMessage> {

    /**
     * Logger for hook notification messages.
     */
    private static final Logger NOTIFICATION_LOGGER = LoggerFactory.getLogger(HookMessageDeserializer.class);


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a hook notification message deserializer.
     */
    public HookMessageDeserializer() {
        super(NotificationInterface.HOOK_VERSIONED_MESSAGE_TYPE,
            AbstractNotification.CURRENT_MESSAGE_VERSION, getDeserializerMap(), NOTIFICATION_LOGGER);
    }


    // ----- helper methods --------------------------------------------------

    private static Map<Type, JsonDeserializer> getDeserializerMap() {
        return Collections.<Type, JsonDeserializer>singletonMap(
            NotificationInterface.HOOK_NOTIFICATION_CLASS, new HookNotification());
    }
}
