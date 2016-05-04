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

import com.google.gson.Gson;
import org.slf4j.Logger;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Deserializer that works with versioned messages.  The version of each deserialized message is checked against an
 * expected version.
 */
public abstract class VersionedMessageDeserializer<T> implements MessageDeserializer<T> {

    public static final String VERSION_MISMATCH_MSG =
        "Notification message version mismatch.  Expected %s but recieved %s";

    private final Type versionedMessageType;
    private final MessageVersion expectedVersion;
    private final Logger notificationLogger;
    private final Gson gson;


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a versioned message deserializer.
     *
     * @param versionedMessageType  the type of the versioned message
     * @param expectedVersion       the expected message version
     * @param gson                  JSON serialization/deserialization
     * @param notificationLogger    logger for message version mismatch
     */
    public VersionedMessageDeserializer(Type versionedMessageType, MessageVersion expectedVersion,
                                        Gson gson, Logger notificationLogger) {
        this.versionedMessageType = versionedMessageType;
        this.expectedVersion = expectedVersion;
        this.gson = gson;
        this.notificationLogger = notificationLogger;
    }


    // ----- MessageDeserializer ---------------------------------------------

    @Override
    public T deserialize(String messageJson) {
        VersionedMessage<T> versionedMessage = gson.fromJson(messageJson, versionedMessageType);

        // older style messages not wrapped with VersionedMessage
        if (versionedMessage.getVersion() == null) {
            Type t = ((ParameterizedType) versionedMessageType).getActualTypeArguments()[0];
            versionedMessage = new VersionedMessage<>(MessageVersion.NO_VERSION, gson.<T>fromJson(messageJson, t));
        }
        checkVersion(versionedMessage, messageJson);

        return versionedMessage.getMessage();
    }


    // ----- helper methods --------------------------------------------------

    /**
     * Check the message version against the expected version.
     *
     * @param versionedMessage  the versioned message
     * @param messageJson       the notification message json
     *
     * @throws IncompatibleVersionException  if the message version is incompatable with the expected version
     */
    protected void checkVersion(VersionedMessage<T> versionedMessage, String messageJson) {
        int comp = versionedMessage.compareVersion(expectedVersion);

        // message has newer version
        if (comp > 0) {
            String msg = String.format(VERSION_MISMATCH_MSG, expectedVersion, versionedMessage.getVersion());
            notificationLogger.error(msg);
            notificationLogger.info(messageJson);
            throw new IncompatibleVersionException(msg);
        }

        // message has older version
        if (comp < 0) {
            notificationLogger.info(
                String.format(VERSION_MISMATCH_MSG, expectedVersion, versionedMessage.getVersion()));

            notificationLogger.info(messageJson);
        }
    }
}
