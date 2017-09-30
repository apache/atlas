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
import org.apache.atlas.notification.AtlasNotificationBaseMessage.CompressionKind;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializer that works with notification messages.  The version of each deserialized message is checked against an
 * expected version.
 */
public abstract class AtlasNotificationMessageDeserializer<T> implements MessageDeserializer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasNotificationMessageDeserializer.class);


    public static final String VERSION_MISMATCH_MSG =
        "Notification message version mismatch. Expected %s but recieved %s. Message %s";

    private final Type notificationMessageType;
    private final Type messageType;
    private final MessageVersion expectedVersion;
    private final Logger notificationLogger;
    private final Gson gson;


    private final Map<String, AtlasNotificationStringMessage[]> splitMsgBuffer = new HashMap<>();

    // ----- Constructors ----------------------------------------------------

    /**
     * Create a notification message deserializer.
     *
     * @param notificationMessageType the type of the notification message
     * @param expectedVersion         the expected message version
     * @param gson                    JSON serialization/deserialization
     * @param notificationLogger      logger for message version mismatch
     */
    public AtlasNotificationMessageDeserializer(Type notificationMessageType, MessageVersion expectedVersion,
                                                Gson gson, Logger notificationLogger) {
        this.notificationMessageType = notificationMessageType;
        this.messageType             = ((ParameterizedType) notificationMessageType).getActualTypeArguments()[0];
        this.expectedVersion         = expectedVersion;
        this.gson                    = gson;
        this.notificationLogger      = notificationLogger;
    }

    // ----- MessageDeserializer ---------------------------------------------

    @Override
    public T deserialize(String messageJson) {
        final T ret;

        AtlasNotificationBaseMessage msg = gson.fromJson(messageJson, AtlasNotificationBaseMessage.class);

        if (msg.getVersion() == null) { // older style messages not wrapped with AtlasNotificationMessage
            ret = gson.fromJson(messageJson, messageType);
        } else  {
            String msgJson = messageJson;

            if (msg.getMsgSplitCount() > 1) { // multi-part message
                AtlasNotificationStringMessage splitMsg = gson.fromJson(msgJson, AtlasNotificationStringMessage.class);

                checkVersion(splitMsg, msgJson);

                String msgId = splitMsg.getMsgId();

                if (StringUtils.isEmpty(msgId)) {
                    LOG.error("Received multi-part message with no message ID. Ignoring message");

                    msg = null;
                } else {
                    final int splitIdx   = splitMsg.getMsgSplitIdx();
                    final int splitCount = splitMsg.getMsgSplitCount();

                    final AtlasNotificationStringMessage[] splitMsgs;

                    if (splitIdx == 0) {
                        splitMsgs = new AtlasNotificationStringMessage[splitCount];

                        splitMsgBuffer.put(msgId, splitMsgs);
                    } else {
                        splitMsgs = splitMsgBuffer.get(msgId);
                    }

                    if (splitMsgs == null) {
                        LOG.error("Received msgID={}: {} of {}, but first message didn't arrive. Ignoring message", msgId, splitIdx + 1, splitCount);

                        msg = null;
                    } else if (splitMsgs.length <= splitIdx) {
                        LOG.error("Received msgID={}: {} of {} - out of bounds. Ignoring message", msgId, splitIdx + 1, splitCount);

                        msg = null;
                    } else {
                        LOG.info("Received msgID={}: {} of {}", msgId, splitIdx + 1, splitCount);

                        splitMsgs[splitIdx] = splitMsg;

                        if (splitIdx == (splitCount - 1)) { // last message
                            splitMsgBuffer.remove(msgId);

                            boolean isValidMessage = true;

                            StringBuilder sb = new StringBuilder();

                            for (int i = 0; i < splitMsgs.length; i++) {
                                splitMsg = splitMsgs[i];

                                if (splitMsg == null) {
                                    LOG.warn("MsgID={}: message {} of {} is missing. Ignoring message", msgId, i + 1, splitCount);

                                    isValidMessage = false;

                                    break;
                                }

                                sb.append(splitMsg.getMessage());
                            }

                            if (isValidMessage) {
                                msgJson = sb.toString();

                                if (CompressionKind.GZIP.equals(splitMsg.getMsgCompressionKind())) {
                                    byte[] encodedBytes = AtlasNotificationBaseMessage.getBytesUtf8(msgJson);
                                    byte[] bytes        = AtlasNotificationBaseMessage.decodeBase64AndGzipUncompress(encodedBytes);

                                    msgJson = AtlasNotificationBaseMessage.getStringUtf8(bytes);

                                    LOG.info("Received msgID={}: splitCount={}, compressed={} bytes, uncompressed={} bytes", msgId, splitCount, encodedBytes.length, bytes.length);
                                } else {
                                    byte[] encodedBytes = AtlasNotificationBaseMessage.getBytesUtf8(msgJson);
                                    byte[] bytes        = AtlasNotificationBaseMessage.decodeBase64(encodedBytes);

                                    msgJson = AtlasNotificationBaseMessage.getStringUtf8(bytes);

                                    LOG.info("Received msgID={}: splitCount={}, length={} bytes", msgId, splitCount, bytes.length);
                                }

                                msg = gson.fromJson(msgJson, AtlasNotificationBaseMessage.class);
                            } else {
                                msg = null;
                            }
                        } else { // more messages to arrive
                            msg = null;
                        }
                    }
                }
            }

            if (msg != null) {
                if (CompressionKind.GZIP.equals(msg.getMsgCompressionKind())) {
                    AtlasNotificationStringMessage compressedMsg = gson.fromJson(msgJson, AtlasNotificationStringMessage.class);

                    byte[] encodedBytes = AtlasNotificationBaseMessage.getBytesUtf8(compressedMsg.getMessage());
                    byte[] bytes        = AtlasNotificationBaseMessage.decodeBase64AndGzipUncompress(encodedBytes);

                    msgJson = AtlasNotificationBaseMessage.getStringUtf8(bytes);

                    LOG.info("Received msgID={}: compressed={} bytes, uncompressed={} bytes", compressedMsg.getMsgId(), encodedBytes.length, bytes.length);
                }

                AtlasNotificationMessage<T> atlasNotificationMessage = gson.fromJson(msgJson, notificationMessageType);

                checkVersion(atlasNotificationMessage, msgJson);

                ret = atlasNotificationMessage.getMessage();
            } else {
                ret = null;
            }
        }

        return ret;
    }

    // ----- helper methods --------------------------------------------------

    /**
     * Check the message version against the expected version.
     *
     * @param notificationMessage the notification message
     * @param messageJson         the notification message json
     *
     * @throws IncompatibleVersionException  if the message version is incompatable with the expected version
     */
    protected void checkVersion(AtlasNotificationBaseMessage notificationMessage, String messageJson) {
        int comp = notificationMessage.compareVersion(expectedVersion);

        // message has newer version
        if (comp > 0) {
            String msg = String.format(VERSION_MISMATCH_MSG, expectedVersion, notificationMessage.getVersion(), messageJson);

            notificationLogger.error(msg);

            throw new IncompatibleVersionException(msg);
        }

        // message has older version
        if (comp < 0) {
            notificationLogger.info(String.format(VERSION_MISMATCH_MSG, expectedVersion, notificationMessage.getVersion(), messageJson));
        }
    }
}
