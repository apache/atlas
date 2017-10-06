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


public class AtlasNotificationStringMessage extends AtlasNotificationBaseMessage {
    private String message = null;

    public AtlasNotificationStringMessage() {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION);
    }

    public AtlasNotificationStringMessage(String message) {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION);

        this.message = message;
    }

    public AtlasNotificationStringMessage(String message, String msgId, CompressionKind compressionKind) {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION, msgId, compressionKind);

        this.message = message;
    }

    public AtlasNotificationStringMessage(String message, String msgId, CompressionKind compressionKind, int msgSplitIdx, int msgSplitCount) {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        this.message = message;
    }

    public AtlasNotificationStringMessage(byte[] encodedBytes, String msgId, CompressionKind compressionKind) {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION, msgId, compressionKind);

        this.message = AtlasNotificationBaseMessage.getStringUtf8(encodedBytes);
    }

    public AtlasNotificationStringMessage(byte[] encodedBytes, int offset, int length, String msgId, CompressionKind compressionKind, int msgSplitIdx, int msgSplitCount) {
        super(AbstractNotification.CURRENT_MESSAGE_VERSION, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        this.message = new String(encodedBytes, offset, length);
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
