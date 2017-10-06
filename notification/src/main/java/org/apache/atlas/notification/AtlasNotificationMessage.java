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

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

/**
 * Represents a notification message that is associated with a version.
 */
public class AtlasNotificationMessage<T> extends AtlasNotificationBaseMessage {
    private String msgSourceIP;
    private String msgCreatedBy;
    private long   msgCreationTime;

    /**
     * The actual message.
     */
    private final T message;


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a notification message.
     *
     * @param version  the message version
     * @param message  the actual message
     */
    public AtlasNotificationMessage(MessageVersion version, T message) {
        this(version, message, null, null);
    }

    public AtlasNotificationMessage(MessageVersion version, T message, String msgSourceIP, String createdBy) {
        super(version);

        this.msgSourceIP     = msgSourceIP;
        this.msgCreatedBy    = createdBy;
        this.msgCreationTime = Instant.now().toDateTime(DateTimeZone.UTC).getMillis();
        this.message         = message;
    }


    public String getMsgSourceIP() {
        return msgSourceIP;
    }

    public void setMsgSourceIP(String msgSourceIP) {
        this.msgSourceIP = msgSourceIP;
    }

    public String getMsgCreatedBy() {
        return msgCreatedBy;
    }

    public void setMsgCreatedBy(String msgCreatedBy) {
        this.msgCreatedBy = msgCreatedBy;
    }

    public long getMsgCreationTime() {
        return msgCreationTime;
    }

    public void setMsgCreationTime(long msgCreationTime) {
        this.msgCreationTime = msgCreationTime;
    }

    public T getMessage() {
        return message;
    }
}
