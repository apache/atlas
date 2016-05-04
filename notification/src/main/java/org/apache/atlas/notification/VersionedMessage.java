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

/**
 * Represents a notification message that is associated with a version.
 */
public class VersionedMessage<T> {

    /**
     * The version of the message.
     */
    private final MessageVersion version;

    /**
     * The actual message.
     */
    private final T message;


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a versioned message.
     *
     * @param version  the message version
     * @param message  the actual message
     */
    public VersionedMessage(MessageVersion version, T message) {
        this.version = version;
        this.message = message;
    }


    // ----- VersionedMessage ------------------------------------------------

    /**
     * Compare the version of this message with the given version.
     *
     * @param compareToVersion  the version to compare to
     *
     * @return a negative integer, zero, or a positive integer as this message's version is less than, equal to,
     *         or greater than the given version.
     */
    public int compareVersion(MessageVersion compareToVersion) {
        return version.compareTo(compareToVersion);
    }


    // ----- accessors -------------------------------------------------------

    public MessageVersion getVersion() {
        return version;
    }

    public T getMessage() {
        return message;
    }
}
