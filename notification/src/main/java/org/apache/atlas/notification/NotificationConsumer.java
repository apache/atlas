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
 * Atlas notification consumer.  This consumer blocks until a notification can be read.
 *
 * @param <T>  the class type of notifications returned by this consumer
 */
public interface NotificationConsumer<T> {
    /**
     * Returns true when the consumer has more notifications.  Blocks until a notification becomes available.
     *
     * @return true when the consumer has notifications to be read
     */
    boolean hasNext();

    /**
     * Returns the next notification.
     *
     * @return the next notification
     */
    T next();

    /**
     * Returns the next notification without advancing.
     *
     * @return the next notification
     */
    T peek();

    /**
     * Commit the offset of messages that have been successfully processed.
     *
     * This API should be called when messages read with {@link #next()} have been successfully processed and
     * the consumer is ready to handle the next message, which could happen even after a normal or an abnormal
     * restart.
     */
    void commit();

    void close();
}
