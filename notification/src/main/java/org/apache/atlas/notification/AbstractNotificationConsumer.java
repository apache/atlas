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
 * Abstract notification consumer.
 */
public abstract class AbstractNotificationConsumer<T> implements NotificationConsumer<T> {

    /**
     * Deserializer used to deserialize notification messages for this consumer.
     */
    private final MessageDeserializer<T> deserializer;


    // ----- Constructors ----------------------------------------------------

    /**
     * Construct an AbstractNotificationConsumer.
     *
     * @param deserializer  the message deserializer used by this consumer
     */
    public AbstractNotificationConsumer(MessageDeserializer<T> deserializer) {
        this.deserializer = deserializer;
    }


    // ----- AbstractNotificationConsumer -------------------------------------

    /**
     * Get the next notification as a string.
     *
     * @return the next notification in string form
     */
    protected abstract String getNext();

    /**
     * Get the next notification as a string without advancing.
     *
     * @return the next notification in string form
     */
    protected abstract String peekMessage();


    // ----- NotificationConsumer ---------------------------------------------

    @Override
    public T next() {
        return deserializer.deserialize(getNext());
    }

    @Override
    public T peek() {
        return deserializer.deserialize(peekMessage());
    }

    public abstract void commit();
}
