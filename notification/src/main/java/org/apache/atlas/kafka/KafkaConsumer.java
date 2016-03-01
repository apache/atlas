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
package org.apache.atlas.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.atlas.notification.AbstractNotificationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka specific notification consumer.
 *
 * @param <T>  the notification type returned by this consumer
 */
public class KafkaConsumer<T> extends AbstractNotificationConsumer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private final int consumerId;
    private final ConsumerIterator iterator;


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a Kafka consumer.
     *
     * @param type        the notification type returned by this consumer
     * @param stream      the underlying Kafka stream
     * @param consumerId  an id value for this consumer
     */
    public KafkaConsumer(Class<T> type, KafkaStream<String, String> stream, int consumerId) {
        super(type);
        this.iterator = stream.iterator();
        this.consumerId = consumerId;
    }


    // ----- NotificationConsumer --------------------------------------------

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }


    // ----- AbstractNotificationConsumer ------------------------------------

    @Override
    public String getNext() {
        MessageAndMetadata message = iterator.next();
        LOG.debug("Read message: conumerId: {}, topic - {}, partition - {}, offset - {}, message - {}",
                consumerId, message.topic(), message.partition(), message.offset(), message.message());
        return (String) message.message();
    }

    @Override
    protected String peekMessage() {
        MessageAndMetadata message = (MessageAndMetadata) iterator.peek();
        return (String) message.message();
    }
}
