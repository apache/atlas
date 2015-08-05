/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.atlas.notification.NotificationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer implements NotificationConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private final int consumerId;
    private final ConsumerIterator iterator;

    public KafkaConsumer(KafkaStream<String, String> stream, int consumerId) {
        this.iterator = stream.iterator();
        this.consumerId = consumerId;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public String next() {
        MessageAndMetadata message = iterator.next();
        LOG.debug("Read message: conumerId: {}, topic - {}, partition - {}, offset - {}, message - {}",
                consumerId, message.topic(), message.partition(), message.offset(), message.message());
        return (String) message.message();
    }
}
