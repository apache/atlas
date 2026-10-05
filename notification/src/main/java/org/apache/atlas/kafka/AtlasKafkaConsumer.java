/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.kafka;

import org.apache.atlas.notification.AbstractNotificationConsumer;
import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka specific notification consumer.
 *
 * @param <T> the notification type returned by this consumer
 */
public class AtlasKafkaConsumer<T> extends AbstractNotificationConsumer<T> {
    private static final Logger   LOG           = LoggerFactory.getLogger(AtlasKafkaConsumer.class);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private volatile KafkaConsumer kafkaConsumer;
    private final    boolean       autoCommitEnabled;
    private final    long          pollTimeoutMilliSeconds;
    private final    Duration      duration;
    private final    Properties    consumerProperties;
    private final    List<String>  topics;

    public AtlasKafkaConsumer(NotificationInterface.NotificationType notificationType, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds) {
        this(notificationType.getDeserializer(), kafkaConsumer, autoCommitEnabled, pollTimeoutMilliSeconds, null, null);
    }

    public AtlasKafkaConsumer(NotificationInterface.NotificationType notificationType, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds,
            Properties consumerProperties, Collection<String> topics) {
        this(notificationType.getDeserializer(), kafkaConsumer, autoCommitEnabled, pollTimeoutMilliSeconds, consumerProperties, topics);
    }

    public AtlasKafkaConsumer(AtlasNotificationMessageDeserializer<T> deserializer, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds) {
        this(deserializer, kafkaConsumer, autoCommitEnabled, pollTimeoutMilliSeconds, null, null);
    }

    public AtlasKafkaConsumer(AtlasNotificationMessageDeserializer<T> deserializer, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds,
            Properties consumerProperties, Collection<String> topics) {
        super(deserializer);

        this.autoCommitEnabled       = autoCommitEnabled;
        this.kafkaConsumer           = kafkaConsumer;
        this.pollTimeoutMilliSeconds = pollTimeoutMilliSeconds;
        this.duration                = Duration.ofMillis(pollTimeoutMilliSeconds);
        this.consumerProperties      = consumerProperties;
        this.topics                  = topics == null ? Collections.emptyList() : new ArrayList<>(topics);
    }

    @Override
    public Set<TopicPartition> getTopicPartition() {
        return kafkaConsumer != null ? kafkaConsumer.assignment() : null;
    }

    @Override
    public Set<String> subscription() {
        return kafkaConsumer != null ? kafkaConsumer.subscription() : null;
    }

    @Override
    public void commit(TopicPartition partition, long offset) {
        if (!autoCommitEnabled) {
            LOG.debug(" committing the offset ==>> {}", offset);

            kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)));
        }
    }

    @Override
    public void close() {
        KafkaConsumer consumer = kafkaConsumer;

        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public void wakeup() {
        KafkaConsumer consumer = kafkaConsumer;

        if (consumer != null) {
            consumer.wakeup();
        }
    }

    @Override
    public void poll() {
        KafkaConsumer consumer = this.kafkaConsumer;

        if (consumer != null) {
            consumer.poll(this.duration);
        }
    }

    @Override
    public synchronized void recover() {
        if (consumerProperties == null || topics.isEmpty()) {
            LOG.warn("Cannot recreate Kafka consumer: missing properties or subscribed topics");
            return;
        }

        LOG.warn("Recreating Kafka consumer for topics {}", topics);

        KafkaConsumer previous = this.kafkaConsumer;

        if (previous != null) {
            try {
                previous.close(CLOSE_TIMEOUT);
            } catch (Exception e) {
                LOG.warn("Failed to close Kafka consumer during recover", e);
            }
        }

        KafkaConsumer created = new KafkaConsumer(consumerProperties);

        created.subscribe(topics);
        this.kafkaConsumer = created;
    }

    public List<AtlasKafkaMessage<T>> receive() {
        return this.receive(this.pollTimeoutMilliSeconds);
    }

    @Override
    public List<AtlasKafkaMessage<T>> receive(long timeoutMilliSeconds) {
        return receive(this.duration, null);
    }

    @Override
    public List<AtlasKafkaMessage<T>> receiveWithCheckedCommit(Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        return receive(this.duration, lastCommittedPartitionOffset);
    }

    @Override
    public List<AtlasKafkaMessage<T>> receiveRawRecordsWithCheckedCommit(Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        return receiveRawRecords(this.duration, lastCommittedPartitionOffset);
    }

    private List<AtlasKafkaMessage<T>> receiveRawRecords(Duration duration, Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        return receive(duration, lastCommittedPartitionOffset, true);
    }

    private List<AtlasKafkaMessage<T>> receive(Duration duration, Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        return receive(duration, lastCommittedPartitionOffset, false);
    }

    private List<AtlasKafkaMessage<T>> receive(Duration duration, Map<TopicPartition, Long> lastCommittedPartitionOffset, boolean isRawDataRequired) {
        List<AtlasKafkaMessage<T>> messages = new ArrayList<>();

        ConsumerRecords<?, ?> records = kafkaConsumer != null ? kafkaConsumer.poll(duration) : null;

        if (records != null) {
            for (ConsumerRecord<?, ?> record : records) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received Message topic ={}, partition ={}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                if (MapUtils.isNotEmpty(lastCommittedPartitionOffset)
                        && lastCommittedPartitionOffset.containsKey(topicPartition)
                        && record.offset() < lastCommittedPartitionOffset.get(topicPartition)) {
                    commit(topicPartition, record.offset());
                    LOG.info("Skipping already processed message: topic={}, partition={} offset={}. Last processed offset={}",
                            record.topic(), record.partition(), record.offset(), lastCommittedPartitionOffset.get(topicPartition));
                    continue;
                }

                T message = null;

                try {
                    message = deserializer.deserialize(record.value().toString());
                } catch (OutOfMemoryError excp) {
                    LOG.error("Ignoring message that failed to deserialize: topic={}, partition={}, offset={}, key={}, value={}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value(), excp);
                }

                if (message == null) {
                    continue;
                }

                AtlasKafkaMessage kafkaMessage;

                if (isRawDataRequired) {
                    kafkaMessage = new AtlasKafkaMessage(message, record.offset(), record.topic(), record.partition(), deserializer.getMsgCreated(), deserializer.getSpooled(), deserializer.getSource(), record.value().toString());
                } else {
                    kafkaMessage = new AtlasKafkaMessage(message, record.offset(), record.topic(), record.partition(), deserializer.getMsgCreated(), deserializer.getSpooled(), deserializer.getSource());
                }

                messages.add(kafkaMessage);
            }
        }

        return messages;
    }
}
