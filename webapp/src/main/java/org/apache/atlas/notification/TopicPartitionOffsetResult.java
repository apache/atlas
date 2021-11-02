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

import org.apache.atlas.notification.pc.Ticket;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public class TopicPartitionOffsetResult {
    private final TopicPartition topicPartition;
    private final long offset;
    private final String key;

    private Set<String> additionalInfo;

    public TopicPartitionOffsetResult(TopicPartition topicPartition, long offset) {
        this.topicPartition = topicPartition;
        this.offset = offset;
        this.key = toKey();
    }

    public TopicPartition getTopicPartition() {
        return this.topicPartition;
    }

    public long getOffset() {
        return this.offset;
    }

    public Set<String> getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(Set<String> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String getKey() {
        return this.key;
    }
    private String toKey() {
        return Ticket.getKey(getTopicPartition().topic(), getTopicPartition().partition(), getOffset());
    }
}
