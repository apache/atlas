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
package org.apache.atlas.notification.pc;

import org.apache.atlas.notification.TopicPartitionOffsetResult;
import org.apache.atlas.pc.StatusReporter;

public class ResultsCollector {
    private static final long STATUS_REPORT_TIMEOUT_DURATION = 20 * 1000; // 20 secs

    private final StatusReporter<String, TopicPartitionOffsetResult> statusReporter;

    private TopicPartitionOffsetResult storedResult;

    public ResultsCollector() {
        this.statusReporter = new StatusReporter<>(STATUS_REPORT_TIMEOUT_DURATION);
    }

    public TopicPartitionOffsetResult getCached() {
        return this.storedResult;
    }

    public TopicPartitionOffsetResult get() {
        this.storedResult = statusReporter.ack();
        return this.storedResult;
    }

    public void markProduced(String key, TopicPartitionOffsetResult value) {
        this.statusReporter.produced(key, value);
    }

    public void markProcessed(TopicPartitionOffsetResult result) { this.statusReporter.processed(result.getKey(), result); }
}
