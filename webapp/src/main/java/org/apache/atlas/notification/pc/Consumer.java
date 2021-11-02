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
package org.apache.atlas.notification.pc;

import org.apache.atlas.notification.SerialEntityProcessor;
import org.apache.atlas.notification.TopicPartitionOffsetResult;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Consumer extends WorkItemConsumer<Ticket> {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private ReferenceKeeper referenceKeeper;
    private final SerialEntityProcessor serialEntityCreator;

    private final AtomicLong counter;
    private String lastKey;

    public Consumer(ReferenceKeeper referenceKeeper,
                    SerialEntityProcessor serialEntityCreator,
                    BlockingQueue queue) {
        super(queue);
        this.referenceKeeper        = referenceKeeper;
        this.serialEntityCreator    = serialEntityCreator;
        this.counter                = new AtomicLong(0);
    }

    @Override
    protected void processItem(Ticket ticket) {
        TopicPartitionOffsetResult result = null;
        long count = counter.incrementAndGet();

        List<String> dependents = ticket.getDependents();
        try {
            lastKey = ticket.getKey();

            referenceKeeper.awaitDependents(dependents);

            result = serialEntityCreator.handleMessage(ticket);
        } catch (Exception e) {
            LOG.error("Error handling message: {}", AtlasType.toJson(ticket.msg), e);
        } finally {
            this.referenceKeeper.countDown(ticket.getKey());
            if (result == null) {
                result = ticket.createTopicPartitionOffsetResult();
            }

            result.setAdditionalInfo(ticket.getQualifiedNamesSet());
            addResult(result);
            if (LOG.isDebugEnabled()) {
                LOG.info("Total: {}: Ticket: {}: Unlocking: {}: QNames: {}: Refs: {}",
                        count, ticket.getKey(), dependents,
                        ticket.getQualifiedNamesSet(), ticket.getReferencedSet());
            }

            LOG.info("Total: {}: Ticket: {}: Dependents: {}:", count, ticket.getKey(), dependents);
        }
    }

    @Override
    protected void doCommit() {
    }

    @Override
    protected void commitDirty() {
        super.commitDirty();
        LOG.debug("{}: Total: {}", lastKey, counter.get());
    }
}
