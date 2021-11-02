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

import org.apache.atlas.notification.TopicPartitionOffsetResult;
import org.apache.atlas.pc.WorkItemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class Manager extends WorkItemManager<Ticket, Consumer> {
    private static final Logger LOG = LoggerFactory.getLogger(Manager.class);
    private static final String MANAGER_NAME_FORMAT = "ingest";

    private final ReferenceKeeper referenceKeeper;

    public Manager(ReferenceKeeper referenceKeeper, int batchSize, int numWorkers, ConsumerBuilder consumerBuilder) {
        super(consumerBuilder, String.format(MANAGER_NAME_FORMAT), batchSize, numWorkers, true);

        this.referenceKeeper = referenceKeeper;
    }

    public TopicPartitionOffsetResult submit(Ticket ticket) {
        try {
            if (!ticket.isMessageHandled()) {
                this.drain();
            }

            this.referenceKeeper.register(ticket);
            super.checkProduce(ticket);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: {}: Types: {}, QNames: {}, Refs: {}", ticket.getKey(), ticket.getMessage().getMessage().getType().name(),
                        ticket.getTypes(), ticket.getQualifiedNamesSet(), ticket.getReferencedSet());
            }
        }
        catch (Exception e) {
            LOG.error("{}: Error: Could not submit!", ticket.getKey(), e);
        }

        return this.referenceKeeper.getCachedResult();
    }

    public void shutdown() {
        try {
            super.shutdown();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted!");
        }
    }

    public TopicPartitionOffsetResult getResult() {
        extractAccumulatedResults();
        return this.referenceKeeper.getResult();
    }

    private void extractAccumulatedResults() {
        List<TopicPartitionOffsetResult> res = super.getTypedResults();
        if (CollectionUtils.isEmpty(res)) {
            return;
        }

        res.parallelStream().forEach(x -> this.referenceKeeper.deregister(x));
        res.clear();
    }
}
