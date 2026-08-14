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
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReferenceKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(ReferenceKeeper.class);

    private final ReferenceFinder                   referenceFinder;
    private final Map<String, Map<String, Integer>> currentlyProcessedEntities;
    private final ResultsCollector                  resultsCollector;
    private final DependentsTracking dependentsTracking;

    public ReferenceKeeper() {
        this.currentlyProcessedEntities = new ConcurrentHashMap<>();
        this.referenceFinder            = new ReferenceFinder();
        this.resultsCollector           = new ResultsCollector();
        this.dependentsTracking = new DependentsTracking();
    }

    public void register(Ticket ticket) {
        this.dependentsTracking.addReference(ticket.getKey());
        this.resultsCollector.markProduced(ticket.getKey(), null);

        Set<String> refs = referenceFinder.find(this.currentlyProcessedEntities, ticket.getQualifiedNamesSet(), ticket.getReferencedSet());
        ticket.addDependents(refs);
        LOG.debug("Ticket: {}: References: {}: Total: {}", ticket.getKey(), refs.size(), this.currentlyProcessedEntities.size());
        addAll(ticket.getQualifiedNamesSet(), ticket.getKey());
    }

    private void addAll(Collection<String> entityKeys, String ticketKey) {
        entityKeys.parallelStream().forEach(x -> {
            if (!currentlyProcessedEntities.containsKey(x)) {
                currentlyProcessedEntities.put(x, new ConcurrentHashMap<>());
            }
            currentlyProcessedEntities.get(x).put(ticketKey, 0);
        });
    }

    public void removeAll(Collection<String> entityKeys, String ticketKey) {
        if (CollectionUtils.isEmpty(entityKeys)) {
            return;
        }

        entityKeys.parallelStream().forEach(key -> {
            if (currentlyProcessedEntities.containsKey(key)) {
                currentlyProcessedEntities.get(key).remove(ticketKey);
                if (currentlyProcessedEntities.get(key).size() == 0) {
                    currentlyProcessedEntities.remove(key);
                }
            }
        });
    }

    public TopicPartitionOffsetResult getCachedResult() {
        return this.resultsCollector.getCached();
    }

    public TopicPartitionOffsetResult getResult() {
        return this.resultsCollector.get();
    }

    public void deregister(TopicPartitionOffsetResult result) {
        removeAll(result.getAdditionalInfo(), result.getKey());

        this.resultsCollector.markProcessed(result);
        this.dependentsTracking.remove(result.getKey());
    }

    public void awaitDependents(List<String> dependents) throws InterruptedException {
        this.dependentsTracking.awaitDependents(dependents);
    }

    public void countDown(String key) {
        this.dependentsTracking.decrementReference(key);
    }

    public static class ReferenceFinder {
        public Set<String> find(Map<String, Map<String, Integer>> currentlyProcessedEntities, Set<String> qualifiedNamesSet, Set<String> referencedSet) {
            Set<String> ret = ConcurrentHashMap.newKeySet();

            isReferenced(currentlyProcessedEntities, ret, referencedSet);
            isReferenced(currentlyProcessedEntities, ret, qualifiedNamesSet);

            return ret;
        }

        private void isReferenced(Map<String, Map<String, Integer>> currentlyProcessedEntities, Set<String> ret, Set<String> incoming) {
            incoming.parallelStream().map(currentlyProcessedEntities::get).filter(Objects::nonNull).forEach(x -> ret.addAll(x.keySet()));
        }
    }
}
