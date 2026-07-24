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
package org.apache.atlas.services;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.FailedEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents exactly one worker batch outcome.
 * Workers enqueue this result instead of mutating PurgeExecutionStats directly.
 */
class PurgeBatchResult {
    private final Set<String> batchGuids;
    private final List<AtlasEntityHeader> purgedEntities;
    private final List<FailedEntity> failedEntities;
    private final boolean hasBatchException;
    private final Throwable batchException;

    public PurgeBatchResult(Set<String> batchGuids, List<AtlasEntityHeader> purgedEntities,
                            List<FailedEntity> failedEntities, boolean hasBatchException,
                            Throwable batchException) {
        // Use defensive copies since worker collections might be cleared after execution
        this.batchGuids = batchGuids != null ? new LinkedHashSet<>(batchGuids) : Collections.emptySet();
        this.purgedEntities = purgedEntities != null ? new ArrayList<>(purgedEntities) : Collections.emptyList();
        this.failedEntities = failedEntities != null ? new ArrayList<>(failedEntities) : Collections.emptyList();
        this.hasBatchException = hasBatchException;
        this.batchException = batchException;
    }

    public Set<String> getBatchGuids() {
        return batchGuids;
    }

    public List<AtlasEntityHeader> getPurgedEntities() {
        return purgedEntities;
    }

    public List<FailedEntity> getFailedEntities() {
        return failedEntities;
    }

    public boolean hasBatchException() {
        return hasBatchException;
    }

    public Throwable getBatchException() {
        return batchException;
    }
}
