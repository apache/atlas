/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.services;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class PurgeBatchExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeBatchExecutor.class);

    private static final int MAX_RETRIES     = 3;
    private static final int BASE_BACKOFF_MS = 500;

    /**
     * Fully-qualified class names treated as retryable lock or backend conflicts during purge batch
     * execution. Names are matched against the throwable cause chain to avoid a compile-time dependency
     * on JanusGraph or Berkeley JE types in the service layer.
     * <p>
     * Design default: {@code PermanentLockingException}. Berkeley JE lock timeouts/deadlocks and
     * {@code PermanentBackendException} are included for the embedded Berkeley backend.
     */
    static final Set<String> RETRYABLE_LOCK_CONFLICT_EXCEPTION_CLASS_NAMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "org.janusgraph.diskstorage.locking.PermanentLockingException",
                    "com.sleepycat.je.LockTimeoutException",
                    "com.sleepycat.je.DeadlockException",
                    "org.janusgraph.diskstorage.PermanentBackendException")));

    private final AtlasEntityStore entityStore;

    public PurgeBatchExecutor(AtlasEntityStore entityStore) {
        this.entityStore = entityStore;
    }

    public AtlasEntityStore getEntityStore() {
        return entityStore;
    }

    public EntityMutationResponse executeBatch(Set<String> batch) throws AtlasBaseException {
        return withRetry(() -> entityStore.purgeEntitiesInBatch(batch));
    }

    /**
     * Returns {@code true} when {@code throwable} or any of its causes matches a known retryable
     * lock or backend conflict type.
     */
    static boolean isRetryableLockConflict(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        for (Throwable c = throwable; c != null; c = c.getCause()) {
            if (RETRYABLE_LOCK_CONFLICT_EXCEPTION_CLASS_NAMES.contains(c.getClass().getName())) {
                return true;
            }
        }

        return false;
    }

    private <T> T withRetry(Callable<T> action) throws AtlasBaseException {
        int attempt = 0;

        while (true) {
            try {
                return action.call();
            } catch (Throwable e) {
                boolean canRetry = isRetryableLockConflict(e) && attempt < (MAX_RETRIES - 1);
                if (canRetry) {
                    GraphTransactionInterceptor.clearCache();
                    RequestContext.get().clearCache();

                    long backoff = (long) BASE_BACKOFF_MS * (attempt + 1);
                    LOG.warn("Lock conflict for purge batch on attempt {}/{}, backing off {} ms",
                            attempt + 1, MAX_RETRIES, backoff);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    attempt++;
                    continue;
                }

                LOG.error("Failed to process purge batch on attempt {}/{}", attempt + 1, MAX_RETRIES, e);
                if (e instanceof AtlasBaseException) {
                    throw (AtlasBaseException) e;
                }
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }
    }
}
