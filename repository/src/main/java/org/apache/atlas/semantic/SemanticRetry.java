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
package org.apache.atlas.semantic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter when {@link SemanticSearchException#isRetryable()} is true.
 */
public final class SemanticRetry {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticRetry.class);

    private SemanticRetry() {
    }

    public static <T> T run(String operationName, Callable<T> operation) throws SemanticSearchException {
        int  maxAttempts = SemanticSearchConfiguration.getRetryMaxAttempts();
        long baseSleepMs = SemanticSearchConfiguration.getRetrySleepMs();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.call();
            } catch (SemanticSearchException e) {
                if (!e.isRetryable() || attempt >= maxAttempts) {
                    throw e;
                }

                long sleepMs = backoffWithJitter(baseSleepMs, attempt);
                LOG.warn("{} failed on attempt {}/{}; retrying in {} ms: {}",
                        operationName, attempt, maxAttempts, sleepMs, e.getMessage());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SemanticSearchException("Retry interrupted for " + sleepMs + " ms wait", ie);
                }
            } catch (Exception e) {
                throw new SemanticSearchException(operationName + " failed", e);
            }
        }

        throw new SemanticSearchException(operationName + " failed after " + maxAttempts + " attempts");
    }

    /**
     * Full jitter on exponential cap: uniform in {@code [0, base * 2^(attempt-1)]}, minimum {@code base}.
     */
    private static long backoffWithJitter(long baseSleepMs, int attempt) {
        long cap = baseSleepMs * (1L << (attempt - 1));
        long jittered = ThreadLocalRandom.current().nextLong(0, cap + 1);
        return Math.max(baseSleepMs, jittered);
    }
}
