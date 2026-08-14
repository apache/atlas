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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class DependentsTracking {
    private static final Logger LOG = LoggerFactory.getLogger(DependentsTracking.class);

    private final Map<String, CountDownLatch> objectSync;

    public DependentsTracking() {
        this.objectSync = new ConcurrentHashMap<>();
    }

    public CountDownLatch get(String key) {
        return objectSync.get(key);
    }

    public void addReference(String key) {
        this.objectSync.put(key, new CountDownLatch(1));
    }

    public void decrementReference(String key) {
        if (!objectSync.containsKey(key)) {
            LOG.warn("Key: {}: Not found!", key);
            return;
        }

        CountDownLatch countDownLatch = objectSync.get(key);
        countDownLatch.countDown();
    }

    public void remove(String key) {
        this.objectSync.remove(key);
    }

    public void awaitDependents(Collection<String> keysToLock) throws InterruptedException {
        for (String key : keysToLock) {
            CountDownLatch latch = objectSync.get(key);
            if (latch == null) {
                continue;
            }

            latch.await();
        }
    }

    public int size() {
        return this.objectSync.size();
    }
}
