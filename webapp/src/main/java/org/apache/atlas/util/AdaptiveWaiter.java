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
package org.apache.atlas.util;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptiveWaiter {
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveWaiter.class);

    private final long increment;
    private final long maxDuration;
    private final long minDuration;
    private final long resetInterval;
    private       long lastWaitAt;

    @VisibleForTesting
    public long waitDuration;

    public AdaptiveWaiter(long minDuration, long maxDuration, long increment) {
        this.minDuration   = minDuration;
        this.maxDuration   = maxDuration;
        this.increment     = increment;
        this.waitDuration  = minDuration;
        this.lastWaitAt    = 0;
        this.resetInterval = maxDuration * 2;
    }

    public void pause(Exception ex) {
        setWaitDurations();

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} in NotificationHookConsumer. Waiting for {} ms for recovery.", ex.getClass().getName(), waitDuration, ex);
            }

            Thread.sleep(waitDuration);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} in NotificationHookConsumer. Waiting for recovery interrupted.", ex.getClass().getName(), e);
            }
        }
    }

    private void setWaitDurations() {
        long timeSinceLastWait = (lastWaitAt == 0) ? 0 : System.currentTimeMillis() - lastWaitAt;

        lastWaitAt = System.currentTimeMillis();

        if (timeSinceLastWait > resetInterval) {
            waitDuration = minDuration;
        } else {
            waitDuration += increment;
            if (waitDuration > maxDuration) {
                waitDuration = maxDuration;
            }
        }
    }

    public long getWaitDuration() {
        return this.waitDuration;
    }
}

