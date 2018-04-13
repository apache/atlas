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

package org.apache.atlas.repository.graphdb.janus.migration.pc;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class WorkItemConsumer<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkItemConsumer.class);

    private static final int POLLING_DURATION_SECONDS = 5;

    private final BlockingQueue<T> queue;
    private       boolean          isDirty              = false;
    private       long             maxCommitTimeSeconds = 0;

    public WorkItemConsumer(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                T item = queue.poll(POLLING_DURATION_SECONDS, TimeUnit.SECONDS);

                if (item == null) {
                    commitDirty();
                    return;
                }

                isDirty = true;

                processItem(item);
            } catch (InterruptedException e) {
                LOG.error("WorkItemConsumer: Interrupted: ", e);
            }
        }
    }

    public long getMaxCommitTimeSeconds() {
        return (this.maxCommitTimeSeconds > 0 ? this.maxCommitTimeSeconds : 15);
    }

    protected void commitDirty() {
        if (!isDirty) {
            return;
        }

        LOG.info("isDirty");
        commit();
    }

    protected void commit() {
        Stopwatch sw = Stopwatch.createStarted();

        doCommit();

        sw.stop();

        updateCommitTime(sw.elapsed(TimeUnit.SECONDS));

        isDirty = false;
    }

    protected abstract void doCommit();

    protected abstract void processItem(T item);

    protected void updateCommitTime(long commitTime) {
        if (this.maxCommitTimeSeconds < commitTime) {
            this.maxCommitTimeSeconds = commitTime;
        }
    }
}
