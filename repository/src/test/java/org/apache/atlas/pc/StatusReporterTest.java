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
package org.apache.atlas.pc;

import org.apache.commons.lang3.RandomUtils;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class StatusReporterTest {
    @Test
    public void statusReporting() throws InterruptedException {
        final int                                           maxItems       = 50;
        WorkItemBuilder<WorkItemConsumer<Integer>, Integer> cb             = new IntegerConsumerBuilder();
        WorkItemManager<Integer, WorkItemConsumer<Integer>> wi             = getWorkItemManger(cb, 5);
        StatusReporter<Integer, Integer>                    statusReporter = new StatusReporter<>();

        for (int i = 0; i < maxItems; i++) {
            wi.produce(i);

            statusReporter.produced(i, i);

            extractResults(wi, statusReporter);
        }

        wi.drain();

        extractResults(wi, statusReporter);

        assertEquals(statusReporter.ack().intValue(), (maxItems - 1));

        wi.shutdown();

        assertEquals(statusReporter.getProducedCount(), 0);
        assertEquals(statusReporter.getProcessedCount(), 0);
    }

    @Test
    public void reportWithTimeout() throws InterruptedException {
        StatusReporter<Integer, Integer> statusReporter = new StatusReporter<>(2000);
        statusReporter.produced(1, 100);
        statusReporter.produced(2, 200);

        statusReporter.processed(2);

        Integer ack = statusReporter.ack();

        assertNull(ack);

        Thread.sleep(3000);

        ack = statusReporter.ack();

        assertNotNull(ack);
        assertEquals(ack, Integer.valueOf(200));
    }

    private WorkItemManager<Integer, WorkItemConsumer<Integer>> getWorkItemManger(WorkItemBuilder<WorkItemConsumer<Integer>, Integer> cb, int numWorkers) {
        return new WorkItemManager<Integer, WorkItemConsumer<Integer>>(cb, "IntegerConsumer", 5, numWorkers, true);
    }

    private void extractResults(WorkItemManager<Integer, WorkItemConsumer<Integer>> wi, StatusReporter<Integer, Integer> statusReporter) {
        Object result;

        while ((result = wi.getResults().poll()) != null) {
            if (!(result instanceof Integer)) {
                continue;
            }

            statusReporter.processed((Integer) result);
        }
    }

    private static class IntegerConsumer extends WorkItemConsumer<Integer> {
        private static final ThreadLocal<Integer> payload = new ThreadLocal<>();

        private Integer current;

        public IntegerConsumer(BlockingQueue<Integer> queue) {
            super(queue);
        }

        @Override
        protected void doCommit() {
            addResult(current);
        }

        @Override
        protected void processItem(Integer item) {
            try {
                this.current = item;

                Thread.sleep(20 + RandomUtils.nextInt(5, 7));

                super.commit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class IntegerConsumerBuilder implements WorkItemBuilder<WorkItemConsumer<Integer>, Integer> {
        @Override
        public IntegerConsumer build(BlockingQueue<Integer> queue) {
            return new IntegerConsumer(queue);
        }
    }
}
