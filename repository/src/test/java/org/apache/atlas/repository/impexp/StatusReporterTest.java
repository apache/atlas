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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.repository.store.graph.v2.bulkimport.pc.StatusReporter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class StatusReporterTest {
    @Test
    public void noneProducedNoneReported() {
        StatusReporter<Integer, Integer> statusReporter = new StatusReporter<>(100);
        assertNull(statusReporter.ack());
    }

    @Test
    public void producedButNotAcknowledged() {
        StatusReporter<Integer, Integer> statusReporter = createStatusReportWithItems();
        assertNull(statusReporter.ack());
    }

    @Test
    public void producedAcknowledged() {
        StatusReporter<Integer, Integer> statusReporter = createStatusReportWithItems();
        statusReporter.processed(1);

        assertEquals(java.util.Optional.of(100).get(), statusReporter.ack());
    }

    @Test
    public void producedAcknowledgeMaxAvailableInSequence() {
        StatusReporter<Integer, Integer> statusReporter = createStatusReportWithItems();

        statusReporter.processed(new Integer[]{1, 3, 5});

        assertEquals(java.util.Optional.of(100).get(), statusReporter.ack());
    }

    @Test
    public void producedAcknowledgeMaxAvailableInSequence2() {
        StatusReporter<Integer, Integer> statusReporter = createStatusReportWithItems();
        statusReporter.processed(new Integer[]{1, 2, 3, 6, 5});

        assertEquals(java.util.Optional.of(300).get(), statusReporter.ack());
    }

    @Test
    public void producedSetDisjointWithAckSet() {
        StatusReporter<Integer, Integer> statusReporter = new StatusReporter(100);
        statusReporter.produced(11, 1000);
        statusReporter.produced(12, 2000);
        statusReporter.produced(13, 3000);

        statusReporter.processed(new Integer[]{1, 11, 12, 13});

        assertEquals(java.util.Optional.of(3000).get(), statusReporter.ack());
    }

    @Test
    public void missingAck() throws InterruptedException {
        StatusReporter<Integer, Integer> statusReporter = createStatusReportWithItems(2, 3, 4);

        assertNull(statusReporter.ack());
        Thread.sleep(1002);
        assertEquals(java.util.Optional.of(100).get(), statusReporter.ack());
    }

    private StatusReporter<Integer, Integer> createStatusReportWithItems(Integer... processed) {
        StatusReporter<Integer, Integer> statusReporter = new StatusReporter(1000);
        statusReporter.produced(1, 100);
        statusReporter.produced(2, 200);
        statusReporter.produced(3, 300);
        statusReporter.produced(4, 400);
        statusReporter.produced(5, 500);
        statusReporter.produced(6, 600);

        statusReporter.processed(processed);

        return statusReporter;
    }
}
