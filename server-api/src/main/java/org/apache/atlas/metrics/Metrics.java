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

package org.apache.atlas.metrics;

import java.util.LinkedHashMap;
import java.util.Map;

public class Metrics {
    public static class Counters {
        private short invocations = 0;
        private long totalTimeMSecs = 0;

        @Override
        public String toString() {
            return "[count=" + invocations + ", totalTimeMSec=" + totalTimeMSecs + "]";
        }

        public short getInvocations() {
            return invocations;
        }

        public long getTotalTimeMSecs() {
            return totalTimeMSecs;
        }
    }

    Map<String, Counters> countersMap = new LinkedHashMap<>();

    public void record(String name, long timeMsecs) {
        Counters counter = countersMap.get(name);
        if (counter == null) {
            counter = new Counters();
            countersMap.put(name, counter);
        }

        counter.invocations++;
        counter.totalTimeMSecs += timeMsecs;
    }

    @Override
    public String toString() {
        return countersMap.toString();
    }

    public boolean isEmpty() {
        return countersMap.isEmpty();
    }

    public Counters getCounters(String name) {
        return countersMap.get(name);
    }
}
