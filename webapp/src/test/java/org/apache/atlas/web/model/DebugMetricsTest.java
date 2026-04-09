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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.web.model;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class DebugMetricsTest {
    private DebugMetrics debugMetrics;

    @BeforeClass
    public void setUp() {
        debugMetrics = new DebugMetrics();
    }

    @Test
    public void testDebugMetricsFunctionality() {
        // Test default constructor
        DebugMetrics defaultMetrics = new DebugMetrics();
        assertNotNull(defaultMetrics);

        // Test parameterized constructor
        String testName = "TestMetric";
        DebugMetrics namedMetrics = new DebugMetrics(testName);
        assertEquals(namedMetrics.getName(), testName);

        // Test all getter and setter methods
        debugMetrics.setName("PerformanceMetric");
        assertEquals(debugMetrics.getName(), "PerformanceMetric");

        debugMetrics.setNumops(100);
        assertEquals(debugMetrics.getNumops(), 100);

        debugMetrics.setMinTime(0.5f);
        assertEquals(debugMetrics.getMinTime(), 0.5f);

        debugMetrics.setMaxTime(2.5f);
        assertEquals(debugMetrics.getMaxTime(), 2.5f);

        debugMetrics.setStdDevTime(0.8f);
        assertEquals(debugMetrics.getStdDevTime(), 0.8f);

        debugMetrics.setAvgTime(1.5f);
        assertEquals(debugMetrics.getAvgTime(), 1.5f);

        // Test with different data types and edge cases
        debugMetrics.setNumops(0);
        assertEquals(debugMetrics.getNumops(), 0);

        debugMetrics.setMinTime(0.0f);
        assertEquals(debugMetrics.getMinTime(), 0.0f);

        debugMetrics.setMaxTime(Float.MAX_VALUE);
        assertEquals(debugMetrics.getMaxTime(), Float.MAX_VALUE);

        debugMetrics.setStdDevTime(-1.0f);
        assertEquals(debugMetrics.getStdDevTime(), -1.0f);

        debugMetrics.setAvgTime(Float.MIN_VALUE);
        assertEquals(debugMetrics.getAvgTime(), Float.MIN_VALUE);

        // Test null name handling
        debugMetrics.setName(null);
        assertNull(debugMetrics.getName());

        // Test empty string name
        debugMetrics.setName("");
        assertEquals(debugMetrics.getName(), "");

        // Test with special characters in name
        debugMetrics.setName("Metric-Name_123");
        assertEquals(debugMetrics.getName(), "Metric-Name_123");
    }
}
