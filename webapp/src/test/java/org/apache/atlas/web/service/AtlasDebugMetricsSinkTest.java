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

package org.apache.atlas.web.service;

import org.apache.atlas.web.model.DebugMetrics;
import org.apache.hadoop.hbase.shaded.org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasDebugMetricsSinkTest {
    @Mock
    private MetricsRecord metricsRecord;

    @Mock
    private AbstractMetric metric;

    @Mock
    private SubsetConfiguration subsetConfiguration;

    private AtlasDebugMetricsSink debugMetricsSink;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        debugMetricsSink = new AtlasDebugMetricsSink();
    }

    @Test
    public void testPutMetricsWithDebugMetricsSource() throws Exception {
        when(metricsRecord.name()).thenReturn("AtlasDebugMetricsSource");

        List<AbstractMetric> metrics = new ArrayList<>();
        metrics.add(metric);
        when(metricsRecord.metrics()).thenReturn(metrics);

        when(metric.name()).thenReturn("entityREST_getByIdnumops");
        when(metric.value()).thenReturn(5.0f);

        // Setup field mapping
        setupFieldMapping();

        debugMetricsSink.putMetrics(metricsRecord);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result);
    }

    @Test
    public void testPutMetricsWithNonDebugMetricsSource() {
        when(metricsRecord.name()).thenReturn("OtherSource");

        debugMetricsSink.putMetrics(metricsRecord);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPutMetricsWithZeroValue() throws Exception {
        when(metricsRecord.name()).thenReturn("AtlasDebugMetricsSource");

        List<AbstractMetric> metrics = new ArrayList<>();
        metrics.add(metric);
        when(metricsRecord.metrics()).thenReturn(metrics);

        when(metric.name()).thenReturn("entityREST_getByIdnumops");
        when(metric.value()).thenReturn(0.0f);

        setupFieldMapping();

        debugMetricsSink.putMetrics(metricsRecord);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPutMetricsWithMaxValue() throws Exception {
        when(metricsRecord.name()).thenReturn("AtlasDebugMetricsSource");

        List<AbstractMetric> metrics = new ArrayList<>();
        metrics.add(metric);
        when(metricsRecord.metrics()).thenReturn(metrics);

        when(metric.name()).thenReturn("entityREST_getByIdnumops");
        when(metric.value()).thenReturn(Float.MAX_VALUE);

        setupFieldMapping();

        debugMetricsSink.putMetrics(metricsRecord);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPutMetricsWithMinValue() throws Exception {
        when(metricsRecord.name()).thenReturn("AtlasDebugMetricsSource");

        List<AbstractMetric> metrics = new ArrayList<>();
        metrics.add(metric);
        when(metricsRecord.metrics()).thenReturn(metrics);

        when(metric.name()).thenReturn("entityREST_getByIdnumops");
        when(metric.value()).thenReturn(Float.MIN_VALUE);

        setupFieldMapping();

        debugMetricsSink.putMetrics(metricsRecord);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testSetMetricsDataWithNumOps() throws Exception {
        when(metric.name()).thenReturn("entityREST_getByIdnumops");
        when(metric.value()).thenReturn(10);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result.get("EntityREST_getById"));
        assertEquals(result.get("EntityREST_getById").getNumops(), 10);
    }

    @Test
    public void testSetMetricsDataWithMinTime() throws Exception {
        when(metric.name()).thenReturn("entityREST_getByIdmintime");
        when(metric.value()).thenReturn(1.5f);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result.get("EntityREST_getById"));
        assertEquals(result.get("EntityREST_getById").getMinTime(), 1.5f);
    }

    @Test
    public void testSetMetricsDataWithMaxTime() throws Exception {
        when(metric.name()).thenReturn("entityREST_getByIdmaxtime");
        when(metric.value()).thenReturn(10.5f);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result.get("EntityREST_getById"));
        assertEquals(result.get("EntityREST_getById").getMaxTime(), 10.5f);
    }

    @Test
    public void testSetMetricsDataWithStdDevTime() throws Exception {
        when(metric.name()).thenReturn("entityREST_getByIdstdevtime");
        when(metric.value()).thenReturn(2.5f);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result.get("EntityREST_getById"));
        assertEquals(result.get("EntityREST_getById").getStdDevTime(), 2.5f);
    }

    @Test
    public void testSetMetricsDataWithAvgTime() throws Exception {
        when(metric.name()).thenReturn("entityREST_getByIdavgtime");
        when(metric.value()).thenReturn(5.5f);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result.get("EntityREST_getById"));
        assertEquals(result.get("EntityREST_getById").getAvgTime(), 5.5f);
    }

    @Test
    public void testSetMetricsDataWithUnknownField() throws Exception {
        when(metric.name()).thenReturn("unknownfield");
        when(metric.value()).thenReturn(5.5f);

        setupFieldMapping();

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("setMetricsData", AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, metric);

        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testInferMeasureType() throws Exception {
        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("inferMeasureType", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "entityREST_getByIdnumops", "entityrest_getbyid");
        assertEquals(result, "entityREST_getByIdnumops");
    }

    @Test
    public void testUpdateMetricTypeNumOps() throws Exception {
        DebugMetrics debugMetrics = new DebugMetrics("test");
        when(metric.value()).thenReturn(15);

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("updateMetricType", DebugMetrics.class, String.class, AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, debugMetrics, "numops", metric);

        assertEquals(debugMetrics.getNumops(), 15);
    }

    @Test
    public void testUpdateMetricTypeMinTime() throws Exception {
        DebugMetrics debugMetrics = new DebugMetrics("test");
        when(metric.value()).thenReturn(2.5f);

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("updateMetricType", DebugMetrics.class, String.class, AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, debugMetrics, "mintime", metric);

        assertEquals(debugMetrics.getMinTime(), 2.5f);
    }

    @Test
    public void testUpdateMetricTypeMaxTime() throws Exception {
        DebugMetrics debugMetrics = new DebugMetrics("test");
        when(metric.value()).thenReturn(12.5f);

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("updateMetricType", DebugMetrics.class, String.class, AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, debugMetrics, "maxtime", metric);

        assertEquals(debugMetrics.getMaxTime(), 12.5f);
    }

    @Test
    public void testUpdateMetricTypeStdDevTime() throws Exception {
        DebugMetrics debugMetrics = new DebugMetrics("test");
        when(metric.value()).thenReturn(3.5f);

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("updateMetricType", DebugMetrics.class, String.class, AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, debugMetrics, "stdevtime", metric);

        assertEquals(debugMetrics.getStdDevTime(), 3.5f);
    }

    @Test
    public void testUpdateMetricTypeAvgTime() throws Exception {
        DebugMetrics debugMetrics = new DebugMetrics("test");
        when(metric.value()).thenReturn(7.5f);

        Method method = AtlasDebugMetricsSink.class.getDeclaredMethod("updateMetricType", DebugMetrics.class, String.class, AbstractMetric.class);
        method.setAccessible(true);
        method.invoke(debugMetricsSink, debugMetrics, "avgtime", metric);

        assertEquals(debugMetrics.getAvgTime(), 7.5f);
    }

    @Test
    public void testFlush() {
        // Test that flush method doesn't throw any exception
        debugMetricsSink.flush();
    }

    @Test
    public void testInit() {
        // Test that init method doesn't throw any exception
        debugMetricsSink.init(subsetConfiguration);
    }

    @Test
    public void testGetMetrics() {
        HashMap<String, DebugMetrics> result = debugMetricsSink.getMetrics();
        assertNotNull(result);
    }

    private void setupFieldMapping() throws Exception {
        // Setup the static field mapping in AtlasDebugMetricsSource
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("fieldLowerCaseUpperCaseMap");
        field.setAccessible(true);
        HashMap<String, String> fieldMap = (HashMap<String, String>) field.get(null);
        fieldMap.put("entityrest_getbyidnumops", "EntityREST_getById");
        fieldMap.put("entityrest_getbyidmintime", "EntityREST_getById");
        fieldMap.put("entityrest_getbyidmaxtime", "EntityREST_getById");
        fieldMap.put("entityrest_getbyidstdevtime", "EntityREST_getById");
        fieldMap.put("entityrest_getbyidavgtime", "EntityREST_getById");
    }
}
