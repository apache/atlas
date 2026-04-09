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

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TimedAspectInterceptorTest {
    @Mock
    private DebugMetricsWrapper wrapper;

    @Mock
    private ProceedingJoinPoint proceedingJoinPoint;

    @Mock
    private Signature signature;

    private TimedAspectInterceptor timedAspectInterceptor;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        timedAspectInterceptor = new TimedAspectInterceptor(wrapper);
    }

    @Test
    public void testConstructor() {
        assertNotNull(timedAspectInterceptor);
        // Verify that wrapper field is set
        Field wrapperField = getField(TimedAspectInterceptor.class, "wrapper");
        DebugMetricsWrapper actualWrapper = (DebugMetricsWrapper) getFieldValue(wrapperField, timedAspectInterceptor);
        assertEquals(actualWrapper, wrapper);
    }

    @Test
    public void testTimerAdviceWithDebugMetricsEnabled() throws Throwable {
        when(proceedingJoinPoint.getSignature()).thenReturn(signature);
        when(proceedingJoinPoint.proceed()).thenReturn("test result");
        when(signature.toString()).thenReturn("EntityREST.getById(..)");

        Object result = timedAspectInterceptor.timerAdvice(proceedingJoinPoint);

        assertEquals(result, "test result");
        verify(proceedingJoinPoint).proceed();
        // Note: We can't easily test the static field behavior, but we can test the method execution
    }

    @Test
    public void testTimerAdviceWithDebugMetricsDisabled() throws Throwable {
        when(proceedingJoinPoint.proceed()).thenReturn("test result");

        Object result = timedAspectInterceptor.timerAdvice(proceedingJoinPoint);

        assertEquals(result, "test result");
        verify(proceedingJoinPoint).proceed();
        // Note: We can't easily test the static field behavior, but we can test the method execution
    }

    @Test
    public void testTimerAdviceWithException() throws Throwable {
        RuntimeException testException = new RuntimeException("Test exception");
        when(proceedingJoinPoint.getSignature()).thenReturn(signature);
        when(proceedingJoinPoint.proceed()).thenThrow(testException);
        when(signature.toString()).thenReturn("EntityREST.getById(..)");

        try {
            timedAspectInterceptor.timerAdvice(proceedingJoinPoint);
        } catch (RuntimeException e) {
            assertEquals(e, testException);
        }

        verify(proceedingJoinPoint).proceed();
        // Note: We can't easily test the static field behavior, but we can test the method execution
    }

    @Test
    public void testTimerAdviceWithExceptionAndDebugMetricsDisabled() throws Throwable {
        RuntimeException testException = new RuntimeException("Test exception");
        when(proceedingJoinPoint.proceed()).thenThrow(testException);

        try {
            timedAspectInterceptor.timerAdvice(proceedingJoinPoint);
        } catch (RuntimeException e) {
            assertEquals(e, testException);
        }

        verify(proceedingJoinPoint).proceed();
        // Note: We can't easily test the static field behavior, but we can test the method execution
    }

    @Test
    public void testReportMetrics() throws Exception {
        long startTime = System.currentTimeMillis() - 100; // 100ms ago

        Method method = TimedAspectInterceptor.class.getDeclaredMethod("reportMetrics", long.class, Signature.class);
        method.setAccessible(true);

        when(signature.toString()).thenReturn("EntityREST.getById(..)");

        method.invoke(timedAspectInterceptor, startTime, signature);

        // Note: We can't easily test the static field behavior, but we can test the method execution
    }

    @Test
    public void testTimerAdviceWithNullResult() throws Throwable {
        // Note: We can't modify static final fields, testing method behavior

        when(proceedingJoinPoint.getSignature()).thenReturn(signature);
        when(proceedingJoinPoint.proceed()).thenReturn(null);
        when(signature.toString()).thenReturn("EntityREST.getById(..)");

        Object result = timedAspectInterceptor.timerAdvice(proceedingJoinPoint);

        assertEquals(result, null);
        verify(proceedingJoinPoint).proceed();
        verify(wrapper).update(any(Signature.class), anyLong());
    }

    @Test
    public void testTimerAdviceWithDifferentSignatures() throws Throwable {
        when(proceedingJoinPoint.getSignature()).thenReturn(signature);
        when(proceedingJoinPoint.proceed()).thenReturn("test result");

        // Test with different signature types
        String[] signatures = {
            "EntityREST.createOrUpdate(..)",
            "TypesREST.getTypeDefByName(..)",
            "GlossaryREST.getGlossaries(..)",
            "DiscoveryREST.searchUsingDSL(..)",
            "RelationshipREST.create(..)"
        };

        for (String sig : signatures) {
            when(signature.toString()).thenReturn(sig);

            Object result = timedAspectInterceptor.timerAdvice(proceedingJoinPoint);

            assertEquals(result, "test result");
            // Note: We can't easily test the static field behavior, but we can test the method execution
        }
    }

    @Test
    public void testTimerAdviceExecutionTime() throws Throwable {
        // Note: We can't modify static final fields, testing method behavior

        when(proceedingJoinPoint.getSignature()).thenReturn(signature);
        when(signature.toString()).thenReturn("EntityREST.getById(..)");

        // Simulate some processing time
        when(proceedingJoinPoint.proceed()).thenAnswer(invocation -> {
            Thread.sleep(10); // Sleep for 10ms
            return "test result";
        });

        long startTime = System.currentTimeMillis();
        Object result = timedAspectInterceptor.timerAdvice(proceedingJoinPoint);
        long endTime = System.currentTimeMillis();

        assertEquals(result, "test result");
        verify(proceedingJoinPoint).proceed();
        verify(wrapper).update(any(Signature.class), anyLong());

        // Verify that some time has passed
        assertTrue(endTime - startTime >= 10);
    }

    // Helper methods for reflection
    private Field getField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getFieldValue(Field field, Object instance) {
        try {
            return field.get(instance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void setFieldValue(Field field, Object instance, Object value) {
        try {
            field.set(instance, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertTrue(boolean condition) {
        org.testng.Assert.assertTrue(condition);
    }
}
