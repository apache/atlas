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

package org.apache.atlas.repository.patches;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class AtlasPatchServiceTest {
    @Mock
    private Configuration configuration;

    @Mock
    private AtlasPatchManager patchManager;

    private AtlasPatchService atlasPatchService;
    private MockedStatic<HAConfiguration> haConfigurationMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        atlasPatchService = new AtlasPatchService(configuration, patchManager);
        haConfigurationMock = mockStatic(HAConfiguration.class);
    }

    @AfterMethod
    public void tearDown() {
        if (haConfigurationMock != null) {
            haConfigurationMock.close();
        }
    }

    @Test
    public void testStartWhenHANotEnabled() throws AtlasException {
        haConfigurationMock.when(() -> HAConfiguration.isHAEnabled(any(Configuration.class))).thenReturn(false);
        doNothing().when(patchManager).applyAll();

        atlasPatchService.start();

        verify(patchManager, times(1)).applyAll();
    }

    @Test
    public void testStartWhenHAEnabled() throws AtlasException {
        haConfigurationMock.when(() -> HAConfiguration.isHAEnabled(any(Configuration.class))).thenReturn(true);

        atlasPatchService.start();

        verify(patchManager, never()).applyAll();
    }

    @Test
    public void testStartInternalSuccess() throws Exception {
        doNothing().when(patchManager).applyAll();

        Method startInternalMethod = AtlasPatchService.class.getDeclaredMethod("startInternal");
        startInternalMethod.setAccessible(true);
        startInternalMethod.invoke(atlasPatchService);

        verify(patchManager, times(1)).applyAll();
    }

    @Test
    public void testStartInternalWithException() throws Exception {
        doThrow(new RuntimeException("Test exception")).when(patchManager).applyAll();

        Method startInternalMethod = AtlasPatchService.class.getDeclaredMethod("startInternal");
        startInternalMethod.setAccessible(true);
        startInternalMethod.invoke(atlasPatchService);

        verify(patchManager, times(1)).applyAll();
    }

    @Test
    public void testStop() {
        // Test stop method - it just logs, no exception should be thrown
        atlasPatchService.stop();
    }

    @Test
    public void testInstanceIsActive() {
        doNothing().when(patchManager).applyAll();

        atlasPatchService.instanceIsActive();

        verify(patchManager, times(1)).applyAll();
    }

    @Test
    public void testInstanceIsPassive() {
        // Test instanceIsPassive method - it just logs, no exception should be thrown
        atlasPatchService.instanceIsPassive();
    }

    @Test
    public void testGetHandlerOrder() {
        int handlerOrder = atlasPatchService.getHandlerOrder();
        assertEquals(handlerOrder, ActiveStateChangeHandler.HandlerOrder.ATLAS_PATCH_SERVICE.getOrder());
    }
}
