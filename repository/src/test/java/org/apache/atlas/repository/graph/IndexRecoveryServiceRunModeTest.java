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
package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.configuration2.Configuration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class IndexRecoveryServiceRunModeTest {
    @Test
    public void instanceIsActive_doesNotStartMonitorWhenRunModeSkipsMetadataServer() throws Exception {
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);

        when(configuration.getBoolean(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(true);
        when(configuration.getLong(Mockito.anyString(), Mockito.anyLong())).thenReturn(50L);
        when(configuration.getString(Mockito.anyString())).thenReturn(null);

        IndexRecoveryService service = new IndexRecoveryService(configuration, graph);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class)) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsMetadataServer()).thenReturn(false);

            service.instanceIsActive();
        }

        Thread monitor = getMonitorThread(service);
        assertEquals(monitor.getState(), Thread.State.NEW);
    }

    private Thread getMonitorThread(IndexRecoveryService service) throws Exception {
        Field field = IndexRecoveryService.class.getDeclaredField("indexHealthMonitor");
        field.setAccessible(true);
        return (Thread) field.get(service);
    }
}
