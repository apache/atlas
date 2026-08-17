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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration2.Configuration;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraphBackedSearchIndexerActivationTest {
    @Test
    public void instanceIsActive_skipsIndexSetupWhenRunModeDoesNotAllowIt() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);

        when(configuration.containsKey("atlas.server.ha.enabled")).thenReturn(true);
        when(configuration.getBoolean("atlas.server.ha.enabled")).thenReturn(true);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class)) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(false);

            indexer.instanceIsActive();
        }

        verify(provider, never()).get();
    }

    @Test
    public void instanceIsActive_whenClaimedByAnotherNode_waitsForPeerIndexSetupCompletion() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphIndex graphIndex = Mockito.mock(AtlasGraphIndex.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);
        when(management.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class);
                MockedConstruction<IndexRecoveryService.RecoveryInfoManagement> claimManagerConstruction =
                        Mockito.mockConstruction(IndexRecoveryService.RecoveryInfoManagement.class,
                                (mock, context) -> when(mock.tryClaimOwnership(anyString(), anyLong())).thenReturn(false))) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(true);

            indexer.instanceIsActive();

            IndexRecoveryService.RecoveryInfoManagement claimManager = claimManagerConstruction.constructed().get(0);
            verify(claimManager, times(1)).tryClaimOwnership(anyString(), anyLong());
            verify(claimManager, never()).releaseOwnership(anyString());
            verify(management, times(1)).setIsSuccess(true);
        }
    }

    @Test
    public void instanceIsActive_whenLockContentionAndOwnershipLost_waitsForPeerIndexSetupCompletion() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement initManagement = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphManagement waitManagement = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphIndex graphIndex = Mockito.mock(AtlasGraphIndex.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(initManagement, waitManagement);

        when(initManagement.getGraphIndex(Constants.VERTEX_INDEX)).thenThrow(new FakePermanentLockingException("lock contention"));

        when(waitManagement.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(waitManagement.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(waitManagement.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class);
                MockedConstruction<IndexRecoveryService.RecoveryInfoManagement> claimManagerConstruction =
                        Mockito.mockConstruction(IndexRecoveryService.RecoveryInfoManagement.class, (mock, context) -> {
                            when(mock.tryClaimOwnership(anyString(), anyLong())).thenReturn(true);
                            when(mock.isOwner(anyString())).thenReturn(false);
                        })) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(true);

            indexer.instanceIsActive();

            IndexRecoveryService.RecoveryInfoManagement claimManager = claimManagerConstruction.constructed().get(0);
            verify(claimManager, times(1)).tryClaimOwnership(anyString(), anyLong());
            verify(claimManager, times(1)).isOwner(anyString());
            verify(claimManager, times(1)).releaseOwnership(anyString());
            verify(waitManagement, times(1)).setIsSuccess(true);
        }
    }

    private static class FakePermanentLockingException extends RuntimeException {
        FakePermanentLockingException(String message) {
            super(message);
        }
    }
}
