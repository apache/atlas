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
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer.UniqueKind;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.tasks.GraphClaim;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration2.Configuration;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraphBackedSearchIndexerActivationTest {
    private static final String INDEX_FIELD_NAME = "awp1_t";

    @Test
    public void createVertexIndex_whenPropertyExistsButMixedIndexFieldMissing_addsMissingMixedIndex() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);
        AtlasPropertyKey propertyKey = Mockito.mock(AtlasPropertyKey.class);

        when(management.getPropertyKey(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(propertyKey);
        when(management.getIndexFieldName(Constants.VERTEX_INDEX, propertyKey, false)).thenReturn(null);
        when(management.addMixedIndex(Constants.VERTEX_INDEX, propertyKey, false)).thenReturn(INDEX_FIELD_NAME);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);
        String indexField = indexer.createVertexIndex(management,
                Constants.TYPEDEF_BOOTSTRAP_FILE_KEY,
                UniqueKind.NONE,
                String.class,
                org.apache.atlas.repository.graphdb.AtlasCardinality.SINGLE,
                false,
                false,
                false);

        verify(management, never()).makePropertyKey(anyString(), any(), any());
        verify(management, times(1)).addMixedIndex(Constants.VERTEX_INDEX, propertyKey, false);
        org.testng.Assert.assertEquals(indexField, INDEX_FIELD_NAME);
    }

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
        AtlasPropertyKey propertyKey = Mockito.mock(AtlasPropertyKey.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);
        when(management.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(graphIndex);
        when(management.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(true);
        when(management.getPropertyKey(anyString())).thenReturn(propertyKey);
        when(management.getIndexFieldName(eq(Constants.VERTEX_INDEX), any(AtlasPropertyKey.class), anyBoolean())).thenReturn(INDEX_FIELD_NAME);

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

            // once for the readiness check, once for reading the index field names
            verify(management, times(2)).setIsSuccess(true);

            // the index field names come from the schema the peer left behind: nothing is created here
            verify(typeRegistry, times(1)).addIndexFieldName(Constants.TYPENAME_PROPERTY_KEY, INDEX_FIELD_NAME);
            verify(typeRegistry, times(1)).addIndexFieldName(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY, INDEX_FIELD_NAME);
            verify(management, never()).makePropertyKey(anyString(), any(), any());
            verify(management, never()).addMixedIndex(anyString(), any(AtlasPropertyKey.class), anyBoolean());
            verify(management, never()).createVertexCompositeIndex(anyString(), anyBoolean(), anyList());
        }
    }

    @Test
    public void instanceIsActive_whenInitialClaimFailsAndNoLiveHolder_retriesClaimWhileWaiting() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement waitManagementFirst = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphManagement waitManagementSecond = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphManagement waitManagementThird = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphIndex graphIndex = Mockito.mock(AtlasGraphIndex.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(waitManagementFirst, waitManagementSecond, waitManagementThird);
        when(waitManagementFirst.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(waitManagementFirst.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(waitManagementFirst.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(waitManagementFirst.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(null);
        when(waitManagementFirst.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(false);
        when(waitManagementSecond.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(waitManagementSecond.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(waitManagementSecond.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(waitManagementSecond.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(null);
        when(waitManagementSecond.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(false);
        when(waitManagementThird.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(waitManagementThird.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(waitManagementThird.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(waitManagementThird.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(graphIndex);
        when(waitManagementThird.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(true);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class);
                MockedStatic<GraphClaim> graphClaimMock = Mockito.mockStatic(GraphClaim.class);
                MockedConstruction<IndexRecoveryService.RecoveryInfoManagement> claimManagerConstruction =
                        Mockito.mockConstruction(IndexRecoveryService.RecoveryInfoManagement.class,
                                (mock, context) -> when(mock.tryClaimOwnership(anyString(), anyLong())).thenReturn(false))) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(true);
            graphClaimMock.when(() -> GraphClaim.hasLiveHolder(graph, Constants.CLAIM_INDEX)).thenReturn(false);
            graphClaimMock.when(() -> GraphClaim.releaseIfNoLiveHolderAndCommit(graph, Constants.CLAIM_INDEX)).thenReturn(true);

            indexer.instanceIsActive();

            IndexRecoveryService.RecoveryInfoManagement claimManager = claimManagerConstruction.constructed().get(0);
            // first claim attempt + one re-attempt during wait when no live holder exists
            verify(claimManager, atLeast(2)).tryClaimOwnership(anyString(), anyLong());
            graphClaimMock.verify(() -> GraphClaim.releaseIfNoLiveHolderAndCommit(graph, Constants.CLAIM_INDEX), atLeast(1));
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
        AtlasPropertyKey propertyKey = Mockito.mock(AtlasPropertyKey.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(initManagement, waitManagement);

        when(initManagement.getGraphIndex(Constants.VERTEX_INDEX)).thenThrow(new FakePermanentLockingException("lock contention"));

        when(waitManagement.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(waitManagement.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(waitManagement.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(waitManagement.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(graphIndex);
        when(waitManagement.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(true);
        when(waitManagement.getPropertyKey(anyString())).thenReturn(propertyKey);
        when(waitManagement.getIndexFieldName(eq(Constants.VERTEX_INDEX), any(AtlasPropertyKey.class), anyBoolean())).thenReturn(INDEX_FIELD_NAME);

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

            // once for the readiness check, once for reading the index field names
            verify(waitManagement, times(2)).setIsSuccess(true);
            verify(typeRegistry, times(1)).addIndexFieldName(Constants.TYPENAME_PROPERTY_KEY, INDEX_FIELD_NAME);
        }
    }

    /**
     * On a cluster coming up for the first time, the peer creates the three mixed indexes - all this
     * node waits for - before the property keys underneath them. A node that stood down must keep
     * looking while the peer still holds the claim, or it settles for the names that happened to
     * exist the moment it looked.
     */
    @Test
    public void standingDown_waitsForThePeerToFinishCreatingThePropertyKeys() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphIndex graphIndex = Mockito.mock(AtlasGraphIndex.class);
        AtlasPropertyKey propertyKey = Mockito.mock(AtlasPropertyKey.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);
        when(management.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(graphIndex);
        when(management.isCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY)).thenReturn(true);
        when(management.getIndexFieldName(eq(Constants.VERTEX_INDEX), any(AtlasPropertyKey.class), anyBoolean())).thenReturn(INDEX_FIELD_NAME);

        // the peer has not created __typeName yet when this node first looks
        when(management.getPropertyKey(Constants.TYPENAME_PROPERTY_KEY)).thenReturn(null).thenReturn(propertyKey);
        when(management.getPropertyKey(Mockito.argThat(name -> !Constants.TYPENAME_PROPERTY_KEY.equals(name)))).thenReturn(propertyKey);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class);
                MockedStatic<GraphClaim> graphClaimMock = Mockito.mockStatic(GraphClaim.class);
                MockedConstruction<IndexRecoveryService.RecoveryInfoManagement> claimManagerConstruction =
                        Mockito.mockConstruction(IndexRecoveryService.RecoveryInfoManagement.class,
                                (mock, context) -> when(mock.tryClaimOwnership(anyString(), anyLong())).thenReturn(false))) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(true);

            graphClaimMock.when(() -> GraphClaim.hasLiveHolder(graph, Constants.CLAIM_INDEX)).thenReturn(true);

            indexer.instanceIsActive();

            // it looked again rather than giving up on the name that was missing the first time
            verify(typeRegistry, times(1)).addIndexFieldName(Constants.TYPENAME_PROPERTY_KEY, INDEX_FIELD_NAME);
        }
    }

    /**
     * A node that stood down from index setup has no index field names registered in its type
     * registry, so it must not publish search weights and overwrite the Solr configuration written
     * by the node that did the setup.
     */
    @Test
    public void onLoadCompletion_doesNotNotifyListenersOnANodeThatStoodDownFromIndexSetup() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);
        IndexChangeListener listener = Mockito.mock(IndexChangeListener.class);

        indexer.addIndexListener(listener);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class)) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);

            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(false);

            indexer.instanceIsActive();
            indexer.onLoadCompletion();
        }

        verify(listener, never()).onInitCompletion(any(ChangedTypeDefs.class));
    }

    /**
     * Standing down is the exception, not the rule: a node that ran index setup - which is every node
     * of a single-node deployment - must publish the search weights, or free text search has nothing
     * to search on.
     */
    @Test
    public void onLoadCompletion_notifiesListenersOnANodeThatRanIndexSetup() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);
        IndexChangeListener listener = Mockito.mock(IndexChangeListener.class);

        indexer.addIndexListener(listener);
        indexer.onLoadCompletion();

        verify(listener, times(1)).onInitCompletion(any(ChangedTypeDefs.class));
    }

    /**
     * The node that owns index setup and creates the indexes must also make the typedef-bootstrap
     * lookup index usable (ENABLED + reindexed) before AtlasTypeDefStoreInitializer - which runs
     * after index setup - queries it; otherwise a REGISTERED-only composite index is ignored by the
     * query planner and bootstrap falls back to full graph scans.
     */
    @Test
    public void instanceIsActive_whenOwnerCreatesIndexes_enablesTypedefBootstrapLookupIndex() throws Exception {
        IAtlasGraphProvider provider = Mockito.mock(IAtlasGraphProvider.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        AtlasTypeRegistry typeRegistry = Mockito.mock(AtlasTypeRegistry.class);
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);
        AtlasGraphIndex graphIndex = Mockito.mock(AtlasGraphIndex.class);
        AtlasPropertyKey propertyKey = Mockito.mock(AtlasPropertyKey.class);

        when(provider.get()).thenReturn(graph);
        when(graph.getManagementSystem()).thenReturn(management);
        when(management.getGraphIndex(Constants.VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.EDGE_INDEX)).thenReturn(graphIndex);
        when(management.getGraphIndex(Constants.FULLTEXT_INDEX)).thenReturn(graphIndex);
        when(management.getPropertyKey(anyString())).thenReturn(propertyKey);
        when(management.getIndexFieldName(eq(Constants.VERTEX_INDEX), any(AtlasPropertyKey.class), anyBoolean())).thenReturn(INDEX_FIELD_NAME);

        GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);

        try (MockedStatic<AtlasRunMode> runModeMock = Mockito.mockStatic(AtlasRunMode.class);
                MockedConstruction<IndexRecoveryService.RecoveryInfoManagement> claimManagerConstruction =
                        Mockito.mockConstruction(IndexRecoveryService.RecoveryInfoManagement.class, (mock, context) -> {
                            when(mock.tryClaimOwnership(anyString(), anyLong())).thenReturn(true);
                            when(mock.isOwner(anyString())).thenReturn(true);
                        })) {
            AtlasRunMode runMode = Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(runMode);
            when(runMode.runsIndexSetup()).thenReturn(true);

            indexer.instanceIsActive();

            verify(management, times(1)).ensureCompositeIndexEnabled(Constants.TYPEDEF_BOOTSTRAP_FILE_KEY);
        }
    }

    private static class FakePermanentLockingException extends RuntimeException {
        FakePermanentLockingException(String message) {
            super(message);
        }
    }
}
