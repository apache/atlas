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
package org.apache.atlas.services;

import org.apache.atlas.DeleteType;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.RequestContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PurgeServiceTest extends AtlasTestBase {

    @Mock
    private AtlasGraph atlasGraph;

    @Mock
    private AtlasEntityStore entityStore;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    private PurgeService purgeService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        RequestContext.clear();
        purgeService = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry));
    }

    @Test
    public void purgeEntitiesNoCandidatesSetsContextAndReturnsEmptyAndDoesNotTouchEntityStore() throws Exception {
        AtlasIndexQuery mockIndexQuery = mock(AtlasIndexQuery.class);
        when(atlasGraph.indexQuery(anyString(), anyString())).thenReturn(mockIndexQuery);
        when(mockIndexQuery.vertices(anyInt(), anyInt())).thenReturn(Collections.<AtlasIndexQuery.Result>emptyList().iterator());

        doReturn(new HashSet<>(Arrays.asList("typeA"))).when(purgeService).getEntityTypes();

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertNotNull(response);
        Assert.assertNull(response.getPurgedEntities());
        Assert.assertEquals(RequestContext.get().getDeleteType(), DeleteType.HARD);
        Assert.assertTrue(RequestContext.get().isPurgeRequested());

        verify(entityStore, never()).accumulateDeletionCandidates(any());
    }

    @Test
    public void purgeEntitiesWithSingleCandidateProducesWorkCollectsResultsAndInvokesNextLevelCalls() throws Exception {
        // mock graph index → iterator with one vertex result
        AtlasIndexQuery mockIndexQuery = mock(AtlasIndexQuery.class);
        when(atlasGraph.indexQuery(anyString(), anyString())).thenReturn(mockIndexQuery);

        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY, String.class)).thenReturn("guid-1");

        AtlasIndexQuery.Result result = mock(AtlasIndexQuery.Result.class);
        when(result.getVertex()).thenReturn(vertex);
        Iterator<AtlasIndexQuery.Result> iterator = Arrays.asList(result).iterator();
        when(mockIndexQuery.vertices(anyInt(), anyInt())).thenReturn(iterator);

        // mock entityStore to expand deletion candidates to include the same vertex
        Set<AtlasVertex> deletionCandidates = new HashSet<>();
        deletionCandidates.add(vertex);
        when(entityStore.accumulateDeletionCandidates(any())).thenReturn(deletionCandidates);

        // mock WorkItemsQualifier and its results queue
        PurgeService.WorkItemsQualifier wiq = mock(PurgeService.WorkItemsQualifier.class);
        Queue results = new ArrayDeque();
        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid("guid-1");
        header.setTypeName("typeA");
        results.add(header);
        when(wiq.getResults()).thenReturn(results);
        doNothing().when(wiq).shutdown();
        doNothing().when(wiq).checkProduce(any());

        // spy createQualifier and getEntityTypes
        doReturn(new HashSet<>(Arrays.asList("typeA"))).when(purgeService).getEntityTypes();
        doReturn(wiq).when(purgeService).createQualifier(any(), any(), any(), anyInt(), anyInt(), Mockito.eq(true));

        EntityMutationResponse response = purgeService.purgeEntities();

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getPurgedEntities());
        Assert.assertEquals(response.getPurgedEntities().size(), 1);

        // verify next-level calls
        verify(purgeService, times(1)).createQualifier(any(), any(), any(), anyInt(), anyInt(), Mockito.eq(true));
        verify(entityStore, times(1)).accumulateDeletionCandidates(any());

        // verify that checkProduce got called at least once with a vertex
        ArgumentCaptor<AtlasVertex> vertexCaptor = ArgumentCaptor.forClass(AtlasVertex.class);
        verify(wiq, times(1)).checkProduce(vertexCaptor.capture());
        Assert.assertEquals(vertexCaptor.getValue(), vertex);
    }

    @Test
    public void softDeleteProcessEntitiesNoResultsShutsDownWorkersAndUsesCleanupQualifier() throws Exception {
        // mock graph query iterator empty on first page
        AtlasIndexQuery mockIndexQuery = mock(AtlasIndexQuery.class);
        when(atlasGraph.indexQuery(anyString(), anyString())).thenReturn(mockIndexQuery);
        when(mockIndexQuery.vertices(anyInt(), anyInt())).thenReturn(Collections.<AtlasIndexQuery.Result>emptyList().iterator());

        PurgeService.WorkItemsQualifier wiq = mock(PurgeService.WorkItemsQualifier.class);
        doNothing().when(wiq).shutdown();

        doReturn(wiq).when(purgeService).createQualifier(any(), any(), any(), anyInt(), anyInt(), Mockito.eq(false));

        purgeService.softDeleteProcessEntities();

        verify(purgeService, times(1)).createQualifier(any(), any(), any(), anyInt(), anyInt(), Mockito.eq(false));
        verify(wiq, times(1)).shutdown();
    }

    

    @Test
    public void startWhenSoftDeletionEnabledLaunchesCleanupThread() throws Exception {
        PurgeService spy = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry));
        doReturn(true).when(spy).getSoftDeletionFlag();
        doNothing().when(spy).launchCleanUp();

        spy.start();

        verify(spy, times(1)).launchCleanUp();
    }

    @Test
    public void startWhenSoftDeletionDisabledDoesNotLaunchCleanupThread() throws Exception {
        PurgeService spy = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry));
        doReturn(false).when(spy).getSoftDeletionFlag();
        doNothing().when(spy).launchCleanUp();

        spy.start();

        verify(spy, never()).launchCleanUp();
    }
}
