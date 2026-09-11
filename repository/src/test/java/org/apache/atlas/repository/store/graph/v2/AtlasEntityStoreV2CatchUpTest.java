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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.TypeRegistryVersionGate;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link AtlasEntityStoreV2#caughtUpMissingEntityType(String)}.
 *
 * <p>Reproduces the active-active read-after-write miss: a peer creates an entity (and its type), the
 * vertex is durably in the shared graph, but this node's in-memory type registry has not caught up, so
 * entity retrieval cannot resolve the type and fails. The version gate reloads the registry and the
 * read is retried only when the type was genuinely absent.
 */
public class AtlasEntityStoreV2CatchUpTest {
    private static final String GUID      = "guid-1";
    private static final String TYPE_NAME = "peer_created_type";

    @Test
    public void catchUp_resolvesMissingTypeAndSignalsRetry() {
        AtlasGraph              graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry       registry = mock(AtlasTypeRegistry.class);
        TypeRegistryVersionGate gate     = mock(TypeRegistryVersionGate.class);
        AtlasVertex             vertex   = mock(AtlasVertex.class);

        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(null, mock(AtlasEntityType.class));
        when(gate.ensureUpToDate()).thenReturn(true);

        AtlasEntityStoreV2 store = newStore(graph, registry, gate);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertTrue(store.caughtUpMissingEntityType(GUID), "should signal retry after resolving the missing type");
        }

        verify(gate, times(1)).ensureUpToDate();
    }

    @Test
    public void catchUp_whenTypeAlreadyKnown_doesNotReloadOrRetry() {
        AtlasGraph              graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry       registry = mock(AtlasTypeRegistry.class);
        TypeRegistryVersionGate gate     = mock(TypeRegistryVersionGate.class);
        AtlasVertex             vertex   = mock(AtlasVertex.class);

        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(mock(AtlasEntityType.class));

        AtlasEntityStoreV2 store = newStore(graph, registry, gate);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the type is already known");
        }

        verify(gate, never()).ensureUpToDate();
    }

    @Test
    public void catchUp_whenVertexMissing_doesNotRetry() {
        AtlasGraph              graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry       registry = mock(AtlasTypeRegistry.class);
        TypeRegistryVersionGate gate     = mock(TypeRegistryVersionGate.class);

        AtlasEntityStoreV2 store = newStore(graph, registry, gate);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(null);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the vertex does not exist");
        }

        verify(gate, never()).ensureUpToDate();
    }

    @Test
    public void catchUp_whenCatchUpUnavailable_doesNotTouchGraph() {
        AtlasGraph        graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry registry = mock(AtlasTypeRegistry.class);

        AtlasEntityStoreV2 store = newStore(graph, registry, null);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            assertFalse(store.caughtUpMissingEntityType(GUID));

            graphUtils.verify(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID), never());
        }
    }

    @Test
    public void catchUp_whenStoreLacksTypeToo_doesNotRetry() {
        AtlasGraph              graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry       registry = mock(AtlasTypeRegistry.class);
        TypeRegistryVersionGate gate     = mock(TypeRegistryVersionGate.class);
        AtlasVertex             vertex   = mock(AtlasVertex.class);

        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(null);
        when(gate.ensureUpToDate()).thenReturn(true);

        AtlasEntityStoreV2 store = newStore(graph, registry, gate);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the type is unresolvable anywhere");
        }
    }

    private static AtlasEntityStoreV2 newStore(AtlasGraph graph, AtlasTypeRegistry registry, TypeRegistryVersionGate gate) {
        AtlasEntityStoreV2 store = new AtlasEntityStoreV2(
                graph,
                mock(DeleteHandlerDelegate.class),
                registry,
                mock(IAtlasEntityChangeNotifier.class),
                mock(EntityGraphMapper.class));

        if (gate != null) {
            store.setTypeRegistryVersionGate(gate);
        }

        return store;
    }
}
