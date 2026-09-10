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
import org.apache.atlas.repository.store.graph.TypeRegistryCatchUp;
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
 * entity retrieval cannot resolve the type and fails. The fix resolves the missing type on demand and
 * retries, but only when the type was genuinely absent - it must never retry (or mask errors) otherwise.
 */
public class AtlasEntityStoreV2CatchUpTest {
    private static final String GUID      = "guid-1";
    private static final String TYPE_NAME = "peer_created_type";

    @Test
    public void catchUp_resolvesMissingTypeAndSignalsRetry() {
        AtlasGraph          graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry   registry = mock(AtlasTypeRegistry.class);
        TypeRegistryCatchUp catchUp  = mock(TypeRegistryCatchUp.class);
        AtlasVertex         vertex   = mock(AtlasVertex.class);

        // Peer wrote the vertex; this node's registry has not rebuilt yet, so the type is absent...
        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(null);
        // ...but an on-demand catch-up finds it in the store and resolves it.
        when(catchUp.entityType(TYPE_NAME)).thenReturn(mock(AtlasEntityType.class));

        AtlasEntityStoreV2 store = newStore(graph, registry, catchUp);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertTrue(store.caughtUpMissingEntityType(GUID), "should signal retry after resolving the missing type");
        }

        verify(catchUp, times(1)).entityType(TYPE_NAME);
    }

    @Test
    public void catchUp_whenTypeAlreadyKnown_doesNotReloadOrRetry() {
        AtlasGraph          graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry   registry = mock(AtlasTypeRegistry.class);
        TypeRegistryCatchUp catchUp  = mock(TypeRegistryCatchUp.class);
        AtlasVertex         vertex   = mock(AtlasVertex.class);

        // The type is already in this node's registry, so the retrieval failure was something else:
        // catching up would be wasteful and retrying would mask the real error.
        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(mock(AtlasEntityType.class));

        AtlasEntityStoreV2 store = newStore(graph, registry, catchUp);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the type is already known");
        }

        verify(catchUp, never()).entityType(TYPE_NAME);
    }

    @Test
    public void catchUp_whenVertexMissing_doesNotRetry() {
        AtlasGraph          graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry   registry = mock(AtlasTypeRegistry.class);
        TypeRegistryCatchUp catchUp  = mock(TypeRegistryCatchUp.class);

        AtlasEntityStoreV2 store = newStore(graph, registry, catchUp);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            // No vertex for this guid: a genuine not-found must surface as-is, not become a retry.
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(null);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the vertex does not exist");
        }

        verify(catchUp, never()).entityType(Mockito.anyString());
    }

    @Test
    public void catchUp_whenCatchUpUnavailable_doesNotTouchGraph() {
        AtlasGraph        graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry registry = mock(AtlasTypeRegistry.class);

        // No catch-up wired (e.g. active-passive): behaviour is unchanged and the graph is not touched.
        AtlasEntityStoreV2 store = newStore(graph, registry, null);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            assertFalse(store.caughtUpMissingEntityType(GUID));

            graphUtils.verify(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID), never());
        }
    }

    @Test
    public void catchUp_whenStoreLacksTypeToo_doesNotRetry() {
        AtlasGraph          graph    = mock(AtlasGraph.class);
        AtlasTypeRegistry   registry = mock(AtlasTypeRegistry.class);
        TypeRegistryCatchUp catchUp  = mock(TypeRegistryCatchUp.class);
        AtlasVertex         vertex   = mock(AtlasVertex.class);

        // Type absent from the registry and the catch-up cannot resolve it either (unknown type):
        // the original failure is real, so do not retry.
        when(registry.getEntityTypeByName(TYPE_NAME)).thenReturn(null);
        when(catchUp.entityType(TYPE_NAME)).thenReturn(null);

        AtlasEntityStoreV2 store = newStore(graph, registry, catchUp);

        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(graph, GUID)).thenReturn(vertex);
            graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn(TYPE_NAME);

            assertFalse(store.caughtUpMissingEntityType(GUID), "must not retry when the type is unresolvable anywhere");
        }
    }

    private static AtlasEntityStoreV2 newStore(AtlasGraph graph, AtlasTypeRegistry registry, TypeRegistryCatchUp catchUp) {
        AtlasEntityStoreV2 store = new AtlasEntityStoreV2(
                graph,
                mock(DeleteHandlerDelegate.class),
                registry,
                mock(IAtlasEntityChangeNotifier.class),
                mock(EntityGraphMapper.class));

        if (catchUp != null) {
            store.setTypeRegistryCatchUp(catchUp);
        }

        return store;
    }
}
