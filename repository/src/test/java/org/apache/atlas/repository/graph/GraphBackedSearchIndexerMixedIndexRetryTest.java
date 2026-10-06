/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration2.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.ha.HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY;
import static org.apache.atlas.repository.Constants.EDGE_INDEX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;

/**
 * Regression tests for the OpenSearch "Limit of total fields [1000] has been exceeded" investigation: a
 * {@code PropertyKey} can durably exist (its {@code makePropertyKey()} call having succeeded) while its
 * {@code addMixedIndex()} registration failed and was never retried (the exception is caught and logged, not
 * rethrown, several layers up in {@code GraphBackedSearchIndexer#onChange}). Previously,
 * {@code createVertexIndex()}/{@code createEdgeIndex()} treated "PropertyKey already exists" as proof that its
 * mixed-index registration also succeeded, and never called {@code addMixedIndex()} again -- confirmed by live
 * reproduction to leave the attribute permanently unindexed, surviving both a raised OpenSearch field limit and a
 * full Atlas restart.
 * <p>
 * These tests verify the fix distinguishes "PropertyKey exists" from "PropertyKey is registered in the mixed
 * index", while leaving the pre-existing, intentional "some types/cardinalities are never mixed-indexed"
 * ({@code isIndexApplicable()}) behavior completely unchanged.
 */
public class GraphBackedSearchIndexerMixedIndexRetryTest {
    private static final String PROPERTY_NAME = "test_type.test_attr";

    private GraphBackedSearchIndexer indexer;

    @BeforeMethod
    public void setUp() throws Exception {
        // HA-enabled skips GraphBackedSearchIndexer's constructor-time initialize(provider.get()) entirely, so the
        // instance under test can be constructed with no graph/management state beyond what each test supplies
        // directly to createVertexIndex()/createEdgeIndex().
        Configuration configuration = mock(Configuration.class);
        when(configuration.containsKey(ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        AtlasTypeRegistry   typeRegistry = mock(AtlasTypeRegistry.class);
        IAtlasGraphProvider provider     = mock(IAtlasGraphProvider.class);

        indexer = new GraphBackedSearchIndexer(provider, configuration, typeRegistry);
    }

    private AtlasPropertyKey mockPropertyKey() {
        AtlasPropertyKey propertyKey = mock(AtlasPropertyKey.class);

        when(propertyKey.getName()).thenReturn(PROPERTY_NAME);

        return propertyKey;
    }

    private AtlasGraphIndex mockGraphIndex(Set<AtlasPropertyKey> fieldKeys) {
        AtlasGraphIndex graphIndex = mock(AtlasGraphIndex.class);

        when(graphIndex.getFieldKeys()).thenReturn(fieldKeys);

        return graphIndex;
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 1 -- New supported property: PropertyKey absent, isIndexApplicable=true, not yet mixed-index member.
    // Expected: makePropertyKey() AND addMixedIndex() are both called.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void newSupportedPropertyCreatesKeyAndRegistersInMixedIndex() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(null);
        when(management.makePropertyKey(eq(PROPERTY_NAME), eq(String.class), any())).thenReturn(propertyKey);
        when(management.getGraphIndex(VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.addMixedIndex(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean())).thenReturn("test_type\u2022test_attr");

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                String.class, AtlasCardinality.SINGLE, false, false, false);

        verify(management, times(1)).makePropertyKey(eq(PROPERTY_NAME), eq(String.class), any());
        verify(management, times(1)).addMixedIndex(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean());
        verify(management, never()).getIndexFieldName(any(), any(), anyBoolean(), anyBoolean());
        org.testng.Assert.assertEquals(indexFieldName, "test_type\u2022test_attr");
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 2 -- Existing supported property already indexed: PropertyKey exists, isIndexApplicable=true, already
    // a member of the mixed index. Expected: addMixedIndex() must NOT be called; getIndexFieldName() is used.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void existingSupportedPropertyAlreadyIndexedDoesNotReRegister() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(new HashSet<>(Collections.singletonList(propertyKey)));

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.getIndexFieldName(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean())).thenReturn("test_type\u2022test_attr");

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                String.class, AtlasCardinality.SINGLE, false, false, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
        verify(management, times(1)).getIndexFieldName(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean());
        org.testng.Assert.assertEquals(indexFieldName, "test_type\u2022test_attr");
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 3 -- THE REGRESSION FIX: existing supported property whose mixed-index registration previously
    // failed. PropertyKey exists (durably, from the earlier partially-failed attempt), isIndexApplicable=true,
    // but it is NOT a member of the mixed index. Expected: addMixedIndex() IS called (the retry).
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void existingSupportedPropertyNotYetRegisteredIsRetried() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        // PropertyKey exists, but the mixed index has no field with this name -- exactly the state left behind by
        // a prior addMixedIndex() call that failed (e.g. OpenSearch "Limit of total fields exceeded") and was
        // swallowed several layers up.
        AtlasGraphIndex graphIndex = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(VERTEX_INDEX)).thenReturn(graphIndex);
        when(management.addMixedIndex(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean())).thenReturn("test_type\u2022test_attr");

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                String.class, AtlasCardinality.SINGLE, false, false, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, times(1)).addMixedIndex(eq(VERTEX_INDEX), eq(propertyKey), anyBoolean(), anyBoolean());
        verify(management, never()).getIndexFieldName(any(), any(), anyBoolean(), anyBoolean());
        org.testng.Assert.assertEquals(indexFieldName, "test_type\u2022test_attr");
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 4 -- Existing, intentionally unsupported property (BigDecimal): PropertyKey exists,
    // isIndexApplicable=false, not a mixed-index member. Expected (UNCHANGED behavior): addMixedIndex() and
    // getIndexFieldName() must NOT be called, regardless of mixed-index membership state.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void existingUnsupportedPropertyNeverRegistersRegardlessOfMembership() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(VERTEX_INDEX)).thenReturn(graphIndex);

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                BigDecimal.class, AtlasCardinality.SINGLE, false, false, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
        verify(management, never()).getIndexFieldName(any(), any(), anyBoolean(), anyBoolean());
        assertNull(indexFieldName);
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 5 -- New, intentionally unsupported property (BigDecimal): PropertyKey absent. Expected (UNCHANGED
    // behavior): makePropertyKey() is still called (the PropertyKey itself is created), but addMixedIndex() must
    // NOT be attempted.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void newUnsupportedPropertyCreatesKeyButNeverRegisters() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(null);
        when(management.makePropertyKey(eq(PROPERTY_NAME), eq(BigDecimal.class), any())).thenReturn(propertyKey);

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                BigDecimal.class, AtlasCardinality.SINGLE, false, false, false);

        verify(management, times(1)).makePropertyKey(eq(PROPERTY_NAME), eq(BigDecimal.class), any());
        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
        verify(management, never()).getIndexFieldName(any(), any(), anyBoolean(), anyBoolean());
        verify(management, never()).getGraphIndex(any());
        assertNull(indexFieldName);
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 6 -- Edge index: same distinction applied to createEdgeIndex(). Existing supported edge property whose
    // registration previously failed must be retried.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void edgeIndexRetriesRegistrationForExistingUnregisteredSupportedProperty() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(EDGE_INDEX)).thenReturn(graphIndex);

        indexer.createEdgeIndex(management, PROPERTY_NAME, String.class, AtlasCardinality.SINGLE, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, times(1)).addMixedIndex(eq(EDGE_INDEX), eq(propertyKey), eq(false));
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 6 (control) -- Edge index: already-registered supported property must not be re-registered.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void edgeIndexDoesNotReRegisterAlreadyIndexedProperty() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(new HashSet<>(Collections.singletonList(propertyKey)));

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(EDGE_INDEX)).thenReturn(graphIndex);

        indexer.createEdgeIndex(management, PROPERTY_NAME, String.class, AtlasCardinality.SINGLE, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
    }

    // ----------------------------------------------------------------------------------------------------------
    // Case 6 (control) -- Edge index: unsupported property type must never be registered, matching
    // createVertexIndex()'s Case 4/5 parity.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void edgeIndexNeverRegistersUnsupportedPropertyType() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();
        AtlasGraphIndex       graphIndex  = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(EDGE_INDEX)).thenReturn(graphIndex);

        indexer.createEdgeIndex(management, PROPERTY_NAME, BigDecimal.class, AtlasCardinality.SINGLE, false);

        verify(management, never()).makePropertyKey(any(), any(), any());
        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
    }

    // ----------------------------------------------------------------------------------------------------------
    // isIndexApplicable() also excludes any "many" cardinality, independent of data type. Verify this pre-existing
    // gate still takes precedence: even a String property with LIST cardinality must never be mixed-indexed,
    // regardless of mixed-index membership state.
    // ----------------------------------------------------------------------------------------------------------
    @Test
    public void manyCardinalityStringPropertyIsNeverRegisteredRegardlessOfMembership() {
        AtlasGraphManagement management  = mock(AtlasGraphManagement.class);
        AtlasPropertyKey      propertyKey = mockPropertyKey();

        AtlasGraphIndex graphIndex = mockGraphIndex(Collections.emptySet());

        when(management.getPropertyKey(PROPERTY_NAME)).thenReturn(propertyKey);
        when(management.getGraphIndex(VERTEX_INDEX)).thenReturn(graphIndex);

        String indexFieldName = indexer.createVertexIndex(management, PROPERTY_NAME, GraphBackedSearchIndexer.UniqueKind.NONE,
                String.class, AtlasCardinality.LIST, false, false, false);

        verify(management, never()).addMixedIndex(any(), any(), anyBoolean(), anyBoolean());
        verify(management, never()).getIndexFieldName(any(), any(), anyBoolean(), anyBoolean());
        assertNull(indexFieldName);
    }
}
