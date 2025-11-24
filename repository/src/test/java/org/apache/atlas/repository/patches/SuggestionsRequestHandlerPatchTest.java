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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.repository.graph.SolrIndexHelper;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SuggestionsRequestHandlerPatchTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    private SuggestionsRequestHandlerPatch patch;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);
        when(patchRegistry.getStatus(any())).thenReturn(null);
        lenient().doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        lenient().doNothing().when(patchRegistry).updateStatus(any(), any());

        patch = new SuggestionsRequestHandlerPatch(patchContext);
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_004");
    }

    @Test
    public void testApplyWithEntityDefs() throws AtlasBaseException {
        Collection<AtlasEntityDef> entityDefs = createMockEntityDefs();
        when(typeRegistry.getAllEntityDefs()).thenReturn(entityDefs);

        try (MockedConstruction<SolrIndexHelper> mockedConstruction = mockConstruction(SolrIndexHelper.class, (mock, context) -> { doNothing().when(mock).onChange(any(ChangedTypeDefs.class)); })) {
            patch.apply();

            assertEquals(patch.getStatus(), APPLIED);

            assertEquals(mockedConstruction.constructed().size(), 1);
            SolrIndexHelper constructedHelper = mockedConstruction.constructed().get(0);

            ArgumentCaptor<ChangedTypeDefs> captor = ArgumentCaptor.forClass(ChangedTypeDefs.class);
            verify(constructedHelper, times(1)).onChange(captor.capture());

            ChangedTypeDefs capturedChangedTypeDefs = captor.getValue();
            assertNotNull(capturedChangedTypeDefs);
        }
    }

    @Test
    public void testApplyWithEmptyEntityDefs() throws AtlasBaseException {
        when(typeRegistry.getAllEntityDefs()).thenReturn(Collections.emptyList());

        try (MockedConstruction<SolrIndexHelper> mockedConstruction = mockConstruction(SolrIndexHelper.class)) {
            patch.apply();

            assertEquals(patch.getStatus(), APPLIED);

            assertEquals(mockedConstruction.constructed().size(), 0);
        }
    }

    @Test
    public void testApplyWithNullEntityDefs() throws AtlasBaseException {
        when(typeRegistry.getAllEntityDefs()).thenReturn(null);

        try (MockedConstruction<SolrIndexHelper> mockedConstruction = mockConstruction(SolrIndexHelper.class)) {
            patch.apply();

            assertEquals(patch.getStatus(), APPLIED);

            assertEquals(mockedConstruction.constructed().size(), 0);
        }
    }

    @Test
    public void testApplyWithMultipleEntityDefs() throws AtlasBaseException {
        Collection<AtlasEntityDef> entityDefs = createLargeEntityDefsList();
        when(typeRegistry.getAllEntityDefs()).thenReturn(entityDefs);

        try (MockedConstruction<SolrIndexHelper> mockedConstruction = mockConstruction(SolrIndexHelper.class, (mock, context) -> { doNothing().when(mock).onChange(any(ChangedTypeDefs.class)); })) {
            patch.apply();

            assertEquals(patch.getStatus(), APPLIED);
            assertEquals(mockedConstruction.constructed().size(), 1);
            SolrIndexHelper constructedHelper = mockedConstruction.constructed().get(0);
            verify(constructedHelper, times(1)).onChange(any(ChangedTypeDefs.class));
        }
    }

    private Collection<AtlasEntityDef> createMockEntityDefs() {
        Collection<AtlasEntityDef> entityDefs = new ArrayList<>();
        AtlasEntityDef entityDef1 = new AtlasEntityDef();
        entityDef1.setName("TestEntity1");
        AtlasEntityDef entityDef2 = new AtlasEntityDef();
        entityDef2.setName("TestEntity2");
        entityDefs.add(entityDef1);
        entityDefs.add(entityDef2);
        return entityDefs;
    }

    private Collection<AtlasEntityDef> createLargeEntityDefsList() {
        Collection<AtlasEntityDef> entityDefs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AtlasEntityDef entityDef = new AtlasEntityDef();
            entityDef.setName("TestEntity" + i);
            entityDefs.add(entityDef);
        }
        return entityDefs;
    }
}
