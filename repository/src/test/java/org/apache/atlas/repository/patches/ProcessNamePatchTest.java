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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ProcessNamePatchTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private AtlasGraphQuery query;

    @Mock
    private AtlasVertex vertex;

    @Mock
    private AtlasEntityType entityType;

    @Mock
    private WorkItemManager<Long, ?> workItemManager;

    private ProcessNamePatch patch;
    private MockedStatic<AtlasConfiguration> atlasConfigurationMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);
        when(patchContext.getIndexer()).thenReturn(indexer);
        when(patchContext.getEntityGraphMapper()).thenReturn(entityGraphMapper);
        when(patchRegistry.getStatus(any())).thenReturn(null);
        lenient().doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        lenient().doNothing().when(patchRegistry).updateStatus(any(), any());

        atlasConfigurationMock = mockStatic(AtlasConfiguration.class);
        patch = new ProcessNamePatch(patchContext);
    }

    @AfterMethod
    public void tearDown() {
        if (atlasConfigurationMock != null) {
            atlasConfigurationMock.close();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_009");
    }

    @Test
    public void testApplyWhenProcessNameUpdatePatchDisabled() throws AtlasBaseException {
        patch.apply();

        verify(graph, never()).query();
    }

    @Test
    public void testProcessNamePatchProcessorConstructor() {
        ProcessNamePatch.ProcessNamePatchProcessor processor = new ProcessNamePatch.ProcessNamePatchProcessor(patchContext);

        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testPrepareForExecution() {
        ProcessNamePatch.ProcessNamePatchProcessor processor = new ProcessNamePatch.ProcessNamePatchProcessor(patchContext);

        processor.prepareForExecution();
    }

    @Test
    public void testSubmitVerticesToUpdate() throws Exception {
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("hive_process"))).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("hive_column_lineage"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList(1L, 2L, 3L));

        ProcessNamePatch.ProcessNamePatchProcessor processor = new ProcessNamePatch.ProcessNamePatchProcessor(patchContext);

        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(6)).checkProduce(any()); // 3 vertices * 2 process types
    }

    @Test
    public void testSubmitVerticesToUpdateWithEmptyResults() throws Exception {
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("hive_process"))).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("hive_column_lineage"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList());

        ProcessNamePatch.ProcessNamePatchProcessor processor = new ProcessNamePatch.ProcessNamePatchProcessor(patchContext);

        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(0)).checkProduce(any());
    }

    @Test
    public void testProcessTypesStaticField() throws Exception {
        Field processTypesField = ProcessNamePatch.ProcessNamePatchProcessor.class.getDeclaredField("processTypes");
        processTypesField.setAccessible(true);

        String[] processTypes = (String[]) processTypesField.get(null);

        assertNotNull(processTypes);
        assertEquals(processTypes.length, 2);
        assertEquals(processTypes[0], "hive_process");
        assertEquals(processTypes[1], "hive_column_lineage");
    }

    @Test
    public void testAttributeNameConstants() throws Exception {
        Field qualifiedNameField = ProcessNamePatch.ProcessNamePatchProcessor.class.getDeclaredField("ATTR_NAME_QUALIFIED_NAME");
        Field nameField = ProcessNamePatch.ProcessNamePatchProcessor.class.getDeclaredField("ATTR_NAME_NAME");

        qualifiedNameField.setAccessible(true);
        nameField.setAccessible(true);

        String qualifiedNameConstant = (String) qualifiedNameField.get(null);
        String nameConstant = (String) nameField.get(null);

        assertEquals(qualifiedNameConstant, "qualifiedName");
        assertEquals(nameConstant, "name");
    }
}
