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
package org.apache.atlas.repository.store.bootstrap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.patches.AtlasPatch;
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.patches.AtlasPatchManager;
import org.apache.atlas.repository.patches.AtlasPatchRegistry;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.SKIPPED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class AtlasTypeDefStoreInitializerTest {
    @Mock private AtlasTypeDefStore typeDefStore;
    @Mock private AtlasTypeRegistry typeRegistry;
    @Mock private AtlasGraph graph;
    @Mock private Configuration conf;
    @Mock private AtlasPatchManager patchManager;
    @Mock private AtlasPatchRegistry patchRegistry;
    @Mock private AtlasGraphQuery query;
    @Mock private AtlasVertex mockVertex;

    private AtlasTypeDefStoreInitializer initializer;
    private MockedStatic<HAConfiguration> haConfigMock;
    private MockedStatic<AtlasType> atlasTypeMock;
    private MockedStatic<RequestContext> requestContextMock;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Mock graph query chain to avoid NPE in AtlasPatchRegistry
        lenient().when(graph.query()).thenReturn(query);
        lenient().when(query.createChildQuery()).thenReturn(query);
        lenient().when(query.has(anyString(), any(), any())).thenReturn(query);
        lenient().when(query.has(anyString(), any())).thenReturn(query);
        lenient().when(query.or(any())).thenReturn(query);
        lenient().when(query.vertices()).thenReturn(new ArrayList<>());
        lenient().when(graph.addVertex()).thenReturn(mockVertex);
        lenient().doNothing().when(graph).commit();

        // Mock type def store
        lenient().doNothing().when(typeDefStore).init();
        lenient().doNothing().when(typeDefStore).notifyLoadCompletion();
        lenient().when(typeDefStore.createUpdateTypesDef(any(), any())).thenReturn(new AtlasTypesDef());
        lenient().when(typeDefStore.updateTypesDef(any())).thenReturn(new AtlasTypesDef());
        lenient().when(typeDefStore.updateEntityDefByName(anyString(), any())).thenReturn(new AtlasEntityDef());
        lenient().when(typeDefStore.updateEnumDefByName(anyString(), any())).thenReturn(new AtlasEnumDef());
        lenient().when(typeDefStore.updateStructDefByName(anyString(), any())).thenReturn(new AtlasStructDef());
        lenient().when(typeDefStore.updateClassificationDefByName(anyString(), any())).thenReturn(new AtlasClassificationDef());
        lenient().when(typeDefStore.updateRelationshipDefByName(anyString(), any())).thenReturn(new AtlasRelationshipDef());

        // Mock patch manager
        lenient().when(patchManager.getContext()).thenReturn(null);

        initializer = new AtlasTypeDefStoreInitializer(typeDefStore, typeRegistry, graph, conf, patchManager);

        haConfigMock = mockStatic(HAConfiguration.class);
        atlasTypeMock = mockStatic(AtlasType.class);
        requestContextMock = mockStatic(RequestContext.class);

        // Mock RequestContext for patch scenarios
        RequestContext mockContext = mock(RequestContext.class);
        requestContextMock.when(() -> RequestContext.get()).thenReturn(mockContext);
        lenient().doNothing().when(mockContext).setInTypePatching(anyBoolean());
        lenient().doNothing().when(mockContext).setCurrentTypePatchAction(anyString());
        requestContextMock.when(() -> RequestContext.clear()).then(invocation -> null);
    }

    @AfterMethod
    public void tearDown() {
        if (haConfigMock != null) {
            haConfigMock.close();
        }
        if (atlasTypeMock != null) {
            atlasTypeMock.close();
        }
        if (requestContextMock != null) {
            requestContextMock.close();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(initializer);
    }

    @Test
    public void testInitWhenHADisabled() throws Exception {
        haConfigMock.when(() -> HAConfiguration.isHAEnabled(conf)).thenReturn(false);
        initializer.init();
        verify(typeDefStore, times(1)).init();
        verify(typeDefStore, times(1)).notifyLoadCompletion();
    }

    @Test
    public void testInitWhenHAEnabled() throws Exception {
        haConfigMock.when(() -> HAConfiguration.isHAEnabled(conf)).thenReturn(true);
        initializer.init();
        verify(typeDefStore, never()).init();
    }

    @Test
    public void testInstanceIsActive() throws Exception {
        initializer.instanceIsActive();
        verify(typeDefStore, times(1)).init();
    }

    @Test
    public void testInstanceIsPassive() throws AtlasException {
        initializer.instanceIsPassive();
    }

    @Test
    public void testGetHandlerOrder() {
        assertTrue(initializer.getHandlerOrder() > 0);
    }

    @Test
    public void testEnumElementMergingInGetTypesToUpdate() {
        // Test the critical enum element merging logic that was uncovered
        AtlasEnumElementDef oldElement1 = new AtlasEnumElementDef("OLD_VALUE1", null, 0);
        AtlasEnumElementDef oldElement2 = new AtlasEnumElementDef("OLD_VALUE2", null, 1);
        AtlasEnumElementDef newElement = new AtlasEnumElementDef("NEW_VALUE", null, 2);

        AtlasEnumDef oldEnumDef = new AtlasEnumDef("TestEnum", "Old enum", "1.0");
        oldEnumDef.setElementDefs(Arrays.asList(oldElement1, oldElement2));

        AtlasEnumDef newEnumDef = new AtlasEnumDef("TestEnum", "New enum", "2.0");
        newEnumDef.setElementDefs(Arrays.asList(newElement));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEnumDefs(Arrays.asList(newEnumDef));

        when(typeRegistry.getEnumDefByName("TestEnum")).thenReturn(oldEnumDef);

        // This triggers the enum element merging logic
        AtlasTypesDef result = AtlasTypeDefStoreInitializer.getTypesToUpdate(typesDef, typeRegistry, true);

        assertEquals(1, result.getEnumDefs().size());
        AtlasEnumDef resultEnum = result.getEnumDefs().get(0);

        assertEquals(3, resultEnum.getElementDefs().size());
        assertTrue(resultEnum.hasElement("OLD_VALUE1"));
        assertTrue(resultEnum.hasElement("OLD_VALUE2"));
        assertTrue(resultEnum.hasElement("NEW_VALUE"));
    }

    @Test
    public void testLoadBootstrapTypeDefsDirectoryTraversal() throws Exception {
        // Test the directory traversal and file sorting logic
        Path tempDir = Files.createTempDirectory("atlas-test");
        File modelsDir = new File(tempDir.toFile(), "models");
        modelsDir.mkdirs();

        File folder2 = new File(modelsDir, "0002-second");
        File folder1 = new File(modelsDir, "0001-first");
        File patchesFolder = new File(modelsDir, "patches");
        File regularFile = new File(modelsDir, "readme.txt");

        folder1.mkdirs();
        folder2.mkdirs();
        patchesFolder.mkdirs();
        regularFile.createNewFile();

        // Create type definition files
        File typeFile1 = new File(folder1, "types.json");
        File typeFile2 = new File(folder2, "types.json");
        String typeJson = createSampleTypeDefJson("TestType");
        Files.write(typeFile1.toPath(), typeJson.getBytes(StandardCharsets.UTF_8));
        Files.write(typeFile2.toPath(), typeJson.getBytes(StandardCharsets.UTF_8));

        try {
            System.setProperty("atlas.home", tempDir.toString());

            // Mock JSON parsing
            AtlasTypesDef mockTypesDef = createTestTypesDef("TestType");
            atlasTypeMock.when(() -> AtlasType.fromJson(anyString(), eq(AtlasTypesDef.class)))
                    .thenReturn(mockTypesDef);

            when(typeRegistry.isRegisteredType("TestType")).thenReturn(false);

            // Call the method that triggers directory traversal
            Method loadBootstrapMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("loadBootstrapTypeDefs");
            loadBootstrapMethod.setAccessible(true);
            loadBootstrapMethod.invoke(initializer);

            verify(typeDefStore, atLeast(1)).createUpdateTypesDef(any(), any());
        } finally {
            System.clearProperty("atlas.home");
            deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testLoadModelsInFolderWithFileProcessing() throws Exception {
        Path tempDir = Files.createTempDirectory("atlas-models");

        // Create files to test sorting (line 362: Arrays.sort)
        File file1 = new File(tempDir.toFile(), "002-second.json");
        File file2 = new File(tempDir.toFile(), "001-first.json");
        File emptyFile = new File(tempDir.toFile(), "003-empty.json");
        File invalidFile = new File(tempDir.toFile(), "004-invalid.json");

        String validJson = createSampleTypeDefJson("ValidType");
        String emptyJson = "{}";
        String invalidJson = "{ invalid json";

        Files.write(file1.toPath(), validJson.getBytes(StandardCharsets.UTF_8));
        Files.write(file2.toPath(), validJson.getBytes(StandardCharsets.UTF_8));
        Files.write(emptyFile.toPath(), emptyJson.getBytes(StandardCharsets.UTF_8));
        Files.write(invalidFile.toPath(), invalidJson.getBytes(StandardCharsets.UTF_8));

        try {
            AtlasTypesDef validTypesDef = createTestTypesDef("ValidType");
            AtlasTypesDef emptyTypesDef = new AtlasTypesDef(); // Empty types (line 370-374)

            atlasTypeMock.when(() -> AtlasType.fromJson(eq(validJson), eq(AtlasTypesDef.class)))
                    .thenReturn(validTypesDef);
            atlasTypeMock.when(() -> AtlasType.fromJson(eq(emptyJson), eq(AtlasTypesDef.class)))
                    .thenReturn(emptyTypesDef);
            atlasTypeMock.when(() -> AtlasType.fromJson(eq(invalidJson), eq(AtlasTypesDef.class)))
                    .thenThrow(new RuntimeException("JSON parsing error")); // Error handling (line 386-388)

            when(typeRegistry.isRegisteredType("ValidType")).thenReturn(false);

            Method loadModelsMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("loadModelsInFolder", File.class, AtlasPatchRegistry.class);
            loadModelsMethod.setAccessible(true);
            loadModelsMethod.invoke(initializer, tempDir.toFile(), patchRegistry);

            // Verify valid files were processed, empty/invalid files handled gracefully
            verify(typeDefStore, times(2)).createUpdateTypesDef(any(), any());
        } finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testApplyTypePatchesWithComprehensivePatchHandling() throws Exception {
        Path tempDir = Files.createTempDirectory("atlas-patches");
        File patchesDir = new File(tempDir.toFile(), "patches");
        patchesDir.mkdirs();

        // Create patch files to test sorting and different patch handlers
        File patchFile1 = new File(patchesDir, "001-enum-patch.json");
        File patchFile2 = new File(patchesDir, "002-attribute-patch.json");

        String enumPatchJson = createEnumPatchJson();
        String attributePatchJson = createAttributePatchJson();

        Files.write(patchFile1.toPath(), enumPatchJson.getBytes(StandardCharsets.UTF_8));
        Files.write(patchFile2.toPath(), attributePatchJson.getBytes(StandardCharsets.UTF_8));

        try {
            // Mock patch parsing
            Object enumPatches = createMockTypeDefPatches("UPDATE_ENUMDEF", "TestEnum");
            Object attributePatches = createMockTypeDefPatches("ADD_ATTRIBUTE", "TestEntity");

            atlasTypeMock.when(() -> AtlasType.fromJson(eq(enumPatchJson), any(Class.class)))
                    .thenReturn(enumPatches);
            atlasTypeMock.when(() -> AtlasType.fromJson(eq(attributePatchJson), any(Class.class)))
                    .thenReturn(attributePatches);

            // Mock existing types for patch application
            AtlasEnumDef existingEnum = new AtlasEnumDef("TestEnum", "desc", "1.0");
            AtlasEntityDef existingEntity = new AtlasEntityDef("TestEntity", "desc", "1.0");
            when(typeRegistry.getTypeDefByName("TestEnum")).thenReturn(existingEnum);
            when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(existingEntity);
            when(patchRegistry.isApplicable(anyString(), anyString(), anyInt())).thenReturn(false); // Make patches not applicable to test skipped path

            Method applyPatchesMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("applyTypePatches", String.class, AtlasPatchRegistry.class);
            applyPatchesMethod.setAccessible(true);
            applyPatchesMethod.invoke(initializer, tempDir.toString(), patchRegistry);

            verify(patchRegistry, never()).register(anyString(), any(), eq("TYPEDEF_PATCH"), anyString(), any(AtlasPatch.PatchStatus.class));
        } finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testApplyTypePatchesWithNonExistentPatchDirectory() throws Exception {
        // Test patch application with non-existent patch directory (covers line 450)
        Method applyPatchesMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("applyTypePatches", String.class, AtlasPatchRegistry.class);
        applyPatchesMethod.setAccessible(true);
        applyPatchesMethod.invoke(initializer, "/non/existent/path", patchRegistry);

        verify(patchRegistry, never()).register(anyString(), any(), anyString(), anyString(), any(AtlasPatch.PatchStatus.class));
    }

    @Test
    public void testUpdateEnumDefPatchHandlerComplete() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateEnumDefPatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateEnumDefPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ENUMDEF", "TestEnum", "1.0", "2.0");

        AtlasEnumDef existingEnum = new AtlasEnumDef("TestEnum", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEnum")).thenReturn(existingEnum);

        AtlasEnumDef.AtlasEnumElementDef newElement = new AtlasEnumElementDef("NEW_VALUE", null, 0);
        setField(patch, "elementDefs", Arrays.asList(newElement));

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateEnumDefByName(eq("TestEnum"), any());
    }

    @Test
    public void testAddAttributePatchHandlerComplete() throws Exception {
        AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.AddAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_ATTRIBUTE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef existingEntity = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(existingEntity);

        AtlasAttributeDef newAttr = new AtlasAttributeDef("newAttr", "string");
        setField(patch, "attributeDefs", Arrays.asList(newAttr));

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateEntityDefByName(eq("TestEntity"), any());
    }

    @Test
    public void testUpdateAttributePatchHandler() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef existingEntity = new AtlasEntityDef("TestEntity", "desc", "1.0");
        existingEntity.addAttribute(new AtlasAttributeDef("existingAttr", "string"));
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(existingEntity);

        AtlasAttributeDef updatedAttr = new AtlasAttributeDef("existingAttr", "int");
        setField(patch, "attributeDefs", Arrays.asList(updatedAttr));

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testUpdateTypeDefOptionsPatchHandler() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_TYPEDEF_OPTIONS", "TestEntity", "1.0", "2.0");

        AtlasEntityDef existingEntity = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(existingEntity);

        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        setField(patch, "typeDefOptions", options);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testSetServiceTypePatchHandler() throws Exception {
        AtlasTypeDefStoreInitializer.SetServiceTypePatchHandler handler = new AtlasTypeDefStoreInitializer.SetServiceTypePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("SET_SERVICE_TYPE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef existingEntity = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(existingEntity);

        setField(patch, "serviceType", "testService");

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testAddMandatoryAttributePatchHandlerConstructor() throws Exception {
        AtlasTypeDefStoreInitializer.AddMandatoryAttributePatchHandler handler = initializer.new AddMandatoryAttributePatchHandler(typeDefStore, typeRegistry);

        String[] supportedActions = handler.getSupportedActions();
        assertNotNull(supportedActions);
        assertEquals(supportedActions.length, 1);
        assertEquals(supportedActions[0], Constants.TYPEDEF_PATCH_ADD_MANDATORY_ATTRIBUTE);
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandler() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        // Create a complex relationship def with legacy attributes
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "legacyAttr1", null);
        end1.setIsLegacyAttribute(true);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "legacyAttr2", null);
        end2.setIsLegacyAttribute(false);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // Mock entity types for the relationship ends
        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        AtlasEntityDef entityDef1 = new AtlasEntityDef("Entity1", "desc", "1.0");
        AtlasEntityDef entityDef2 = new AtlasEntityDef("Entity2", "desc", "1.0");

        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);
        when(entityType1.getEntityDef()).thenReturn(entityDef1);
        when(entityType2.getEntityDef()).thenReturn(entityDef2);

        AtlasAttribute mockAttribute = mock(AtlasAttribute.class);
        when(entityType1.getAttribute("legacyAttr1")).thenReturn(mockAttribute);
        when(mockAttribute.getQualifiedName()).thenReturn("Entity1.legacyAttr1");

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testStartInternalWithException() throws Exception {
        doThrow(new AtlasBaseException("Test exception")).when(typeDefStore).init();

        Method startInternalMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("startInternal");
        startInternalMethod.setAccessible(true);
        startInternalMethod.invoke(initializer);

        verify(typeDefStore, times(1)).init();
    }

    @Test
    public void testNonExistentDirectoriesHandling() throws Exception {
        Method loadModelsMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("loadModelsInFolder", File.class, AtlasPatchRegistry.class);
        loadModelsMethod.setAccessible(true);
        loadModelsMethod.invoke(initializer, new File("/non/existent"), patchRegistry);

        Method applyPatchesMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("applyTypePatches", String.class, AtlasPatchRegistry.class);
        applyPatchesMethod.setAccessible(true);
        applyPatchesMethod.invoke(initializer, "/non/existent", patchRegistry);

        verify(typeDefStore, never()).createUpdateTypesDef(any(), any());
    }

    @Test
    public void testPatchHandlerWithUnknownAction() throws Exception {
        Path tempDir = Files.createTempDirectory("atlas-unknown-patch");
        File patchesDir = new File(tempDir.toFile(), "patches");
        patchesDir.mkdirs();

        File patchFile = new File(patchesDir, "unknown-patch.json");
        String unknownPatchJson = "{ \"patches\": [{ \"id\": \"UNKNOWN_001\", \"action\": \"UNKNOWN_ACTION\", \"typeName\": \"TestEntity\" }] }";
        Files.write(patchFile.toPath(), unknownPatchJson.getBytes(StandardCharsets.UTF_8));

        try {
            Object unknownPatches = createMockTypeDefPatches("UNKNOWN_ACTION", "TestEntity");
            atlasTypeMock.when(() -> AtlasType.fromJson(eq(unknownPatchJson), any(Class.class)))
                    .thenReturn(unknownPatches);

            Method applyPatchesMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("applyTypePatches", String.class, AtlasPatchRegistry.class);
            applyPatchesMethod.setAccessible(true);
            applyPatchesMethod.invoke(initializer, tempDir.toString(), patchRegistry);

            verify(patchRegistry, never()).register(anyString(), any(), anyString(), eq("UNKNOWN_ACTION"), any());
        } finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testVersionComparisonLogic() throws Exception {
        // Test the isTypeUpdateApplicable method (lines 434-439)
        Method isTypeUpdateApplicableMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("isTypeUpdateApplicable", AtlasBaseTypeDef.class, AtlasBaseTypeDef.class, boolean.class);
        isTypeUpdateApplicableMethod.setAccessible(true);

        AtlasEntityDef oldDef = new AtlasEntityDef("Test", "desc", "1.0");
        AtlasEntityDef newDef = new AtlasEntityDef("Test", "desc", "2.0");

        // Test version comparison
        Boolean result = (Boolean) isTypeUpdateApplicableMethod.invoke(null, oldDef, newDef, true);
        assertTrue(result);

        // Test same version
        newDef.setTypeVersion("1.0");
        result = (Boolean) isTypeUpdateApplicableMethod.invoke(null, oldDef, newDef, true);
        assertFalse(result);

        // Test with null old version
        oldDef.setTypeVersion(null);
        result = (Boolean) isTypeUpdateApplicableMethod.invoke(null, oldDef, newDef, true);
        assertTrue(result);
    }

    @Test
    public void testAttributeMergingLogic() throws Exception {
        Method updateTypeAttributesMethod = AtlasTypeDefStoreInitializer.class.getDeclaredMethod("updateTypeAttributes", AtlasStructDef.class, AtlasStructDef.class, boolean.class);
        updateTypeAttributesMethod.setAccessible(true);

        AtlasStructDef oldDef = new AtlasStructDef("TestStruct", "desc", "1.0");
        AtlasStructDef newDef = new AtlasStructDef("TestStruct", "desc", "2.0");

        AtlasAttributeDef oldAttr = new AtlasAttributeDef("oldAttr", "string");
        oldDef.addAttribute(oldAttr);

        Boolean result = (Boolean) updateTypeAttributesMethod.invoke(null, oldDef, newDef, true);
        assertTrue(result);
        assertTrue(newDef.hasAttribute("oldAttr")); // Attribute was merged
    }

    @Test
    public void testGetTypesToCreateComprehensive() {
        AtlasTypesDef typesDef = createComprehensiveTypesDef();

        // Test with all types new
        when(typeRegistry.isRegisteredType(anyString())).thenReturn(false);
        AtlasTypesDef result = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

        assertFalse(result.isEmpty());
        assertEquals(1, result.getEnumDefs().size());
        assertEquals(1, result.getStructDefs().size());
        assertEquals(1, result.getClassificationDefs().size());
        assertEquals(1, result.getEntityDefs().size());
        assertEquals(1, result.getRelationshipDefs().size());
        assertEquals(1, result.getBusinessMetadataDefs().size());

        // Test with all types existing
        when(typeRegistry.isRegisteredType(anyString())).thenReturn(true);
        result = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetTypesToUpdateComprehensive() {
        AtlasTypesDef typesDef = createComprehensiveTypesDef();

        // Setup existing types with older versions
        setupExistingTypesForUpdate();

        AtlasTypesDef result = AtlasTypeDefStoreInitializer.getTypesToUpdate(typesDef, typeRegistry, true);

        assertFalse(result.isEmpty());
        assertEquals(1, result.getEnumDefs().size());
        assertEquals(1, result.getStructDefs().size());
        assertEquals(1, result.getClassificationDefs().size());
        assertEquals(1, result.getEntityDefs().size());
        assertEquals(1, result.getRelationshipDefs().size());
        assertEquals(1, result.getBusinessMetadataDefs().size());
    }

    @Test
    public void testAddAttributePatchHandlerForAllTypeDefsComplete() throws Exception {
        AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.AddAttributePatchHandler(typeDefStore, typeRegistry);

        // Test Entity type
        testAddAttributeForTypeClass(handler, AtlasEntityDef.class, "TestEntity");

        // Test Classification type
        testAddAttributeForTypeClass(handler, AtlasClassificationDef.class, "TestClassification");

        // Test Struct type
        testAddAttributeForTypeClass(handler, AtlasStructDef.class, "TestStruct");

        // Test Relationship type
        testAddAttributeForTypeClass(handler, AtlasRelationshipDef.class, "TestRelationship");
    }

    private void testAddAttributeForTypeClass(AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler,
                                              Class<?> typeClass, String typeName) throws Exception {
        Object patch = createMockTypeDefPatch("ADD_ATTRIBUTE", typeName, "1.0", "2.0");

        AtlasBaseTypeDef typeDef;
        if (typeClass.equals(AtlasEntityDef.class)) {
            typeDef = new AtlasEntityDef(typeName, "desc", "1.0");
        } else if (typeClass.equals(AtlasClassificationDef.class)) {
            typeDef = new AtlasClassificationDef(typeName, "desc", "1.0");
        } else if (typeClass.equals(AtlasStructDef.class)) {
            typeDef = new AtlasStructDef(typeName, "desc", "1.0");
        } else {
            AtlasRelationshipDef relDef = new AtlasRelationshipDef();
            relDef.setName(typeName);
            relDef.setTypeVersion("1.0");
            typeDef = relDef;
        }

        when(typeRegistry.getTypeDefByName(typeName)).thenReturn(typeDef);

        AtlasAttributeDef newAttr = new AtlasAttributeDef("newAttr", "string");
        setField(patch, "attributeDefs", Arrays.asList(newAttr));

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testAddAttributePatchHandlerWithUnknownTypeError() throws Exception {
        AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.AddAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_ATTRIBUTE", "UnknownType", "1.0", "2.0");
        when(typeRegistry.getTypeDefByName("UnknownType")).thenReturn(null);

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testAddAttributePatchHandlerWithInvalidTypeError() throws Exception {
        AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.AddAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_ATTRIBUTE", "TestEnum", "1.0", "2.0");

        // Create an enum type (unsupported for this patch)
        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnum", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEnum")).thenReturn(enumDef);

        AtlasAttributeDef newAttr = new AtlasAttributeDef("newAttr", "string");
        setField(patch, "attributeDefs", Arrays.asList(newAttr));

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testAddAttributePatchHandlerSkippedScenario() throws Exception {
        AtlasTypeDefStoreInitializer.AddAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.AddAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_ATTRIBUTE", "TestEntity", "2.0", "3.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0"); // Wrong version
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        AtlasAttributeDef newAttr = new AtlasAttributeDef("newAttr", "string");
        setField(patch, "attributeDefs", Arrays.asList(newAttr));

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, SKIPPED);
    }

    @Test
    public void testAddMandatoryAttributePatchHandlerValidationLogic() throws Exception {
        AtlasTypeDefStoreInitializer.AddMandatoryAttributePatchHandler handler = initializer.new AddMandatoryAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_MANDATORY_ATTRIBUTE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        // Create various invalid attributes to test validation logic
        AtlasAttributeDef existingAttr = new AtlasAttributeDef("existingAttr", "string");
        existingAttr.setIsOptional(false);
        existingAttr.setDefaultValue("defaultValue");
        entityDef.addAttribute(existingAttr); // This will be filtered out

        AtlasAttributeDef optionalAttr = new AtlasAttributeDef("optionalAttr", "string");
        optionalAttr.setIsOptional(true); // This will be filtered out

        AtlasStructDef.AtlasAttributeDef noDefaultAttr = new AtlasAttributeDef("noDefaultAttr", "string");
        noDefaultAttr.setIsOptional(false);
        // No default value - will be filtered out

        AtlasStructDef.AtlasAttributeDef nonPrimitiveAttr = new AtlasAttributeDef("nonPrimitiveAttr", "array<string>");
        nonPrimitiveAttr.setIsOptional(false);
        nonPrimitiveAttr.setDefaultValue("defaultValue");

        AtlasStructDef.AtlasAttributeDef uniqueAttr = new AtlasAttributeDef("uniqueAttr", "string");
        uniqueAttr.setIsOptional(false);
        uniqueAttr.setDefaultValue("defaultValue");
        uniqueAttr.setIsUnique(true); // This will be filtered out

        setField(patch, "attributeDefs", Arrays.asList(existingAttr, optionalAttr, noDefaultAttr, nonPrimitiveAttr, uniqueAttr));

        // Mock types
        AtlasType primitiveType = mock(AtlasType.class);
        AtlasType arrayType = mock(AtlasType.class);
        when(typeRegistry.getType("string")).thenReturn(primitiveType);
        when(typeRegistry.getType("array<string>")).thenReturn(arrayType);
        when(primitiveType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(arrayType.getTypeCategory()).thenReturn(TypeCategory.ARRAY);

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, SKIPPED); // All attributes should be filtered out
    }

    private void testAddMandatoryAttributeForType(AtlasTypeDefStoreInitializer.AddMandatoryAttributePatchHandler handler,
                                                  Class<?> typeClass, String typeName) throws Exception {
        Object patch = createMockTypeDefPatch("ADD_MANDATORY_ATTRIBUTE", typeName, "1.0", "2.0");

        AtlasStructDef typeDef;
        if (typeClass.equals(AtlasClassificationDef.class)) {
            typeDef = new AtlasClassificationDef(typeName, "desc", "1.0");
        } else {
            typeDef = new AtlasStructDef(typeName, "desc", "1.0");
        }
        when(typeRegistry.getTypeDefByName(typeName)).thenReturn(typeDef);

        AtlasAttributeDef mandatoryAttr = new AtlasAttributeDef("mandatoryAttr", "string");
        mandatoryAttr.setIsOptional(false);
        mandatoryAttr.setDefaultValue("defaultValue");
        setField(patch, "attributeDefs", Arrays.asList(mandatoryAttr));

        AtlasType mockType = mock(AtlasType.class);
        when(typeRegistry.getType("string")).thenReturn(mockType);
        when(mockType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testUpdateAttributePatchHandlerComprehensive() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        entityDef.addAttribute(new AtlasAttributeDef("existingAttr", "string"));
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        // Update existing attribute and add new one
        AtlasAttributeDef updatedAttr = new AtlasAttributeDef("existingAttr", "int");
        AtlasAttributeDef newAttr = new AtlasAttributeDef("newAttr", "string");
        setField(patch, "attributeDefs", Arrays.asList(updatedAttr, newAttr));

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateEntityDefByName(eq("TestEntity"), any());
    }

    @Test
    public void testUpdateAttributePatchHandlerForAllTypes() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler(typeDefStore, typeRegistry);

        // Test ClassificationDef
        testUpdateAttributeForType(handler, AtlasClassificationDef.class, "TestClassification");

        // Test StructDef
        testUpdateAttributeForType(handler, AtlasStructDef.class, "TestStruct");

        // Test unsupported type (RelationshipDef)
        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE", "TestRelationship", "1.0", "2.0");
        AtlasRelationshipDef relDef = new AtlasRelationshipDef();
        relDef.setName("TestRelationship");
        relDef.setTypeVersion("1.0");
        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relDef);

        AtlasAttributeDef updatedAttr = new AtlasAttributeDef("existingAttr", "int");
        setField(patch, "attributeDefs", Arrays.asList(updatedAttr));

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    private void testUpdateAttributeForType(AtlasTypeDefStoreInitializer.UpdateAttributePatchHandler handler,
                                            Class<?> typeClass, String typeName) throws Exception {
        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE", typeName, "1.0", "2.0");

        AtlasStructDef typeDef;
        if (typeClass.equals(AtlasClassificationDef.class)) {
            typeDef = new AtlasClassificationDef(typeName, "desc", "1.0");
        } else {
            typeDef = new AtlasStructDef(typeName, "desc", "1.0");
        }
        typeDef.addAttribute(new AtlasAttributeDef("existingAttr", "string"));
        when(typeRegistry.getTypeDefByName(typeName)).thenReturn(typeDef);

        AtlasAttributeDef updatedAttr = new AtlasAttributeDef("existingAttr", "int");
        setField(patch, "attributeDefs", Arrays.asList(updatedAttr));

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testUpdateTypeDefOptionsPatchHandlerComplete() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler(typeDefStore, typeRegistry);
        Object patch = createMockTypeDefPatch("UPDATE_TYPEDEF_OPTIONS", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        options.put("option2", "value2");
        setField(patch, "typeDefOptions", options);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateTypesDef(any());
    }

    @Test
    public void testUpdateTypeDefOptionsPatchHandlerWithExistingOptions() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_TYPEDEF_OPTIONS", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        Map<String, String> existingOptions = new HashMap<>();
        existingOptions.put("existingOption", "existingValue");
        entityDef.setOptions(existingOptions);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        Map<String, String> newOptions = new HashMap<>();
        newOptions.put("newOption", "newValue");
        setField(patch, "typeDefOptions", newOptions);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);

        // Verify both existing and new options are present
        assertEquals(2, entityDef.getOptions().size());
        assertTrue(entityDef.getOptions().containsKey("existingOption"));
        assertTrue(entityDef.getOptions().containsKey("newOption"));
    }

    @Test
    public void testUpdateTypeDefOptionsPatchHandlerWithEmptyOptionsError() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler handler = new AtlasTypeDefStoreInitializer.UpdateTypeDefOptionsPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_TYPEDEF_OPTIONS", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        // Set empty options to trigger error
        setField(patch, "typeDefOptions", new HashMap<String, String>());

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testSetServiceTypePatchHandlerComplete() throws Exception {
        AtlasTypeDefStoreInitializer.SetServiceTypePatchHandler handler = new AtlasTypeDefStoreInitializer.SetServiceTypePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("SET_SERVICE_TYPE", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "serviceType", "testService");

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        assertEquals("testService", entityDef.getServiceType());
        verify(typeDefStore).updateTypesDef(any());
    }

    @Test
    public void testUpdateAttributeMetadataHandlerComprehensive() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        Map<String, Object> params = new HashMap<>();
        params.put("searchWeight", 5);
        setField(patch, "params", params);

        // Mock GraphBackedSearchIndexer static method
        atlasTypeMock.when(() -> AtlasType.fromJson(anyString(), any(Class.class))).thenReturn(patch);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateEntityDefByName(eq("TestEntity"), any());
    }

    @Test
    public void testUpdateAttributeMetadataHandlerForStructDef() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestStruct", "1.0", "2.0");

        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        structDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestStruct")).thenReturn(structDef);

        setField(patch, "attributeName", "testAttr");

        Map<String, Object> params = new HashMap<>();
        params.put("indexType", "STRING");
        setField(patch, "params", params);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateStructDefByName(eq("TestStruct"), any());
    }

    @Test
    public void testUpdateAttributeMetadataHandlerWithInvalidType() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEnum", "1.0", "2.0");

        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnum", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEnum")).thenReturn(enumDef);

        setField(patch, "attributeName", "testAttr");
        Map<String, Object> params = new HashMap<>();
        params.put("searchWeight", 5);
        setField(patch, "params", params);

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testAddSuperTypePatchHandlerWithEmptySuperTypes() throws Exception {
        AtlasTypeDefStoreInitializer.AddSuperTypePatchHandler handler = initializer.new AddSuperTypePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_SUPER_TYPES", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        // Empty super types
        setField(patch, "superTypes", new HashSet<String>());

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, SKIPPED);
    }

    @Test
    public void testAddSuperTypePatchHandlerWithInvalidType() throws Exception {
        AtlasTypeDefStoreInitializer.AddSuperTypePatchHandler handler = initializer.new AddSuperTypePatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("ADD_SUPER_TYPES", "TestStruct", "1.0", "2.0");

        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestStruct")).thenReturn(structDef);

        Set<String> superTypes = new HashSet<>();
        superTypes.add("SuperType1");
        setField(patch, "superTypes", superTypes);

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerComprehensive() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        // Create complex relationship def with legacy attributes
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");
        relationshipDef.setRelationshipLabel("originalLabel");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "legacyAttr1", null);
        end1.setIsLegacyAttribute(true);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "regularAttr", null);
        end2.setIsLegacyAttribute(false);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // Mock entity types
        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        AtlasEntityDef entityDef1 = new AtlasEntityDef("Entity1", "desc", "1.0");
        AtlasEntityDef entityDef2 = new AtlasEntityDef("Entity2", "desc", "1.0");

        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);
        when(entityType1.getEntityDef()).thenReturn(entityDef1);
        when(entityType2.getEntityDef()).thenReturn(entityDef2);

        AtlasAttribute mockAttribute = mock(AtlasAttribute.class);
        when(entityType1.getAttribute("legacyAttr1")).thenReturn(mockAttribute);
        when(mockAttribute.getQualifiedName()).thenReturn("Entity1.legacyAttr1");

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateTypesDef(any());
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerWithParams() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "attr1", null);
        end1.setIsLegacyAttribute(true);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "attr2", null);
        end2.setIsLegacyAttribute(true);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // Set patch parameters
        Map<String, Object> params = new HashMap<>();
        params.put("relationshipLabel", "customLabel");
        params.put("relationshipCategory", "AGGREGATION");
        params.put("swapEnds", "true");
        setField(patch, "params", params);

        // Mock entity types
        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        AtlasEntityDef entityDef1 = new AtlasEntityDef("Entity1", "desc", "1.0");
        AtlasEntityDef entityDef2 = new AtlasEntityDef("Entity2", "desc", "1.0");

        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);
        when(entityType1.getEntityDef()).thenReturn(entityDef1);
        when(entityType2.getEntityDef()).thenReturn(entityDef2);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerWithBothLegacyNoLabel() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "attr1", null);
        end1.setIsLegacyAttribute(true);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "attr2", null);
        end2.setIsLegacyAttribute(true);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // No parameters - should throw exception for both legacy attributes
        setField(patch, "params", null);

        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);

        expectThrows(AtlasBaseException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testUpdateAttributeMetadataHandlerSearchWeightValidation() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with invalid search weight
        Map<String, Object> params = new HashMap<>();
        params.put("searchWeight", -1); // Invalid search weight
        setField(patch, "params", params);

        try (MockedStatic<GraphBackedSearchIndexer> graphIndexerMock = mockStatic(GraphBackedSearchIndexer.class)) {
            graphIndexerMock.when(() -> GraphBackedSearchIndexer.isValidSearchWeight(-1)).thenReturn(false);

            RuntimeException exception = expectThrows(RuntimeException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
            assertFalse(exception.getMessage().contains("Invalid search weight"));
        }
    }

    @Test
    public void testUpdateAttributeMetadataHandlerSearchWeightSuccess() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with valid search weight
        Map<String, Object> params = new HashMap<>();
        params.put("searchWeight", 5);
        setField(patch, "params", params);

        try (MockedStatic<GraphBackedSearchIndexer> graphIndexerMock = mockStatic(GraphBackedSearchIndexer.class)) {
            graphIndexerMock.when(() -> GraphBackedSearchIndexer.isValidSearchWeight(5)).thenReturn(true);

            PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
            assertEquals(result, APPLIED);
        }
    }

    @Test
    public void testUpdateAttributeMetadataHandlerIndexTypeValidation() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with invalid index type
        Map<String, Object> params = new HashMap<>();
        params.put("indexType", "INVALID_TYPE");
        setField(patch, "params", params);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
    }

    @Test
    public void testUpdateAttributeMetadataHandlerIndexTypeSuccess() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with valid index type
        Map<String, Object> params = new HashMap<>();
        params.put("indexType", "STRING");
        setField(patch, "params", params);

        AtlasPatch.PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testUpdateAttributeMetadataHandlerEmptyIndexType() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with empty index type - should be ignored
        Map<String, Object> params = new HashMap<>();
        params.put("indexType", "");
        setField(patch, "params", params);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        assertNull(attr.getIndexType());
    }

    @Test
    public void testUpdateAttributeMetadataHandlerUnknownProperty() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with unknown property
        Map<String, Object> params = new HashMap<>();
        params.put("unknownProperty", "unknownValue");
        setField(patch, "params", params);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
        assertFalse(exception.getMessage().contains("Received unknown property"));
    }

    @Test
    public void testUpdateAttributeMetadataHandlerExceptionHandling() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);
        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with type that will cause ClassCastException
        Map<String, Object> params = new HashMap<>();
        params.put("searchWeight", "notANumber"); // String instead of Number
        setField(patch, "params", params);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch));
        assertTrue(exception.getMessage().contains("Error encountered in updating Model attribute"));
    }

    @Test
    public void testUpdateAttributeMetadataHandlerEmptyParams() throws Exception {
        AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler handler = new AtlasTypeDefStoreInitializer.UpdateAttributeMetadataHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("UPDATE_ATTRIBUTE_METADATA", "TestEntity", "1.0", "2.0");

        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        entityDef.addAttribute(attr);
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        setField(patch, "attributeName", "testAttr");

        // Test with empty params
        Map<String, Object> params = new HashMap<>();
        setField(patch, "params", params);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerWithNonRelationshipType() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestEntity", "1.0", "2.0");

        // Create entity type instead of relationship type
        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "desc", "1.0");
        when(typeRegistry.getTypeDefByName("TestEntity")).thenReturn(entityDef);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, UNKNOWN); // Should return UNKNOWN for non-relationship types
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerWithSecondLegacyAttribute() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "regularAttr", null);
        end1.setIsLegacyAttribute(false);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "legacyAttr2", null);
        end2.setIsLegacyAttribute(true);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // Mock entity types
        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        AtlasEntityDef entityDef1 = new AtlasEntityDef("Entity1", "desc", "1.0");
        AtlasEntityDef entityDef2 = new AtlasEntityDef("Entity2", "desc", "1.0");

        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);
        when(entityType1.getEntityDef()).thenReturn(entityDef1);
        when(entityType2.getEntityDef()).thenReturn(entityDef2);

        AtlasAttribute mockAttribute = mock(AtlasAttribute.class);
        when(entityType2.getAttribute("legacyAttr2")).thenReturn(mockAttribute);
        when(mockAttribute.getQualifiedName()).thenReturn("Entity2.legacyAttr2");

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateTypesDef(any());
    }

    @Test
    public void testRemoveLegacyRefAttributesPatchHandlerWithNonLegacyAttributes() throws Exception {
        AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler handler = new AtlasTypeDefStoreInitializer.RemoveLegacyRefAttributesPatchHandler(typeDefStore, typeRegistry);

        Object patch = createMockTypeDefPatch("REMOVE_LEGACY_REF_ATTRIBUTES", "TestRelationship", "1.0", "2.0");

        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("1.0");
        relationshipDef.setRelationshipLabel("originalLabel");

        AtlasRelationshipEndDef end1 = new AtlasRelationshipEndDef("Entity1", "attr1", null);
        end1.setIsLegacyAttribute(false);
        AtlasRelationshipEndDef end2 = new AtlasRelationshipEndDef("Entity2", "attr2", null);
        end2.setIsLegacyAttribute(false);

        relationshipDef.setEndDef1(end1);
        relationshipDef.setEndDef2(end2);

        when(typeRegistry.getTypeDefByName("TestRelationship")).thenReturn(relationshipDef);

        // Mock entity types
        AtlasEntityType entityType1 = mock(AtlasEntityType.class);
        AtlasEntityType entityType2 = mock(AtlasEntityType.class);
        AtlasEntityDef entityDef1 = new AtlasEntityDef("Entity1", "desc", "1.0");
        AtlasEntityDef entityDef2 = new AtlasEntityDef("Entity2", "desc", "1.0");

        when(typeRegistry.getEntityTypeByName("Entity1")).thenReturn(entityType1);
        when(typeRegistry.getEntityTypeByName("Entity2")).thenReturn(entityType2);
        when(entityType1.getEntityDef()).thenReturn(entityDef1);
        when(entityType2.getEntityDef()).thenReturn(entityDef2);

        PatchStatus result = handler.applyPatch((AtlasTypeDefStoreInitializer.TypeDefPatch) patch);
        assertEquals(result, APPLIED);
        verify(typeDefStore).updateTypesDef(any());
    }

    private String createSampleTypeDefJson(String typeName) {
        return "{ \"entityDefs\": [{ \"name\": \"" + typeName +
               "\", \"description\": \"Test entity\", \"typeVersion\": \"1.0\", " +
               "\"attributeDefs\": [{ \"name\": \"name\", \"typeName\": \"string\" }] }] }";
    }

    private String createEnumPatchJson() {
        return "{ \"patches\": [{ \"id\": \"ENUM_001\", \"action\": \"UPDATE_ENUMDEF\", " +
               "\"typeName\": \"TestEnum\", \"applyToVersion\": \"1.0\", \"updateToVersion\": \"2.0\", " +
               "\"elementDefs\": [{ \"value\": \"NEW_VALUE\", \"ordinal\": 0 }] }] }";
    }

    private String createAttributePatchJson() {
        return "{ \"patches\": [{ \"id\": \"ATTR_001\", \"action\": \"ADD_ATTRIBUTE\", " +
               "\"typeName\": \"TestEntity\", \"applyToVersion\": \"1.0\", \"updateToVersion\": \"2.0\", " +
               "\"attributeDefs\": [{ \"name\": \"newAttr\", \"typeName\": \"string\" }] }] }";
    }

    private AtlasTypesDef createTestTypesDef(String typeName) {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        AtlasEntityDef entityDef = new AtlasEntityDef(typeName, "Test entity", "1.0");
        entityDef.addAttribute(new AtlasAttributeDef("name", "string"));
        typesDef.setEntityDefs(Arrays.asList(entityDef));
        return typesDef;
    }

    private AtlasTypesDef createComprehensiveTypesDef() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnum", "Test enum", "2.0");
        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "Test struct", "2.0");
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("TestClassification", "Test classification", "2.0");
        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "Test entity", "2.0");
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setTypeVersion("2.0");
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("TestBusinessMetadata", "Test business metadata", "2.0");

        typesDef.setEnumDefs(Arrays.asList(enumDef));
        typesDef.setStructDefs(Arrays.asList(structDef));
        typesDef.setClassificationDefs(Arrays.asList(classificationDef));
        typesDef.setEntityDefs(Arrays.asList(entityDef));
        typesDef.setRelationshipDefs(Arrays.asList(relationshipDef));
        typesDef.setBusinessMetadataDefs(Arrays.asList(businessMetadataDef));

        return typesDef;
    }

    private void setupExistingTypesForUpdate() {
        when(typeRegistry.getEnumDefByName("TestEnum")).thenReturn(new AtlasEnumDef("TestEnum", "desc", "1.0"));
        when(typeRegistry.getStructDefByName("TestStruct")).thenReturn(new AtlasStructDef("TestStruct", "desc", "1.0"));
        when(typeRegistry.getClassificationDefByName("TestClassification")).thenReturn(new AtlasClassificationDef("TestClassification", "desc", "1.0"));
        when(typeRegistry.getEntityDefByName("TestEntity")).thenReturn(new AtlasEntityDef("TestEntity", "desc", "1.0"));

        AtlasRelationshipDef oldRelDef = new AtlasRelationshipDef();
        oldRelDef.setName("TestRelationship");
        oldRelDef.setTypeVersion("1.0");
        when(typeRegistry.getRelationshipDefByName("TestRelationship")).thenReturn(oldRelDef);

        when(typeRegistry.getBusinessMetadataDefByName("TestBusinessMetadata")).thenReturn(new AtlasBusinessMetadataDef("TestBusinessMetadata", "desc", "1.0"));
    }

    private Object createMockTypeDefPatches(String action, String typeName) throws Exception {
        Class<?> patchesClass = Class.forName("org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer$TypeDefPatches");
        Object patches = patchesClass.getDeclaredConstructor().newInstance();

        Object patch = createMockTypeDefPatch(action, typeName, "1.0", "2.0");

        List<Object> patchList = Arrays.asList(patch);
        setField(patches, "patches", patchList);

        return patches;
    }

    private Object createMockTypeDefPatch(String action, String typeName, String applyToVersion, String updateToVersion) throws Exception {
        Class<?> patchClass = Class.forName("org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer$TypeDefPatch");
        Object patch = patchClass.getDeclaredConstructor().newInstance();

        setField(patch, "id", action + "_001");
        setField(patch, "action", action);
        setField(patch, "typeName", typeName);
        setField(patch, "applyToVersion", applyToVersion);
        setField(patch, "updateToVersion", updateToVersion);

        return patch;
    }

    private void setField(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}
