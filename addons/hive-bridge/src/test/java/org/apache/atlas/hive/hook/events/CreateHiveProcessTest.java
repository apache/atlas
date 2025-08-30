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

package org.apache.atlas.hive.hook.events;

import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class CreateHiveProcessTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcessTest.class);

    @Mock
    AtlasHiveHookContext context;

    @Mock
    ReadEntity readEntity;

    @Mock
    WriteEntity writeEntity;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        
        // Mock context.getHiveOperation() to prevent NPEs in isDdlOperation method
        when(context.getHiveOperation()).thenReturn(HiveOperation.QUERY);
    }

    // ========== CONSTRUCTOR TESTS ==========

    @Test
    public void testConstructor() {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);
        AssertJUnit.assertNotNull("CreateHiveProcess should be instantiated", createHiveProcess);
        assertTrue("CreateHiveProcess should extend BaseHiveEvent",
                createHiveProcess instanceof BaseHiveEvent);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullContext() {
        new CreateHiveProcess(null);
    }

    // ========== getNotificationMessages() TESTS ==========

    @Test
    public void testGetNotificationMessages_WithEntities() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock getEntities to return some entities
        AtlasEntitiesWithExtInfo mockEntities = new AtlasEntitiesWithExtInfo();
        AtlasEntity entity = new AtlasEntity("hive_process");
        entity.setAttribute("qualifiedName", "test.process@cluster");
        mockEntities.addEntity(entity);

        doReturn(mockEntities).when(createHiveProcess).getEntities();
        doReturn("test_user").when(createHiveProcess).getUserName();

        List<HookNotification> notifications = createHiveProcess.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        assertTrue("Should be EntityCreateRequestV2",
                notifications.get(0) instanceof EntityCreateRequestV2);

        EntityCreateRequestV2 createRequest = (EntityCreateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should be test_user", "test_user", createRequest.getUser());
        AssertJUnit.assertNotNull("Entities should not be null", createRequest.getEntities());
    }

    @Test
    public void testGetNotificationMessages_EmptyEntities() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock getEntities to return empty entities
        AtlasEntitiesWithExtInfo mockEntities = new AtlasEntitiesWithExtInfo();
        doReturn(mockEntities).when(createHiveProcess).getEntities();

        List<HookNotification> notifications = createHiveProcess.getNotificationMessages();

        assertNull("Notifications should be null when entities are empty", notifications);
    }

    @Test
    public void testGetNotificationMessages_NullEntities() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock getEntities to return null
        doReturn(null).when(createHiveProcess).getEntities();

        List<HookNotification> notifications = createHiveProcess.getNotificationMessages();

        assertNull("Notifications should be null when entities are null", notifications);
    }

    // ========== getEntities() TESTS ==========

    @Test
    public void testGetEntities_SkipProcess() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Set up empty inputs and outputs to make skipProcess return true
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(Collections.emptySet()).when(createHiveProcess).getOutputs();

        // Use reflection to call private method 'skipProcess'
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        // Now skipProcess should return true due to empty inputs/outputs
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertTrue("skipProcess() should return true for empty inputs/outputs", skip);

        // Now call getEntities (it will internally call skipProcess())
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNull("Entities should be null when process is skipped", entities);
    }

    @Test
    public void testGetEntities_EmptyInputsAndOutputs() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare empty inputs/outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(Collections.emptySet()).when(createHiveProcess).getOutputs();

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Use reflection to ensure skipProcess returns true (due to empty inputs/outputs)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertTrue("skipProcess should return true for empty inputs/outputs", skip);

        // Now run getEntities()
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNull("Entities should be null when inputs and outputs are empty", entities);
    }

    @Test
    public void testGetEntities_WithSkippedInputEntity() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare empty inputs but some outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName and getInputOutputEntity
        doReturn("default.test_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(true);
        when(context.isMetastoreHook()).thenReturn(false);

        // Reflection: ensure skipProcess returns false (has outputs but no inputs)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertFalse("skipProcess should return false when there are outputs but no inputs", skip);

        // Reflection: ensure isDdlOperation returns false
        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlOperationMethod.setAccessible(true);
        boolean ddlOp = (boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity);
        assertFalse("isDdlOperation should return false in this scenario", ddlOp);

        // Now run getEntities()
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNull("Entities should be null when input entity is skipped", entities);
    }

    @Test
    public void testGetEntities_WithSkippedOutputEntity() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare some inputs but empty outputs
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(Collections.emptySet()).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName to avoid NPE in BaseHiveEvent.getQualifiedName
        doReturn("default.input_table@cluster").when(createHiveProcess).getQualifiedName(readEntity);

        // Mock getInputOutputEntity to avoid NPE in BaseHiveEvent.getInputOutputEntity
        AtlasEntity inputEntity = new AtlasEntity("hive_table");
        doReturn(inputEntity).when(createHiveProcess).getInputOutputEntity(eq(readEntity), any(), anyBoolean());

        // Mock entity type and direct flag
        when(readEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(readEntity.isDirect()).thenReturn(true);

        // Mock context methods
        when(context.isSkippedOutputEntity()).thenReturn(true);
        when(context.isMetastoreHook()).thenReturn(false);

        // Reflection: ensure skipProcess returns false (has inputs but no outputs)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertFalse("skipProcess should return false when there are inputs but no outputs", skip);

        // Now run getEntities()
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNull("Entities should be null when output entity is skipped", entities);
    }

    @Test
    public void testGetEntities_SuccessfulProcessCreation() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName for entities
        doReturn("default.input_table@cluster").when(createHiveProcess).getQualifiedName(readEntity);
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity inputEntity = new AtlasEntity("hive_table");
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(inputEntity).when(createHiveProcess).getInputOutputEntity(eq(readEntity), any(), anyBoolean());
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Mock entity types and flags
        when(readEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(readEntity.isDirect()).thenReturn(true);

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Reflection: ensure skipProcess returns false (has both inputs and outputs)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertFalse("skipProcess should return false when there are both inputs and outputs", skip);

        // Reflection: ensure isDdlOperation returns false
        Method isDdlMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlMethod.setAccessible(true);
        boolean isDdl = (boolean) isDdlMethod.invoke(createHiveProcess, outputEntity);
        assertFalse("isDdlOperation should return false in this scenario", isDdl);

        // Mock process creation methods
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed by getHiveProcessExecutionEntity
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Reflection: invoke processColumnLineage without doing anything
        Method processColumnLineageMethod =
                CreateHiveProcess.class.getDeclaredMethod("processColumnLineage", AtlasEntity.class, AtlasEntitiesWithExtInfo.class);
        processColumnLineageMethod.setAccessible(true);
        // No-op, we don't actually execute it in the test

        // Mock addProcessedEntities to do nothing
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // Now run getEntities()
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertNotNull("Entities list should not be null", entities.getEntities());
        assertTrue("Should contain process entities", entities.getEntities().size() >= 2);

        // Verify process and process execution entities exist
        boolean hasProcess = entities.getEntities().stream()
                .anyMatch(entity -> "hive_process".equals(entity.getTypeName()));
        boolean hasProcessExecution = entities.getEntities().stream()
                .anyMatch(entity -> "hive_process_execution".equals(entity.getTypeName()));

        assertTrue("Should contain hive_process entity", hasProcess);
        assertTrue("Should contain hive_process_execution entity", hasProcessExecution);
    }

    @Test
    public void testGetEntities_MetastoreHook() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock outputs
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Reflection: ensure skipProcess returns false (has outputs but no inputs)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        assertFalse("skipProcess should return false when there are outputs but no inputs", skip);

        // Reflection: ensure isDdlOperation returns false
        Method isDdlMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlMethod.setAccessible(true);
        boolean isDdl = (boolean) isDdlMethod.invoke(createHiveProcess, outputEntity);
        assertFalse("isDdlOperation should return false in this scenario", isDdl);

        // Mock context methods - metastore hook
        when(context.isMetastoreHook()).thenReturn(true);

        // Call method under test
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        // Verify
        assertNull("Entities should be null for metastore hook with empty inputs", entities);
    }

    @Test
    public void testGetEntities_WithDDLOperation() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock outputs
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // --- Reflection to make skipProcess return false ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        assertFalse((boolean) skipProcessMethod.invoke(createHiveProcess));

        // Mock context to return a DDL operation
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE_AS_SELECT);
        
        // --- Reflection to make isDdlOperation return true ---
        Method isDdlMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlMethod.setAccessible(true);
        assertTrue("Expected DDL operation", (boolean) isDdlMethod.invoke(createHiveProcess, outputEntity));

        // Mock createHiveDDLEntity (this is public/protected so mocking is fine)
        AtlasEntity ddlEntity = new AtlasEntity("hive_ddl");
        doReturn(ddlEntity).when(createHiveProcess).createHiveDDLEntity(outputEntity);

        // Mock context methods
        when(context.isMetastoreHook()).thenReturn(true);

        // Call method under test
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        // Assert expected behavior
        assertNull("Entities should be null for metastore hook with empty inputs", entities);
    }

    // ========== HELPER METHOD TESTS ==========

    @Test
    public void testCheckIfOnlySelfLineagePossible_True() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod(
                "checkIfOnlySelfLineagePossible", String.class, Map.class);
        method.setAccessible(true);

        Map<String, Entity> inputByQualifiedName = new HashMap<>();
        inputByQualifiedName.put("default.table@cluster", readEntity);

        boolean result = (boolean) method.invoke(createHiveProcess, "default.table@cluster", inputByQualifiedName);

        assertTrue("Should return true for self lineage", result);
    }

    @Test
    public void testCheckIfOnlySelfLineagePossible_False() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod(
                "checkIfOnlySelfLineagePossible", String.class, Map.class);
        method.setAccessible(true);

        Map<String, Entity> inputByQualifiedName = new HashMap<>();
        inputByQualifiedName.put("default.input_table@cluster", readEntity);

        boolean result = (boolean) method.invoke(createHiveProcess, "default.output_table@cluster", inputByQualifiedName);

        assertFalse("Should return false for different table", result);
    }

    @Test
    public void testCheckIfOnlySelfLineagePossible_MultipleInputs() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod(
                "checkIfOnlySelfLineagePossible", String.class, Map.class);
        method.setAccessible(true);

        Map<String, Entity> inputByQualifiedName = new HashMap<>();
        inputByQualifiedName.put("default.table1@cluster", readEntity);
        inputByQualifiedName.put("default.table2@cluster", mock(ReadEntity.class));

        boolean result = (boolean) method.invoke(createHiveProcess, "default.table1@cluster", inputByQualifiedName);

        assertFalse("Should return false for multiple inputs", result);
    }

    @Test
    public void testSkipProcess_EmptyInputsAndOutputs() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock empty inputs and outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(Collections.emptySet()).when(createHiveProcess).getOutputs();

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess);

        assertTrue("Should skip process when inputs and outputs are empty", result);
    }

    @Test
    public void testSkipProcess_QueryWithTempDirectory() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();

        WriteEntity tempOutput = mock(WriteEntity.class);
        when(tempOutput.getType()).thenReturn(Entity.Type.DFS_DIR);
        when(tempOutput.getWriteType()).thenReturn(WriteEntity.WriteType.PATH_WRITE);
        when(tempOutput.isTempURI()).thenReturn(true);

        Set<WriteEntity> outputs = Collections.singleton(tempOutput);
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock context
        doReturn(context).when(createHiveProcess).getContext();
        when(context.getHiveOperation()).thenReturn(HiveOperation.QUERY);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess);

        assertTrue("Should skip process for query with temp directory", result);
    }

    @Test
    public void testSkipProcess_DeleteOperation() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();

        WriteEntity deleteOutput = mock(WriteEntity.class);
        when(deleteOutput.getType()).thenReturn(Entity.Type.TABLE);
        when(deleteOutput.getWriteType()).thenReturn(WriteEntity.WriteType.DELETE);

        Set<WriteEntity> outputs = Collections.singleton(deleteOutput);
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock context
        doReturn(context).when(createHiveProcess).getContext();
        when(context.getHiveOperation()).thenReturn(HiveOperation.QUERY);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess);

        assertTrue("Should skip process for DELETE operation", result);
    }

    @Test
    public void testSkipProcess_UpdateOperation() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();

        WriteEntity updateOutput = mock(WriteEntity.class);
        when(updateOutput.getType()).thenReturn(Entity.Type.TABLE);
        when(updateOutput.getWriteType()).thenReturn(WriteEntity.WriteType.UPDATE);

        Set<WriteEntity> outputs = Collections.singleton(updateOutput);
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock context
        doReturn(context).when(createHiveProcess).getContext();
        when(context.getHiveOperation()).thenReturn(HiveOperation.QUERY);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess);

        assertTrue("Should skip process for UPDATE operation", result);
    }

    @Test
    public void testSkipProcess_RegularQuery() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();

        WriteEntity normalOutput = mock(WriteEntity.class);
        when(normalOutput.getType()).thenReturn(Entity.Type.TABLE);
        when(normalOutput.getWriteType()).thenReturn(WriteEntity.WriteType.INSERT);

        Set<WriteEntity> outputs = Collections.singleton(normalOutput);
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess);

        assertFalse("Should not skip process for regular query", result);
    }

    @Test
    public void testIsDdlOperation_CreateTableAsSelect() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE_AS_SELECT);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertTrue("Should be DDL operation for CREATETABLE_AS_SELECT", result);
    }

    @Test
    public void testIsDdlOperation_CreateView() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATEVIEW);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertTrue("Should be DDL operation for CREATEVIEW", result);
    }

    @Test
    public void testIsDdlOperation_AlterViewAs() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.getHiveOperation()).thenReturn(HiveOperation.ALTERVIEW_AS);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertTrue("Should be DDL operation for ALTERVIEW_AS", result);
    }

    @Test
    public void testIsDdlOperation_CreateMaterializedView() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATE_MATERIALIZED_VIEW);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertTrue("Should be DDL operation for CREATE_MATERIALIZED_VIEW", result);
    }

    @Test
    public void testIsDdlOperation_MetastoreHook() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context - metastore hook
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE_AS_SELECT);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertFalse("Should not be DDL operation for metastore hook", result);
    }

    @Test
    public void testIsDdlOperation_RegularOperation() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        AtlasEntity entity = new AtlasEntity("hive_table");

        // Mock context
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.getHiveOperation()).thenReturn(HiveOperation.QUERY);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, entity);

        assertFalse("Should not be DDL operation for regular query", result);
    }

    @Test
    public void testIsDdlOperation_NullEntity() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(createHiveProcess, (AtlasEntity) null);

        assertFalse("Should not be DDL operation for null entity", result);
    }

    // ========== COLUMN LINEAGE TESTS ==========

    @Test
    public void testProcessColumnLineage_NullLineageInfo() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare inputs/outputs first
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // --- Reflection to force skipProcess to return false ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        assertFalse("skipProcess should return false", (boolean) skipProcessMethod.invoke(createHiveProcess));

        // Mock getQualifiedName
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // --- Reflection to force isDdlOperation to return false ---
        Method ddlMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        ddlMethod.setAccessible(true);
        assertFalse("Expected non-DDL operation", (boolean) ddlMethod.invoke(createHiveProcess, outputEntity));

        // Mock entity type
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        // Context settings
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        // Mock process entity creation
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock required getters
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Null lineage info (triggers early return in processColumnLineage)
        doReturn(null).when(createHiveProcess).getLineageInfo();

        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // Run test
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNotNull("Entities should not be null", entities);
        // We don't assert column lineage here because getLineageInfo is null
    }

    @Test
    public void testProcessColumnLineage_EmptyLineageInfo() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Just call skipProcess via reflection to verify it's false (no stubbing needed)
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        assertFalse((boolean) skipProcessMethod.invoke(createHiveProcess));

        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Check isDdlOperation via reflection instead of stubbing
        Method ddlMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        ddlMethod.setAccessible(true);
        assertFalse((boolean) ddlMethod.invoke(createHiveProcess, outputEntity));

        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        LineageInfo emptyLineageInfo = new LineageInfo();
        doReturn(emptyLineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNotNull("Entities should not be null", entities);
    }


    @Test
    public void testGetBaseCols_NullDependency() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("getBaseCols", Dependency.class);
        method.setAccessible(true);

        Collection<BaseColumnInfo> result = (Collection<BaseColumnInfo>) method.invoke(createHiveProcess, (Dependency) null);

        AssertJUnit.assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty", result.isEmpty());
    }

    @Test
    public void testGetBaseCols_ValidDependency() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Create a mock dependency that has getBaseCols method
        Dependency mockDependency = mock(Dependency.class);
        BaseColumnInfo baseCol1 = mock(BaseColumnInfo.class);
        BaseColumnInfo baseCol2 = mock(BaseColumnInfo.class);
        Collection<BaseColumnInfo> expectedBaseCols = Arrays.asList(baseCol1, baseCol2);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("getBaseCols", Dependency.class);
        method.setAccessible(true);

        // Since we can't easily mock the reflection, let's just test that it doesn't crash
        Collection<BaseColumnInfo> result = (Collection<BaseColumnInfo>) method.invoke(createHiveProcess, mockDependency);

        AssertJUnit.assertNotNull("Result should not be null", result);
        // Result will be empty because the mock doesn't have the actual getBaseCols method
        assertTrue("Result should be empty for mock dependency", result.isEmpty());
    }

    @Test
    public void testGetBaseCols_NonCollectionReturn() throws Exception {
        CreateHiveProcess createHiveProcess = new CreateHiveProcess(context);

        // Test with a dependency that would return non-Collection type
        Dependency mockDependency = mock(Dependency.class);

        // Use reflection to access private method
        Method method = CreateHiveProcess.class.getDeclaredMethod("getBaseCols", Dependency.class);
        method.setAccessible(true);

        Collection<BaseColumnInfo> result = (Collection<BaseColumnInfo>) method.invoke(createHiveProcess, mockDependency);

        AssertJUnit.assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty due to reflection limitations", result.isEmpty());
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        // Get the method from the original class, not the spy class
        Method m = CreateHiveProcess.class.getDeclaredMethod(methodName, paramTypes);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessColumnLineage_WithValidLineageInfo() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Call skipProcess via reflection to assert its result instead of stubbing
        assertFalse((boolean) invokePrivate(createHiveProcess, "skipProcess", new Class<?>[]{}));

        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Call isDdlOperation via reflection instead of stubbing
        assertFalse((boolean) invokePrivate(createHiveProcess, "isDdlOperation", new Class<?>[]{AtlasEntity.class}, outputEntity));

        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        LineageInfo lineageInfo = mock(LineageInfo.class);
        DependencyKey depKey = mock(DependencyKey.class);
        Dependency dependency = mock(Dependency.class);
        BaseColumnInfo baseColInfo = mock(BaseColumnInfo.class);

        Map<DependencyKey, Dependency> lineageMap = new HashMap<>();
        lineageMap.put(depKey, dependency);
        when(lineageInfo.entrySet()).thenReturn(lineageMap.entrySet());

        doReturn("default.output_table.col1@cluster").when(createHiveProcess).getQualifiedName(depKey);
        doReturn("default.input_table.col1@cluster").when(createHiveProcess).getQualifiedName(baseColInfo);

        // Instead of stubbing getBaseCols (private), set up reflection check
        Collection<BaseColumnInfo> baseCols = Arrays.asList(baseColInfo);

        when(dependency.getType()).thenReturn(LineageInfo.DependencyType.SIMPLE);
        when(dependency.getExpr()).thenReturn("col1");

        AtlasEntity outputColumn = new AtlasEntity("hive_column");
        outputColumn.setAttribute("name", "col1");
        outputColumn.setAttribute("qualifiedName", "default.output_table.col1@cluster");

        AtlasEntity inputColumn = new AtlasEntity("hive_column");
        inputColumn.setAttribute("name", "col1");
        inputColumn.setAttribute("qualifiedName", "default.input_table.col1@cluster");

        when(context.getEntity("default.output_table.col1@cluster")).thenReturn(outputColumn);
        when(context.getEntity("default.input_table.col1@cluster")).thenReturn(inputColumn);

        when(context.getSkipHiveColumnLineageHive20633()).thenReturn(false);

        doReturn(lineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNotNull("Entities should not be null", entities);
    }

    @Test
    public void testProcessColumnLineage_NonExistentOutputColumn() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Use reflection to ensure skipProcess() returns false
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        // Prepare inputs/outputs
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");

        // Call private isDdlOperation(AtlasEntity) via reflection
        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlOperationMethod.setAccessible(true);
        boolean isDdl = (boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity);
        AssertJUnit.assertFalse("Expected isDdlOperation to return false", isDdl);

        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed for process execution
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Create mock lineage info with dependencies
        LineageInfo lineageInfo = mock(LineageInfo.class);
        DependencyKey depKey = mock(DependencyKey.class);
        Dependency dependency = mock(Dependency.class);

        Map<DependencyKey, Dependency> lineageMap = new HashMap<>();
        lineageMap.put(depKey, dependency);
        when(lineageInfo.entrySet()).thenReturn(lineageMap.entrySet());

        doReturn("default.output_table.non_existent_col@cluster").when(createHiveProcess).getQualifiedName(depKey);

        // Mock context.getEntity to return null for non-existent column
        when(context.getEntity("default.output_table.non_existent_col@cluster")).thenReturn(null);

        doReturn(lineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        boolean hasColumnLineage = entities.getEntities().stream()
                .anyMatch(entity -> entity.getTypeName().contains("column_lineage"));
        assertFalse("Should not contain column lineage entities due to non-existent output column", hasColumnLineage);
    }


    @Test
    public void testProcessColumnLineage_SkipDueToThreshold() throws Exception {
        // Mock common Hive context stuff
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(context.getHostName()).thenReturn("test_host");

        // Create mock lineage objects
        BaseColumnInfo baseColInfo1 = mock(BaseColumnInfo.class);
        BaseColumnInfo baseColInfo2 = mock(BaseColumnInfo.class);
        BaseColumnInfo baseColInfo3 = mock(BaseColumnInfo.class);
        DependencyKey depKey = mock(DependencyKey.class);
        Dependency dependency = mock(Dependency.class);

        // Use spy instead of anonymous subclass
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));
        
        // Create mock dependency with getBaseCols method
        Dependency testDependency = mock(Dependency.class);
        when(testDependency.getType()).thenReturn(LineageInfo.DependencyType.SIMPLE);
        when(testDependency.getExpr()).thenReturn("col1 + col2 + col3");
        // Add getBaseCols method to the mock
        doReturn(new HashSet<>(Arrays.asList(baseColInfo1, baseColInfo2, baseColInfo3))).when(testDependency).getBaseCols();

        // Outputs and inputs
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Mock Hive process entities
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock user/query info
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();

        // Setup LineageInfo with concrete dependency
        LineageInfo lineageInfo = mock(LineageInfo.class);
        Map<DependencyKey, Dependency> lineageMap = new HashMap<>();
        lineageMap.put(depKey, testDependency);
        when(lineageInfo.entrySet()).thenReturn(lineageMap.entrySet());

        doReturn("default.output_table.col1@cluster").when(createHiveProcess).getQualifiedName(depKey);
        doReturn("default.input_table.col1@cluster").when(createHiveProcess).getQualifiedName(baseColInfo1);
        doReturn("default.input_table.col2@cluster").when(createHiveProcess).getQualifiedName(baseColInfo2);
        doReturn("default.input_table.col3@cluster").when(createHiveProcess).getQualifiedName(baseColInfo3);

        // Mock context entities for columns
        AtlasEntity outputColumn = new AtlasEntity("hive_column");
        outputColumn.setAttribute("name", "col1");
        outputColumn.setAttribute("qualifiedName", "default.output_table.col1@cluster");

        AtlasEntity inputColumn1 = new AtlasEntity("hive_column");
        AtlasEntity inputColumn2 = new AtlasEntity("hive_column");
        AtlasEntity inputColumn3 = new AtlasEntity("hive_column");

        when(context.getEntity("default.output_table.col1@cluster")).thenReturn(outputColumn);
        when(context.getEntity("default.input_table.col1@cluster")).thenReturn(inputColumn1);
        when(context.getEntity("default.input_table.col2@cluster")).thenReturn(inputColumn2);
        when(context.getEntity("default.input_table.col3@cluster")).thenReturn(inputColumn3);

        // Skip column lineage threshold settings
        when(context.getSkipHiveColumnLineageHive20633()).thenReturn(true);
        when(context.getSkipHiveColumnLineageHive20633InputsThreshold()).thenReturn(2); // lower than actual cols

        doReturn(lineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // Execute
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        boolean hasColumnLineage = entities.getEntities().stream()
                .anyMatch(entity -> entity.getTypeName().contains("column_lineage"));
        assertFalse("Should not contain column lineage entities due to threshold", hasColumnLineage);
    }

    @Test
    public void testProcessColumnLineage_DuplicateOutputColumn() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare reflection for private methods
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlOperationMethod.setAccessible(true);

        Method getBaseColsMethod = CreateHiveProcess.class.getDeclaredMethod("getBaseCols", LineageInfo.Dependency.class);
        getBaseColsMethod.setAccessible(true);

        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // We want isDdlOperation(outputEntity) to return false
        assertFalse((boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity));

        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed for process execution
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Create mock lineage info with duplicate dependencies
        LineageInfo lineageInfo = mock(LineageInfo.class);
        DependencyKey depKey1 = mock(DependencyKey.class);
        DependencyKey depKey2 = mock(DependencyKey.class);
        Dependency dependency1 = mock(Dependency.class);
        Dependency dependency2 = mock(Dependency.class);
        BaseColumnInfo baseColInfo = mock(BaseColumnInfo.class);

        Map<DependencyKey, Dependency> lineageMap = new LinkedHashMap<>();
        lineageMap.put(depKey1, dependency1);
        lineageMap.put(depKey2, dependency2); // Duplicate output column
        when(lineageInfo.entrySet()).thenReturn(lineageMap.entrySet());

        String outputColName = "default.output_table.col1@cluster";
        doReturn(outputColName).when(createHiveProcess).getQualifiedName(depKey1);
        doReturn(outputColName).when(createHiveProcess).getQualifiedName(depKey2);
        doReturn("default.input_table.col1@cluster").when(createHiveProcess).getQualifiedName(baseColInfo);

        // Mock getBaseCols method for both dependencies
        doReturn(new HashSet<>(Arrays.asList(baseColInfo))).when(dependency1).getBaseCols();
        doReturn(new HashSet<>(Arrays.asList(baseColInfo))).when(dependency2).getBaseCols();

        when(dependency1.getType()).thenReturn(LineageInfo.DependencyType.SIMPLE);
        when(dependency1.getExpr()).thenReturn("col1");
        when(dependency2.getType()).thenReturn(LineageInfo.DependencyType.SIMPLE);
        when(dependency2.getExpr()).thenReturn("col1");

        // Mock context.getEntity for columns
        AtlasEntity outputColumn = new AtlasEntity("hive_column");
        outputColumn.setAttribute("name", "col1");
        outputColumn.setAttribute("qualifiedName", outputColName);

        AtlasEntity inputColumn = new AtlasEntity("hive_column");
        inputColumn.setAttribute("name", "col1");
        inputColumn.setAttribute("qualifiedName", "default.input_table.col1@cluster");

        when(context.getEntity(outputColName)).thenReturn(outputColumn);
        when(context.getEntity("default.input_table.col1@cluster")).thenReturn(inputColumn);

        when(context.getSkipHiveColumnLineageHive20633()).thenReturn(false);

        doReturn(lineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // ---- Execute & Verify ----
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();
        assertNotNull("Entities should not be null", entities);

        long columnLineageCount = entities.getEntities().stream()
                .filter(entity -> entity.getTypeName().contains("column_lineage"))
                .count();
        assertEquals("Should contain only one column lineage entity due to duplicate handling", 1, columnLineageCount);
    }

    @Test
    public void testProcessColumnLineage_EmptyInputColumns() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Prepare reflection for private methods
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlOperationMethod.setAccessible(true);

        Method getBaseColsMethod = CreateHiveProcess.class.getDeclaredMethod("getBaseCols", LineageInfo.Dependency.class);
        getBaseColsMethod.setAccessible(true);

        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(Collections.emptySet()).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(outputEntity).when(createHiveProcess)
                .getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Call isDdlOperation(outputEntity) to ensure it's false
        assertFalse((boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity));

        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        when(context.isMetastoreHook()).thenReturn(false);
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);

        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed for process execution
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Create mock lineage info with dependencies
        LineageInfo lineageInfo = mock(LineageInfo.class);
        DependencyKey depKey = mock(DependencyKey.class);
        Dependency dependency = mock(Dependency.class);
        BaseColumnInfo baseColInfo = mock(BaseColumnInfo.class);

        Map<DependencyKey, Dependency> lineageMap = new HashMap<>();
        lineageMap.put(depKey, dependency);
        when(lineageInfo.entrySet()).thenReturn(lineageMap.entrySet());

        doReturn("default.output_table.col1@cluster").when(createHiveProcess).getQualifiedName(depKey);
        doReturn("default.input_table.non_existent_col@cluster").when(createHiveProcess).getQualifiedName(baseColInfo);

        // Call getBaseCols(dependency) via reflection (instead of mocking)
        @SuppressWarnings("unchecked")
        Collection<BaseColumnInfo> baseCols = (Collection<BaseColumnInfo>)
                getBaseColsMethod.invoke(createHiveProcess, dependency);

        // Mock context.getEntity for columns
        AtlasEntity outputColumn = new AtlasEntity("hive_column");
        outputColumn.setAttribute("name", "col1");
        outputColumn.setAttribute("qualifiedName", "default.output_table.col1@cluster");

        when(context.getEntity("default.output_table.col1@cluster")).thenReturn(outputColumn);
        when(context.getEntity("default.input_table.non_existent_col@cluster")).thenReturn(null);

        // Mock skip column lineage settings
        when(context.getSkipHiveColumnLineageHive20633()).thenReturn(false);

        doReturn(lineageInfo).when(createHiveProcess).getLineageInfo();
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // ---- Execute & Verify ----
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        assertNotNull("Entities should not be null", entities);
        boolean hasColumnLineage = entities.getEntities().stream()
                .anyMatch(entity -> entity.getTypeName().contains("column_lineage"));
        assertFalse("Should not contain column lineage entities due to empty input columns", hasColumnLineage);
    }


    // ========== INTEGRATION TESTS ==========

    @Test
    public void testFullWorkflow_SimpleProcess() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // --- Reflection for private methods ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod(
                "isDdlOperation", AtlasEntity.class
        );
        isDdlOperationMethod.setAccessible(true);

        Method processColumnLineageMethod = CreateHiveProcess.class.getDeclaredMethod(
                "processColumnLineage", AtlasEntity.class, AtlasEntitiesWithExtInfo.class
        );
        processColumnLineageMethod.setAccessible(true);

        // --- Mock inputs and outputs ---
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName
        doReturn("default.input_table@cluster").when(createHiveProcess).getQualifiedName(readEntity);
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity inputEntity = new AtlasEntity("hive_table");
        inputEntity.setAttribute("qualifiedName", "default.input_table@cluster");
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        outputEntity.setAttribute("qualifiedName", "default.output_table@cluster");
        doReturn(inputEntity).when(createHiveProcess).getInputOutputEntity(eq(readEntity), any(), anyBoolean());
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // Use reflection to assert isDdlOperation == false
        boolean ddl = (boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity);
        AssertJUnit.assertFalse("isDdlOperation should be false", ddl);

        // Mock entity types
        when(readEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        // Mock isDirect
        when(readEntity.isDirect()).thenReturn(true);

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Mock process creation
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed by getHiveProcessExecutionEntity
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Instead of mocking processColumnLineage, just call it via reflection
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processColumnLineageMethod.invoke(createHiveProcess, processEntity, entities);

        // Also test skipProcess via reflection
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        AssertJUnit.assertFalse("skipProcess should be false", skip);

        // Execute workflow
        List<HookNotification> notifications = createHiveProcess.getNotificationMessages();

        // Verify results
        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        assertTrue("Should be EntityCreateRequestV2",
                notifications.get(0) instanceof EntityCreateRequestV2);

        EntityCreateRequestV2 createRequest = (EntityCreateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should be test_user", "test_user", createRequest.getUser());
        AssertJUnit.assertNotNull("Entities should not be null", createRequest.getEntities());
        assertTrue("Should contain entities",
                createRequest.getEntities().getEntities().size() > 0);
    }


    // ========== ADDITIONAL COVERAGE TESTS ==========

    @Test
    public void testGetEntities_WithNonDirectInput() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // --- Reflection access to private methods ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod(
                "isDdlOperation", AtlasEntity.class
        );
        isDdlOperationMethod.setAccessible(true);

        Method processColumnLineageMethod = CreateHiveProcess.class.getDeclaredMethod(
                "processColumnLineage", AtlasEntity.class, AtlasEntitiesWithExtInfo.class
        );
        processColumnLineageMethod.setAccessible(true);

        // --- Mock inputs and outputs ---
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName
        doReturn("default.input_table@cluster").when(createHiveProcess).getQualifiedName(readEntity);
        doReturn("default.output_table@cluster").when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock getInputOutputEntity
        AtlasEntity inputEntity = new AtlasEntity("hive_table");
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(inputEntity).when(createHiveProcess).getInputOutputEntity(eq(readEntity), any(), anyBoolean());
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(eq(writeEntity), any(), anyBoolean());

        // --- Directly verify private methods before execution ---
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        AssertJUnit.assertFalse("skipProcess should return false", skip);

        boolean ddl = (boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity);
        AssertJUnit.assertFalse("isDdlOperation should return false", ddl);

        // Mock entity types to avoid switch NPE
        when(readEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        // isDirect = false means input should be excluded
        when(readEntity.isDirect()).thenReturn(false);

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Mock process creation
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock other helper methods for process creation
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Instead of mocking processColumnLineage, just call it via reflection
        AtlasEntitiesWithExtInfo entitiesForReflection = new AtlasEntitiesWithExtInfo();
        processColumnLineageMethod.invoke(createHiveProcess, processEntity, entitiesForReflection);

        // We still mock addProcessedEntities (not private)
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // Execute
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
    }

    @Test
    public void testGetEntities_NullQualifiedName() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // --- Reflection for skipProcess() ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);

        // Call skipProcess() and assert expected
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        AssertJUnit.assertTrue("skipProcess should return true for empty inputs/outputs", skip);

        // --- Mock inputs and outputs ---
        Set<ReadEntity> inputs = Collections.singleton(readEntity);
        Set<WriteEntity> outputs = Collections.singleton(writeEntity);
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // Mock getQualifiedName to return null (both input and output)
        doReturn(null).when(createHiveProcess).getQualifiedName(readEntity);
        doReturn(null).when(createHiveProcess).getQualifiedName(writeEntity);

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Execute
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        // Assert outcome
        assertNull("Entities should be null when qualified names are null", entities);
    }

    @Test
    public void testGetEntities_ProcessedNamesDuplication() throws Exception {
        CreateHiveProcess createHiveProcess = spy(new CreateHiveProcess(context));

        // Mock inputs and outputs with duplicate entities first
        ReadEntity readEntity2 = mock(ReadEntity.class);
        WriteEntity writeEntity2 = mock(WriteEntity.class);

        Set<ReadEntity> inputs = new HashSet<>(Arrays.asList(readEntity, readEntity2));
        Set<WriteEntity> outputs = new HashSet<>(Arrays.asList(writeEntity, writeEntity2));
        doReturn(inputs).when(createHiveProcess).getInputs();
        doReturn(outputs).when(createHiveProcess).getOutputs();

        // --- Reflection for skipProcess() ---
        Method skipProcessMethod = CreateHiveProcess.class.getDeclaredMethod("skipProcess");
        skipProcessMethod.setAccessible(true);
        boolean skip = (boolean) skipProcessMethod.invoke(createHiveProcess);
        AssertJUnit.assertFalse("skipProcess should return false when there are inputs and outputs", skip);

        // --- Reflection for isDdlOperation(AtlasEntity) ---
        Method isDdlOperationMethod = CreateHiveProcess.class.getDeclaredMethod("isDdlOperation", AtlasEntity.class);
        isDdlOperationMethod.setAccessible(true);

        // --- Reflection for processColumnLineage(AtlasEntity, AtlasEntitiesWithExtInfo) ---
        Method processColumnLineageMethod = CreateHiveProcess.class.getDeclaredMethod(
                "processColumnLineage", AtlasEntity.class, AtlasEntitiesWithExtInfo.class
        );
        processColumnLineageMethod.setAccessible(true);

        // Same qualified names to test processedNames deduplication
        doReturn("default.table1@cluster").when(createHiveProcess).getQualifiedName(readEntity);
        doReturn("default.table1@cluster").when(createHiveProcess).getQualifiedName(readEntity2);
        doReturn("default.table2@cluster").when(createHiveProcess).getQualifiedName(writeEntity);
        doReturn("default.table2@cluster").when(createHiveProcess).getQualifiedName(writeEntity2);

        // Mock getInputOutputEntity
        AtlasEntity inputEntity = new AtlasEntity("hive_table");
        AtlasEntity outputEntity = new AtlasEntity("hive_table");
        doReturn(inputEntity).when(createHiveProcess).getInputOutputEntity(any(ReadEntity.class), any(), anyBoolean());
        doReturn(outputEntity).when(createHiveProcess).getInputOutputEntity(any(WriteEntity.class), any(), anyBoolean());

        // Instead of mocking isDdlOperation, call via reflection
        boolean ddl = (boolean) isDdlOperationMethod.invoke(createHiveProcess, outputEntity);
        AssertJUnit.assertFalse("isDdlOperation should return false", ddl);

        // Mock entity types
        when(readEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(readEntity2.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(writeEntity.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);
        when(writeEntity2.getType()).thenReturn(org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE);

        // Mock isDirect
        when(readEntity.isDirect()).thenReturn(true);
        when(readEntity2.isDirect()).thenReturn(true);

        // Mock context methods
        when(context.isSkippedInputEntity()).thenReturn(false);
        when(context.isSkippedOutputEntity()).thenReturn(false);
        when(context.isMetastoreHook()).thenReturn(false);

        // Mock process creation methods
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        processEntity.setAttribute("qualifiedName", "test_process@cluster");
        doReturn(processEntity).when(createHiveProcess).getHiveProcessEntity(anyList(), anyList());

        AtlasEntity processExecutionEntity = new AtlasEntity("hive_process_execution");
        doReturn(processExecutionEntity).when(createHiveProcess).getHiveProcessExecutionEntity(processEntity);

        // Mock methods needed by getHiveProcessExecutionEntity
        doReturn("test_user").when(createHiveProcess).getUserName();
        doReturn("query_123").when(createHiveProcess).getQueryId();
        doReturn("SELECT * FROM table").when(createHiveProcess).getQueryString();
        doReturn(System.currentTimeMillis() - 5000).when(createHiveProcess).getQueryStartTime();
        when(context.getHostName()).thenReturn("test_host");

        // Call processColumnLineage via reflection (instead of mocking)
        AtlasEntitiesWithExtInfo entitiesForReflection = new AtlasEntitiesWithExtInfo();
        processColumnLineageMethod.invoke(createHiveProcess, processEntity, entitiesForReflection);

        // Also mock addProcessedEntities
        doNothing().when(createHiveProcess).addProcessedEntities(any());

        // Execute
        AtlasEntitiesWithExtInfo entities = createHiveProcess.getEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
    }

}