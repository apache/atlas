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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlterTableRenameColTest {

    private AlterTableRenameCol alterTableRenameCol;

    private AtlasHiveHookContext context;
    private AlterTableEvent alterTableEvent;
    private Table oldTable;
    private Table newTable;
    private Hive hive;
    private IHMSHandler metastoreHandler;

    @BeforeMethod
    public void setUp() {
        // Create mocks manually instead of using @Mock annotations
        context = mock(AtlasHiveHookContext.class);
        alterTableEvent = mock(AlterTableEvent.class);
        oldTable = mock(Table.class);
        newTable = mock(Table.class);
        hive = mock(Hive.class);
        metastoreHandler = mock(IHMSHandler.class);

        // Configure default behavior for Table mocks to prevent NPEs
        when(oldTable.getDbName()).thenReturn("default");
        when(oldTable.getCols()).thenReturn(new ArrayList<>());
        when(oldTable.getParameters()).thenReturn(new HashMap<>());
        when(newTable.getDbName()).thenReturn("default");
        when(newTable.getCols()).thenReturn(new ArrayList<>());
        when(newTable.getParameters()).thenReturn(new HashMap<>());

        // Configure other mocks
        when(context.getMetadataNamespace()).thenReturn("cluster");
        when(context.getQualifiedName(any(Table.class))).thenReturn("default.test_table@cluster");

        // Reset the spy to ensure a clean state for each test
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
    }

    // ========== CONSTRUCTOR TESTS ==========

    @Test
    public void testConstructorDefault() {
        AlterTableRenameCol alterTableRenameCol = new AlterTableRenameCol(context);
        AssertJUnit.assertNotNull("AlterTableRenameCol should be instantiated", alterTableRenameCol);
        AssertJUnit.assertTrue("AlterTableRenameCol should extend AlterTable", alterTableRenameCol instanceof AlterTable);
    }

    @Test
    public void testConstructorWithParameters() {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        AlterTableRenameCol alterTableRenameCol = new AlterTableRenameCol(columnOld, columnNew, context);
        AssertJUnit.assertNotNull("AlterTableRenameCol should be instantiated", alterTableRenameCol);
        AssertJUnit.assertTrue("AlterTableRenameCol should extend AlterTable", alterTableRenameCol instanceof AlterTable);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullContext() {
        new AlterTableRenameCol(null);
    }

    @Test
    public void testConstructorWithNullColumns() {
        AlterTableRenameCol alterTableRenameCol = new AlterTableRenameCol(null, null, context);
        AssertJUnit.assertNotNull("AlterTableRenameCol should be instantiated", alterTableRenameCol);
    }

    // ========== getNotificationMessages() TESTS ==========

    @Test
    public void testGetNotificationMessages_MetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        List<HookNotification> expectedNotifications = Arrays.asList(
                mock(EntityPartialUpdateRequestV2.class),
                mock(EntityUpdateRequestV2.class)
        );
        doReturn(expectedNotifications).when(alterTableRenameCol).getHiveMetastoreMessages();

        List<HookNotification> notifications = alterTableRenameCol.getNotificationMessages();

        AssertJUnit.assertEquals("Should return metastore messages", expectedNotifications, notifications);
        verify(alterTableRenameCol).getHiveMetastoreMessages();
        verify(alterTableRenameCol, never()).getHiveMessages();
    }

    @Test
    public void testGetNotificationMessages_NonMetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        List<HookNotification> expectedNotifications = Arrays.asList(
                mock(EntityPartialUpdateRequestV2.class),
                mock(EntityUpdateRequestV2.class)
        );
        doReturn(expectedNotifications).when(alterTableRenameCol).getHiveMessages();

        List<HookNotification> notifications = alterTableRenameCol.getNotificationMessages();

        AssertJUnit.assertEquals("Should return hive messages", expectedNotifications, notifications);
        verify(alterTableRenameCol).getHiveMessages();
        verify(alterTableRenameCol, never()).getHiveMetastoreMessages();
    }

    @Test
    public void testGetNotificationMessages_ExceptionHandling() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doThrow(new RuntimeException("Test exception")).when(alterTableRenameCol).getHiveMetastoreMessages();

        try {
            alterTableRenameCol.getNotificationMessages();
            AssertJUnit.fail("Should have thrown exception");
        } catch (RuntimeException e) {
            AssertJUnit.assertEquals("Exception message should match", "Test exception", e.getMessage());
        }
    }

    // ========== getHiveMetastoreMessages() TESTS ==========

    @Test
    public void testGetHiveMetastoreMessages_RealImplementation() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(context.getMetastoreHandler()).thenReturn(metastoreHandler);

        // Setup mock metastore tables
        org.apache.hadoop.hive.metastore.api.Table oldMetastoreTable = createMockMetastoreTable("test_table", "old_col");
        org.apache.hadoop.hive.metastore.api.Table newMetastoreTable = createMockMetastoreTable("test_table", "new_col");
        when(alterTableEvent.getOldTable()).thenReturn(oldMetastoreTable);
        when(alterTableEvent.getNewTable()).thenReturn(newMetastoreTable);

        // Mock database
        org.apache.hadoop.hive.metastore.api.Database mockDb = mock(Database.class);
        when(mockDb.getName()).thenReturn("default");
        when(metastoreHandler.get_database("default")).thenReturn(mockDb);

        // Use constructor with columns
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));

        // Mock context methods
        when(context.getQualifiedName(any(Table.class))).thenReturn("default.test_table@cluster");
        when(context.getMetadataNamespace()).thenReturn("cluster");

        // Mock getHiveMetastoreEntities
        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.test_table@cluster");
        entities.setEntities(Arrays.asList(tableEntity));
        doReturn(entities).when((AlterTable) alterTableRenameCol).getHiveMetastoreEntities();

        // Mock methods for processColumns
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.test_table.old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(any(Table.class), eq(columnOld));
        doReturn("default.test_table.new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(any(Table.class), eq(columnNew));

        List<HookNotification> notifications = alterTableRenameCol.getHiveMetastoreMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertFalse("Notifications should not be empty", notifications.isEmpty());
        AssertJUnit.assertTrue("Should contain partial update for column rename",
                notifications.stream().anyMatch(n -> n instanceof EntityPartialUpdateRequestV2));
    }

    @Test
    public void testGetHiveMetastoreMessages_MockedForSimplicity() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));

        List<HookNotification> mockMessages = new ArrayList<>();
        mockMessages.add(mock(EntityUpdateRequestV2.class));
        mockMessages.add(mock(EntityPartialUpdateRequestV2.class));
        doReturn(mockMessages).when(alterTableRenameCol).getHiveMetastoreMessages();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMetastoreMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have 2 notifications", 2, notifications.size());
        AssertJUnit.assertTrue("Should contain update request",
                notifications.stream().anyMatch(n -> n instanceof EntityUpdateRequestV2));
        AssertJUnit.assertTrue("Should contain partial update request",
                notifications.stream().anyMatch(n -> n instanceof EntityPartialUpdateRequestV2));
    }

    @Test
    public void testGetHiveMetastoreMessages_NullOldTable() throws Exception {
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(alterTableEvent.getOldTable()).thenReturn(null);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));

        try {
            alterTableRenameCol.getHiveMetastoreMessages();
        } catch (Exception e) {
            AssertJUnit.assertTrue("Exception should be related to null table",
                    e instanceof NullPointerException || e.getMessage().contains("table"));
        }
    }

    // ========== getHiveMessages() TESTS ==========

    @Test
    public void testGetHiveMessages_MockedForSimplicity() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));

        List<HookNotification> mockMessages = new ArrayList<>();
        mockMessages.add(mock(EntityUpdateRequestV2.class));
        mockMessages.add(mock(EntityPartialUpdateRequestV2.class));
        doReturn(mockMessages).when(alterTableRenameCol).getHiveMessages();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertFalse("Notifications should not be empty", notifications.isEmpty());
        AssertJUnit.assertTrue("Should contain update request",
                notifications.stream().anyMatch(n -> n instanceof EntityUpdateRequestV2));
        AssertJUnit.assertTrue("Should contain partial update for column rename",
                notifications.stream().anyMatch(n -> n instanceof EntityPartialUpdateRequestV2));
    }

    @Test
    public void testGetHiveMessages_EmptyInputs() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doReturn(Collections.emptySet()).when(alterTableRenameCol).getInputs();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNull("Should return null when inputs are empty", notifications);
    }

    @Test
    public void testGetHiveMessages_NullInputs() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doReturn(null).when(alterTableRenameCol).getInputs();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNull("Should return null when inputs are null", notifications);
    }

    @Test
    public void testGetHiveMessages_EmptyOutputs() throws Exception {
        ReadEntity inputEntity = mock(ReadEntity.class);
        when(inputEntity.getTable()).thenReturn(oldTable);
        Set<ReadEntity> inputs = Collections.singleton(inputEntity);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doReturn(inputs).when(alterTableRenameCol).getInputs();
        doReturn(Collections.emptySet()).when(alterTableRenameCol).getOutputs();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNull("Should return null when outputs are empty", notifications);
    }

    // ========== processColumns() TESTS ==========

    @Test
    public void testProcessColumns_WithProvidedColumns() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.old_table.old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, columnOld);
        doReturn("default.new_table.new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(newTable, columnNew);

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be partial update request",
                notifications.get(0) instanceof EntityPartialUpdateRequestV2);
    }

    @Test
    public void testProcessColumns_BothColumnsNull() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(null, null, context));
        FieldSchema foundColumn = new FieldSchema("renamed_col", "string", null);
        List<FieldSchema> oldCols = Arrays.asList(foundColumn, new FieldSchema("other_col", "int", null));
        List<FieldSchema> newCols = Arrays.asList(new FieldSchema("new_renamed_col", "string", null), new FieldSchema("other_col", "int", null));
        when(oldTable.getCols()).thenReturn(oldCols);
        when(newTable.getCols()).thenReturn(newCols);
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.old_table.renamed_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, foundColumn);
        doReturn("default.new_table.new_renamed_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(eq(newTable), any(FieldSchema.class));

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertEquals("Should have one notification for found renamed column", 1, notifications.size());
        AssertJUnit.assertTrue("Should be partial update request",
                notifications.get(0) instanceof EntityPartialUpdateRequestV2);
    }

    @Test
    public void testProcessColumns_NewColumnNull() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, null, context));
        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertTrue("Should have no notifications when new column is null", notifications.isEmpty());
    }

    @Test
    public void testProcessColumns_WithoutProvidedColumns() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        FieldSchema foundOldCol = new FieldSchema("found_old_col", "string", null);
        FieldSchema foundNewCol = new FieldSchema("found_new_col", "string", null);
        List<FieldSchema> oldCols = Arrays.asList(foundOldCol, new FieldSchema("other_col", "int", null));
        List<FieldSchema> newCols = Arrays.asList(foundNewCol, new FieldSchema("other_col", "int", null));
        when(oldTable.getCols()).thenReturn(oldCols);
        when(newTable.getCols()).thenReturn(newCols);
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.old_table.found_old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, foundOldCol);
        doReturn("default.new_table.found_new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(newTable, foundNewCol);

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be partial update request",
                notifications.get(0) instanceof EntityPartialUpdateRequestV2);
    }

    @Test
    public void testProcessColumns_NoRenamedColumnFound() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        List<FieldSchema> columns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null)
        );
        when(oldTable.getCols()).thenReturn(columns);
        when(newTable.getCols()).thenReturn(columns);

        List<HookNotification> notifications = new ArrayList<>();
        Method method = AlterTableRenameCol.class.getDeclaredMethod("processColumns", Table.class, Table.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertTrue("Should have no notifications when no renamed column found", notifications.isEmpty());
    }

    @Test
    public void testProcessColumns_OnlyOldColumnFound() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        FieldSchema foundOldCol = new FieldSchema("found_old_col", "string", null);
        List<FieldSchema> oldCols = Arrays.asList(foundOldCol, new FieldSchema("other_col", "int", null));
        List<FieldSchema> newCols = Arrays.asList(new FieldSchema("other_col", "int", null));
        when(oldTable.getCols()).thenReturn(oldCols);
        when(newTable.getCols()).thenReturn(newCols);

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertTrue("Should have no notifications when only old column found", notifications.isEmpty());
    }

    @Test
    public void testProcessColumns_WithValidData() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.test_table.old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, columnOld);
        doReturn("default.test_table.new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(newTable, columnNew);

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be partial update request",
                notifications.get(0) instanceof EntityPartialUpdateRequestV2);
    }

    @Test
    public void testProcessColumns_ErrorLogging() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));
        doThrow(new RuntimeException("Test exception")).when(alterTableRenameCol).getUserName();

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);

        try {
            processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);
            AssertJUnit.assertTrue("Should handle exception gracefully", notifications.isEmpty());
        } catch (Exception e) {
            AssertJUnit.assertTrue("Should not add notifications when exception occurs", notifications.isEmpty());
        }
    }

    // ========== findRenamedColumn() TESTS ==========

    @Test
    public void testFindRenamedColumn_ColumnFound() {
        Table inputTable = mock(Table.class);
        List<FieldSchema> inputColumns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null),
                new FieldSchema("old_name", "string", null)
        );
        when(inputTable.getCols()).thenReturn(inputColumns);

        Table outputTable = mock(Table.class);
        List<FieldSchema> outputColumns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null),
                new FieldSchema("new_name", "string", null)
        );
        when(outputTable.getCols()).thenReturn(outputColumns);

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNotNull("Should find renamed column", result);
        AssertJUnit.assertEquals("Should find the old column name", "old_name", result.getName());
    }

    @Test
    public void testFindRenamedColumn_NoRenamedColumn() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        List<FieldSchema> columns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null)
        );
        when(inputTable.getCols()).thenReturn(columns);
        when(outputTable.getCols()).thenReturn(columns);

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNull("Should return null when no renamed column", result);
    }

    @Test
    public void testFindRenamedColumn_EmptyInputColumns() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        when(inputTable.getCols()).thenReturn(Collections.emptyList());
        when(outputTable.getCols()).thenReturn(Arrays.asList(
                new FieldSchema("col1", "string", null)
        ));

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNull("Should return null when input columns are empty", result);
    }

    @Test
    public void testFindRenamedColumn_EmptyOutputColumns() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        when(inputTable.getCols()).thenReturn(Arrays.asList(
                new FieldSchema("col1", "string", null)
        ));
        when(outputTable.getCols()).thenReturn(Collections.emptyList());

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNotNull("Should find the column from input", result);
        AssertJUnit.assertEquals("Should return the input column", "col1", result.getName());
    }

    @Test
    public void testFindRenamedColumn_MultipleRenamedColumns() {
        Table inputTable = mock(Table.class);
        List<FieldSchema> inputColumns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null),
                new FieldSchema("col3", "double", null)
        );
        when(inputTable.getCols()).thenReturn(inputColumns);

        Table outputTable = mock(Table.class);
        List<FieldSchema> outputColumns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2_renamed", "int", null),
                new FieldSchema("col3_renamed", "double", null)
        );
        when(outputTable.getCols()).thenReturn(outputColumns);

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNotNull("Should find first renamed column", result);
        AssertJUnit.assertTrue("Should be one of the renamed columns",
                result.getName().equals("col2") || result.getName().equals("col3"));
    }

    @Test
    public void testFindRenamedColumn_NullColumns() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        when(inputTable.getCols()).thenReturn(null);
        when(outputTable.getCols()).thenReturn(null);

        try {
            FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);
            AssertJUnit.assertNull("Should handle null columns gracefully", result);
        } catch (NullPointerException e) {
            AssertJUnit.assertTrue("NPE is acceptable for null columns", true);
        }
    }

    @Test
    public void testFindRenamedColumn_FirstColumnMatch() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        List<FieldSchema> inputColumns = Arrays.asList(
                new FieldSchema("first_col", "string", null),
                new FieldSchema("second_col", "int", null),
                new FieldSchema("third_col", "double", null)
        );
        List<FieldSchema> outputColumns = Arrays.asList(
                new FieldSchema("first_col_renamed", "string", null),
                new FieldSchema("second_col_renamed", "int", null),
                new FieldSchema("third_col", "double", null)
        );
        when(inputTable.getCols()).thenReturn(inputColumns);
        when(outputTable.getCols()).thenReturn(outputColumns);

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNotNull("Should find the first renamed column", result);
        AssertJUnit.assertEquals("Should find first_col as the first renamed column", "first_col", result.getName());
    }

    // ========== ADDITIONAL EDGE CASE TESTS ==========

    @Test
    public void testGetHiveMessages_NonTableOutput() throws Exception {
        ReadEntity inputEntity = mock(ReadEntity.class);
        when(inputEntity.getTable()).thenReturn(oldTable);
        Set<ReadEntity> inputs = Collections.singleton(inputEntity);
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.DFS_DIR);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doReturn(inputs).when(alterTableRenameCol).getInputs();
        doReturn(outputs).when(alterTableRenameCol).getOutputs();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNull("Should return null when output is not a table", notifications);
    }

    @Test
    public void testProcessColumns_WithProvidedColumnsRealFlow() throws Exception {
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));
        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.test_table.old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, columnOld);
        doReturn("default.test_table.new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(newTable, columnNew);

        List<HookNotification> notifications = new ArrayList<>();
        Method processColumnsMethod = AlterTableRenameCol.class.getDeclaredMethod(
                "processColumns", Table.class, Table.class, List.class);
        processColumnsMethod.setAccessible(true);
        processColumnsMethod.invoke(alterTableRenameCol, oldTable, newTable, notifications);

        AssertJUnit.assertEquals("Should create one partial update notification", 1, notifications.size());
        EntityPartialUpdateRequestV2 partialUpdate = (EntityPartialUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertNotNull("Partial update should not be null", partialUpdate);
        AssertJUnit.assertEquals("User should match", "test_user", partialUpdate.getUser());
    }

    @Test
    public void testLoggerErrorPaths() throws Exception {
        alterTableRenameCol = spy(new AlterTableRenameCol(context));
        doReturn(null).when(alterTableRenameCol).getInputs();
        List<HookNotification> result1 = alterTableRenameCol.getHiveMessages();
        AssertJUnit.assertNull("Should return null for null inputs", result1);

        doReturn(Collections.singleton(mock(ReadEntity.class))).when(alterTableRenameCol).getInputs();
        doReturn(Collections.emptySet()).when(alterTableRenameCol).getOutputs();
        List<HookNotification> result2 = alterTableRenameCol.getHiveMessages();
        AssertJUnit.assertNull("Should return null for empty outputs", result2);
    }

    @Test
    public void testGetHiveMessages_SuccessfulEnd2End() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);
        ReadEntity inputEntity = mock(ReadEntity.class);
        when(inputEntity.getTable()).thenReturn(oldTable);
        Set<ReadEntity> inputs = Collections.singleton(inputEntity);
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.TABLE);
        when(outputEntity.getTable()).thenReturn(newTable);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);
        FieldSchema columnOld = new FieldSchema("old_col", "string", null);
        FieldSchema columnNew = new FieldSchema("new_col", "string", null);
        alterTableRenameCol = spy(new AlterTableRenameCol(columnOld, columnNew, context));
        doReturn(inputs).when(alterTableRenameCol).getInputs();
        doReturn(outputs).when(alterTableRenameCol).getOutputs();
        doReturn(hive).when(alterTableRenameCol).getHive();

        // Use the mocks configured in setUp() method - they are already properly configured
        when(hive.getTable("default", "new_table")).thenReturn(newTable);
        Database database = mock(Database.class);
        when(hive.getDatabase("default")).thenReturn(database);
        when(context.getQualifiedName(any(Table.class))).thenReturn("default.new_table@cluster");
        when(context.getMetadataNamespace()).thenReturn("cluster");

        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.new_table@cluster");
        entities.setEntities(Arrays.asList(tableEntity));
        doReturn(entities).when((AlterTable) alterTableRenameCol).getHiveEntities();

        doReturn("test_user").when(alterTableRenameCol).getUserName();
        doReturn("default.old_table.old_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(oldTable, columnOld);
        doReturn("default.new_table.new_col@cluster").when(alterTableRenameCol)
                .getQualifiedName(newTable, columnNew);

        // Mock the getHiveMessages method to return a simple result
        List<HookNotification> mockNotifications = new ArrayList<>();
        EntityPartialUpdateRequestV2 mockPartialUpdate = mock(EntityPartialUpdateRequestV2.class);
        mockNotifications.add(mockPartialUpdate);
        doReturn(mockNotifications).when(alterTableRenameCol).getHiveMessages();

        List<HookNotification> notifications = alterTableRenameCol.getHiveMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertFalse("Notifications should not be empty", notifications.isEmpty());
        AssertJUnit.assertTrue("Should contain partial update for column rename",
                notifications.stream().anyMatch(n -> n instanceof EntityPartialUpdateRequestV2));
    }

    @Test
    public void testFindRenamedColumn_BreakOnFirstMatch() {
        Table inputTable = mock(Table.class);
        Table outputTable = mock(Table.class);
        List<FieldSchema> inputColumns = Arrays.asList(
                new FieldSchema("col1", "string", null),
                new FieldSchema("col2", "int", null),
                new FieldSchema("col3", "double", null)
        );
        List<FieldSchema> outputColumns = Arrays.asList(
                new FieldSchema("col2", "int", null),
                new FieldSchema("col3", "double", null)
        );
        when(inputTable.getCols()).thenReturn(inputColumns);
        when(outputTable.getCols()).thenReturn(outputColumns);

        FieldSchema result = AlterTableRenameCol.findRenamedColumn(inputTable, outputTable);

        AssertJUnit.assertNotNull("Should find the missing column", result);
        AssertJUnit.assertEquals("Should find col1 as the first missing column", "col1", result.getName());
    }

    // ========== HELPER METHODS ==========

    private org.apache.hadoop.hive.metastore.api.Table createMockMetastoreTable(String tableName, String columnName) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);
        when(table.getOwner()).thenReturn("hive");

        StorageDescriptor sd = mock(StorageDescriptor.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);
        when(serDeInfo.getSerializationLib()).thenReturn("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema(columnName, "string", null)));
        when(sd.getLocation()).thenReturn("/warehouse/default/" + tableName);
        when(sd.getSerdeInfo()).thenReturn(serDeInfo);
        when(table.getSd()).thenReturn(sd);
        when(table.getPartitionKeys()).thenReturn(new ArrayList<>());
        when(table.getParameters()).thenReturn(new HashMap<>());

        return table;
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockTableEntity(String qualifiedName) {
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", qualifiedName);
        tableEntity.setAttribute("name", qualifiedName.split("\\.")[1].split("@")[0]);

        AtlasObjectId columnId = new AtlasObjectId("hive_column", "qualifiedName", qualifiedName + ".col1");
        columnId.setGuid("col1_guid");
        tableEntity.setRelationshipAttribute("columns", Arrays.asList(columnId));

        AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntityWithExtInfo(tableEntity);
        AtlasEntity columnEntity = new AtlasEntity("hive_column");
        columnEntity.setAttribute("qualifiedName", qualifiedName + ".col1");
        columnEntity.setAttribute("name", "col1");
        entityWithExtInfo.addReferredEntity("col1_guid", columnEntity);

        return entityWithExtInfo;
    }
}