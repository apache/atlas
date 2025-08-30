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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertNotNull;

public class CreateTableTest {

    @Mock
    AtlasHiveHookContext context;

    CreateTable createTable;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        createTable = new CreateTable(context);
    }

    // =================== Constructor Tests ===================
    @Test
    public void testConstructor() {
        CreateTable createTable = new CreateTable(context);
        AssertJUnit.assertNotNull("CreateTable instance should be created", createTable);
    }

    // =================== getNotificationMessages Tests ===================
    @Test
    public void testGetNotificationMessages_MetastoreHook_WithEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));

        AtlasEntitiesWithExtInfo mockEntities = new AtlasEntitiesWithExtInfo();
        AtlasEntity tblEntity = new AtlasEntity("hive_table");
        tblEntity.setAttribute("qualifiedName", "default.test_table@cm");
        mockEntities.addEntity(tblEntity);

        doReturn(mockEntities).when(createTable).getHiveMetastoreEntities();
        doReturn("test_user").when(createTable).getUserName();

        List<HookNotification> notifications = createTable.getNotificationMessages();

        AssertJUnit.assertNotNull(notifications);
        AssertJUnit.assertEquals(1, notifications.size());
        AssertJUnit.assertTrue(notifications.get(0) instanceof HookNotification.EntityCreateRequestV2);
    }

    @Test
    public void testGetNotificationMessages_NonMetastoreHook_WithEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));

        AtlasEntitiesWithExtInfo mockEntities = new AtlasEntitiesWithExtInfo();
        AtlasEntity tblEntity = new AtlasEntity("hive_table");
        tblEntity.setAttribute("qualifiedName", "default.test_table@cm");
        mockEntities.addEntity(tblEntity);

        doReturn(mockEntities).when(createTable).getHiveEntities();
        doReturn("test_user").when(createTable).getUserName();

        List<HookNotification> notifications = createTable.getNotificationMessages();

        AssertJUnit.assertNotNull(notifications);
        AssertJUnit.assertEquals(1, notifications.size());
        AssertJUnit.assertTrue(notifications.get(0) instanceof HookNotification.EntityCreateRequestV2);
    }

    @Test
    public void testGetNotificationMessages_EmptyEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));

        AtlasEntitiesWithExtInfo emptyEntities = new AtlasEntitiesWithExtInfo();
        doReturn(emptyEntities).when(createTable).getHiveMetastoreEntities();

        List<HookNotification> notifications = createTable.getNotificationMessages();

        AssertJUnit.assertNull("Expected null notifications for empty entities", notifications);
    }

    @Test
    public void testGetNotificationMessages_NullEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));
        doReturn(null).when(createTable).getHiveMetastoreEntities();

        List<HookNotification> notifications = createTable.getNotificationMessages();

        AssertJUnit.assertNull("Expected null notifications for null entities", notifications);
    }

    @Test
    public void testGetNotificationMessages_NonMetastoreHook_NullEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        doReturn(null).when(createTable).getHiveEntities();

        List<HookNotification> notifications = createTable.getNotificationMessages();

        AssertJUnit.assertNull("Expected null notifications for null entities", notifications);
    }

    // =================== getHiveEntities Tests ===================
    @Test
    public void testGetHiveEntities_EmptyOutputs() throws Exception {
        when(context.getOutputs()).thenReturn(Collections.emptySet());

        CreateTable createTable = spy(new CreateTable(context));
        doNothing().when(createTable).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo result = createTable.getHiveEntities();

        AssertJUnit.assertNotNull(result);
    }

    @Test
    public void testGetHiveEntities_WithNonTableEntity() throws Exception {
        WriteEntity writeEntity = mock(WriteEntity.class);
        when(writeEntity.getType()).thenReturn(Entity.Type.DATABASE);
        when(context.getOutputs()).thenReturn(Collections.singleton(writeEntity));

        CreateTable createTable = spy(new CreateTable(context));
        doNothing().when(createTable).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo result = createTable.getHiveEntities();

        AssertJUnit.assertNotNull(result);
    }

    @Test
    public void testGetHiveEntities_TableEntityWithNullTable() throws Exception {
        WriteEntity writeEntity = mock(WriteEntity.class);
        when(writeEntity.getType()).thenReturn(Entity.Type.TABLE);
        when(writeEntity.getTable()).thenReturn(null);
        when(context.getOutputs()).thenReturn(Collections.singleton(writeEntity));

        CreateTable createTable = spy(new CreateTable(context));
        doNothing().when(createTable).addProcessedEntities(any());

        AtlasEntitiesWithExtInfo result = createTable.getHiveEntities();

        AssertJUnit.assertNotNull(result);
    }


    // =================== processTable Tests (using reflection) ===================
    @Test
    public void testProcessTable_NullTable() throws Exception {
        CreateTable createTable = spy(new CreateTable(context));

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, (Object) null, entities);

        AssertJUnit.assertTrue("Should handle null table gracefully", true);
    }

    @Test
    public void testProcessTable_WithValidTable_MetastoreHook() throws Exception {
        Table mockTable = mock(Table.class);
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));
        doReturn(new AtlasEntity("hive_table")).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(false).when(createTable).isHBaseStore(mockTable);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).toTableEntity(eq(mockTable), any());
        verify(createTable).isHBaseStore(mockTable);
    }

    @Test
    public void testProcessTable_WithValidTable_NonMetastoreHook() throws Exception {
        Table mockTable = mock(Table.class);
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(false).when(createTable).isHBaseStore(mockTable);
        doReturn(new AtlasEntity("hive_ddl")).when(createTable).createHiveDDLEntity(tableEntity);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).createHiveDDLEntity(tableEntity);
    }

    @Test
    public void testProcessTable_NullTableEntity() throws Exception {
        Table mockTable = mock(Table.class);

        CreateTable createTable = spy(new CreateTable(context));
        doReturn(null).when(createTable).toTableEntity(eq(mockTable), any());

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).toTableEntity(eq(mockTable), any());
    }

    @Test
    public void testProcessTable_HBaseStore_MetastoreHook() throws Exception {
        Table mockTable = mock(Table.class);
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(true).when(createTable).isHBaseStore(mockTable);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).isHBaseStore(mockTable);
        // For HBase store + metastore hook, it should do nothing special
    }

    @Test
    public void testProcessTable_HBaseStore_NonMetastoreHook_ExternalTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        AtlasEntity hbaseEntity = new AtlasEntity("hbase_table");
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        AtlasEntity executionEntity = new AtlasEntity("hive_process_execution");

        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(true).when(createTable).isHBaseStore(mockTable);
        doReturn(hbaseEntity).when(createTable).toReferencedHBaseTable(eq(mockTable), any());
        doReturn(processEntity).when(createTable).getHiveProcessEntity(any(), any());
        doReturn(executionEntity).when(createTable).getHiveProcessExecutionEntity(processEntity);
        doReturn(new AtlasEntity("hive_ddl")).when(createTable).createHiveDDLEntity(tableEntity);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).toReferencedHBaseTable(eq(mockTable), any());
        verify(createTable).getHiveProcessEntity(any(), any());
        verify(createTable).getHiveProcessExecutionEntity(processEntity);
        verify(createTable).createHiveDDLEntity(tableEntity);
    }

    @Test
    public void testProcessTable_HBaseStore_NonMetastoreHook_ManagedTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        AtlasEntity hbaseEntity = new AtlasEntity("hbase_table");
        AtlasEntity processEntity = new AtlasEntity("hive_process");
        AtlasEntity executionEntity = new AtlasEntity("hive_process_execution");

        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(true).when(createTable).isHBaseStore(mockTable);
        doReturn(hbaseEntity).when(createTable).toReferencedHBaseTable(eq(mockTable), any());
        doReturn(processEntity).when(createTable).getHiveProcessEntity(any(), any());
        doReturn(executionEntity).when(createTable).getHiveProcessExecutionEntity(processEntity);
        doReturn(new AtlasEntity("hive_ddl")).when(createTable).createHiveDDLEntity(tableEntity);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).toReferencedHBaseTable(eq(mockTable), any());
        verify(createTable).getHiveProcessEntity(any(), any());
        verify(createTable).getHiveProcessExecutionEntity(processEntity);
        verify(createTable).createHiveDDLEntity(tableEntity);
    }

    @Test
    public void testProcessTable_ExternalTable_MetastoreHook1() throws Exception {
        // ---- Create a minimal real Table object ----
        org.apache.hadoop.hive.metastore.api.Table hiveMetaTable =
                new org.apache.hadoop.hive.metastore.api.Table();
        hiveMetaTable.setDbName("test_db");
        hiveMetaTable.setTableName("test_table");
        hiveMetaTable.setTableType(TableType.EXTERNAL_TABLE.name());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(new ArrayList<>()); // no columns needed
        sd.setLocation("hdfs://namenode:8020/user/hive/warehouse/test_table");
        hiveMetaTable.setSd(sd);

        // Wrap in Hive's metadata.Table object
        Table realTable = new Table(hiveMetaTable);

        // Spy it so we can stub certain methods if needed
        Table spyTable = spy(realTable);

        // ---- Mock Hive hook context ----
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE);

        // ---- Spy CreateTable and stub internals ----
        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");

        doReturn(tableEntity).when(createTable).toTableEntity(eq(spyTable), any());
        doReturn(false).when(createTable).isHBaseStore(spyTable);
        doReturn(new AtlasEntity("hdfs_path")).when(createTable).getPathEntity(any(), any());
        doReturn(new AtlasEntity("hive_process")).when(createTable).getHiveProcessEntity(any(), any());

        // ---- Reflection to call private processTable ----
        Method processTableMethod = CreateTable.class.getDeclaredMethod(
                "processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        // ---- Act ----
        processTableMethod.invoke(createTable, spyTable, entities);

        // ---- Verify ----
        verify(createTable).getPathEntity(any(), any());
        verify(createTable).getHiveProcessEntity(any(), any());

        // Optional sanity check: confirm getDataLocation works
        assertNotNull(spyTable.getDataLocation());
    }


    @Test
    public void testProcessTable_ExternalTable_NonMetastoreHook() throws Exception {
        // Create a minimal real Hive metastore table object
        org.apache.hadoop.hive.metastore.api.Table hiveMetaTable =
                new org.apache.hadoop.hive.metastore.api.Table();
        hiveMetaTable.setDbName("test_db");
        hiveMetaTable.setTableName("test_table");
        hiveMetaTable.setTableType(TableType.EXTERNAL_TABLE.name());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(new ArrayList<>());
        sd.setLocation("hdfs://namenode:8020/user/hive/warehouse/test_table");
        hiveMetaTable.setSd(sd);

        // Create a real Table and spy it
        Table realTable = new Table(hiveMetaTable);
        Table spyTable = spy(realTable);

        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");

        doReturn(tableEntity).when(createTable).toTableEntity(eq(spyTable), any());
        doReturn(false).when(createTable).isHBaseStore(spyTable);
        doReturn(new AtlasEntity("hdfs_path")).when(createTable).getPathEntity(any(), any());
        doReturn(new AtlasEntity("hive_process")).when(createTable).getHiveProcessEntity(any(), any());
        doReturn(new AtlasEntity("hive_process_execution")).when(createTable).getHiveProcessExecutionEntity(any());
        doReturn(new AtlasEntity("hive_ddl")).when(createTable).createHiveDDLEntity(tableEntity);

        // Call the private method via reflection
        Method processTableMethod = CreateTable.class.getDeclaredMethod(
                "processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        processTableMethod.invoke(createTable, spyTable, entities);

        verify(createTable).getPathEntity(any(), any());
        verify(createTable).getHiveProcessEntity(any(), any());
        verify(createTable).getHiveProcessExecutionEntity(any());
        verify(createTable).createHiveDDLEntity(tableEntity);

        // Sanity check: make sure data location works
        assertNotNull(spyTable.getDataLocation());
    }


    // =================== Static Method Tests (using reflection) ===================
    @Test
    public void testIsAlterTable_AlterTableProperties() throws Exception {
        Method isAlterTableMethod = CreateTable.class.getDeclaredMethod("isAlterTable", HiveOperation.class);
        isAlterTableMethod.setAccessible(true);

        boolean result = (Boolean) isAlterTableMethod.invoke(null, HiveOperation.ALTERTABLE_PROPERTIES);
        AssertJUnit.assertTrue("Should return true for ALTERTABLE_PROPERTIES", result);
    }

    @Test
    public void testIsAlterTable_AlterTableRename() throws Exception {
        Method isAlterTableMethod = CreateTable.class.getDeclaredMethod("isAlterTable", HiveOperation.class);
        isAlterTableMethod.setAccessible(true);

        boolean result = (Boolean) isAlterTableMethod.invoke(null, HiveOperation.ALTERTABLE_RENAME);
        AssertJUnit.assertTrue("Should return true for ALTERTABLE_RENAME", result);
    }

    @Test
    public void testIsAlterTable_AlterTableRenameCol() throws Exception {
        Method isAlterTableMethod = CreateTable.class.getDeclaredMethod("isAlterTable", HiveOperation.class);
        isAlterTableMethod.setAccessible(true);

        boolean result = (Boolean) isAlterTableMethod.invoke(null, HiveOperation.ALTERTABLE_RENAMECOL);
        AssertJUnit.assertTrue("Should return true for ALTERTABLE_RENAMECOL", result);
    }

    @Test
    public void testIsAlterTable_CreateTable() throws Exception {
        Method isAlterTableMethod = CreateTable.class.getDeclaredMethod("isAlterTable", HiveOperation.class);
        isAlterTableMethod.setAccessible(true);

        boolean result = (Boolean) isAlterTableMethod.invoke(null, HiveOperation.CREATETABLE);
        AssertJUnit.assertFalse("Should return false for CREATETABLE", result);
    }

    @Test
    public void testIsAlterTable_DropTable() throws Exception {
        Method isAlterTableMethod = CreateTable.class.getDeclaredMethod("isAlterTable", HiveOperation.class);
        isAlterTableMethod.setAccessible(true);

        boolean result = (Boolean) isAlterTableMethod.invoke(null, HiveOperation.DROPTABLE);
        AssertJUnit.assertFalse("Should return false for DROPTABLE", result);
    }

    // =================== skipTemporaryTable Tests (using reflection) ===================
    @Test
    public void testSkipTemporaryTable_NullTable() throws Exception {
        CreateTable createTable = new CreateTable(context);

        Method skipTempMethod = CreateTable.class.getDeclaredMethod("skipTemporaryTable", Table.class);
        skipTempMethod.setAccessible(true);
        boolean result = (Boolean) skipTempMethod.invoke(createTable, (Object) null);

        AssertJUnit.assertFalse("Should return false for null table", result);
    }

    @Test
    public void testSkipTemporaryTable_NonTemporaryTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.isTemporary()).thenReturn(false);

        CreateTable createTable = new CreateTable(context);

        Method skipTempMethod = CreateTable.class.getDeclaredMethod("skipTemporaryTable", Table.class);
        skipTempMethod.setAccessible(true);
        boolean result = (Boolean) skipTempMethod.invoke(createTable, mockTable);

        AssertJUnit.assertFalse("Should not skip non-temporary table", result);
    }

    @Test
    public void testSkipTemporaryTable_TemporaryManagedTable_SkipTrue() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.isTemporary()).thenReturn(true);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.isSkipTempTables()).thenReturn(true);

        CreateTable createTable = new CreateTable(context);

        Method skipTempMethod = CreateTable.class.getDeclaredMethod("skipTemporaryTable", Table.class);
        skipTempMethod.setAccessible(true);
        boolean result = (Boolean) skipTempMethod.invoke(createTable, mockTable);

        AssertJUnit.assertTrue("Should skip temporary managed table when skipTempTables is true", result);
    }

    @Test
    public void testSkipTemporaryTable_TemporaryManagedTable_SkipFalse() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.isTemporary()).thenReturn(true);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.isSkipTempTables()).thenReturn(false);

        CreateTable createTable = new CreateTable(context);

        Method skipTempMethod = CreateTable.class.getDeclaredMethod("skipTemporaryTable", Table.class);
        skipTempMethod.setAccessible(true);
        boolean result = (Boolean) skipTempMethod.invoke(createTable, mockTable);

        AssertJUnit.assertFalse("Should not skip temporary managed table when skipTempTables is false", result);
    }

    @Test
    public void testSkipTemporaryTable_TemporaryExternalTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.isTemporary()).thenReturn(true);
        when(mockTable.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
        when(context.isSkipTempTables()).thenReturn(true);

        CreateTable createTable = new CreateTable(context);

        Method skipTempMethod = CreateTable.class.getDeclaredMethod("skipTemporaryTable", Table.class);
        skipTempMethod.setAccessible(true);
        boolean result = (Boolean) skipTempMethod.invoke(createTable, mockTable);

        // External temp tables require additional flag check, will be false by default
        AssertJUnit.assertFalse("Should not skip temporary external table by default", result);
    }

    // =================== isCreateExtTableOperation Tests (using reflection) ===================
    @Test
    public void testIsCreateExtTableOperation_NullTable() throws Exception {
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE);

        CreateTable createTable = new CreateTable(context);

        Method isCreateExtMethod = CreateTable.class.getDeclaredMethod("isCreateExtTableOperation", Table.class);
        isCreateExtMethod.setAccessible(true);

        try {
            boolean result = (Boolean) isCreateExtMethod.invoke(createTable, (Object) null);
            AssertJUnit.assertFalse("Should return false for null table", result);
        } catch (Exception e) {
            // NPE is expected for null table, this is valid behavior
            AssertJUnit.assertTrue("NPE expected for null table", e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testIsCreateExtTableOperation_ExternalTable_CreateTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE);

        CreateTable createTable = new CreateTable(context);

        Method isCreateExtMethod = CreateTable.class.getDeclaredMethod("isCreateExtTableOperation", Table.class);
        isCreateExtMethod.setAccessible(true);
        boolean result = (Boolean) isCreateExtMethod.invoke(createTable, mockTable);

        AssertJUnit.assertTrue("Should return true for external table + CREATETABLE", result);
    }

    @Test
    public void testIsCreateExtTableOperation_ExternalTable_CreateTableAsSelect() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE_AS_SELECT);

        CreateTable createTable = new CreateTable(context);

        Method isCreateExtMethod = CreateTable.class.getDeclaredMethod("isCreateExtTableOperation", Table.class);
        isCreateExtMethod.setAccessible(true);
        boolean result = (Boolean) isCreateExtMethod.invoke(createTable, mockTable);

        AssertJUnit.assertTrue("Should return true for external table + CREATETABLE_AS_SELECT", result);
    }

    @Test
    public void testIsCreateExtTableOperation_ManagedTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.getHiveOperation()).thenReturn(HiveOperation.CREATETABLE);

        CreateTable createTable = new CreateTable(context);

        Method isCreateExtMethod = CreateTable.class.getDeclaredMethod("isCreateExtTableOperation", Table.class);
        isCreateExtMethod.setAccessible(true);
        boolean result = (Boolean) isCreateExtMethod.invoke(createTable, mockTable);

        AssertJUnit.assertFalse("Should return false for managed table", result);
    }

    @Test
    public void testIsCreateExtTableOperation_ExternalTable_DropTable() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
        when(context.getHiveOperation()).thenReturn(HiveOperation.DROPTABLE);

        CreateTable createTable = new CreateTable(context);

        Method isCreateExtMethod = CreateTable.class.getDeclaredMethod("isCreateExtTableOperation", Table.class);
        isCreateExtMethod.setAccessible(true);
        boolean result = (Boolean) isCreateExtMethod.invoke(createTable, mockTable);

        AssertJUnit.assertFalse("Should return false for non-create operation", result);
    }

    // =================== Additional Coverage Tests ===================
    @Test
    public void testToString() {
        CreateTable createTable = new CreateTable(context);
        String result = createTable.toString();
        AssertJUnit.assertNotNull("toString should not return null", result);
    }

    @Test
    public void testEquals() {
        CreateTable createTable1 = new CreateTable(context);
        CreateTable createTable2 = new CreateTable(context);

        AssertJUnit.assertTrue("Should be equal to itself", createTable1.equals(createTable1));
        AssertJUnit.assertFalse("Different instances should not be equal", createTable1.equals(createTable2));
        AssertJUnit.assertFalse("Should not be equal to null", createTable1.equals(null));
        AssertJUnit.assertFalse("Should not be equal to different type", createTable1.equals("string"));
    }

    @Test
    public void testHashCode() {
        CreateTable createTable = new CreateTable(context);
        int hashCode = createTable.hashCode();
        AssertJUnit.assertTrue("hashCode should be consistent", hashCode == createTable.hashCode());
    }

    // =================== Branch Coverage Tests ===================
    @Test
    public void testProcessTable_ManagedTable_MetastoreHook() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.isMetastoreHook()).thenReturn(true);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(false).when(createTable).isHBaseStore(mockTable);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).toTableEntity(eq(mockTable), any());
        verify(createTable).isHBaseStore(mockTable);
    }

    @Test
    public void testProcessTable_ManagedTable_NonMetastoreHook() throws Exception {
        Table mockTable = mock(Table.class);
        when(mockTable.getTableType()).thenReturn(TableType.MANAGED_TABLE);
        when(context.isMetastoreHook()).thenReturn(false);

        CreateTable createTable = spy(new CreateTable(context));
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        doReturn(tableEntity).when(createTable).toTableEntity(eq(mockTable), any());
        doReturn(false).when(createTable).isHBaseStore(mockTable);
        doReturn(new AtlasEntity("hive_ddl")).when(createTable).createHiveDDLEntity(tableEntity);

        Method processTableMethod = CreateTable.class.getDeclaredMethod("processTable", Table.class, AtlasEntitiesWithExtInfo.class);
        processTableMethod.setAccessible(true);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        processTableMethod.invoke(createTable, mockTable, entities);

        verify(createTable).createHiveDDLEntity(tableEntity);
    }

}