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
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class AlterTableTest {

    @Mock
    AtlasHiveHookContext context;

    @Mock
    AlterTableEvent alterTableEvent;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    // ========== SPECIFIC ALTER TABLE OPERATION TESTS ==========

    @Test
    public void testAlterTable_AddPartition() throws Exception {
        // Test ALTER TABLE ADD PARTITION scenario
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(context.getHiveOperation()).thenReturn(HiveOperation.ALTERTABLE_ADDPARTS);

        org.apache.hadoop.hive.metastore.api.Table oldTable = createPartitionedTable("sales_data", 2);
        org.apache.hadoop.hive.metastore.api.Table newTable = createPartitionedTable("sales_data", 2);

        when(alterTableEvent.getOldTable()).thenReturn(oldTable);
        when(alterTableEvent.getNewTable()).thenReturn(newTable);

        AlterTable alterTable = spy(new AlterTable(context));

        // Mock partition-related entities
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.sales_data@cluster");
        entities.addEntity(tableEntity);

        AtlasEntity partitionEntity = new AtlasEntity("hive_partition");
        partitionEntity.setAttribute("qualifiedName", "default.sales_data.year=2023-month=12@cluster");
        entities.addEntity(partitionEntity);

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn("partition_admin").when(alterTable).getUserName();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be update request", notifications.get(0) instanceof EntityUpdateRequestV2);

        EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("Should have 2 entities", 2, updateRequest.getEntities().getEntities().size());
    }

    @Test
    public void testAlterTable_ChangeStorageFormat() throws Exception {
        // Test ALTER TABLE SET FILEFORMAT scenario
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(context.getHiveOperation()).thenReturn(HiveOperation.ALTERTABLE_FILEFORMAT);

        org.apache.hadoop.hive.metastore.api.Table oldTable = createTableWithFormat("format_test", "TextInputFormat");
        org.apache.hadoop.hive.metastore.api.Table newTable = createTableWithFormat("format_test", "OrcInputFormat");

        when(alterTableEvent.getOldTable()).thenReturn(oldTable);
        when(alterTableEvent.getNewTable()).thenReturn(newTable);

        AlterTable alterTable = spy(new AlterTable(context));

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.format_test@cluster");
        tableEntity.setAttribute("inputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        entities.addEntity(tableEntity);

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn("format_user").when(alterTable).getUserName();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should match", "format_user", updateRequest.getUser());
    }

    @Test
    public void testAlterTable_ChangeLocation() throws Exception {
        // Test ALTER TABLE SET LOCATION scenario
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(context.getHiveOperation()).thenReturn(HiveOperation.ALTERTABLE_LOCATION);

        org.apache.hadoop.hive.metastore.api.Table oldTable = createTableWithLocation("location_test", "/old/path");
        org.apache.hadoop.hive.metastore.api.Table newTable = createTableWithLocation("location_test", "/new/path");

        when(alterTableEvent.getOldTable()).thenReturn(oldTable);
        when(alterTableEvent.getNewTable()).thenReturn(newTable);

        AlterTable alterTable = spy(new AlterTable(context));

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.location_test@cluster");
        entities.addEntity(tableEntity);

        AtlasEntity oldPathEntity = new AtlasEntity("hdfs_path");
        oldPathEntity.setAttribute("qualifiedName", "/old/path@cluster");
        entities.addEntity(oldPathEntity);

        AtlasEntity newPathEntity = new AtlasEntity("hdfs_path");
        newPathEntity.setAttribute("qualifiedName", "/new/path@cluster");
        entities.addEntity(newPathEntity);

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn("location_admin").when(alterTable).getUserName();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("Should have 3 entities", 3, updateRequest.getEntities().getEntities().size());
    }

    @Test
    public void testAlterTable_RenameColumn() throws Exception {
        // Test ALTER TABLE CHANGE COLUMN scenario
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);
        when(context.getHiveOperation()).thenReturn(HiveOperation.ALTERTABLE_RENAMECOL);

        org.apache.hadoop.hive.metastore.api.Table oldTable = createTableWithColumns("column_test",
                Arrays.asList("old_col_name", "other_col"));
        org.apache.hadoop.hive.metastore.api.Table newTable = createTableWithColumns("column_test",
                Arrays.asList("new_col_name", "other_col"));

        when(alterTableEvent.getOldTable()).thenReturn(oldTable);
        when(alterTableEvent.getNewTable()).thenReturn(newTable);

        AlterTable alterTable = spy(new AlterTable(context));

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.column_test@cluster");
        entities.addEntity(tableEntity);

        AtlasEntity oldColumnEntity = new AtlasEntity("hive_column");
        oldColumnEntity.setAttribute("qualifiedName", "default.column_test.old_col_name@cluster");
        oldColumnEntity.setAttribute("name", "old_col_name");
        entities.addEntity(oldColumnEntity);

        AtlasEntity newColumnEntity = new AtlasEntity("hive_column");
        newColumnEntity.setAttribute("qualifiedName", "default.column_test.new_col_name@cluster");
        newColumnEntity.setAttribute("name", "new_col_name");
        entities.addEntity(newColumnEntity);

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn("column_admin").when(alterTable).getUserName();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("Should have 3 entities", 3, updateRequest.getEntities().getEntities().size());
    }

    // ========== SPECIAL EDGE CASE TESTS ==========

    @Test
    public void testAlterTable_EmptyNotificationReturnedCorrectly() throws Exception {
        // Test when CollectionUtils.isEmpty returns true
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);

        AlterTable alterTable = spy(new AlterTable(context));

        // Create entities but make the collection appear empty to CollectionUtils
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        // Don't add any entities - this should result in empty collection
        doReturn(entities).when(alterTable).getHiveMetastoreEntities();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNull("Should return null for empty entities collection", notifications);
    }

    @Test
    public void testAlterTable_SpecialCharactersInTableName() throws Exception {
        // Test with special characters in table and column names
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);

        AlterTable alterTable = spy(new AlterTable(context));

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", "default.test_table_$special@cluster");
        tableEntity.setAttribute("name", "test_table_$special");
        entities.addEntity(tableEntity);

        AtlasEntity columnEntity = new AtlasEntity("hive_column");
        columnEntity.setAttribute("qualifiedName", "default.test_table_$special.col_with-dash@cluster");
        columnEntity.setAttribute("name", "col_with-dash");
        entities.addEntity(columnEntity);

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn("special_user").when(alterTable).getUserName();

        List<HookNotification> notifications = alterTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("Should handle special characters", "special_user", updateRequest.getUser());
    }

    @Test
    public void testAlterTable_InheritanceChain() {
        // Test to ensure AlterTable properly inherits from CreateTable and BaseHiveEvent
        AlterTable alterTable = new AlterTable(context);

        // Verify inheritance chain
        AssertJUnit.assertTrue("Should extend CreateTable", alterTable instanceof CreateTable);
        AssertJUnit.assertTrue("Should extend BaseHiveEvent indirectly",
                alterTable.getClass().getSuperclass().getSuperclass().getSimpleName().contains("BaseHiveEvent") ||
                        alterTable.getClass().getSuperclass().getSimpleName().contains("BaseHiveEvent"));
    }

    @Test
    public void testAlterTable_OverriddenMethodBehavior() throws Exception {
        // Test that overridden method behaves differently from parent
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);

        AlterTable alterTable = spy(new AlterTable(context));
        CreateTable createTable = spy(new CreateTable(context));

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        entities.addEntity(new AtlasEntity("hive_table"));

        doReturn(entities).when(alterTable).getHiveMetastoreEntities();
        doReturn(entities).when(createTable).getHiveMetastoreEntities();
        doReturn("test_user").when(alterTable).getUserName();
        doReturn("test_user").when(createTable).getUserName();

        List<HookNotification> alterNotifications = alterTable.getNotificationMessages();
        List<HookNotification> createNotifications = createTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Alter notifications should not be null", alterNotifications);
        AssertJUnit.assertNotNull("Create notifications should not be null", createNotifications);

        // Verify different notification types
        AssertJUnit.assertTrue("Alter should return EntityUpdateRequestV2",
                alterNotifications.get(0) instanceof EntityUpdateRequestV2);
        AssertJUnit.assertFalse("Create should not return EntityUpdateRequestV2",
                createNotifications.get(0) instanceof EntityUpdateRequestV2);
    }

    // ========== HELPER METHODS ==========

    private org.apache.hadoop.hive.metastore.api.Table createPartitionedTable(String tableName, int partitionCols) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);
        when(table.getTableType()).thenReturn(MANAGED_TABLE.toString());

        List<FieldSchema> partitionKeys = new ArrayList<>();
        for (int i = 0; i < partitionCols; i++) {
            partitionKeys.add(new FieldSchema("part_col" + i, "string", null));
        }
        when(table.getPartitionKeys()).thenReturn(partitionKeys);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema("data_col", "string", null)));
        when(table.getSd()).thenReturn(sd);

        return table;
    }

    private org.apache.hadoop.hive.metastore.api.Table createTableWithFormat(String tableName, String inputFormat) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        when(sd.getInputFormat()).thenReturn("org.apache.hadoop.mapred." + inputFormat);
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema("col1", "string", null)));
        when(table.getSd()).thenReturn(sd);

        return table;
    }

    private org.apache.hadoop.hive.metastore.api.Table createTableWithLocation(String tableName, String location) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        when(sd.getLocation()).thenReturn(location);
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema("col1", "string", null)));
        when(table.getSd()).thenReturn(sd);

        return table;
    }

    private org.apache.hadoop.hive.metastore.api.Table createTableWithColumns(String tableName, List<String> columnNames) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);

        StorageDescriptor sd = mock(StorageDescriptor.class);
        List<FieldSchema> columns = new ArrayList<>();
        for (String colName : columnNames) {
            columns.add(new FieldSchema(colName, "string", null));
        }
        when(sd.getCols()).thenReturn(columns);
        when(table.getSd()).thenReturn(sd);

        return table;
    }
}