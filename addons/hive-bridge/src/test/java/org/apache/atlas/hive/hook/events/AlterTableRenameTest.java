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
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlterTableRenameTest {

    @Mock
    AtlasHiveHookContext context;

    @Mock
    AlterTableEvent alterTableEvent;

    @Mock
    Table oldTable;

    @Mock
    Table newTable;

    @Mock
    Hive hive;

    @Mock
    HiveConf hiveConf;

    @Mock
    SessionState sessionState;

    @Mock
    IHMSHandler metastoreHandler;

    @BeforeMethod
    public void setUp() throws HiveException, TException {
        // Create mocks manually to control initialization order
        context = mock(AtlasHiveHookContext.class);
        alterTableEvent = mock(AlterTableEvent.class);
        oldTable = mock(Table.class);
        newTable = mock(Table.class);
        hive = mock(Hive.class);
        hiveConf = mock(HiveConf.class);
        sessionState = mock(SessionState.class);
        metastoreHandler = mock(IHMSHandler.class);

        // Create mock metastore Table objects for tTable
        org.apache.hadoop.hive.metastore.api.Table oldMetastoreTable = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        org.apache.hadoop.hive.metastore.api.Table newMetastoreTable = mock(org.apache.hadoop.hive.metastore.api.Table.class);

        // Create StorageDescriptor mocks
        StorageDescriptor oldStorageDescriptor = mock(StorageDescriptor.class);
        StorageDescriptor newStorageDescriptor = mock(StorageDescriptor.class);

        // Create SerDeInfo mocks
        SerDeInfo oldSerDeInfo = mock(SerDeInfo.class);
        SerDeInfo newSerDeInfo = mock(SerDeInfo.class);

        // Configure metastore Table mocks to prevent NPEs
        when(oldMetastoreTable.getDbName()).thenReturn("default");
        when(oldMetastoreTable.getTableName()).thenReturn("old_table");
        when(oldMetastoreTable.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(oldMetastoreTable.getOwner()).thenReturn("hive");
        when(oldMetastoreTable.getSd()).thenReturn(oldStorageDescriptor);
        when(oldMetastoreTable.getParameters()).thenReturn(new HashMap<>());
        when(oldTable.getCols()).thenReturn(new ArrayList<>());
        when(oldMetastoreTable.getPartitionKeys()).thenReturn(new ArrayList<>());

        when(newMetastoreTable.getDbName()).thenReturn("default");
        when(newMetastoreTable.getTableName()).thenReturn("new_table");
        when(newMetastoreTable.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(newMetastoreTable.getOwner()).thenReturn("hive");
        when(newMetastoreTable.getSd()).thenReturn(newStorageDescriptor);
        when(newMetastoreTable.getParameters()).thenReturn(new HashMap<>());
        when(newTable.getCols()).thenReturn(new ArrayList<>());
        when(newMetastoreTable.getPartitionKeys()).thenReturn(new ArrayList<>());

        // Configure StorageDescriptor mocks
        when(oldStorageDescriptor.getCols()).thenReturn(new ArrayList<>());
        when(oldStorageDescriptor.getSerdeInfo()).thenReturn(oldSerDeInfo);
        when(oldStorageDescriptor.getLocation()).thenReturn("/warehouse/default/old_table");
        when(newStorageDescriptor.getCols()).thenReturn(new ArrayList<>());
        when(newStorageDescriptor.getSerdeInfo()).thenReturn(newSerDeInfo);
        when(newStorageDescriptor.getLocation()).thenReturn("/warehouse/default/new_table");

        // Configure SerDeInfo mocks
        when(oldSerDeInfo.getSerializationLib()).thenReturn("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        when(newSerDeInfo.getSerializationLib()).thenReturn("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

        // Configure Table mocks to prevent NPEs
        when(oldTable.getDbName()).thenReturn("default");
        when(oldTable.getCols()).thenReturn(new ArrayList<>());
        when(oldTable.getParameters()).thenReturn(new HashMap<>());
        when(newTable.getDbName()).thenReturn("default");
        when(newTable.getCols()).thenReturn(new ArrayList<>());
        when(newTable.getParameters()).thenReturn(new HashMap<>());

        // Configure context mocks
        when(context.getMetadataNamespace()).thenReturn("cluster");
        when(context.getQualifiedName(any(Table.class))).thenReturn("default.test_table@cluster");
        when(context.getHive()).thenReturn(hive);
        when(context.getMetastoreHandler()).thenReturn(metastoreHandler);

        // Configure Hive and metastore handler
        org.apache.hadoop.hive.metastore.api.Database mockDb = mock(org.apache.hadoop.hive.metastore.api.Database.class);
        when(mockDb.getName()).thenReturn("default");
        when(hive.getDatabase("default")).thenReturn(mockDb);
        when(metastoreHandler.get_database("default")).thenReturn(mockDb);

        // Initialize Mockito annotations after manual mock setup
        MockitoAnnotations.initMocks(this);
    }

    // ========== CONSTRUCTOR TESTS ==========

    @Test
    public void testConstructor() {
        AlterTableRename alterTableRename = new AlterTableRename(context);
        AssertJUnit.assertNotNull("AlterTableRename should be instantiated", alterTableRename);
        AssertJUnit.assertTrue("AlterTableRename should extend BaseHiveEvent",
                alterTableRename instanceof BaseHiveEvent);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullContext() {
        AlterTableRename alterTableRename = new AlterTableRename(null);
    }

    // ========== getNotificationMessages() TESTS ==========

    @Test
    public void testGetNotificationMessages_MetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        List<HookNotification> expectedNotifications = Arrays.asList(
                mock(HookNotification.EntityPartialUpdateRequestV2.class),
                mock(HookNotification.EntityUpdateRequestV2.class)
        );
        doReturn(expectedNotifications).when(alterTableRename).getHiveMetastoreMessages();

        List<HookNotification> notifications = alterTableRename.getNotificationMessages();

        AssertJUnit.assertEquals("Should return metastore messages", expectedNotifications, notifications);
        verify(alterTableRename).getHiveMetastoreMessages();
        verify(alterTableRename, never()).getHiveMessages();
    }

    @Test
    public void testGetNotificationMessages_NonMetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);

        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        List<HookNotification> expectedNotifications = Arrays.asList(
                mock(HookNotification.EntityPartialUpdateRequestV2.class),
                mock(HookNotification.EntityUpdateRequestV2.class),
                mock(HookNotification.EntityCreateRequestV2.class)
        );
        doReturn(expectedNotifications).when(alterTableRename).getHiveMessages();

        List<HookNotification> notifications = alterTableRename.getNotificationMessages();

        AssertJUnit.assertEquals("Should return hive messages", expectedNotifications, notifications);
        verify(alterTableRename).getHiveMessages();
        verify(alterTableRename, never()).getHiveMetastoreMessages();
    }

    @Test
    public void testGetNotificationMessages_ExceptionHandling() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        doThrow(new RuntimeException("Test exception")).when(alterTableRename).getHiveMetastoreMessages();

        try {
            alterTableRename.getNotificationMessages();
            AssertJUnit.fail("Should have thrown exception");
        } catch (RuntimeException e) {
            AssertJUnit.assertEquals("Exception message should match", "Test exception", e.getMessage());
        }
    }

    // ========== getHiveMetastoreMessages() TESTS ==========

    @Test
    public void testGetHiveMessages_EmptyInputs() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        doReturn(Collections.emptySet()).when(alterTableRename).getInputs();

        List<HookNotification> notifications = alterTableRename.getHiveMessages();

        AssertJUnit.assertTrue("Should return empty list when inputs are empty", notifications.isEmpty());
    }

    @Test
    public void testGetHiveMessages_NullInputs() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        doReturn(null).when(alterTableRename).getInputs();

        List<HookNotification> notifications = alterTableRename.getHiveMessages();

        AssertJUnit.assertTrue("Should return empty list when inputs are null", notifications.isEmpty());
    }

    @Test
    public void testGetHiveMessages_NoTableInOutputs() throws Exception {
        // Setup inputs
        ReadEntity inputEntity = mock(ReadEntity.class);
        when(inputEntity.getTable()).thenReturn(oldTable);
        Set<ReadEntity> inputs = Collections.singleton(inputEntity);

        // Setup outputs with non-table entity
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.PARTITION);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);

        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        doReturn(inputs).when(alterTableRename).getInputs();
        doReturn(outputs).when(alterTableRename).getOutputs();

        List<HookNotification> notifications = alterTableRename.getHiveMessages();

        AssertJUnit.assertTrue("Should return empty list when no table in outputs", notifications.isEmpty());
    }

    // ========== processTables() TESTS ==========

    @Test
    public void testProcessTables_OldTableEntityNull_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        // Return null for old table entity
        doReturn(null).when(alterTableRename).toTableEntity(oldTable);
        doReturn(createMockTableEntity("default.new_table@cluster")).when(alterTableRename).toTableEntity(newTable);

        List<HookNotification> notifications = new ArrayList<>();

        // Use reflection to call private method
        Method method = AlterTableRename.class.getDeclaredMethod("processTables",
                Table.class, Table.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, oldTable, newTable, notifications);

        // Assert output
        AssertJUnit.assertTrue("Notifications should be empty when old table entity is null",
                notifications.isEmpty());
    }


    @Test
    public void testProcessTables_NewTableEntityNull_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        doReturn(createMockTableEntity("default.old_table@cluster")).when(alterTableRename).toTableEntity(oldTable);
        doReturn(null).when(alterTableRename).toTableEntity(newTable);

        List<HookNotification> notifications = new ArrayList<>();

        // Use reflection to invoke private method
        Method method = AlterTableRename.class.getDeclaredMethod("processTables",
                Table.class, Table.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, oldTable, newTable, notifications);

        AssertJUnit.assertTrue("Notifications should be empty when new table entity is null",
                notifications.isEmpty());
    }

    // ========== renameColumns() TESTS ==========

    @Test
    public void testRenameColumns_Success_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));
        doReturn("test_user").when(alterTableRename).getUserName();
        doReturn("default.new_table.col1@cluster").when(alterTableRename)
                .getColumnQualifiedName("default.new_table@cluster", "col1");

        // Create mock column
        AtlasEntity columnEntity = new AtlasEntity("hive_column");
        columnEntity.setGuid("col1_guid");
        columnEntity.setAttribute("qualifiedName", "default.old_table.col1@cluster");
        columnEntity.setAttribute("name", "col1");

        AtlasObjectId columnId = new AtlasObjectId("hive_column", "qualifiedName", "default.old_table.col1@cluster");
        columnId.setGuid("col1_guid");

        AtlasEntity.AtlasEntityExtInfo oldEntityExtInfo = mock(AtlasEntity.AtlasEntityExtInfo.class);
        when(oldEntityExtInfo.getEntity("col1_guid")).thenReturn(columnEntity);

        List<AtlasObjectId> columns = Arrays.asList(columnId);
        List<HookNotification> notifications = new ArrayList<>();

        // Use reflection to call private method renameColumns
        Method method = AlterTableRename.class.getDeclaredMethod("renameColumns",
                List.class, AtlasEntity.AtlasEntityExtInfo.class, String.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, columns, oldEntityExtInfo, "default.new_table@cluster", notifications);

        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be partial update request",
                notifications.get(0) instanceof HookNotification.EntityPartialUpdateRequestV2);
    }


    @Test
    public void testRenameColumns_EmptyColumns_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        List<HookNotification> notifications = new ArrayList<>();

        // Use reflection to invoke private renameColumns method
        Method method = AlterTableRename.class.getDeclaredMethod("renameColumns",
                List.class, AtlasEntity.AtlasEntityExtInfo.class, String.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, Collections.emptyList(), mock(AtlasEntity.AtlasEntityExtInfo.class),
                "default.new_table@cluster", notifications);

        AssertJUnit.assertTrue("Should have no notifications for empty columns", notifications.isEmpty());
    }


    @Test
    public void testRenameColumns_NullColumns_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        List<HookNotification> notifications = new ArrayList<>();

        // Use reflection to invoke private renameColumns method
        Method method = AlterTableRename.class.getDeclaredMethod("renameColumns",
                List.class, AtlasEntity.AtlasEntityExtInfo.class, String.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, (Object) null, mock(AtlasEntity.AtlasEntityExtInfo.class),
                "default.new_table@cluster", notifications);

        AssertJUnit.assertTrue("Should have no notifications for null columns", notifications.isEmpty());
    }


    // ========== renameStorageDesc() TESTS ==========

    @Test
    public void testRenameStorageDesc_OldSdNull_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        // Setup oldEntityExtInfo and newEntityExtInfo with appropriate mocks
        AtlasEntity.AtlasEntityWithExtInfo oldEntityExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity.AtlasEntityWithExtInfo newEntityExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);

        // Just ensure getEntity returns null for oldEntityExtInfo
        when(oldEntityExtInfo.getEntity(anyString())).thenReturn(null);

        // And return a non-null entity for newEntityExtInfo
        when(newEntityExtInfo.getEntity(anyString())).thenReturn(new AtlasEntity("hive_storagedesc"));

        List<HookNotification> notifications = new ArrayList<>();

        // Invoke private renameStorageDesc using reflection
        Method method = AlterTableRename.class.getDeclaredMethod("renameStorageDesc",
                AtlasEntity.AtlasEntityWithExtInfo.class, AtlasEntity.AtlasEntityWithExtInfo.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, oldEntityExtInfo, newEntityExtInfo, notifications);

        AssertJUnit.assertTrue("Should have no notifications when old SD is null", notifications.isEmpty());
    }


    @Test
    public void testRenameStorageDesc_NewSdNull_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        AtlasEntity.AtlasEntityWithExtInfo oldEntityExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity.AtlasEntityWithExtInfo newEntityExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);

        // Simulate old SD entity exists
        when(oldEntityExtInfo.getEntity(anyString())).thenReturn(new AtlasEntity("hive_storagedesc"));
        // Simulate new SD entity does NOT exist (return null)
        when(newEntityExtInfo.getEntity(anyString())).thenReturn(null);

        List<HookNotification> notifications = new ArrayList<>();

        // Invoke private renameStorageDesc via reflection
        Method method = AlterTableRename.class.getDeclaredMethod("renameStorageDesc",
                AtlasEntity.AtlasEntityWithExtInfo.class, AtlasEntity.AtlasEntityWithExtInfo.class, List.class);
        method.setAccessible(true);
        method.invoke(alterTableRename, oldEntityExtInfo, newEntityExtInfo, notifications);

        AssertJUnit.assertTrue("Should have no notifications when new SD is null", notifications.isEmpty());
    }


    // ========== getStorageDescEntity() TESTS ==========

    @Test
    public void testGetStorageDescEntity_Success_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = new AlterTableRename(context);

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        AtlasObjectId sdId = new AtlasObjectId("hive_storagedesc", "qualifiedName", "sd_qualified_name");
        sdId.setGuid("sd_guid");
        tableEntity.setRelationshipAttribute("sd", sdId);

        AtlasEntity sdEntity = new AtlasEntity("hive_storagedesc");

        AtlasEntity.AtlasEntityWithExtInfo tableEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(tableEntity);
        tableEntityWithExtInfo.addReferredEntity("sd_guid", sdEntity);

        // Use reflection to invoke private method getStorageDescEntity
        Method method = AlterTableRename.class.getDeclaredMethod("getStorageDescEntity", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        AtlasEntity result = (AtlasEntity) method.invoke(alterTableRename, tableEntityWithExtInfo);

        AssertJUnit.assertEquals("Should return the storage descriptor entity", sdEntity, result);
    }

    @Test
    public void testGetStorageDescEntity_NullTableEntity_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = new AlterTableRename(context);

        // Get private method via reflection
        Method method = AlterTableRename.class.getDeclaredMethod("getStorageDescEntity", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        // Invoke method with null argument
        AtlasEntity result = (AtlasEntity) method.invoke(alterTableRename, (AtlasEntity.AtlasEntityWithExtInfo) null);

        AssertJUnit.assertNull("Should return null for null table entity", result);
    }

    @Test
    public void testGetStorageDescEntity_NullEntity_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = new AlterTableRename(context);

        AtlasEntity.AtlasEntityWithExtInfo tableEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();

        Method method = AlterTableRename.class.getDeclaredMethod("getStorageDescEntity", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        AtlasEntity result = (AtlasEntity) method.invoke(alterTableRename, tableEntityWithExtInfo);

        AssertJUnit.assertNull("Should return null when entity is null", result);
    }

    @Test
    public void testGetStorageDescEntity_NoStorageDescAttribute_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = new AlterTableRename(context);

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        // No storage descriptor attribute set

        AtlasEntity.AtlasEntityWithExtInfo tableEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(tableEntity);

        Method method = AlterTableRename.class.getDeclaredMethod("getStorageDescEntity", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        AtlasEntity result = (AtlasEntity) method.invoke(alterTableRename, tableEntityWithExtInfo);

        AssertJUnit.assertNull("Should return null when no storage descriptor attribute", result);
    }


    @Test
    public void testGetStorageDescEntity_NonAtlasObjectIdAttribute_UsingReflection() throws Exception {
        AlterTableRename alterTableRename = new AlterTableRename(context);

        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setRelationshipAttribute("sd", "not_an_atlas_object_id"); // Invalid type

        AtlasEntity.AtlasEntityWithExtInfo tableEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(tableEntity);

        Method method = AlterTableRename.class.getDeclaredMethod("getStorageDescEntity", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        AtlasEntity result = (AtlasEntity) method.invoke(alterTableRename, tableEntityWithExtInfo);

        AssertJUnit.assertNull("Should return null when attribute is not AtlasObjectId", result);
    }


    // ========== INTEGRATION TESTS ==========

    @Test
    public void testFullWorkflow_MetastoreHook() throws Exception {
        // Setup metastore event
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(alterTableEvent);

        // Create mock tables explicitly
        org.apache.hadoop.hive.metastore.api.Table oldMetastoreTable = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        org.apache.hadoop.hive.metastore.api.Table newMetastoreTable = mock(org.apache.hadoop.hive.metastore.api.Table.class);

        when(oldMetastoreTable.getDbName()).thenReturn("default");
        when(oldMetastoreTable.getTableName()).thenReturn("old_table");
        when(oldMetastoreTable.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(oldMetastoreTable.getOwner()).thenReturn("hive");

        when(newMetastoreTable.getDbName()).thenReturn("default");
        when(newMetastoreTable.getTableName()).thenReturn("new_table");
        when(newMetastoreTable.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(newMetastoreTable.getOwner()).thenReturn("hive");

        // Mock StorageDescriptor and SerDeInfo
        StorageDescriptor oldSd = mock(StorageDescriptor.class);
        StorageDescriptor newSd = mock(StorageDescriptor.class);

        SerDeInfo serDeInfo = mock(SerDeInfo.class);
        when(serDeInfo.getSerializationLib()).thenReturn("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

        when(oldSd.getCols()).thenReturn(Collections.singletonList(new FieldSchema("col1", "string", null)));
        when(oldSd.getLocation()).thenReturn("/warehouse/default/old_table");
        when(oldSd.getSerdeInfo()).thenReturn(serDeInfo);

        when(newSd.getCols()).thenReturn(Collections.singletonList(new FieldSchema("col1", "string", null)));
        when(newSd.getLocation()).thenReturn("/warehouse/default/new_table");
        when(newSd.getSerdeInfo()).thenReturn(serDeInfo);

        when(oldMetastoreTable.getSd()).thenReturn(oldSd);
        when(oldMetastoreTable.getPartitionKeys()).thenReturn(new ArrayList<>());
        when(oldMetastoreTable.getParameters()).thenReturn(new HashMap<>());

        when(newMetastoreTable.getSd()).thenReturn(newSd);
        when(newMetastoreTable.getPartitionKeys()).thenReturn(new ArrayList<>());
        when(newMetastoreTable.getParameters()).thenReturn(new HashMap<>());

        // Mock AlterTableEvent responses
        when(alterTableEvent.getOldTable()).thenReturn(oldMetastoreTable);
        when(alterTableEvent.getNewTable()).thenReturn(newMetastoreTable);

        // Create mock entity wrappers
        AtlasEntity.AtlasEntityWithExtInfo oldTableEntity = createMockTableEntity("default.old_table@cluster");
        AtlasEntity.AtlasEntityWithExtInfo newTableEntity = createMockTableEntity("default.new_table@cluster");

        // Create spy without overriding processTables
        AlterTableRename alterTableRename = spy(new AlterTableRename(context));

        doReturn(oldTableEntity).when(alterTableRename).toTableEntity(oldTable);
        doReturn(newTableEntity).when(alterTableRename).toTableEntity(newTable);
        doReturn("admin").when(alterTableRename).getUserName();
        doReturn("default.new_table.col@cluster")
                .when(alterTableRename).getColumnQualifiedName(anyString(), anyString());

        // Stub getQualifiedName to prevent NPE
        doReturn("default.old_table@cluster").when(alterTableRename).getQualifiedName(any(org.apache.hadoop.hive.ql.metadata.Table.class));
        doReturn("default.new_table@cluster").when(alterTableRename).getQualifiedName(any(org.apache.hadoop.hive.ql.metadata.Table.class), any(FieldSchema.class));

        // Mock database to avoid NPE in getDatabases
        org.apache.hadoop.hive.metastore.api.Database mockDb = mock(org.apache.hadoop.hive.metastore.api.Database.class);
        when(mockDb.getName()).thenReturn("default");

        IHMSHandler metastoreHandler = mock(IHMSHandler.class);
        when(context.getMetastoreHandler()).thenReturn(metastoreHandler);
        when(metastoreHandler.get_database("default")).thenReturn(mockDb);

        // Execute
        List<HookNotification> notifications = alterTableRename.getNotificationMessages();

        // Assertions
        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertFalse("Notifications should not be empty", notifications.isEmpty());

        // Validate types
        AssertJUnit.assertTrue("Should contain partial update",
                notifications.stream().anyMatch(n -> n instanceof HookNotification.EntityPartialUpdateRequestV2));
        AssertJUnit.assertTrue("Should contain full update",
                notifications.stream().anyMatch(n -> n instanceof HookNotification.EntityUpdateRequestV2));
    }

    // ========== HELPER METHODS ==========

    private org.apache.hadoop.hive.metastore.api.Table createMockMetastoreTable(String tableName) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);
        when(table.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(table.getOwner()).thenReturn("hive");

        StorageDescriptor sd = mock(StorageDescriptor.class);
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema("col1", "string", null)));
        when(sd.getLocation()).thenReturn("/warehouse/default/" + tableName);
        when(table.getSd()).thenReturn(sd);
        when(table.getPartitionKeys()).thenReturn(new ArrayList<>());
        when(table.getParameters()).thenReturn(new HashMap<>());

        return table;
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockTableEntity(String qualifiedName) {
        AtlasEntity tableEntity = new AtlasEntity("hive_table");
        tableEntity.setAttribute("qualifiedName", qualifiedName);
        tableEntity.setAttribute("name", qualifiedName.split("\\.")[1].split("@")[0]);

        // Add relationship attributes
        AtlasObjectId columnId = new AtlasObjectId("hive_column", "qualifiedName", qualifiedName + ".col1");
        columnId.setGuid("col1_guid");
        tableEntity.setRelationshipAttribute("columns", Arrays.asList(columnId));
        tableEntity.setRelationshipAttribute("partitionKeys", new ArrayList<>());

        AtlasObjectId sdId = new AtlasObjectId("hive_storagedesc", "qualifiedName", qualifiedName + "_storage");
        sdId.setGuid("sd_guid");
        tableEntity.setRelationshipAttribute("sd", sdId);

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(tableEntity);

        // Add referred entities
        AtlasEntity columnEntity = new AtlasEntity("hive_column");
        columnEntity.setAttribute("qualifiedName", qualifiedName + ".col1");
        columnEntity.setAttribute("name", "col1");
        entityWithExtInfo.addReferredEntity("col1_guid", columnEntity);

        AtlasEntity sdEntity = new AtlasEntity("hive_storagedesc");
        sdEntity.setAttribute("qualifiedName", qualifiedName + "_storage");
        entityWithExtInfo.addReferredEntity("sd_guid", sdEntity);

        return entityWithExtInfo;
    }
}
