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
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DropTableTest {

    @Mock
    AtlasHiveHookContext context;

    @Mock
    DropTableEvent dropTableEvent;

    @Mock
    Table table;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    // ========== CONSTRUCTOR TESTS ==========

    @Test
    public void testConstructor() {
        DropTable dropTable = new DropTable(context);
        AssertJUnit.assertNotNull("DropTable should be instantiated", dropTable);
        AssertJUnit.assertTrue("DropTable should extend BaseHiveEvent",
                dropTable instanceof BaseHiveEvent);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullContext() {
        new DropTable(null);
    }

    // ========== getNotificationMessages() TESTS ==========

    @Test
    public void testGetNotificationMessages_MetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getHiveMetastoreEntities to return some entities
        List<AtlasObjectId> mockEntities = Arrays.asList(
                new AtlasObjectId("hive_table", "qualifiedName", "default.test_table@cluster")
        );
        doReturn(mockEntities).when(dropTable).getHiveMetastoreEntities();

        // Mock getUserName
        doReturn("test_user").when(dropTable).getUserName();

        List<HookNotification> notifications = dropTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be EntityDeleteRequestV2",
                notifications.get(0) instanceof EntityDeleteRequestV2);
        
        EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should be test_user", "test_user", deleteRequest.getUser());
        AssertJUnit.assertEquals("Should have one entity to delete", 1, deleteRequest.getEntities().size());

        verify(dropTable).getHiveMetastoreEntities();
        verify(dropTable, never()).getHiveEntities();
    }

    @Test
    public void testGetNotificationMessages_NonMetastoreHook() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getHiveEntities to return some entities
        List<AtlasObjectId> mockEntities = Arrays.asList(
                new AtlasObjectId("hive_table", "qualifiedName", "default.test_table@cluster")
        );
        doReturn(mockEntities).when(dropTable).getHiveEntities();

        // Mock getUserName
        doReturn("test_user").when(dropTable).getUserName();

        List<HookNotification> notifications = dropTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        AssertJUnit.assertTrue("Should be EntityDeleteRequestV2",
                notifications.get(0) instanceof EntityDeleteRequestV2);

        verify(dropTable).getHiveEntities();
        verify(dropTable, never()).getHiveMetastoreEntities();
    }

    @Test
    public void testGetNotificationMessages_EmptyEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getHiveMetastoreEntities to return empty list
        doReturn(Collections.emptyList()).when(dropTable).getHiveMetastoreEntities();

        List<HookNotification> notifications = dropTable.getNotificationMessages();

        AssertJUnit.assertNull("Notifications should be null when no entities", notifications);
    }

    @Test
    public void testGetNotificationMessages_NullEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(false);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getHiveEntities to return null
        doReturn(null).when(dropTable).getHiveEntities();

        List<HookNotification> notifications = dropTable.getNotificationMessages();

        AssertJUnit.assertNull("Notifications should be null when entities are null", notifications);
    }

    @Test
    public void testGetNotificationMessages_MultipleEntities() throws Exception {
        when(context.isMetastoreHook()).thenReturn(true);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getHiveMetastoreEntities to return multiple entities
        List<AtlasObjectId> mockEntities = Arrays.asList(
                new AtlasObjectId("hive_table", "qualifiedName", "default.table1@cluster"),
                new AtlasObjectId("hive_table", "qualifiedName", "default.table2@cluster")
        );
        doReturn(mockEntities).when(dropTable).getHiveMetastoreEntities();

        // Mock getUserName
        doReturn("test_user").when(dropTable).getUserName();

        List<HookNotification> notifications = dropTable.getNotificationMessages();

        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have two notifications", 2, notifications.size());
        
        // Verify each notification is correct
        for (HookNotification notification : notifications) {
            AssertJUnit.assertTrue("Should be EntityDeleteRequestV2",
                    notification instanceof EntityDeleteRequestV2);
            EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) notification;
            AssertJUnit.assertEquals("User should be test_user", "test_user", deleteRequest.getUser());
            AssertJUnit.assertEquals("Should have one entity to delete", 1, deleteRequest.getEntities().size());
        }
    }

    // ========== getHiveMetastoreEntities() TESTS ==========

    @Test
    public void testGetHiveMetastoreEntities_Success() throws Exception {
        // Setup metastore event
        when(context.getMetastoreEvent()).thenReturn(dropTableEvent);

        // Create mock metastore table
        org.apache.hadoop.hive.metastore.api.Table metastoreTable = createMockMetastoreTable("test_table");
        when(dropTableEvent.getTable()).thenReturn(metastoreTable);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getQualifiedName to avoid complex dependencies
        doReturn("default.test_table@cluster").when(dropTable).getQualifiedName(any(Table.class));

        List<AtlasObjectId> entities = dropTable.getHiveMetastoreEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertEquals("Should have one entity", 1, entities.size());
        
        AtlasObjectId entity = entities.get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        AssertJUnit.assertEquals("Should have correct qualified name", "default.test_table@cluster",
                entity.getUniqueAttributes().get("qualifiedName"));

        // Verify context.removeFromKnownTable was called
        verify(context).removeFromKnownTable("default.test_table@cluster");
    }

    @Test
    public void testGetHiveMetastoreEntities_DifferentTableName() throws Exception {
        // Setup metastore event
        when(context.getMetastoreEvent()).thenReturn(dropTableEvent);

        // Create mock metastore table with different name
        org.apache.hadoop.hive.metastore.api.Table metastoreTable = createMockMetastoreTable("another_table");
        when(dropTableEvent.getTable()).thenReturn(metastoreTable);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock getQualifiedName
        doReturn("prod.another_table@cluster2").when(dropTable).getQualifiedName(any(Table.class));

        List<AtlasObjectId> entities = dropTable.getHiveMetastoreEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertEquals("Should have one entity", 1, entities.size());
        
        AtlasObjectId entity = entities.get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        AssertJUnit.assertEquals("Should have correct qualified name", "prod.another_table@cluster2",
                entity.getUniqueAttributes().get("qualifiedName"));

        // Verify context.removeFromKnownTable was called with correct name
        verify(context).removeFromKnownTable("prod.another_table@cluster2");
    }

    // ========== getHiveEntities() TESTS ==========

    @Test
    public void testGetHiveEntities_Success() throws Exception {
        // Setup output entities
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.TABLE);
        when(outputEntity.getTable()).thenReturn(table);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);

        DropTable dropTable = spy(new DropTable(context));
        doReturn(outputs).when(dropTable).getOutputs();
        
        // Mock getQualifiedName
        doReturn("default.test_table@cluster").when(dropTable).getQualifiedName(table);

        List<AtlasObjectId> entities = dropTable.getHiveEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertEquals("Should have one entity", 1, entities.size());
        
        AtlasObjectId entity = entities.get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        AssertJUnit.assertEquals("Should have correct qualified name", "default.test_table@cluster",
                entity.getUniqueAttributes().get("qualifiedName"));

        // Verify context.removeFromKnownTable was called
        verify(context).removeFromKnownTable("default.test_table@cluster");
    }

    @Test
    public void testGetHiveEntities_EmptyOutputs() throws Exception {
        DropTable dropTable = spy(new DropTable(context));
        doReturn(Collections.emptySet()).when(dropTable).getOutputs();

        List<AtlasObjectId> entities = dropTable.getHiveEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertTrue("Entities should be empty", entities.isEmpty());

        // Verify no calls to removeFromKnownTable
        verify(context, never()).removeFromKnownTable(any(String.class));
    }

    @Test
    public void testGetHiveEntities_NonTableEntity() throws Exception {
        // Setup output entities with non-table type
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.DATABASE);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);

        DropTable dropTable = spy(new DropTable(context));
        doReturn(outputs).when(dropTable).getOutputs();

        List<AtlasObjectId> entities = dropTable.getHiveEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertTrue("Entities should be empty", entities.isEmpty());

        // Verify no calls to removeFromKnownTable
        verify(context, never()).removeFromKnownTable(any(String.class));
    }

    @Test
    public void testGetHiveEntities_MultipleTables() throws Exception {
        // Setup multiple table entities
        WriteEntity table1Entity = mock(WriteEntity.class);
        when(table1Entity.getType()).thenReturn(Entity.Type.TABLE);
        Table table1 = mock(Table.class);
        when(table1Entity.getTable()).thenReturn(table1);

        WriteEntity table2Entity = mock(WriteEntity.class);
        when(table2Entity.getType()).thenReturn(Entity.Type.TABLE);
        Table table2 = mock(Table.class);
        when(table2Entity.getTable()).thenReturn(table2);

        Set<WriteEntity> outputs = new HashSet<>(Arrays.asList(table1Entity, table2Entity));

        DropTable dropTable = spy(new DropTable(context));
        doReturn(outputs).when(dropTable).getOutputs();
        
        // Mock getQualifiedName for both tables
        doReturn("default.table1@cluster").when(dropTable).getQualifiedName(table1);
        doReturn("default.table2@cluster").when(dropTable).getQualifiedName(table2);

        List<AtlasObjectId> entities = dropTable.getHiveEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertEquals("Should have two entities", 2, entities.size());
        
        // Verify both entities have correct type
        for (AtlasObjectId entity : entities) {
            AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        }

        // Verify context.removeFromKnownTable was called for both tables
        verify(context).removeFromKnownTable("default.table1@cluster");
        verify(context).removeFromKnownTable("default.table2@cluster");
    }

    @Test
    public void testGetHiveEntities_MixedEntityTypes() throws Exception {
        // Setup mixed entity types
        WriteEntity tableEntity = mock(WriteEntity.class);
        when(tableEntity.getType()).thenReturn(Entity.Type.TABLE);
        when(tableEntity.getTable()).thenReturn(table);

        WriteEntity dbEntity = mock(WriteEntity.class);
        when(dbEntity.getType()).thenReturn(Entity.Type.DATABASE);

        WriteEntity partitionEntity = mock(WriteEntity.class);
        when(partitionEntity.getType()).thenReturn(Entity.Type.PARTITION);

        Set<WriteEntity> outputs = new HashSet<>(Arrays.asList(tableEntity, dbEntity, partitionEntity));

        DropTable dropTable = spy(new DropTable(context));
        doReturn(outputs).when(dropTable).getOutputs();
        
        // Mock getQualifiedName for table only
        doReturn("default.test_table@cluster").when(dropTable).getQualifiedName(table);

        List<AtlasObjectId> entities = dropTable.getHiveEntities();

        AssertJUnit.assertNotNull("Entities should not be null", entities);
        AssertJUnit.assertEquals("Should have one entity (only table)", 1, entities.size());
        
        AtlasObjectId entity = entities.get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        AssertJUnit.assertEquals("Should have correct qualified name", "default.test_table@cluster",
                entity.getUniqueAttributes().get("qualifiedName"));

        // Verify context.removeFromKnownTable was called only for table
        verify(context).removeFromKnownTable("default.test_table@cluster");
        verify(context, times(1)).removeFromKnownTable(any(String.class));
    }


    // ========== INTEGRATION TESTS ==========

    @Test
    public void testFullWorkflow_MetastoreHook() throws Exception {
        // Setup for metastore hook workflow
        when(context.isMetastoreHook()).thenReturn(true);
        when(context.getMetastoreEvent()).thenReturn(dropTableEvent);

        // Create mock metastore table
        org.apache.hadoop.hive.metastore.api.Table metastoreTable = createMockMetastoreTable("integration_test");
        when(dropTableEvent.getTable()).thenReturn(metastoreTable);

        DropTable dropTable = spy(new DropTable(context));
        
        // Mock dependencies
        doReturn("default.integration_test@cluster").when(dropTable).getQualifiedName(any(Table.class));
        doReturn("integration_user").when(dropTable).getUserName();

        // Execute the full workflow
        List<HookNotification> notifications = dropTable.getNotificationMessages();

        // Verify results
        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        
        EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should be integration_user", "integration_user", deleteRequest.getUser());
        AssertJUnit.assertEquals("Should have one entity to delete", 1, deleteRequest.getEntities().size());
        
        AtlasObjectId entity = deleteRequest.getEntities().get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());
        AssertJUnit.assertEquals("Should have correct qualified name", "default.integration_test@cluster",
                entity.getUniqueAttributes().get("qualifiedName"));

        // Verify context.removeFromKnownTable was called
        verify(context).removeFromKnownTable("default.integration_test@cluster");
    }

    @Test
    public void testFullWorkflow_HiveHook() throws Exception {
        // Setup for hive hook workflow
        when(context.isMetastoreHook()).thenReturn(false);

        // Setup output entities
        WriteEntity outputEntity = mock(WriteEntity.class);
        when(outputEntity.getType()).thenReturn(Entity.Type.TABLE);
        when(outputEntity.getTable()).thenReturn(table);
        Set<WriteEntity> outputs = Collections.singleton(outputEntity);

        DropTable dropTable = spy(new DropTable(context));
        doReturn(outputs).when(dropTable).getOutputs();
        
        // Mock dependencies
        doReturn("default.hive_test@cluster").when(dropTable).getQualifiedName(table);
        doReturn("hive_user").when(dropTable).getUserName();

        // Execute the full workflow
        List<HookNotification> notifications = dropTable.getNotificationMessages();

        // Verify results
        AssertJUnit.assertNotNull("Notifications should not be null", notifications);
        AssertJUnit.assertEquals("Should have one notification", 1, notifications.size());
        
        EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) notifications.get(0);
        AssertJUnit.assertEquals("User should be hive_user", "hive_user", deleteRequest.getUser());
        AssertJUnit.assertEquals("Should have one entity to delete", 1, deleteRequest.getEntities().size());
        
        AtlasObjectId entity = deleteRequest.getEntities().get(0);
        AssertJUnit.assertEquals("Should be hive_table type", "hive_table", entity.getTypeName());

        // Verify context.removeFromKnownTable was called
        verify(context).removeFromKnownTable("default.hive_test@cluster");
    }

    // ========== HELPER METHODS ==========

    private org.apache.hadoop.hive.metastore.api.Table createMockMetastoreTable(String tableName) {
        org.apache.hadoop.hive.metastore.api.Table table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
        when(table.getDbName()).thenReturn("default");
        when(table.getTableName()).thenReturn(tableName);
        when(table.getTableType()).thenReturn(MANAGED_TABLE.toString());
        when(table.getOwner()).thenReturn("hive");

        StorageDescriptor sd = mock(StorageDescriptor.class);
        SerDeInfo serDeInfo = mock(SerDeInfo.class);
        when(serDeInfo.getSerializationLib()).thenReturn("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        
        when(sd.getCols()).thenReturn(Arrays.asList(new FieldSchema("col1", "string", null)));
        when(sd.getLocation()).thenReturn("/warehouse/default/" + tableName);
        when(sd.getSerdeInfo()).thenReturn(serDeInfo);
        
        when(table.getSd()).thenReturn(sd);
        when(table.getPartitionKeys()).thenReturn(new ArrayList<>());
        when(table.getParameters()).thenReturn(new HashMap<>());

        return table;
    }
}