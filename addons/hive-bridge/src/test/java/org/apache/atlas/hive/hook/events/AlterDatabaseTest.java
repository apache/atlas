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

import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.hive.hook.HiveHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public class AlterDatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AlterDatabaseTest.class);

    AlterDatabase alterDatabase;

    @Mock
    AtlasHiveHookContext context;

    @Test
    public void testGetNotificationMessages() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(context.isMetastoreHook()).thenReturn(true);

        AlterDatabaseEvent alterDatabaseEvent = mock(AlterDatabaseEvent.class);
        when(context.getMetastoreEvent()).thenReturn(alterDatabaseEvent);

        HiveHook hook = mock(HiveHook.class);
        when(hook.getMetadataNamespace()).thenReturn("cm");

        Database oldDatabase = mock(Database.class);
        Database newDatabase = mock(Database.class);
        when(alterDatabaseEvent.getOldDatabase()).thenReturn(oldDatabase);
        when(alterDatabaseEvent.getNewDatabase()).thenReturn(newDatabase);

        newDatabase.setName("new_hive_owner");
        when(newDatabase.getName()).thenReturn("jacocoDb2");
        when(newDatabase.getDescription()).thenReturn(null);
        when(newDatabase.getLocationUri()).thenReturn("hdfs://ccycloud-1.fsknox.root.comops.site:8020/warehouse/tablespace/external/hive/jacocoDb2");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("comment", "Similar to Default Hive database");
        parameters.put("owner_name", "public");
        parameters.put("owner_type", "USER");
        when(newDatabase.getParameters()).thenReturn(parameters);

        when(newDatabase.getOwnerName()).thenReturn("new_hive_owner");
        when(newDatabase.getOwnerType()).thenReturn(PrincipalType.USER);

        when(newDatabase.getCatalogName()).thenReturn("new_hive_owner");

        IHMSHandler handler = mock(IHMSHandler.class);
        when(context.getMetastoreHandler()).thenReturn(handler);

        when(handler.get_database("jacocoDb2")).thenReturn(newDatabase);

        alterDatabase = new AlterDatabase(context);

        List<HookNotification> hookNotificationList = alterDatabase.getNotificationMessages();

        assertNotNull(hookNotificationList);
        assertEquals(1, hookNotificationList.size());
    }

    @Test
    public void testGetHiveEntitiesog() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Setup context mock
        when(context.isSkipTempTables()).thenReturn(false);
        
        HookContext hookContext = mock(HookContext.class);
        WriteEntity writeEntity = mock(WriteEntity.class);
        Entity entity =mock(Entity.class);
        Entity.Type type = Entity.Type.DATABASE;
        AtlasEntity atlasEntity = mock(AtlasEntity.class);
        BaseHiveEvent baseHiveEvent = mock(BaseHiveEvent.class);

        //DB
        Database database = mock(Database.class);
        database.setName("new_hive_owner");
        when(database.getName()).thenReturn("jacocoDb2");
        when(database.getDescription()).thenReturn(null);
        when(database.getLocationUri()).thenReturn("hdfs://ccycloud-1.fsknox.root.comops.site:8020/warehouse/tablespace/external/hive/jacocoDb2");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("comment", "Similar to Default Hive database");
        parameters.put("owner_name", "public");
        parameters.put("owner_type", "USER");
        when(database.getParameters()).thenReturn(parameters);

        when(database.getOwnerName()).thenReturn("new_hive_owner");
        when(database.getOwnerType()).thenReturn(PrincipalType.USER);

        when(database.getCatalogName()).thenReturn("hive");

        //WriteType
        WriteEntity.WriteType writeType = WriteEntity.WriteType.DDL_EXCLUSIVE;

        //Hive
        Hive hive = mock(Hive.class);

        //BaseHiveEvent
        when(baseHiveEvent.getHive()).thenReturn(hive);

        //HiveMetaStoreBridge
        HiveMetaStoreBridge hiveMetaStoreBridge = mock(HiveMetaStoreBridge.class);

        when(hive.getDatabase(hiveMetaStoreBridge.getDatabaseName(database))).thenReturn(database);

        when(entity.getType()).thenReturn(Entity.Type.DATABASE);

        writeEntity = new WriteEntity(database, WriteEntity.WriteType.DDL_EXCLUSIVE);

        Set<WriteEntity> writeEntities = new HashSet<>();
        writeEntities.add(writeEntity);

        when(hookContext.getOutputs()).thenReturn(writeEntities);
        when(context.getOutputs()).thenReturn(writeEntities);

        AtlasObjectId atlasObjectId = mock(AtlasObjectId.class);
        when(atlasEntity.getGuid()).thenReturn("1234-guid");
        when(atlasEntity.getTypeName()).thenReturn("hive_db");
        when(atlasEntity.getAttribute("qualifiedName")).thenReturn("jacocoDb@cm");

        // Create AlterDatabase instance and test basic functionality
        AlterDatabase alterDatabase = new AlterDatabase(context);
        
        // Test that the instance is created successfully
        assertNotNull(alterDatabase);
    }
}
