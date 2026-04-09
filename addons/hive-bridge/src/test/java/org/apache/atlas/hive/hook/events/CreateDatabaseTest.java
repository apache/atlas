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
import org.apache.atlas.model.notification.HookNotification;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

public class CreateDatabaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateDatabaseTest.class);

    CreateDatabase createDatabase;

    @Mock
    AtlasHiveHookContext context;

    @Test
    public void testGetNotificationMessagesGHMS() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(context.isMetastoreHook()).thenReturn(true);

        CreateDatabaseEvent createDatabaseEvent = mock(CreateDatabaseEvent.class);
        when(context.getMetastoreEvent()).thenReturn(createDatabaseEvent);

        HiveHook hook = mock(HiveHook.class);
        when(hook.getMetadataNamespace()).thenReturn("cm");

        Database database = mock(Database.class);
        when(createDatabaseEvent.getDatabase()).thenReturn(database);
        database.setName("hive");
        when(database.getName()).thenReturn("jacocoDb");
        when(database.getDescription()).thenReturn(null);
        when(database.getLocationUri()).thenReturn("hdfs://ccycloud-1.fsknox.root.comops.site:8020/warehouse/tablespace/external/hive/jacocoDb");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("comment", "Similar to Default Hive database");
        parameters.put("owner_name", "public");
        parameters.put("owner_type", "ROLE");
        when(database.getParameters()).thenReturn(parameters);

        when(database.getOwnerName()).thenReturn("hive");
        when(database.getOwnerType()).thenReturn(PrincipalType.USER);

        when(database.getCatalogName()).thenReturn("hive");

        IHMSHandler handler = mock(IHMSHandler.class);
        when(context.getMetastoreHandler()).thenReturn(handler);

        when(handler.get_database("jacocoDb")).thenReturn(database);

        createDatabase = new CreateDatabase(context);

        List<HookNotification> hookNotificationList = createDatabase.getNotificationMessages();

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
        AtlasEntity atlasEntity = mock(AtlasEntity.class);
        BaseHiveEvent baseHiveEvent = mock(BaseHiveEvent.class);

        //DB
        Database database = mock(Database.class);
        database.setName("hive");
        when(database.getName()).thenReturn("jacocoDb");
        when(database.getDescription()).thenReturn(null);
        when(database.getLocationUri()).thenReturn("hdfs://ccycloud-1.fsknox.root.comops.site:8020/warehouse/tablespace/external/hive/jacocoDb");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("comment", "Similar to Default Hive database");
        parameters.put("owner_name", "public");
        parameters.put("owner_type", "ROLE");
        when(database.getParameters()).thenReturn(parameters);

        when(database.getOwnerName()).thenReturn("hive");
        when(database.getOwnerType()).thenReturn(PrincipalType.USER);

        when(database.getCatalogName()).thenReturn("hive");

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

        when(atlasEntity.getGuid()).thenReturn("1234-guid");
        when(atlasEntity.getTypeName()).thenReturn("hive_db");
        when(atlasEntity.getAttribute("qualifiedName")).thenReturn("jacocoDb@cm");

        CreateDatabase createDatabase = spy(new CreateDatabase(context));

        doReturn(hive).when(createDatabase).getHive();

        doReturn(null).when(createDatabase).createHiveDDLEntity(any(AtlasEntity.class));
    }

    @Test
    public void testGetNotificationMessages_fromReplicatedMetastoreHook() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(context.isMetastoreHook()).thenReturn(true);

        CreateDatabaseEvent createDatabaseEvent = mock(CreateDatabaseEvent.class);
        when(context.getMetastoreEvent()).thenReturn(createDatabaseEvent);

        createDatabase = new CreateDatabase(context);
        List<HookNotification> notifications = createDatabase.getNotificationMessages();

        assertNull(notifications);
    }
}
