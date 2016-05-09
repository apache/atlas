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

package org.apache.atlas.hive.bridge;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import java.util.List;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveMetaStoreBridgeTest {

    private static final String TEST_DB_NAME = "default";
    public static final String CLUSTER_NAME = "primary";
    public static final String TEST_TABLE_NAME = "test_table";

    @Mock
    private Hive hiveClient;

    @Mock
    private AtlasClient atlasClient;

    @BeforeMethod
    public void initializeMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testImportThatUpdatesRegisteredDatabase() throws Exception {
        // setup database
        when(hiveClient.getAllDatabases()).thenReturn(Arrays.asList(new String[]{TEST_DB_NAME}));
        String description = "This is a default database";
        when(hiveClient.getDatabase(TEST_DB_NAME)).thenReturn(
                new Database(TEST_DB_NAME, description, "/user/hive/default", null));
        when(hiveClient.getAllTables(TEST_DB_NAME)).thenReturn(Arrays.asList(new String[]{}));

        returnExistingDatabase(TEST_DB_NAME, atlasClient, CLUSTER_NAME);

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(CLUSTER_NAME, hiveClient, atlasClient);
        bridge.importHiveMetadata();

        // verify update is called
        verify(atlasClient).updateEntity(eq("72e06b34-9151-4023-aa9d-b82103a50e76"),
                (Referenceable) argThat(
                        new MatchesReferenceableProperty(HiveMetaStoreBridge.DESCRIPTION_ATTR, description)));
    }

    @Test
    public void testImportThatUpdatesRegisteredTable() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);

        setupTable(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME);

        returnExistingDatabase(TEST_DB_NAME, atlasClient, CLUSTER_NAME);

        // return existing table
        when(atlasClient.searchByDSL(HiveMetaStoreBridge.getTableDSLQuery(CLUSTER_NAME, TEST_DB_NAME, TEST_TABLE_NAME,
                HiveDataTypes.HIVE_TABLE.getName(), false))).thenReturn(
                getEntityReference("82e06b34-9151-4023-aa9d-b82103a50e77"));
        when(atlasClient.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77")).thenReturn(createTableReference());

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(CLUSTER_NAME, hiveClient, atlasClient);
        bridge.importHiveMetadata();

        // verify update is called on table
        verify(atlasClient).updateEntity(eq("82e06b34-9151-4023-aa9d-b82103a50e77"),
                (Referenceable) argThat(new MatchesReferenceableProperty(HiveDataModelGenerator.TABLE_TYPE_ATTR,
                        TableType.EXTERNAL_TABLE.name())));
    }

    private void returnExistingDatabase(String databaseName, AtlasClient atlasClient, String clusterName)
            throws AtlasServiceException, JSONException {
        when(atlasClient.searchByDSL(HiveMetaStoreBridge.getDatabaseDSLQuery(clusterName, databaseName,
                HiveDataTypes.HIVE_DB.getName()))).thenReturn(
                getEntityReference("72e06b34-9151-4023-aa9d-b82103a50e76"));
    }

    private Table setupTable(Hive hiveClient, String databaseName, String tableName) throws HiveException {
        when(hiveClient.getAllTables(databaseName)).thenReturn(Arrays.asList(new String[]{tableName}));
        Table testTable = createTestTable(databaseName, tableName);
        when(hiveClient.getTable(databaseName, tableName)).thenReturn(testTable);
        return testTable;
    }

    private void setupDB(Hive hiveClient, String databaseName) throws HiveException {
        when(hiveClient.getAllDatabases()).thenReturn(Arrays.asList(new String[]{databaseName}));
        when(hiveClient.getDatabase(databaseName)).thenReturn(
                new Database(databaseName, "Default database", "/user/hive/default", null));
    }

    @Test
    public void testImportWhenPartitionKeysAreNull() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);
        Table hiveTable = setupTable(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME);

        returnExistingDatabase(TEST_DB_NAME, atlasClient, CLUSTER_NAME);

        when(atlasClient.searchByDSL(HiveMetaStoreBridge.getTableDSLQuery(CLUSTER_NAME, TEST_DB_NAME,
            TEST_TABLE_NAME,
            HiveDataTypes.HIVE_TABLE.getName(), false))).thenReturn(
            getEntityReference("82e06b34-9151-4023-aa9d-b82103a50e77"));
        when(atlasClient.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77")).thenReturn(createTableReference());

        Partition partition = mock(Partition.class);
        when(partition.getTable()).thenReturn(hiveTable);
        List partitionValues = Arrays.asList(new String[]{});
        when(partition.getValues()).thenReturn(partitionValues);

        when(hiveClient.getPartitions(hiveTable)).thenReturn(Arrays.asList(new Partition[]{partition}));

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(CLUSTER_NAME, hiveClient, atlasClient);
        try {
            bridge.importHiveMetadata();
        } catch (Exception e) {
            Assert.fail("Partition with null key caused import to fail with exception ", e);
        }
    }

    private JSONArray getEntityReference(String id) throws JSONException {
        return new JSONArray(String.format("[{\"$id$\":{\"id\":\"%s\"}}]", id));
    }

    private Referenceable createTableReference() {
        Referenceable tableReference = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        Referenceable sdReference = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());
        tableReference.set(HiveDataModelGenerator.STORAGE_DESC, sdReference);
        return tableReference;
    }

    private Table createTestTable(String databaseName, String tableName) throws HiveException {
        Table table = new Table(databaseName, tableName);
        table.setInputFormatClass(TextInputFormat.class);
        table.setTableType(TableType.EXTERNAL_TABLE);
        return table;
    }

    private class MatchesReferenceableProperty extends ArgumentMatcher<Object> {
        private final String attrName;
        private final Object attrValue;

        public MatchesReferenceableProperty(String attrName, Object attrValue) {
            this.attrName = attrName;
            this.attrValue = attrValue;
        }

        @Override
        public boolean matches(Object o) {
            return attrValue.equals(((Referenceable) o).get(attrName));
        }
    }


}
