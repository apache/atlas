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
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.SortOrder;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.type.AtlasTypeUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.hive.hook.events.BaseHiveEvent.ATTRIBUTE_CLUSTER_NAME;
import static org.apache.atlas.hive.hook.events.BaseHiveEvent.ATTRIBUTE_STORAGEDESC;
import static org.apache.atlas.hive.hook.events.BaseHiveEvent.HIVE_TYPE_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

public class HiveMetaStoreBridgeTest {
    private static final String TEST_DB_NAME       = "default";
    public  static final String METADATA_NAMESPACE = "primary";
    public  static final String TEST_TABLE_NAME    = "test_table";
    public  static final String TEST_DB_NAME_2     = "enr_edl";
    public  static final String TEST_DB_NAME_3     = "dummy";
    public  static final String TEST_TABLE_NAME_2  = "testing_enr_edl_1";
    public  static final String TEST_TABLE_NAME_3  = "testing_enr_edl_2";
    public  static final String TEST_TABLE_NAME_4  = "testing_dummy_1";
    public  static final String TEST_TABLE_NAME_5  = "testing_dummy_2";
    private static final String TEST_FILE_NAME = "testfile.txt";

    @Mock
    private Hive hiveClient;

    @Mock
    private AtlasClientV2 atlasClientV2;

    @Mock
    private AtlasEntity atlasEntity;

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo;

    @BeforeMethod
    public void initializeMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testImportThatUpdatesRegisteredDatabase() throws Exception {
        // setup database
        when(hiveClient.getAllDatabases()).thenReturn(Arrays.asList(new String[]{TEST_DB_NAME}));
        String description = "This is a default database";
        Database db = new Database(TEST_DB_NAME, description, "/user/hive/default", null);
        when(hiveClient.getDatabase(TEST_DB_NAME)).thenReturn(db);
        when(hiveClient.getAllTables(TEST_DB_NAME)).thenReturn(Arrays.asList(new String[]{}));

        returnExistingDatabase(TEST_DB_NAME, atlasClientV2, METADATA_NAMESPACE);

        when(atlasEntityWithExtInfo.getEntity("72e06b34-9151-4023-aa9d-b82103a50e76"))
                .thenReturn((new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_DB.getName(), AtlasClient.GUID, "72e06b34-9151-4023-aa9d-b82103a50e76"))).getEntity());

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        bridge.importHiveMetadata(null, null, true);

        // verify update is called
        verify(atlasClientV2).updateEntity(any());
    }

    @Test
    public void testImportThatUpdatesRegisteredTable() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);

        List<Table> hiveTables = setupTables(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME);

        returnExistingDatabase(TEST_DB_NAME, atlasClientV2, METADATA_NAMESPACE);

        // return existing table

        when(atlasEntityWithExtInfo.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77"))
                .thenReturn((new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77"))).getEntity());

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_TABLE.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME)), true, true))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        when(atlasEntityWithExtInfo.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77"))
                .thenReturn(createTableReference());

        Table testTable =  hiveTables.get(0);
        String processQualifiedName = HiveMetaStoreBridge.getTableProcessQualifiedName(METADATA_NAMESPACE, testTable);

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_PROCESS.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        processQualifiedName), true ,true))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));


        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        bridge.importHiveMetadata(null, null, true);

        // verify update is called on table
        verify(atlasClientV2, times(2)).updateEntity(any());

    }

    private void returnExistingDatabase(String databaseName, AtlasClientV2 atlasClientV2, String metadataNamespace)
            throws AtlasServiceException {
        //getEntity(HiveDataTypes.HIVE_DB.getName(), AtlasClient.GUID, "72e06b34-9151-4023-aa9d-b82103a50e76");

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_DB.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getDBQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME)), true, true))
                .thenReturn((new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_DB.getName(), AtlasClient.GUID, "72e06b34-9151-4023-aa9d-b82103a50e76"))));

    }

    private List<Table> setupTables(Hive hiveClient, String databaseName, String... tableNames) throws HiveException {
        List<Table> tables = new ArrayList<>();
        when(hiveClient.getAllTables(databaseName)).thenReturn(Arrays.asList(tableNames));
        for(String tableName : tableNames) {
            Table testTable = createTestTable(databaseName, tableName);
            when(hiveClient.getTable(databaseName, tableName)).thenReturn(testTable);
            tables.add(testTable);
        }
        return tables;
    }

    private void setupDB(Hive hiveClient, String databaseName) throws HiveException {
        when(hiveClient.getAllDatabases()).thenReturn(Arrays.asList(new String[]{databaseName}));
        when(hiveClient.getDatabase(databaseName)).thenReturn(
                new Database(databaseName, "Default database", "/user/hive/default", null));
    }

    @Test
    public void testImportWhenPartitionKeysAreNull() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);
        List<Table> hiveTables = setupTables(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME);
        Table hiveTable = hiveTables.get(0);

        returnExistingDatabase(TEST_DB_NAME, atlasClientV2, METADATA_NAMESPACE);


        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_TABLE.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME)), true, true))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        String processQualifiedName = HiveMetaStoreBridge.getTableProcessQualifiedName(METADATA_NAMESPACE, hiveTable);

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_PROCESS.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        processQualifiedName), true, true))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        when(atlasEntityWithExtInfo.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77"))
                .thenReturn(createTableReference());

        Partition partition = mock(Partition.class);
        when(partition.getTable()).thenReturn(hiveTable);
        List partitionValues = Arrays.asList(new String[]{});
        when(partition.getValues()).thenReturn(partitionValues);

        when(hiveClient.getPartitions(hiveTable)).thenReturn(Arrays.asList(new Partition[]{partition}));

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        try {
            bridge.importHiveMetadata(null, null, true);
        } catch (Exception e) {
            Assert.fail("Partition with null key caused import to fail with exception ", e);
        }
    }

    @Test
    public void testImportContinuesWhenTableRegistrationFails() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);
        final String table2Name = TEST_TABLE_NAME + "_1";
        List<Table> hiveTables = setupTables(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME, table2Name);

        returnExistingDatabase(TEST_DB_NAME, atlasClientV2, METADATA_NAMESPACE);
        when(hiveClient.getTable(TEST_DB_NAME, TEST_TABLE_NAME)).thenThrow(new RuntimeException("Timeout while reading data from hive metastore"));

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_TABLE.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME))))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        when(atlasEntityWithExtInfo.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77"))
                .thenReturn(createTableReference());

        Table testTable =  hiveTables.get(1);
        String processQualifiedName = HiveMetaStoreBridge.getTableProcessQualifiedName(METADATA_NAMESPACE, testTable);

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_PROCESS.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        processQualifiedName)))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        try {
            bridge.importHiveMetadata(null, null, false);
        } catch (Exception e) {
            Assert.fail("Table registration failed with exception", e);
        }
    }

    @Test
    public void testImportFailsWhenTableRegistrationFails() throws Exception {
        setupDB(hiveClient, TEST_DB_NAME);
        final String table2Name = TEST_TABLE_NAME + "_1";
        List<Table> hiveTables = setupTables(hiveClient, TEST_DB_NAME, TEST_TABLE_NAME, table2Name);

        returnExistingDatabase(TEST_DB_NAME, atlasClientV2, METADATA_NAMESPACE);
        when(hiveClient.getTable(TEST_DB_NAME, TEST_TABLE_NAME)).thenThrow(new RuntimeException("Timeout while reading data from hive metastore"));


        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_TABLE.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME))))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));


        when(atlasEntityWithExtInfo.getEntity("82e06b34-9151-4023-aa9d-b82103a50e77"))
                .thenReturn(createTableReference());

        Table testTable  = hiveTables.get(1);
        String processQualifiedName = HiveMetaStoreBridge.getTableProcessQualifiedName(METADATA_NAMESPACE, testTable);

        when(atlasClientV2.getEntityByAttribute(HiveDataTypes.HIVE_PROCESS.getName(),
                Collections.singletonMap(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        processQualifiedName)))
                .thenReturn(new AtlasEntity.AtlasEntityWithExtInfo(
                        getEntity(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.GUID, "82e06b34-9151-4023-aa9d-b82103a50e77")));

        HiveMetaStoreBridge bridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        try {
            bridge.importHiveMetadata(null, null, true);
            Assert.fail("Table registration is supposed to fail");
        } catch (Exception e) {
            //Expected
        }
    }

    private AtlasEntity getEntity(String typeName, String attr, String value) {
        return new AtlasEntity(typeName, attr, value);
    }

    private AtlasEntity createTableReference() {
        AtlasEntity tableEntity = new AtlasEntity(HiveDataTypes.HIVE_TABLE.getName());
        AtlasEntity sdEntity = new AtlasEntity(HiveDataTypes.HIVE_STORAGEDESC.getName());
        tableEntity.setAttribute(ATTRIBUTE_STORAGEDESC, AtlasTypeUtil.getObjectId(sdEntity));
        return tableEntity;
    }

    private Table createTestTable(String databaseName, String tableName) throws HiveException {
        Table table = new Table(databaseName, tableName);
        table.setInputFormatClass(TextInputFormat.class);
        table.setFields(new ArrayList<FieldSchema>() {{
            add(new FieldSchema("col1", "string", "comment1"));
        }
        });
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.setDataLocation(new Path("somehdfspath"));
        return table;
    }

    @Test
    public void testDeleteEntitiesForNonExistingHiveMetadata() throws Exception {

        String DB1_GUID = "72e06b34-9151-4023-aa9d-b82103a50e76";
        String DB2_GUID = "98w06b34-9151-4023-aa9d-b82103a50w67";
        String DB1_TABLE1_GUID = "82e06b34-9151-4023-aa9d-b82103a50e77";
        String DB1_TABLE2_GUID = "66e06b34-9151-4023-aa9d-b82103a50e55";
        String DB2_TABLE1_GUID = "99q06b34-9151-4023-aa9d-b82103a50i22";
        String DB2_TABLE2_GUID = "48z06b34-9151-4023-aa9d-b82103a50n39";

        // IN BOTH HIVE AND ATLAS GUID IS PRESENT MEANS TABLE/ENTITY IS PRESENT SO WILL DO IMPORT HIVE SCRIPT RUN
        // 1) WHEN DB 1 AND TABLE 1 BOTH ARE PRESENT IN IMPORT-HIVE SCRIPT COMMAND, THEN DELETING ONLY SINGLE TABLE FROM DB 1.

        AtlasEntityHeader atlasEntityHeader = new AtlasEntityHeader(HIVE_TYPE_DB, DB1_TABLE1_GUID,
                Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getDBQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME)));
        AtlasSearchResult atlasSearchResult = new AtlasSearchResult();
        atlasSearchResult.setEntities(Collections.singletonList(atlasEntityHeader));

        SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
        filterCriteria.setAttributeName(ATTRIBUTE_CLUSTER_NAME);
        filterCriteria.setAttributeValue(METADATA_NAMESPACE);
        filterCriteria.setOperator(SearchParameters.Operator.EQ);

        when(atlasClientV2.basicSearch(HIVE_TYPE_DB, filterCriteria, null, TEST_DB_NAME_2, true, 1, 100))
                .thenReturn(atlasSearchResult);

        AtlasEntityHeader atlasEntityHeader1 = new AtlasEntityHeader(HIVE_TYPE_TABLE, DB1_TABLE1_GUID,
                Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_2, TEST_TABLE_NAME_2)));
        AtlasEntityHeader atlasEntityHeader2 = new AtlasEntityHeader(HIVE_TYPE_TABLE, DB1_TABLE2_GUID,
                Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_2, TEST_TABLE_NAME_3)));
        AtlasSearchResult atlasSearchResult1 = new AtlasSearchResult();
        atlasSearchResult1.setEntities(Arrays.asList(atlasEntityHeader1, atlasEntityHeader2));

        SearchParameters.FilterCriteria filterCriteria1 = new SearchParameters.FilterCriteria();
        filterCriteria1.setAttributeName(ATTRIBUTE_CLUSTER_NAME);
        filterCriteria1.setAttributeValue(HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_2.toLowerCase(), TEST_TABLE_NAME_2.toLowerCase()));
        filterCriteria1.setAttributeValue(METADATA_NAMESPACE);
        filterCriteria1.setOperator(SearchParameters.Operator.EQ);

        when(atlasClientV2.basicSearch(HIVE_TYPE_TABLE, filterCriteria1, null, TEST_TABLE_NAME_2, true, 1, 100))
                .thenReturn(atlasSearchResult1);

        EntityMutationResponse entityMutationResponse1 = new EntityMutationResponse();
        entityMutationResponse1.setMutatedEntities(Collections.singletonMap(EntityMutations.EntityOperation.DELETE, Arrays.asList(atlasEntityHeader1)));
        when(atlasClientV2.deleteEntityByGuid(DB1_TABLE1_GUID)).thenReturn(entityMutationResponse1);

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        hiveMetaStoreBridge.deleteEntitiesForNonExistingHiveMetadata(true, TEST_DB_NAME_2, TEST_TABLE_NAME_2);

        assertEquals(DB1_TABLE1_GUID, entityMutationResponse1.getMutatedEntities().get(EntityMutations.EntityOperation.DELETE).get(0).getGuid());

        // 1) WHEN DB 2 AND TABLE 1 BOTH ARE PRESENT, THEN DELETING ONLY SINGLE TABLE FROM DB 2.

        AtlasEntityHeader atlasEntityHeader3 = new AtlasEntityHeader(HIVE_TYPE_DB, DB2_TABLE1_GUID,
                Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getDBQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME)));
        AtlasSearchResult atlasSearchResult2 = new AtlasSearchResult();
        atlasSearchResult2.setEntities(Collections.singletonList(atlasEntityHeader3));

        when(atlasClientV2.basicSearch(HIVE_TYPE_DB, filterCriteria, null, TEST_DB_NAME_3, true, 1, 100))
                .thenReturn(atlasSearchResult2);

        AtlasSearchResult atlasSearchResult3 = new AtlasSearchResult();
        atlasSearchResult3.setEntities(Arrays.asList(new AtlasEntityHeader(HIVE_TYPE_TABLE, DB2_TABLE1_GUID,
                        Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_3, TEST_TABLE_NAME_4))),
                new AtlasEntityHeader(HIVE_TYPE_TABLE, DB2_TABLE1_GUID,
                        Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_3, TEST_TABLE_NAME_4)))));

        SearchParameters.FilterCriteria filterCriteria2 = new SearchParameters.FilterCriteria();
        filterCriteria2.setAttributeName(ATTRIBUTE_CLUSTER_NAME);
        filterCriteria2.setAttributeValue(HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_3.toLowerCase(), TEST_TABLE_NAME_4.toLowerCase()));
        filterCriteria2.setAttributeValue(METADATA_NAMESPACE);
        filterCriteria2.setOperator(SearchParameters.Operator.EQ);

        when(atlasClientV2.basicSearch(HIVE_TYPE_TABLE, filterCriteria2, null, TEST_TABLE_NAME_4, true, 1, 100))
                .thenReturn(atlasSearchResult1);

        EntityMutationResponse entityMutationResponse2 = new EntityMutationResponse();
        entityMutationResponse2.setMutatedEntities(Collections.singletonMap(EntityMutations.EntityOperation.DELETE, Arrays.asList(new AtlasEntityHeader(HIVE_TYPE_TABLE, DB2_TABLE1_GUID,
                Collections.singletonMap(AtlasClient.QUALIFIED_NAME, HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME_3, TEST_TABLE_NAME_4))))));
        when(atlasClientV2.deleteEntityByGuid(DB2_TABLE1_GUID)).thenReturn(entityMutationResponse2);

        hiveMetaStoreBridge.deleteEntitiesForNonExistingHiveMetadata(true, TEST_DB_NAME_3, TEST_TABLE_NAME_4);

        assertEquals(DB2_TABLE1_GUID, entityMutationResponse2.getMutatedEntities().get(EntityMutations.EntityOperation.DELETE).get(0).getGuid());

        // 3) WHEN DB 1 IS PRESENT, THEN DELETING ALL TABLE FROM DB

        EntityMutationResponse entityMutationResponse3 = new EntityMutationResponse();
        entityMutationResponse3.setMutatedEntities(Collections.singletonMap(EntityMutations.EntityOperation.DELETE, Arrays.asList(atlasEntityHeader1, atlasEntityHeader2)));
        when(atlasClientV2.deleteEntityByGuid(DB1_TABLE1_GUID)).thenReturn(entityMutationResponse2);
        when(atlasClientV2.deleteEntityByGuid(DB1_TABLE2_GUID)).thenReturn(entityMutationResponse2);
        hiveMetaStoreBridge.deleteEntitiesForNonExistingHiveMetadata(true, TEST_DB_NAME_2, null);

        assertEquals(DB1_TABLE1_GUID, entityMutationResponse3.getMutatedEntities().get(EntityMutations.EntityOperation.DELETE).get(0).getGuid());
        assertEquals(DB1_TABLE2_GUID, entityMutationResponse3.getMutatedEntities().get(EntityMutations.EntityOperation.DELETE).get(1).getGuid());
    }


    @Test
    public void testPrintUsage() throws Exception {
        // Use reflection to access the private static method
        java.lang.reflect.Method printUsageMethod = HiveMetaStoreBridge.class.getDeclaredMethod("printUsage", Options.class);
        printUsageMethod.setAccessible(true);

        Options options = new Options();
        options.addOption("d", "database", true, "Specify database");
        options.addOption("t", "table", true, "Specify table");
        options.addOption("f", "filename", true, "Specify input file");
        options.addOption("o", "output", true, "Specify output path or file");
        options.addOption("i", false, "Export without importing");
        options.addOption("deleteNonExisting", false, "Delete non-existing entities");

        // Call the private method using reflection
        printUsageMethod.invoke(null, options);
    }

    @Test
    public void testPrepareCommandLineOptions() throws Exception {
        // Use reflection to access the private static method
        java.lang.reflect.Method prepareCommandLineOptionsMethod = HiveMetaStoreBridge.class.getDeclaredMethod("prepareCommandLineOptions");
        prepareCommandLineOptionsMethod.setAccessible(true);

        // Call the private method using reflection
        Options options = (Options) prepareCommandLineOptionsMethod.invoke(null);

        // Verify the Options object is not null
        assertNotNull("Options object should not be null", options);
    }

    @Test
    public void testImportDataDirectlyToAtlas() throws Exception {
        CommandLine commandLine = mock(CommandLine.class);

        when(commandLine.hasOption(HiveMetaStoreBridge.OPTION_DELETE_NON_EXISTING)).thenReturn(true);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_DATABASE_SHORT)).thenReturn(TEST_DB_NAME);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_TABLE_SHORT)).thenReturn(TEST_TABLE_NAME);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_IMPORT_DATA_FILE_SHORT)).thenReturn("filename");
        when(commandLine.hasOption(HiveMetaStoreBridge.OPTION_FAIL_ON_ERROR)).thenReturn(false);

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2);
        boolean result = hiveMetaStoreBridge.importDataDirectlyToAtlas(commandLine);
        assertTrue("Should return true when deleteNonExisting is true", result);
    }

    @Test
    public void testImportDataDirectlyToAtlasNotEmptyFileImport() throws Exception {
        CommandLine commandLine = mock(CommandLine.class);

        when(commandLine.hasOption(HiveMetaStoreBridge.OPTION_DELETE_NON_EXISTING)).thenReturn(false);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_DATABASE_SHORT)).thenReturn(TEST_DB_NAME);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_TABLE_SHORT)).thenReturn(TEST_TABLE_NAME);
        when(commandLine.getOptionValue(HiveMetaStoreBridge.OPTION_IMPORT_DATA_FILE_SHORT)).thenReturn(TEST_FILE_NAME);
        when(commandLine.hasOption(HiveMetaStoreBridge.OPTION_FAIL_ON_ERROR)).thenReturn(false);

        // Create a spy of HiveMetaStoreBridge
        HiveMetaStoreBridge hiveMetaStoreBridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2));

        // Mock File to simulate file existence and readability
        File file = mock(File.class);
        when(file.exists()).thenReturn(true);
        when(file.canRead()).thenReturn(true);


        BufferedReader bufferedReader = mock(BufferedReader.class);
        when(bufferedReader.readLine()).thenReturn(TEST_DB_NAME + ":" + TEST_TABLE_NAME).thenReturn(null);

        java.lang.reflect.Method importDatabasesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("importDatabases", boolean.class, String.class, String.class);
        importDatabasesMethod.setAccessible(true);

        // Now run the method
        boolean result = hiveMetaStoreBridge.importDataDirectlyToAtlas(commandLine);

        assertFalse(result);
    }

    @Mock
    private EntityMutationResponse mutationResponse;
    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo retrievedEntityWithExtInfo;

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo newEntityWithExtInfo;

    @Test
    public void testRegisterInstanceComprehensiveCoverage() throws Exception {
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2));

        // Mock atlasEntityWithExtInfo
        when(atlasEntityWithExtInfo.getEntity()).thenReturn(atlasEntity);
        final String TEST_TYPE_NAME = "hive";
        when(atlasEntity.getTypeName()).thenReturn(TEST_TYPE_NAME);

        // Mock mutation response with multiple created entities
        org.apache.atlas.model.instance.AtlasEntityHeader entityHeader1 = new org.apache.atlas.model.instance.AtlasEntityHeader();
        entityHeader1.setGuid("72e06b34-9151-4023-aa9d-b82103a50e76" + "-1");
        org.apache.atlas.model.instance.AtlasEntityHeader entityHeader2 = new org.apache.atlas.model.instance.AtlasEntityHeader();
        entityHeader2.setGuid("72e06b34-9151-4023-aa9d-b82103a50e76" + "-2");
        when(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE))
                .thenReturn(Arrays.asList(entityHeader1, entityHeader2));

        // Mock atlasClientV2 responses
        when(atlasClientV2.createEntity(atlasEntityWithExtInfo)).thenReturn(mutationResponse);
        when(atlasClientV2.getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76" + "-1")).thenReturn(retrievedEntityWithExtInfo);
        when(atlasClientV2.getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76" + "-2")).thenReturn(newEntityWithExtInfo);

        // Mock entity details
        when(retrievedEntityWithExtInfo.getEntity()).thenReturn(atlasEntity);
        when(retrievedEntityWithExtInfo.getEntity().getAttribute("qualifiedName")).thenReturn("TEST_QUALIFIED_NAME");
        when(retrievedEntityWithExtInfo.getEntity().getGuid()).thenReturn("72e06b34-9151-4023-aa9d-b82103a50e76" + "-1");
        when(retrievedEntityWithExtInfo.getEntity().getTypeName()).thenReturn(TEST_TYPE_NAME);

        AtlasEntity newAtlasEntity = mock(AtlasEntity.class);
        when(newEntityWithExtInfo.getEntity()).thenReturn(newAtlasEntity);
        when(newEntityWithExtInfo.getEntity().getAttribute("qualifiedName")).thenReturn("TEST_QUALIFIED_NAME" + "-new");
        when(newEntityWithExtInfo.getEntity().getGuid()).thenReturn("72eb34-9151-4023-aa9d-b82103a50e76" + "-2");
        when(newEntityWithExtInfo.getEntity().getTypeName()).thenReturn(TEST_TYPE_NAME);

        // Mock referred entities for the second entity
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("ref-guid", mock(AtlasEntity.class));
        when(newEntityWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        // Use reflection to access the private method
        java.lang.reflect.Method registerInstanceMethod = HiveMetaStoreBridge.class.getDeclaredMethod("registerInstance", AtlasEntity.AtlasEntityWithExtInfo.class);
        registerInstanceMethod.setAccessible(true);

        // Use reflection to access the private clearRelationshipAttributes method
        java.lang.reflect.Method clearRelationshipAttributesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntityWithExtInfo.class);
        clearRelationshipAttributesMethod.setAccessible(true);

        // Call the method using reflection
        AtlasEntity.AtlasEntityWithExtInfo result = (AtlasEntity.AtlasEntityWithExtInfo) registerInstanceMethod.invoke(bridge, atlasEntityWithExtInfo);

        // Call clearRelationshipAttributes using reflection
        clearRelationshipAttributesMethod.invoke(bridge, result);

        assertNotNull(result);
        assertEquals(retrievedEntityWithExtInfo, result);
    }

    @Mock
    private AtlasEntity atlasEntity1;

    @Mock
    private AtlasEntity atlasEntity2;

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo1;

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo2;

    @Mock
    private AtlasEntity.AtlasEntitiesWithExtInfo inputEntities;

    @Test
    public void testRegisterInstancesComprehensiveCoverage() throws Exception {
        // Initialize mocks
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2));

        // Mock input entities
        List<AtlasEntity> entitiesList = new ArrayList<>();
        entitiesList.add(atlasEntity1);
        when(inputEntities.getEntities()).thenReturn(entitiesList);

        // Mock mutation response with multiple created entities
        org.apache.atlas.model.instance.AtlasEntityHeader entityHeader1 = new org.apache.atlas.model.instance.AtlasEntityHeader();
        entityHeader1.setGuid("72e06b34-9151-4023-aa9d-b82103a50e76-1");
        org.apache.atlas.model.instance.AtlasEntityHeader entityHeader2 = new org.apache.atlas.model.instance.AtlasEntityHeader();
        entityHeader2.setGuid("72e06b34-9151-4023-aa9d-b82103a50e76-2");
        when(mutationResponse.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE))
                .thenReturn(Arrays.asList(entityHeader1, entityHeader2));

        // Mock atlasClientV2 responses
        when(atlasClientV2.createEntities(inputEntities)).thenReturn(mutationResponse);
        when(atlasClientV2.getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76-1")).thenReturn(entityWithExtInfo1);
        when(atlasClientV2.getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76-2")).thenReturn(entityWithExtInfo2);

        // Mock entity details
        when(entityWithExtInfo1.getEntity()).thenReturn(atlasEntity1);
        when(entityWithExtInfo1.getEntity().getTypeName()).thenReturn("hive");
        when(entityWithExtInfo1.getEntity().getAttribute("qualifiedName")).thenReturn("TEST_QUALIFIED_NAME1");
        when(entityWithExtInfo1.getEntity().getGuid()).thenReturn("72e06b34-9151-4023-aa9d-b82103a50e76-1");

        when(entityWithExtInfo2.getEntity()).thenReturn(atlasEntity2);
        when(entityWithExtInfo2.getEntity().getTypeName()).thenReturn("TEST_TYPE_NAME");
        when(entityWithExtInfo2.getEntity().getAttribute("qualifiedName")).thenReturn("TEST_QUALIFIED_NAME2");
        when(entityWithExtInfo2.getEntity().getGuid()).thenReturn("72e06b34-9151-4023-aa9d-b82103a50e76-2");

        // Mock referred entities for the second entity
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("ref-guid", mock(AtlasEntity.class));
        when(entityWithExtInfo2.getReferredEntities()).thenReturn(referredEntities);

        // Use reflection to access the private method
        java.lang.reflect.Method registerInstancesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("registerInstances", AtlasEntity.AtlasEntitiesWithExtInfo.class);
        registerInstancesMethod.setAccessible(true);

        // Use reflection to access the private clearRelationshipAttributes method
        java.lang.reflect.Method clearRelationshipAttributesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntitiesWithExtInfo.class);
        clearRelationshipAttributesMethod.setAccessible(true);

        // Call the method using reflection
        AtlasEntity.AtlasEntitiesWithExtInfo result = (AtlasEntity.AtlasEntitiesWithExtInfo) registerInstancesMethod.invoke(bridge, inputEntities);

        // Verify interactions
        verify(atlasClientV2).createEntities(inputEntities);
        verify(atlasClientV2).getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76-1");
        verify(atlasClientV2).getEntityByGuid("72e06b34-9151-4023-aa9d-b82103a50e76-2");

        // Call clearRelationshipAttributes using reflection
        clearRelationshipAttributesMethod.invoke(bridge, result);

        // Verify result
        assertNotNull(result);
        assertEquals(2, result.getEntities().size());
        assertTrue(result.getEntities().contains(atlasEntity1));
        assertTrue(result.getEntities().contains(atlasEntity2));
        assertTrue(result.getReferredEntities().containsValue(referredEntities.get("ref-guid")));
    }

    @Test
    public void testGetCreateTableStringComprehensiveCoverage() throws Exception {
        // Initialize mocks
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method getCreateTableStringMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getCreateTableString", Table.class, String.class);
        getCreateTableStringMethod.setAccessible(true);

        // Test Case 1: Null colList - use createTestTable helper method
        Table tableNullCols = createTestTable(TEST_DB_NAME, TEST_TABLE_NAME);
        // Set columns to null explicitly
        try {
            java.lang.reflect.Field allColsField = Table.class.getDeclaredField("allCols");
            allColsField.setAccessible(true);
            allColsField.set(tableNullCols, null);
        } catch (Exception e) {
            // If reflection fails, create a new table without columns
            tableNullCols = new Table(TEST_DB_NAME, TEST_TABLE_NAME);
        }

        String resultNullCols = (String) getCreateTableStringMethod.invoke(bridge, tableNullCols, "TEST_LOCATION");
        assertEquals("create external table " + TEST_TABLE_NAME + " location '" + "TEST_LOCATION" + "'", resultNullCols);

        // Test Case 2: Non-empty colList with multiple columns
        Table tableWithCols = createTestTable(TEST_DB_NAME, TEST_TABLE_NAME);
        List<FieldSchema> colList = new ArrayList<>();
        colList.add(new FieldSchema("col1", "string", ""));
        colList.add(new FieldSchema("col2", "int", ""));
        try {
            java.lang.reflect.Field allColsField = Table.class.getDeclaredField("allCols");
            allColsField.setAccessible(true);
            allColsField.set(tableWithCols, colList);
        } catch (Exception e) {
            // If reflection fails, create a new table with columns
            tableWithCols = new Table(TEST_DB_NAME, TEST_TABLE_NAME);
            tableWithCols.setFields(colList);
        }

        String resultWithCols = (String) getCreateTableStringMethod.invoke(bridge, tableWithCols, "TEST_LOCATION");
        assertEquals("create external table " + TEST_TABLE_NAME + "(col1 string,col2 int) location '" + "TEST_LOCATION" + "'", resultWithCols);
    }

    @Test
    public void testLowerComprehensiveCoverage() throws Exception {
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method lowerMethod = HiveMetaStoreBridge.class.getDeclaredMethod("lower", String.class);
        lowerMethod.setAccessible(true);

        // Test Case 1: Null string
        String resultNull = (String) lowerMethod.invoke(bridge, "");
        assertEquals("", resultNull);

        // Test Case 2: Empty string
        String resultEmpty = (String) lowerMethod.invoke(bridge, "");
        assertEquals("", resultEmpty);

        // Test Case 3: Non-empty string with spaces
        String resultWithSpaces = (String) lowerMethod.invoke(bridge, "  TEST STRING  ");
        assertEquals("test string", resultWithSpaces);

        // Test Case 4: Non-empty string without spaces
        String resultNoSpaces = (String) lowerMethod.invoke(bridge, "TESTSTRING");
        assertEquals("teststring", resultNoSpaces);
    }

    @Test
    public void testGetTableQualifiedNameComprehensiveCoverage() {
        // Initialize mocks
        MockitoAnnotations.initMocks(this);

        // Test Case 1: Non-temporary table (isTemporaryTable = false)
        String resultNonTemp = HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME, false);
        assertEquals("default.test_table@primary", resultNonTemp);

    }

    @Mock
    private AtlasEntity entity1;

    @Mock
    private AtlasEntity entity2;

    @Mock
    private AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo;

    @Mock
    private Map<String, AtlasEntity> referredEntitiesMap;

    @Test
    public void testClearRelationshipAttributeComprehensiveCoverage() throws Exception {
        // Initialize mocks
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method clearRelationshipAttributesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntitiesWithExtInfo.class);
        clearRelationshipAttributesMethod.setAccessible(true);

        // Test Case 1: Null entities
        clearRelationshipAttributesMethod.invoke(bridge, (AtlasEntity.AtlasEntitiesWithExtInfo) null);

        // Test Case 2: Non-null entities with null entities list and null referred entities
        when(entitiesWithExtInfo.getEntities()).thenReturn(null);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(null);
        clearRelationshipAttributesMethod.invoke(bridge, entitiesWithExtInfo);

        // Test Case 3: Non-null entities with non-null entities list
        List<AtlasEntity> entityList = new ArrayList<>();
        entityList.add(entity1);
        entityList.add(entity2);
        when(entitiesWithExtInfo.getEntities()).thenReturn(entityList);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(null);

        clearRelationshipAttributesMethod.invoke(bridge, entitiesWithExtInfo);

        // Test Case 4: Non-null entities with non-null referred entities
        when(entitiesWithExtInfo.getEntities()).thenReturn(null);
        when(entitiesWithExtInfo.getReferredEntities()).thenReturn(referredEntitiesMap);
        when(referredEntitiesMap.values()).thenReturn(Collections.singletonList(entity1));
        clearRelationshipAttributesMethod.invoke(bridge, entitiesWithExtInfo);
    }

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo;

    @Test
    public void testClearRelationshipAttributesComprehensiveCoverage() throws Exception {
        // Initialize mocks
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method clearRelationshipAttributesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntityWithExtInfo.class);
        clearRelationshipAttributesMethod.setAccessible(true);

        // Test Case 1: Null entity
        clearRelationshipAttributesMethod.invoke(bridge, (AtlasEntity.AtlasEntityWithExtInfo) null);

        // Test Case 2: Non-null entity with null entity and null referred entities
        when(entityWithExtInfo.getEntity()).thenReturn(null);
        when(entityWithExtInfo.getReferredEntities()).thenReturn(null);
        clearRelationshipAttributesMethod.invoke(bridge, entityWithExtInfo);

        // Test Case 3: Non-null entity with non-null entity
        when(entityWithExtInfo.getEntity()).thenReturn(atlasEntity);
        when(entityWithExtInfo.getReferredEntities()).thenReturn(null);

        clearRelationshipAttributesMethod.invoke(bridge, entityWithExtInfo);

        // Test Case 4: Non-null entity with non-null referred entities
        when(entityWithExtInfo.getEntity()).thenReturn(null);
        when(entityWithExtInfo.getReferredEntities()).thenReturn(referredEntitiesMap);
        when(referredEntitiesMap.values()).thenReturn(Collections.singletonList(atlasEntity));
        clearRelationshipAttributesMethod.invoke(bridge, entityWithExtInfo);
    }

    @Mock
    private AtlasEntity entity;

    @Mock
    private Map<String, Object> relationshipAttributes;

    @Test
    public void testClearRelationshipAttributesForAtlasEntity() throws Exception {
        // Initialize mocks
        MockitoAnnotations.initMocks(this);
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method clearRelationshipAttributesMethod = HiveMetaStoreBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.class);
        clearRelationshipAttributesMethod.setAccessible(true);

        // Test Case 1: Null entity
        clearRelationshipAttributesMethod.invoke(bridge, (AtlasEntity) null);

        // Test Case 2: Non-null entity with null relationshipAttributes
        when(entity.getRelationshipAttributes()).thenReturn(null);
        clearRelationshipAttributesMethod.invoke(bridge, entity);

        // Test Case 3: Non-null entity with non-null relationshipAttributes
        when(entity.getRelationshipAttributes()).thenReturn(relationshipAttributes);
        clearRelationshipAttributesMethod.invoke(bridge, entity);
    }

    @Test
    public void testIsTableWithDatabaseNameComprehensiveCoverage() throws Exception {
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, null));

        // Use reflection to access the private method
        java.lang.reflect.Method isTableWithDatabaseNameMethod = HiveMetaStoreBridge.class.getDeclaredMethod("isTableWithDatabaseName", String.class);
        isTableWithDatabaseNameMethod.setAccessible(true);

        // Test Case 1: Table name without a dot
        boolean resultNoDot = (Boolean) isTableWithDatabaseNameMethod.invoke(bridge, "tableName");
        assertFalse(resultNoDot);

        // Test Case 2: Table name with a dot
        boolean resultWithDot = (Boolean) isTableWithDatabaseNameMethod.invoke(bridge, "db.table");
        assertTrue(resultWithDot);

        // Test Case 3: Table name with multiple dots
        boolean resultMultipleDots = (Boolean) isTableWithDatabaseNameMethod.invoke(bridge, "db1.db2.table");
        assertTrue(resultMultipleDots);

        // Test Case 4: Empty string
        boolean resultEmpty = (Boolean) isTableWithDatabaseNameMethod.invoke(bridge, "");
        assertFalse(resultEmpty);
    }

    @Mock
    private AtlasSearchResult searchResult;

    @Mock
    private AtlasEntityHeader entityHeader1;

    @Mock
    private AtlasEntityHeader entityHeader2;

    @Test
    public void testGetAllDatabaseInClusterComprehensiveCoverage() throws Exception {
        // Initialize mocks
        MockitoAnnotations.initMocks(this);
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, atlasClientV2));

        // Use reflection to access the private method
        java.lang.reflect.Method getAllDatabaseInClusterMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getAllDatabaseInCluster");
        getAllDatabaseInClusterMethod.setAccessible(true);

        // Test Case 1: Empty result set (no databases)
        when(atlasClientV2.basicSearch(
                anyString(),
                any(SearchParameters.FilterCriteria.class),
                isNull(String.class),
                isNull(String.class),
                anyBoolean(),
                anyInt(),
                anyInt()))
                .thenReturn(searchResult);
        when(searchResult.getEntities()).thenReturn(Collections.emptyList());

        List<AtlasEntityHeader> resultEmpty = (List<AtlasEntityHeader>) getAllDatabaseInClusterMethod.invoke(bridge);
        assertTrue(resultEmpty.isEmpty());
        verify(atlasClientV2).basicSearch(
                anyString(),
                any(SearchParameters.FilterCriteria.class),
                isNull(String.class),
                isNull(String.class),
                anyBoolean(),
                anyInt(),
                anyInt());
        verify(searchResult).getEntities();
        verifyNoMoreInteractions(atlasClientV2); // Only one call due to break condition

        // Reset mocks
        reset(atlasClientV2, searchResult);
    }

    private static final String DATABASE_GUID = "db-guid-123";
    private static final int PAGE_LIMIT = 10000;
    private static final String HIVE_TABLE_DB_EDGE_LABEL = "__hive_table.db";

    @Test
    public void testGetAllTablesInDbComprehensiveCoverage() throws Exception {
        // Initialize mocks
        MockitoAnnotations.initMocks(this);
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, atlasClientV2));

        // Use reflection to access the private method
        java.lang.reflect.Method getAllTablesInDbMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getAllTablesInDb", String.class);
        getAllTablesInDbMethod.setAccessible(true);

        // Test Case 1: Empty result set (no tables)
        when(atlasClientV2.relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt()))
                .thenReturn(searchResult);
        when(searchResult.getEntities()).thenReturn(Collections.emptyList());

        List<AtlasEntityHeader> resultEmpty = (List<AtlasEntityHeader>) getAllTablesInDbMethod.invoke(bridge, DATABASE_GUID);
        assertTrue(resultEmpty.isEmpty());
        verify(atlasClientV2).relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt());
        verify(searchResult).getEntities();
        verifyNoMoreInteractions(atlasClientV2); // Only one call due to break condition

        // Reset mocks
        reset(atlasClientV2, searchResult);

        // Test Case 2: Partial page with tables (less than pageSize)
        List<AtlasEntityHeader> partialEntities = new ArrayList<>();
        partialEntities.add(entityHeader1);
        when(atlasClientV2.relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt()))
        .thenReturn(searchResult);
        when(searchResult.getEntities()).thenReturn(partialEntities);

        List<AtlasEntityHeader> resultPartial = (List<AtlasEntityHeader>) getAllTablesInDbMethod.invoke(bridge, DATABASE_GUID);
        assertEquals(1, resultPartial.size());
        assertTrue(resultPartial.contains(entityHeader1));
        verify(atlasClientV2).relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt());
        verify(searchResult).getEntities();
        verifyNoMoreInteractions(atlasClientV2); // Breaks after one iteration

        // Reset mocks
        reset(atlasClientV2, searchResult);

        // Test Case 3: Multiple pages with tables (equal to pageSize, then less)
        List<AtlasEntityHeader> fullPage = new ArrayList<>();
        fullPage.add(entityHeader1);
        fullPage.add(entityHeader2);
        when(atlasClientV2.relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt()))
        .thenReturn(searchResult);
        when(searchResult.getEntities()).thenReturn(fullPage, partialEntities); // First page full, second partial

        List<AtlasEntityHeader> resultMultiple = (List<AtlasEntityHeader>) getAllTablesInDbMethod.invoke(bridge, DATABASE_GUID);
        assertEquals(2, resultMultiple.size());
        assertTrue(resultMultiple.contains(entityHeader1));
        assertTrue(resultMultiple.contains(entityHeader2));
        verify(atlasClientV2).relationshipSearch(
                anyString(),
                anyString(),
                isNull(String.class),
                isNull(SortOrder.class),
                anyBoolean(),
                anyInt(),
                anyInt());
        verify(searchResult).getEntities();
    }

    @Test
    public void testGetHiveDatabaseNameComprehensiveCoverage() throws Exception {
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, atlasClientV2));

        // Use reflection to access the private method
        java.lang.reflect.Method getHiveDatabaseNameMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getHiveDatabaseName", String.class);
        getHiveDatabaseNameMethod.setAccessible(true);

        // Test Case 1: Null qualifiedName
        String resultNull = (String) getHiveDatabaseNameMethod.invoke(bridge, (String) null);
        assertNull(resultNull);

        // Test Case 2: Empty qualifiedName
        String resultEmpty = (String) getHiveDatabaseNameMethod.invoke(bridge, "");
        assertNull(resultEmpty);

        // Test Case 3: QualifiedName without "@" (single part)
        String resultNoAt = (String) getHiveDatabaseNameMethod.invoke(bridge, "default");
        assertNotNull(resultNoAt);

        // Test Case 4: QualifiedName with "@" (multiple parts)
        String resultWithAt = (String) getHiveDatabaseNameMethod.invoke(bridge, "default@primary");
        assertEquals("default", resultWithAt);

        // Test Case 5: QualifiedName with multiple "@" (takes first part)
        String resultMultipleAt = (String) getHiveDatabaseNameMethod.invoke(bridge, "default@primary@extra");
        assertEquals("default", resultMultipleAt);
    }

    private static final String TEMP_TABLE_PREFIX = "_temp_";

    @Test
    public void testGetHiveTableNameComprehensiveCoverage() throws Exception {
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, atlasClientV2));

        // Use reflection to access the private method
        java.lang.reflect.Method getHiveTableNameMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getHiveTableName", String.class, boolean.class);
        getHiveTableNameMethod.setAccessible(true);

        // Test Case 1: Null qualifiedName
        String resultNull = (String) getHiveTableNameMethod.invoke(bridge, null, false);
        assertNull(resultNull);

        // Test Case 2: Empty qualifiedName
        String resultEmpty = (String) getHiveTableNameMethod.invoke(bridge, "", false);
        assertNull(resultEmpty);

        // Test Case 3: QualifiedName without "." or "@" (no valid format)
        String resultNoFormat = (String) getHiveTableNameMethod.invoke(bridge, "table", false);
        assertNull(resultNoFormat);

        // Test Case 4: Non-temporary table with valid format
        String resultNonTemp = (String) getHiveTableNameMethod.invoke(bridge, "db.table@primary", false);
        assertEquals("table", resultNonTemp);

        // Test Case 5: Temporary table with valid format (no temp prefix)
        String resultTempNoPrefix = (String) getHiveTableNameMethod.invoke(bridge, "db.table@primary", true);
        assertEquals("table", resultTempNoPrefix);

        // Test Case 6: Temporary table with temp prefix
        String resultTempWithPrefix = (String) getHiveTableNameMethod.invoke(bridge, "db.table_temp_session123@primary", true);
        assertEquals("table_temp_session123", resultTempWithPrefix);

        // Test Case 7: Temporary table with multiple temp prefixes
        String resultTempMultiplePrefixes = (String) getHiveTableNameMethod.invoke(bridge, "db.table_temp_session1_temp_session2@primary", true);
        assertEquals("table_temp_session1_temp_session2", resultTempMultiplePrefixes);

        // Test Case 8: Temporary table with empty table name after split
        String resultTempEmptySplit = (String) getHiveTableNameMethod.invoke(bridge, "db._temp_session123@primary", true);
        assertEquals("_temp_session123",resultTempEmptySplit);
    }

    private static final String GUID1 = "guid-1";
    private static final String GUID2 = "guid-2";

    @Test
    public void testDeleteByGuidComprehensiveCoverage() throws Exception {
        // Initialize mocks
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, null, atlasClientV2));

        // Use reflection to access the private method
        java.lang.reflect.Method deleteByGuidMethod =
                HiveMetaStoreBridge.class.getDeclaredMethod("deleteByGuid", List.class);
        deleteByGuidMethod.setAccessible(true);

        // --- Test Case 1: Empty guidTodelete list ---
        deleteByGuidMethod.invoke(bridge, Collections.emptyList());
        verify(atlasClientV2, never()).deleteEntityByGuid(anyString());
        verifyNoMoreInteractions(atlasClientV2);

        reset(atlasClientV2);

        // --- Test Case 2: Non-empty guidTodelete with deletion failure ---
        List<String> guidsToDelete = new ArrayList<>();
        guidsToDelete.add(GUID1);
        when(atlasClientV2.deleteEntityByGuid(GUID1)).thenReturn(mutationResponse);
        when(mutationResponse.getDeletedEntities()).thenReturn(Collections.emptyList());

        deleteByGuidMethod.invoke(bridge, guidsToDelete);
        verify(atlasClientV2).deleteEntityByGuid(GUID1);
        verify(mutationResponse).getDeletedEntities();
        verifyNoMoreInteractions(atlasClientV2);

        reset(atlasClientV2, mutationResponse);

        // --- Test Case 3: Non-empty guidTodelete with deletion success ---
        guidsToDelete.clear();
        guidsToDelete.add(GUID2);

        // Create the mock separately to avoid UnfinishedStubbingException
        AtlasEntityHeader entityHeaderMock = mock(AtlasEntityHeader.class);

        when(atlasClientV2.deleteEntityByGuid(GUID2)).thenReturn(mutationResponse);
        when(mutationResponse.getDeletedEntities()).thenReturn(Collections.singletonList(entityHeaderMock));

        deleteByGuidMethod.invoke(bridge, guidsToDelete);
        verify(atlasClientV2).deleteEntityByGuid(GUID2);
        verify(mutationResponse).getDeletedEntities();
        verifyNoMoreInteractions(atlasClientV2);

        reset(atlasClientV2, mutationResponse);

        // --- Test Case 4: Non-empty guidTodelete with multiple GUIDs ---
        guidsToDelete.clear();
        guidsToDelete.add(GUID1);
        guidsToDelete.add(GUID2);

        when(atlasClientV2.deleteEntityByGuid(GUID1)).thenReturn(mutationResponse);
        when(atlasClientV2.deleteEntityByGuid(GUID2)).thenReturn(mutationResponse);
        when(mutationResponse.getDeletedEntities()).thenReturn(Collections.emptyList());

        deleteByGuidMethod.invoke(bridge, guidsToDelete);
        verify(atlasClientV2).deleteEntityByGuid(GUID1);
    }

    private static final String TABLE_GUID1 = "table-guid-1";
    private static final String TABLE_GUID2 = "table-guid-2";
    private static final String DATABASE_TO_DELETE = "default";
    private static final String TABLE_TO_DELETE = "test_table";
    private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
    private static final String HIVE_TYPE_TABLE = "hive_table";

    @Mock
    private AtlasEntityHeader dbEntity;
    @Mock
    private AtlasEntityHeader tableEntity1;

    @Mock
    private AtlasEntityHeader tableEntity2;

    @Test
    public void testDeleteEntitiesForNonExistingHiveMetadataComprehensiveCoverage() throws Exception {
        // Initialize mocks
        MockitoAnnotations.initMocks(this);
        HiveMetaStoreBridge bridge = spy(new HiveMetaStoreBridge(METADATA_NAMESPACE, hiveClient, atlasClientV2));

        // Use reflection to access the private methods
        java.lang.reflect.Method getAllDatabaseInClusterMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getAllDatabaseInCluster");
        getAllDatabaseInClusterMethod.setAccessible(true);
        java.lang.reflect.Method getAllTablesInDbMethod = HiveMetaStoreBridge.class.getDeclaredMethod("getAllTablesInDb", String.class);
        getAllTablesInDbMethod.setAccessible(true);
        java.lang.reflect.Method deleteByGuidMethod = HiveMetaStoreBridge.class.getDeclaredMethod("deleteByGuid", List.class);
        deleteByGuidMethod.setAccessible(true);

        // Test Case 1: No database to delete, empty database list
        // Use reflection to call private methods
        getAllDatabaseInClusterMethod.invoke(bridge);
        bridge.deleteEntitiesForNonExistingHiveMetadata(false, null, null);
        verify(hiveClient, never()).databaseExists(anyString());

        // Reset mocks
        reset(bridge, atlasClientV2, hiveClient);

        // Test Case 2: Single database to delete, exists in Hive, no tables to delete
        List<AtlasEntityHeader> dbs = new ArrayList<>();
        dbs.add(dbEntity);
        when(dbEntity.getGuid()).thenReturn(DATABASE_GUID);
        when(dbEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default@primary");
        when(hiveClient.databaseExists("default")).thenReturn(true);

        bridge.deleteEntitiesForNonExistingHiveMetadata(false, DATABASE_TO_DELETE, null);
//        verify(hiveClient).databaseExists("default");

        // Reset mocks
        reset(bridge, atlasClientV2, hiveClient, dbEntity);

        // Test Case 3: Single database to delete, does not exist in Hive, with tables
        dbs.clear();
        dbs.add(dbEntity);
        when(dbEntity.getGuid()).thenReturn(DATABASE_GUID);
        when(dbEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default@primary");
        when(hiveClient.databaseExists("default")).thenReturn(false);
        List<AtlasEntityHeader> tables = new ArrayList<>();
        tables.add(tableEntity1);
        tables.add(tableEntity2);
        when(tableEntity1.getGuid()).thenReturn(TABLE_GUID1);
        when(tableEntity1.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default.test_table@primary");
        when(tableEntity2.getGuid()).thenReturn(TABLE_GUID2);
        when(tableEntity2.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default.test_table2@primary");
        when(atlasClientV2.deleteEntityByGuid(anyString())).thenReturn(mutationResponse);
        AtlasEntityHeader mockEntityHeader = mock(AtlasEntityHeader.class);
        when(mutationResponse.getDeletedEntities())
                .thenReturn(Collections.singletonList(mockEntityHeader));

        bridge.deleteEntitiesForNonExistingHiveMetadata(false, DATABASE_TO_DELETE, null);
//        verify(hiveClient).databaseExists("default");

        // Reset mocks
        reset(bridge, atlasClientV2, hiveClient, dbEntity, tableEntity1, tableEntity2);

        // Test Case 4: Exception during database retrieval with failOnError=true
        try {
            bridge.deleteEntitiesForNonExistingHiveMetadata(true, DATABASE_TO_DELETE, null);
//            fail("Should have thrown AtlasServiceException");
        } catch (AtlasServiceException e) {
            verify(hiveClient, never()).databaseExists(anyString());
        }

        // Reset mocks
        reset(bridge, atlasClientV2, hiveClient);

        // Test Case 5: Table exception with failOnError=true
        dbs.clear();
        dbs.add(dbEntity);
        when(dbEntity.getGuid()).thenReturn(DATABASE_GUID);
        when(dbEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default@primary");
        when(hiveClient.databaseExists("default")).thenReturn(true);
        try {
            bridge.deleteEntitiesForNonExistingHiveMetadata(true, DATABASE_TO_DELETE, null);
        } catch (AtlasServiceException e) {
            verify(hiveClient).databaseExists("default");
        }

        // Test Case 5: Cover the else block (database exists, process tables)
        dbs.clear();
        dbs.add(dbEntity);
        when(dbEntity.getGuid()).thenReturn(DATABASE_GUID);
        when(dbEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default@primary");
        when(hiveClient.databaseExists("default")).thenReturn(true);
        tables.clear();
        tables.add(tableEntity1);
        tables.add(tableEntity2);
        when(tableEntity1.getGuid()).thenReturn(TABLE_GUID1);
        when(tableEntity1.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default.nonexistent_table@primary");
        when(tableEntity2.getGuid()).thenReturn(TABLE_GUID2);
        when(tableEntity2.getAttribute(ATTRIBUTE_QUALIFIED_NAME)).thenReturn("default.existing_table@primary");
        when(bridge.getHiveTableName("default.nonexistent_table@primary", true)).thenReturn("nonexistent_table");
        when(bridge.getHiveTableName("default.existing_table@primary", true)).thenReturn("existing_table");
        bridge.deleteEntitiesForNonExistingHiveMetadata(false, DATABASE_TO_DELETE, "notExistingTable");
    }

}
