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
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.hive.hook.events.BaseHiveEvent.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Mock
    private Hive hiveClient;

    @Mock
    private AtlasClient atlasClient;

    @Mock
    private AtlasClientV2 atlasClientV2;

    @Mock
    private AtlasEntity atlasEntity;

    @Mock
    private AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo;

    @Mock
    EntityMutationResponse entityMutationResponse;

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
                HiveMetaStoreBridge.getTableQualifiedName(METADATA_NAMESPACE, TEST_DB_NAME, TEST_TABLE_NAME)), true, true ))
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

    private class MatchesReferenceableProperty implements ArgumentMatcher<Object> {
        private final String attrName;
        private final Object attrValue;

        public MatchesReferenceableProperty(String attrName, Object attrValue) {
            this.attrName = attrName;
            this.attrValue = attrValue;
        }

        @Override
        public boolean matches(Object o) {
            return attrValue.equals(((AtlasEntity) o).getAttribute(attrName));
        }
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


}
