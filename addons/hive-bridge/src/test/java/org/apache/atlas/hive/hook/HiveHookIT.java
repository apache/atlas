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

package org.apache.atlas.hive.hook;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.atlas.AtlasClient.NAME;
import static org.apache.atlas.hive.hook.HiveHook.entityComparator;
import static org.apache.atlas.hive.hook.HiveHook.getProcessQualifiedName;
import static org.apache.atlas.hive.hook.HiveHook.lower;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class HiveHookIT {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HiveHookIT.class);

    private static final String DGI_URL = "http://localhost:21000/";
    private static final String CLUSTER_NAME = "test";
    public static final String DEFAULT_DB = "default";
    private Driver driver;
    private AtlasClient atlasClient;
    private HiveMetaStoreBridge hiveMetaStoreBridge;
    private SessionState ss;

    private HiveConf conf;
    
    private static final String INPUTS = AtlasClient.PROCESS_ATTRIBUTE_INPUTS;
    private static final String OUTPUTS = AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS;

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        conf = new HiveConf();
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        driver = new Driver(conf);
        ss = new SessionState(conf);
        ss = SessionState.start(ss);

        SessionState.setCurrentSessionState(ss);

        Configuration configuration = ApplicationProperties.get();
        atlasClient = new AtlasClient(configuration.getString(HiveMetaStoreBridge.ATLAS_ENDPOINT, DGI_URL));

        hiveMetaStoreBridge = new HiveMetaStoreBridge(conf, atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();
    }

    private void runCommand(String cmd) throws Exception {
        runCommandWithDelay(cmd, 0);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1', 'p2'='v2')");
        String dbId = assertDatabaseIsRegistered(dbName);

        Referenceable definition = atlasClient.getEntity(dbId);
        Map params = (Map) definition.get(HiveDataModelGenerator.PARAMETERS);
        Assert.assertNotNull(params);
        Assert.assertEquals(params.size(), 2);
        Assert.assertEquals(params.get("p1"), "v1");

        //There should be just one entity per dbname
        runCommand("drop database " + dbName);
        assertDBIsNotRegistered(dbName);

        runCommand("create database " + dbName);
        String dbid = assertDatabaseIsRegistered(dbName);

        //assert on qualified name
        Referenceable dbEntity = atlasClient.getEntity(dbid);
        Assert.assertEquals(dbEntity.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), dbName.toLowerCase() + "@" + CLUSTER_NAME);

    }

    private String dbName() {
        return "db" + random();
    }

    private String createDatabase() throws Exception {
        String dbName = dbName();
        runCommand("create database " + dbName);
        return dbName;
    }

    private String tableName() {
        return "table" + random();
    }

    private String columnName() {
        return "col" + random();
    }

    private String createTable() throws Exception {
        return createTable(false);
    }

    private String createTable(boolean isPartitioned) throws Exception {
        String tableName = tableName();
        runCommand("create table " + tableName + "(id int, name string) comment 'table comment' " + (isPartitioned ?
                " partitioned by(dt string)" : ""));
        return tableName;
    }

    private String createTable(boolean isExternal, boolean isPartitioned, boolean isTemporary) throws Exception {
        String tableName = tableName();

        String location = "";
        if (isExternal) {
            location = " location '" +  createTestDFSPath("someTestPath") + "'";
        }
        runCommand("create " + (isExternal ? " EXTERNAL " : "") + (isTemporary ? "TEMPORARY " : "") + "table " + tableName + "(id int, name string) comment 'table comment' " + (isPartitioned ?
            " partitioned by(dt string)" : "") + location);

        return tableName;
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        String colName = columnName();
        runCommand("create table " + dbName + "." + tableName + "(" + colName + " int, name string)");
        String tableId = assertTableIsRegistered(dbName, tableName);

        //there is only one instance of column registered
        String colId = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName), colName));
        Referenceable colEntity = atlasClient.getEntity(colId);
        Assert.assertEquals(colEntity.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), String.format("%s.%s.%s@%s", dbName.toLowerCase(),
                tableName.toLowerCase(), colName.toLowerCase(), CLUSTER_NAME));
        Assert.assertNotNull(colEntity.get(HiveDataModelGenerator.TABLE));
        Assert.assertEquals(((Id) colEntity.get(HiveDataModelGenerator.TABLE))._getId(), tableId);

        //assert that column.owner = table.owner
        Referenceable tableRef = atlasClient.getEntity(tableId);
        assertEquals(tableRef.get(AtlasClient.OWNER), colEntity.get(AtlasClient.OWNER));

        //create table where db is not registered
        tableName = createTable();
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        tableRef = atlasClient.getEntity(tableId);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.TABLE_TYPE_ATTR), TableType.MANAGED_TABLE.name());
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.COMMENT), "table comment");
        String entityName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        Assert.assertEquals(tableRef.get(AtlasClient.NAME), tableName.toLowerCase());
        Assert.assertEquals(tableRef.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), entityName);

        Table t = hiveMetaStoreBridge.hiveClient.getTable(DEFAULT_DB, tableName);
        long createTime = Long.parseLong(t.getMetadata().getProperty(hive_metastoreConstants.DDL_TIME)) * HiveMetaStoreBridge.MILLIS_CONVERT_FACTOR;

        verifyTimestamps(tableRef, HiveDataModelGenerator.CREATE_TIME, createTime);
        verifyTimestamps(tableRef, HiveDataModelGenerator.LAST_ACCESS_TIME, createTime);

        final Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_IS_STORED_AS_SUB_DIRS), false);
        Assert.assertNotNull(sdRef.get(HiveDataModelGenerator.TABLE));
        Assert.assertEquals(((Id) sdRef.get(HiveDataModelGenerator.TABLE))._getId(), tableId);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered(DEFAULT_DB);
    }

    private void verifyTimestamps(Referenceable ref, String property, long expectedTime) throws ParseException {
        //Verify timestamps.
        String createTimeStr = (String) ref.get(property);
        Date createDate = TypeSystem.getInstance().getDateFormat().parse(createTimeStr);
        Assert.assertNotNull(createTimeStr);

        if (expectedTime > 0) {
            Assert.assertEquals(expectedTime, createDate.getTime());
        }
    }

    private void verifyTimestamps(Referenceable ref, String property) throws ParseException {
        verifyTimestamps(ref, property, 0);
    }

    @Test
    public void testCreateExternalTable() throws Exception {
        String tableName = tableName();
        String colName = columnName();

        String pFile = createTestDFSPath("parentPath");
        final String query = String.format("create TEMPORARY EXTERNAL table %s.%s( %s, %s) location '%s'", DEFAULT_DB , tableName , colName + " int", "name string",  pFile);
        runCommand(query);
        assertTableIsRegistered(DEFAULT_DB, tableName, null, true);
        String processId = assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName, true), null);
        Referenceable processReference = atlasClient.getEntity(processId);
        assertEquals(processReference.get("userName"), UserGroupInformation.getCurrentUser().getShortUserName());

        verifyTimestamps(processReference, "startTime");
        verifyTimestamps(processReference, "endTime");

        validateHDFSPaths(processReference, INPUTS, pFile);
    }

    private List<Entity> getInputs(String inputName, Entity.Type entityType) {
        final ReadEntity entity = new ReadEntity();

        if ( Entity.Type.DFS_DIR.equals(entityType)) {
            entity.setName(lower(new Path(inputName).toString()));
            entity.setTyp(Entity.Type.DFS_DIR);
        } else {
            entity.setName(getQualifiedTblName(inputName));
            entity.setTyp(Entity.Type.TABLE);
        }

        return new ArrayList<Entity>() {{ add(entity); }};
    }


    private List<Entity> getOutputs(String inputName, Entity.Type entityType) {
        final WriteEntity entity = new WriteEntity();

        if ( Entity.Type.DFS_DIR.equals(entityType) || Entity.Type.LOCAL_DIR.equals(entityType)) {
            entity.setName(lower(new Path(inputName).toString()));
            entity.setTyp(entityType);
        } else {
            entity.setName(getQualifiedTblName(inputName));
            entity.setTyp(Entity.Type.TABLE);
        }

        return new ArrayList<Entity>() {{ add(entity); }};
    }


    private void validateOutputTables(Referenceable processReference, List<Entity> expectedTables) throws Exception {
       validateTables(processReference, OUTPUTS, expectedTables);
    }

    private void validateInputTables(Referenceable processReference, List<Entity> expectedTables) throws Exception {
        validateTables(processReference, INPUTS, expectedTables);
    }

    private void validateTables(Referenceable processReference, String attrName, List<Entity> expectedTables) throws Exception {
        List<Id> tableRef = (List<Id>) processReference.get(attrName);
        for(int i = 0; i < expectedTables.size(); i++) {
            Referenceable entity = atlasClient.getEntity(tableRef.get(i)._getId());
            LOG.debug("Validating output {} {} ", i, entity);
            Assert.assertEquals(entity.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), expectedTables.get(i).getName());
        }
    }

    private String assertColumnIsRegistered(String colName) throws Exception {
        return assertColumnIsRegistered(colName, null);
    }

    private String assertColumnIsRegistered(String colName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for column {}", colName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            colName, assertPredicate);
    }

    private String assertSDIsRegistered(String sdQFName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for sd {}", sdQFName.toLowerCase());
        return assertEntityIsRegistered(HiveDataTypes.HIVE_STORAGEDESC.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            sdQFName.toLowerCase(), assertPredicate);
    }

    private void assertColumnIsNotRegistered(String colName) throws Exception {
        LOG.debug("Searching for column {}", colName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            colName);
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = createTable();
        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        final ReadEntity entity = new ReadEntity();
        entity.setName(getQualifiedTblName(tableName));
        entity.setTyp(Entity.Type.TABLE);

        final WriteEntity writeEntity = new WriteEntity();
        writeEntity.setTyp(Entity.Type.TABLE);
        writeEntity.setName(getQualifiedTblName(ctasTableName));

        assertProcessIsRegistered(query, HiveOperation.CREATETABLE_AS_SELECT, new ArrayList<Entity>() {{ add(entity); }}, new ArrayList<Entity>() {{ add(writeEntity); }});
        assertTableIsRegistered(DEFAULT_DB, ctasTableName);
    }

    @Test
    public void testDropAndRecreateCTASOutput() throws Exception {
        String tableName = createTable();
        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, ctasTableName);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs =  getOutputs(ctasTableName, Entity.Type.TABLE);

        String processId = assertProcessIsRegistered(query, HiveOperation.CREATETABLE_AS_SELECT, inputs, outputs);

        final String drpquery = String.format("drop table %s ", ctasTableName);
        runCommand(drpquery);
        assertTableIsNotRegistered(DEFAULT_DB, ctasTableName);

        //Fix after ATLAS-876
        runCommand(query);
        assertTableIsRegistered(DEFAULT_DB, ctasTableName);
        String process2Id = assertProcessIsRegistered(query, HiveOperation.CREATETABLE_AS_SELECT, inputs, outputs);

        Assert.assertEquals(process2Id, processId);

        Referenceable processRef = atlasClient.getEntity(processId);

        validateInputTables(processRef, inputs);
        outputs.add(outputs.get(0));
        validateOutputTables(processRef, outputs);
    }

    @Test
    public void testCreateView() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query, HiveOperation.CREATEVIEW, getInputs(tableName, Entity.Type.TABLE), getOutputs(viewName, Entity.Type.TABLE));
        assertTableIsRegistered(DEFAULT_DB, viewName);
    }

    @Test
    public void testAlterViewAsSelect() throws Exception {

        //Create the view from table1
        String table1Name = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + table1Name;
        runCommand(query);

        String table1Id = assertTableIsRegistered(DEFAULT_DB, table1Name);
        assertProcessIsRegistered(query, HiveOperation.CREATEVIEW, getInputs(table1Name, Entity.Type.TABLE), getOutputs(viewName, Entity.Type.TABLE));
        String viewId = assertTableIsRegistered(DEFAULT_DB, viewName);

        //Check lineage which includes table1
        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        assertTrue(vertices.has(viewId));
        assertTrue(vertices.has(table1Id));

        //Alter the view from table2
        String table2Name = createTable();
        query = "alter view " + viewName + " as select * from " + table2Name;
        runCommand(query);

        //Check if alter view process is reqistered
        assertProcessIsRegistered(query, HiveOperation.CREATEVIEW, getInputs(table2Name, Entity.Type.TABLE), getOutputs(viewName, Entity.Type.TABLE));
        String table2Id = assertTableIsRegistered(DEFAULT_DB, table2Name);
        Assert.assertEquals(assertTableIsRegistered(DEFAULT_DB, viewName), viewId);

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        response = atlasClient.getInputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        assertTrue(vertices.has(viewId));

        //This is through the alter view process
        assertTrue(vertices.has(table2Id));

        //This is through the Create view process
        assertTrue(vertices.has(table1Id));

        //Outputs dont exist
        response = atlasClient.getOutputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 0);
    }

    private String createTestDFSPath(String path) throws Exception {
        return "pfile://" + mkdir(path);
    }

    private String createTestDFSFile(String path) throws Exception {
        return "pfile://" + file(path);
    }

    @Test
    public void testLoadLocalPath() throws Exception {
        String tableName = createTable(false);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName;
        runCommand(query);

        List<Entity> outputs = getOutputs(tableName, Entity.Type.TABLE);

        assertProcessIsRegistered(query, HiveOperation.LOAD, null, outputs);
    }

    @Test
    public void testLoadLocalPathIntoPartition() throws Exception {
        String tableName = createTable(true);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName +  " partition(dt = '2015-01-01')";
        runCommand(query);

        validateProcess(query, HiveOperation.LOAD, null, getOutputs(tableName, Entity.Type.TABLE));
    }

    @Test
    public void testLoadDFSPath() throws Exception {
        String tableName = createTable(true, true, false);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String loadFile = createTestDFSFile("loadDFSFile");
        String query = "load data inpath '" + loadFile + "' into table " + tableName + " partition(dt = '2015-01-01')";
        runCommand(query);

        final List<Entity> outputs = getOutputs(tableName, Entity.Type.TABLE);
        Referenceable processReference = validateProcess(query, HiveOperation.LOAD, getInputs(loadFile, Entity.Type.DFS_DIR), outputs);

        validateHDFSPaths(processReference, INPUTS, loadFile);

        validateOutputTables(processReference, outputs);
    }

    private String getQualifiedTblName(String inputTable) {
        String inputtblQlfdName = inputTable;

        if (inputTable != null && !inputTable.contains(".")) {
            inputtblQlfdName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, inputTable);
        }
        return inputtblQlfdName;
    }

    private Referenceable validateProcess(String query, HiveOperation op, List<Entity> inputTables, List<Entity> outputTables) throws Exception {
        String processId = assertProcessIsRegistered(query, op, inputTables, outputTables);
        Referenceable process = atlasClient.getEntity(processId);
        if (inputTables == null) {
            Assert.assertNull(process.get(INPUTS));
        } else {
            Assert.assertEquals(((List<Referenceable>) process.get(INPUTS)).size(), inputTables.size());
            validateInputTables(process, inputTables);
        }

        if (outputTables == null) {
            Assert.assertNull(process.get(OUTPUTS));
        } else {
            Assert.assertEquals(((List<Id>) process.get(OUTPUTS)).size(), outputTables.size());
            validateOutputTables(process, outputTables);
        }

        return process;
    }

    @Test
    public void testInsertIntoTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable();
        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertTableIsRegistered(DEFAULT_DB, insertTableName);

        String query = "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);
        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs = getOutputs(insertTableName, Entity.Type.TABLE);
        ((WriteEntity)outputs.get(0)).setWriteType(WriteEntity.WriteType.INSERT);

        Referenceable processRef1 = validateProcess(query, HiveOperation.QUERY, inputs, outputs);

        //Rerun same query. Should result in same process
        runCommandWithDelay(query, 1000);
        Referenceable processRef2 = validateProcess(query, HiveOperation.QUERY, inputs, outputs);
        Assert.assertEquals(processRef1.getId()._getId(), processRef2.getId()._getId());

    }

    @Test
    public void testInsertIntoLocalDir() throws Exception {
        String tableName = createTable();
        File randomLocalPath = File.createTempFile("hiverandom", ".tmp");
        String query =
            "insert overwrite LOCAL DIRECTORY '" + randomLocalPath.getAbsolutePath() + "' select id, name from " + tableName;

        runCommand(query);
        validateProcess(query, HiveOperation.QUERY, getInputs(tableName, Entity.Type.TABLE), null);

        assertTableIsRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testUpdateProcess() throws Exception {
        String tableName = createTable();
        String pFile1 = createTestDFSPath("somedfspath1");
        String query =
            "insert overwrite DIRECTORY '" + pFile1  + "' select id, name from " + tableName;

        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        final List<Entity> outputs = getOutputs(pFile1, Entity.Type.DFS_DIR);
        ((WriteEntity)outputs.get(0)).setWriteType(WriteEntity.WriteType.PATH_WRITE);

        Referenceable processReference = validateProcess(query, HiveOperation.QUERY, inputs, outputs);
        validateHDFSPaths(processReference, OUTPUTS, pFile1);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        validateInputTables(processReference, inputs);

        //Rerun same query with same HDFS path

        runCommand(query);
        Referenceable process2Reference = validateProcess(query,  HiveOperation.QUERY, inputs, outputs);
        validateHDFSPaths(process2Reference, OUTPUTS, pFile1);

        Assert.assertEquals(process2Reference.getId()._getId(), processReference.getId()._getId());

        //Rerun same query with a new HDFS path. Will result in same process since HDFS paths are not part of qualifiedName.
        final String pFile2 = createTestDFSPath("somedfspath2");
        query = "insert overwrite DIRECTORY '" + pFile2  + "' select id, name from " + tableName;
        runCommand(query);
        List<Entity> p3Outputs = new ArrayList<Entity>() {{
            addAll(getOutputs(pFile2, Entity.Type.DFS_DIR));
            addAll(outputs);
        }};

        Referenceable process3Reference = validateProcess(query,  HiveOperation.QUERY, inputs, p3Outputs);
        validateHDFSPaths(process3Reference, OUTPUTS, pFile2);

        Assert.assertEquals(process3Reference.getId()._getId(), processReference.getId()._getId());
    }

    @Test
    public void testInsertIntoDFSDir() throws Exception {
        String tableName = createTable();
        String pFile1 = createTestDFSPath("somedfspath1");
        String query =
            "insert overwrite DIRECTORY '" + pFile1  + "' select id, name from " + tableName;

        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        final List<Entity> outputs = getOutputs(pFile1, Entity.Type.DFS_DIR);
        ((WriteEntity)outputs.get(0)).setWriteType(WriteEntity.WriteType.PATH_WRITE);

        Referenceable processReference = validateProcess(query,  HiveOperation.QUERY, inputs, outputs);
        validateHDFSPaths(processReference, OUTPUTS, pFile1);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        validateInputTables(processReference, inputs);

        //Rerun same query with different HDFS path

        final String pFile2 = createTestDFSPath("somedfspath2");
        query =
            "insert overwrite DIRECTORY '" + pFile2  + "' select id, name from " + tableName;

        runCommand(query);
        List<Entity> p2Outputs = new ArrayList<Entity>() {{
            addAll(getOutputs(pFile2, Entity.Type.DFS_DIR));
            addAll(outputs);
        }};

        Referenceable process2Reference = validateProcess(query, HiveOperation.QUERY, inputs, p2Outputs);
        validateHDFSPaths(process2Reference, OUTPUTS, pFile2);

        Assert.assertEquals(process2Reference.getId()._getId(), processReference.getId()._getId());
    }

    @Test
    public void testInsertIntoTempTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable(false, false, true);
        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertTableIsNotRegistered(DEFAULT_DB, insertTableName, true);

        String query =
            "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs = getOutputs(insertTableName, Entity.Type.TABLE);
        outputs.get(0).setName(getQualifiedTblName(insertTableName + HiveMetaStoreBridge.TEMP_TABLE_PREFIX + SessionState.get().getSessionId()));
        ((WriteEntity)outputs.get(0)).setWriteType(WriteEntity.WriteType.INSERT);

        validateProcess(query,  HiveOperation.QUERY, inputs, outputs);

        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertTableIsRegistered(DEFAULT_DB, insertTableName, null, true);
    }

    @Test
    public void testInsertIntoPartition() throws Exception {
        String tableName = createTable(true);
        String insertTableName = createTable(true);
        String query =
            "insert into " + insertTableName + " partition(dt = '2015-01-01') select id, name from " + tableName
                + " where dt = '2015-01-01'";
        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs = getOutputs(insertTableName, Entity.Type.TABLE);
        ((WriteEntity)outputs.get(0)).setWriteType(WriteEntity.WriteType.INSERT);

        validateProcess(query,  HiveOperation.QUERY, inputs, outputs);

        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertTableIsRegistered(DEFAULT_DB, insertTableName);
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    private String file(String tag) throws Exception {
        String filename = "./target/" + tag + "-data-" + random();
        File file = new File(filename);
        file.createNewFile();
        return file.getAbsolutePath();
    }

    private String mkdir(String tag) throws Exception {
        String filename = "./target/" + tag + "-data-" + random();
        File file = new File(filename);
        file.mkdirs();
        return file.getAbsolutePath();
    }

    @Test
    public void testExportImportUnPartitionedTable() throws Exception {
        String tableName = createTable(false);

        assertTableIsRegistered(DEFAULT_DB, tableName);

        String filename = "pfile://" + mkdir("export");
        String query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs = getOutputs(filename, Entity.Type.DFS_DIR);

        Referenceable processReference = validateProcess(query, HiveOperation.EXPORT, inputs, outputs);

        validateHDFSPaths(processReference, OUTPUTS, filename);
        validateInputTables(processReference, inputs);

        //Import
        tableName = createTable(false);
        assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        outputs = getOutputs(tableName, Entity.Type.TABLE);
        processReference = validateProcess(query, HiveOperation.IMPORT, getInputs(filename, Entity.Type.DFS_DIR), outputs);
        validateHDFSPaths(processReference, INPUTS, filename);

        validateOutputTables(processReference, outputs);
    }

    @Test
    public void testExportImportPartitionedTable() throws Exception {
        String tableName = createTable(true);
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        //Add a partition
        String partFile = "pfile://" + mkdir("partition");
        String query = "alter table " + tableName + " add partition (dt='2015-01-01') location '" + partFile + "'";
        runCommand(query);

        String filename = "pfile://" + mkdir("export");
        query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);

        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        List<Entity> outputs = getOutputs(filename, Entity.Type.DFS_DIR);

        Referenceable processReference = validateProcess(query, HiveOperation.EXPORT, inputs, outputs);
        validateHDFSPaths(processReference, OUTPUTS, filename);

        validateInputTables(processReference, inputs);

        //Import
        tableName = createTable(true);
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);

        outputs = getOutputs(tableName, Entity.Type.TABLE);
        processReference = validateProcess(query, HiveOperation.IMPORT, getInputs(filename, Entity.Type.DFS_DIR), outputs);
        validateHDFSPaths(processReference, INPUTS, filename);

        validateOutputTables(processReference, outputs);
    }

    @Test
    public void testIgnoreSelect() throws Exception {
        String tableName = createTable();
        String query = "select * from " + tableName;
        runCommand(query);
        List<Entity> inputs = getInputs(tableName, Entity.Type.TABLE);
        assertProcessIsNotRegistered(query, HiveOperation.QUERY, inputs, null);

        //check with uppercase table name
        query = "SELECT * from " + tableName.toUpperCase();
        runCommand(query);
        assertProcessIsNotRegistered(query, HiveOperation.QUERY, inputs, null);
    }

    @Test
    public void testAlterTableRenameAliasRegistered() throws Exception{
        String tableName = createTable(false);
        String tableGuid = assertTableIsRegistered(DEFAULT_DB, tableName);
        String newTableName = tableName();
        String query = String.format("alter table %s rename to %s", tableName, newTableName);
        runCommand(query);
        String newTableGuid = assertTableIsRegistered(DEFAULT_DB, newTableName);
        Map<String, Object> valueMap = atlasClient.getEntity(newTableGuid).getValuesMap();
        Iterable<String> aliasList = (Iterable<String>) valueMap.get("aliases");
        String aliasTableName = aliasList.iterator().next();
        assert tableName.toLowerCase().equals(aliasTableName);
    }

    @Test
    public void testAlterTableRename() throws Exception {
        String tableName = createTable(true);
        final String newDBName = createDatabase();

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableEntity = atlasClient.getEntity(tableId);
        final String createTime = (String)tableEntity.get(HiveDataModelGenerator.CREATE_TIME);
        Assert.assertNotNull(createTime);

        String columnGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), NAME));
        String sdGuid = assertSDIsRegistered(HiveMetaStoreBridge.getStorageDescQFName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName)), null);
        assertDatabaseIsRegistered(newDBName);

        //Add trait to column
        String colTraitDetails = createTrait(columnGuid);

        //Add trait to sd
        String sdTraitDetails = createTrait(sdGuid);

        String partColumnGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "dt"));
        //Add trait to part col keys
        String partColTraitDetails = createTrait(partColumnGuid);

        final String newTableName = tableName();
        String query = String.format("alter table %s rename to %s", DEFAULT_DB + "." + tableName, newDBName + "." + newTableName);
        runCommandWithDelay(query, 1000);

        String newColGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, newTableName), NAME));
        Assert.assertEquals(newColGuid, columnGuid);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, tableName), NAME));

        assertTrait(columnGuid, colTraitDetails);
        String newSdGuid = assertSDIsRegistered(HiveMetaStoreBridge.getStorageDescQFName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, newTableName)), null);
        Assert.assertEquals(newSdGuid, sdGuid);

        assertTrait(sdGuid, sdTraitDetails);
        assertTrait(partColumnGuid, partColTraitDetails);

        assertTableIsNotRegistered(DEFAULT_DB, tableName);

        assertTableIsRegistered(newDBName, newTableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(final Referenceable entity) throws Exception {
                Referenceable sd = ((Referenceable) entity.get(HiveDataModelGenerator.STORAGE_DESC));
                String location = (String) sd.get(HiveDataModelGenerator.LOCATION);
                assertTrue(location.contains(newTableName));
                Assert.assertEquals(entity.get(HiveDataModelGenerator.CREATE_TIME), createTime);
            }
        });
    }

    private List<Referenceable> getColumns(String dbName, String tableName) throws Exception {
        String tableId = assertTableIsRegistered(dbName, tableName);
        Referenceable tableRef = atlasClient.getEntity(tableId);

        //with soft delete, the deleted columns are returned as well. So, filter the deleted ones
        List<Referenceable> columns = ((List<Referenceable>) tableRef.get(HiveDataModelGenerator.COLUMNS));
        List<Referenceable> activeColumns = new ArrayList<>();
        for (Referenceable col : columns) {
            if (col.getId().getState() == Id.EntityState.ACTIVE) {
                activeColumns.add(col);
            }
        }
        return activeColumns;
    }


    private String createTrait(String guid) throws AtlasServiceException, JSONException {
        //add trait
        String traitName = "PII_Trait" + RandomStringUtils.random(10);
        atlasClient.createTraitType(traitName);

        Struct traitInstance = new Struct(traitName);
        atlasClient.addTrait(guid, traitInstance);
        return traitName;
    }

    private void assertTrait(String guid, String traitName) throws AtlasServiceException, JSONException {
        List<String> traits = atlasClient.listTraits(guid);
        Assert.assertEquals(traits.get(0), traitName);
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        String tableName = createTable();
        String column = columnName();
        String query = "alter table " + tableName + " add columns (" + column + " string)";
        runCommand(query);

        assertColumnIsRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                column));

        //Verify the number of columns present in the table
        final List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 3);
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        String tableName = createTable();
        final String colDropped = "id";
        String query = "alter table " + tableName + " replace columns (name string)";
        runCommand(query);

        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                colDropped));

        //Verify the number of columns present in the table
        List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        assertEquals(columns.size(), 1);
        assertEquals(columns.get(0).get(NAME), "name");
    }

    @Test
    public void testAlterTableChangeColumn() throws Exception {
        //Change name
        String oldColName = NAME;
        String newColName = "name1";
        String tableName = createTable();
        String query = String.format("alter table %s change %s %s string", tableName, oldColName, newColName);
        runCommandWithDelay(query, 1000);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName));

        //Verify the number of columns present in the table
        List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        //Change column type
        oldColName = "name1";
        newColName = "name2";
        final String newColType = "int";
        query = String.format("alter table %s change column %s %s %s", tableName, oldColName, newColName, newColType);
        runCommandWithDelay(query, 1000);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        String newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                assertEquals(entity.get("type"), "int");
            }
        });

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        //Change name and add comment
        oldColName = "name2";
        newColName = "name3";
        final String comment = "added comment";
        query = String.format("alter table %s change column %s %s %s COMMENT '%s' after id", tableName, oldColName,
            newColName, newColType, comment);
        runCommandWithDelay(query, 1000);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                assertEquals(entity.get(HiveDataModelGenerator.COMMENT), comment);
            }
        });

        //Change column position
        oldColName = "name3";
        newColName = "name4";
        query = String.format("alter table %s change column %s %s %s first", tableName, oldColName, newColName,
                newColType);
        runCommandWithDelay(query, 1000);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        final String finalNewColName = newColName;
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    List<Referenceable> columns = (List<Referenceable>) entity.get(HiveDataModelGenerator.COLUMNS);
                    assertEquals(columns.get(0).get(NAME), finalNewColName);
                    assertEquals(columns.get(1).get(NAME), "id");
                }
            }
        );

        //Change col position again
        oldColName = "name4";
        newColName = "name5";
        query = String.format("alter table %s change column %s %s %s after id", tableName, oldColName, newColName, newColType);
        runCommandWithDelay(query, 1000);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        //Check col position
        final String finalNewColName2 = newColName;
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    List<Referenceable> columns = (List<Referenceable>) entity.get(HiveDataModelGenerator.COLUMNS);
                    assertEquals(columns.get(1).get(NAME), finalNewColName2);
                    assertEquals(columns.get(0).get(NAME), "id");
                }
            }
        );
    }

    private void runCommandWithDelay(String cmd, int sleepMs) throws CommandNeedRetryException, InterruptedException {
        LOG.debug("Running command '{}'", cmd);
        ss.setCommandType(null);
        CommandProcessorResponse response = driver.run(cmd);
        assertEquals(response.getResponseCode(), 0);
        if (sleepMs != 0) {
            Thread.sleep(sleepMs);
        }
    }

    @Test
    public void testTruncateTable() throws Exception {
        String tableName = createTable(false);
        String query = String.format("truncate table %s", tableName);
        runCommand(query);

        List<Entity> outputs = getInputs(tableName, Entity.Type.TABLE);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        validateProcess(query, HiveOperation.TRUNCATETABLE, null, outputs);

        //Check lineage
        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        //Below should be assertTrue - Fix https://issues.apache.org/jira/browse/ATLAS-653
        Assert.assertFalse(vertices.has(tableId));
    }

    @Test
    public void testAlterTablePartitionColumnType() throws Exception {
        String tableName = createTable(true, true, false);
        final String newType = "int";
        String query = String.format("ALTER TABLE %s PARTITION COLUMN (dt %s)", tableName, newType);
        runCommand(query);

        String colQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "dt");
        final String dtColId = assertColumnIsRegistered(colQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable column) throws Exception {
                Assert.assertEquals(column.get("type"), newType);
            }
        });

        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable table) throws Exception {
                final List<Referenceable> partitionKeys = (List<Referenceable>) table.get("partitionKeys");
                Assert.assertEquals(partitionKeys.size(), 1);
                Assert.assertEquals(partitionKeys.get(0).getId()._getId(), dtColId);

            }
        });
    }

    @Test
    public void testAlterTableWithoutHookConf() throws Exception {
        HiveConf conf = new HiveConf();
        conf.set("hive.exec.post.hooks", "");
        SessionState ss = new SessionState(conf);
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);
        Driver driver = new Driver(conf);
        String tableName = tableName();
        String createCommand = "create table " + tableName + " (id int, name string)";
        driver.run(createCommand);
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
        String command = "alter table " + tableName + " change id id_new string";
        runCommand(command);
        assertTableIsRegistered(DEFAULT_DB, tableName);
        String tbqn = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(tbqn, "id_new"));
    }

    @Test
    public void testTraitsPreservedOnColumnRename() throws Exception {
        String dbName = createDatabase();
        String tableName = tableName();
        String createQuery = String.format("create table %s.%s (id int, name string)", dbName, tableName);
        runCommand(createQuery);
        String tbqn = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName);
        String guid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(tbqn, "id"));
        String trait = createTrait(guid);
        String oldColName = "id";
        String newColName = "id_new";
        String query = String.format("alter table %s.%s change %s %s string", dbName, tableName, oldColName, newColName);
        runCommand(query);

        String guid2 = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(tbqn, "id_new"));
        assertEquals(guid2, guid);

        assertTrue(atlasClient.getEntity(guid2).getTraits().contains(trait));
    }

    @Test
    public void testAlterViewRename() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String newName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        query = "alter view " + viewName + " rename to " + newName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, newName);
        assertTableIsNotRegistered(DEFAULT_DB, viewName);
    }

    @Test
    public void testAlterTableLocation() throws Exception {
        //Its an external table, so the HDFS location should also be registered as an entity
        String tableName = createTable(true, true, false);
        final String testPath = createTestDFSPath("testBaseDir");
        String query = "alter table " + tableName + " set location '" + testPath + "'";
        runCommand(query);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Assert.assertEquals(new Path((String)sdRef.get(HiveDataModelGenerator.LOCATION)).toString(), new Path(testPath).toString());
            }
        });

        List<Entity> inputs = getInputs(testPath, Entity.Type.DFS_DIR);
        List<Entity> outputs = getOutputs(tableName, Entity.Type.TABLE);

        Referenceable processReference = validateProcess(query, HiveOperation.ALTERTABLE_LOCATION, inputs, outputs);
        validateHDFSPaths(processReference, INPUTS, testPath);
    }

    private void validateHDFSPaths(Referenceable processReference, String attributeName, String... testPaths) throws Exception {
        List<Id> hdfsPathRefs = (List<Id>) processReference.get(attributeName);

        for (int i = 0; i < testPaths.length; i++) {
            final String testPathNormed = lower(new Path(testPaths[i]).toString());
            String hdfsPathId = assertHDFSPathIsRegistered(testPathNormed);
            Assert.assertEquals(hdfsPathRefs.get(0)._getId(), hdfsPathId);

            Referenceable hdfsPathRef = atlasClient.getEntity(hdfsPathId);
            Assert.assertEquals(hdfsPathRef.get("path"), testPathNormed);
            Assert.assertEquals(hdfsPathRef.get(NAME), new Path(testPathNormed).getName());
            Assert.assertEquals(hdfsPathRef.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), testPathNormed);
        }
    }

    private String assertHDFSPathIsRegistered(String path) throws Exception {
        LOG.debug("Searching for hdfs path {}", path);
        return assertEntityIsRegistered(FSDataTypes.HDFS_PATH().toString(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, path, null);
    }

    @Test
    public void testAlterTableFileFormat() throws Exception {
        String tableName = createTable();
        final String testFormat = "orc";
        String query = "alter table " + tableName + " set FILEFORMAT " + testFormat;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT),
                    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT),
                    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
                Assert.assertNotNull(sdRef.get("serdeInfo"));

                Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
                Assert.assertEquals(serdeInfo.get("serializationLib"), "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
                Assert.assertNotNull(serdeInfo.get(HiveDataModelGenerator.PARAMETERS));
                Assert.assertEquals(
                    ((Map<String, String>) serdeInfo.get(HiveDataModelGenerator.PARAMETERS))
                        .get("serialization.format"),
                    "1");
            }
        });


        /**
         * Hive 'alter table stored as' is not supported - See https://issues.apache.org/jira/browse/HIVE-9576
         * query = "alter table " + tableName + " STORED AS " + testFormat.toUpperCase();
         * runCommand(query);

         * tableRef = atlasClient.getEntity(tableId);
         * sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
         * Assert.assertEquals(((Map) sdRef.get(HiveDataModelGenerator.PARAMETERS)).get("orc.compress"), "ZLIB");
         */
    }

    @Test
    public void testAlterTableBucketingClusterSort() throws Exception {
        String tableName = createTable();
        ImmutableList<String> cols = ImmutableList.of("id");
        runBucketSortQuery(tableName, 5, cols, cols);

        cols = ImmutableList.of("id", NAME);
        runBucketSortQuery(tableName, 2, cols, cols);
    }

    private void runBucketSortQuery(String tableName, final int numBuckets,  final ImmutableList<String> bucketCols,
                                    final ImmutableList<String> sortCols) throws Exception {
        final String fmtQuery = "alter table %s CLUSTERED BY (%s) SORTED BY (%s) INTO %s BUCKETS";
        String query = String.format(fmtQuery, tableName, stripListBrackets(bucketCols.toString()),
                stripListBrackets(sortCols.toString()), numBuckets);
        runCommand(query);
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                verifyBucketSortingProperties(entity, numBuckets, bucketCols, sortCols);
            }
        });
    }

    private String stripListBrackets(String listElements) {
        return StringUtils.strip(StringUtils.strip(listElements, "["), "]");
    }

    private void verifyBucketSortingProperties(Referenceable tableRef, int numBuckets,
                                               ImmutableList<String> bucketColNames,
                                               ImmutableList<String>  sortcolNames) throws Exception {
        Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(((scala.math.BigInt) sdRef.get(HiveDataModelGenerator.STORAGE_NUM_BUCKETS)).intValue(),
            numBuckets);
        Assert.assertEquals(sdRef.get("bucketCols"), bucketColNames);

        List<Struct> hiveOrderStructList = (List<Struct>) sdRef.get("sortCols");
        Assert.assertNotNull(hiveOrderStructList);
        Assert.assertEquals(hiveOrderStructList.size(), sortcolNames.size());

        for (int i = 0; i < sortcolNames.size(); i++) {
            Assert.assertEquals(hiveOrderStructList.get(i).get("col"), sortcolNames.get(i));
            Assert.assertEquals(((scala.math.BigInt) hiveOrderStructList.get(i).get("order")).intValue(), 1);
        }
    }

    @Test
    public void testAlterTableSerde() throws Exception {
        //SERDE PROPERTIES
        String tableName = createTable();
        Map<String, String> expectedProps = new HashMap<String, String>() {{
            put("key1", "value1");
        }};

        runSerdePropsQuery(tableName, expectedProps);

        expectedProps.put("key2", "value2");

        //Add another property
        runSerdePropsQuery(tableName, expectedProps);
    }

    @Test
    public void testDropTable() throws Exception {
        //Test Deletion of tables and its corrresponding columns
        String tableName = createTable(true, true, false);

        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "id"));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), NAME));

        final String query = String.format("drop table %s ", tableName);
        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                NAME));
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testDropDatabaseWithCascade() throws Exception {
        //Test Deletion of database and its corresponding tables
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1')");

        final int numTables = 10;
        String[] tableNames = new String[numTables];
        for(int i = 0; i < numTables; i++) {
            tableNames[i] = createTable(true, true, false);
        }

        final String query = String.format("drop database %s cascade", dbName);
        runCommand(query);

        //Verify columns are not registered for one of the tables
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableNames[0]), "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableNames[0]),
                NAME));

        for(int i = 0; i < numTables; i++) {
            assertTableIsNotRegistered(dbName, tableNames[i]);
        }
        assertDBIsNotRegistered(dbName);
    }

    @Test
    public void testDropDatabaseWithoutCascade() throws Exception {
        //Test Deletion of database and its corresponding tables
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1')");

        final int numTables = 10;
        String[] tableNames = new String[numTables];
        for(int i = 0; i < numTables; i++) {
            tableNames[i] = createTable(true, true, false);
            String query = String.format("drop table %s", tableNames[i]);
            runCommand(query);
            assertTableIsNotRegistered(dbName, tableNames[i]);
        }

        final String query = String.format("drop database %s", dbName);
        runCommand(query);

        assertDBIsNotRegistered(dbName);
    }

    @Test
    public void testDropNonExistingDB() throws Exception {
        //Test Deletion of a non existing DB
        final String dbName = "nonexistingdb";
        assertDBIsNotRegistered(dbName);
        final String query = String.format("drop database if exists %s cascade", dbName);
        runCommand(query);

        //Should have no effect
        assertDBIsNotRegistered(dbName);
    }

    @Test
    public void testDropNonExistingTable() throws Exception {
        //Test Deletion of a non existing table
        final String tableName = "nonexistingtable";
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
        final String query = String.format("drop table if exists %s", tableName);
        runCommand(query);

        //Should have no effect
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testDropView() throws Exception {
        //Test Deletion of tables and its corrresponding columns
        String tableName = createTable(true, true, false);
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, viewName);
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName), "id"));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName), NAME));

        query = String.format("drop view %s ", viewName);

        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName),
                    "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName),
                NAME));
        assertTableIsNotRegistered(DEFAULT_DB, viewName);
    }

    private void runSerdePropsQuery(String tableName, Map<String, String> expectedProps) throws Exception {

        final String serdeLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

        final String serializedProps = getSerializedProps(expectedProps);
        String query = String.format("alter table %s set SERDE '%s' WITH SERDEPROPERTIES (%s)", tableName, serdeLib, serializedProps);
        runCommand(query);

        verifyTableSdProperties(tableName, serdeLib, expectedProps);
    }

    private String getSerializedProps(Map<String, String> expectedProps) {
        StringBuilder sb = new StringBuilder();
        for(String expectedPropKey : expectedProps.keySet()) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append("'").append(expectedPropKey).append("'");
            sb.append("=");
            sb.append("'").append(expectedProps.get(expectedPropKey)).append("'");
        }
        return sb.toString();
    }

    @Test
    public void testAlterDBOwner() throws Exception {
        String dbName = createDatabase();
        assertDatabaseIsRegistered(dbName);

        final String owner = "testOwner";
        final String fmtQuery = "alter database %s set OWNER %s %s";
        String query = String.format(fmtQuery, dbName, "USER", owner);

        runCommand(query);

        assertDatabaseIsRegistered(dbName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) {
                assertEquals(entity.get(AtlasClient.OWNER), owner);
            }
        });
    }

    @Test
    public void testAlterDBProperties() throws Exception {
        String dbName = createDatabase();
        final String fmtQuery = "alter database %s %s DBPROPERTIES (%s)";
        testAlterProperties(Entity.Type.DATABASE, dbName, fmtQuery);
    }

    @Test
    public void testAlterTableProperties() throws Exception {
        String tableName = createTable();
        final String fmtQuery = "alter table %s %s TBLPROPERTIES (%s)";
        testAlterProperties(Entity.Type.TABLE, tableName, fmtQuery);
    }

    private void testAlterProperties(Entity.Type entityType, String entityName, String fmtQuery) throws Exception {
        final String SET_OP = "set";
        final String UNSET_OP = "unset";

        final Map<String, String> expectedProps = new HashMap<String, String>() {{
            put("testPropKey1", "testPropValue1");
            put("comment", "test comment");
        }};

        String query = String.format(fmtQuery, entityName, SET_OP, getSerializedProps(expectedProps));
        runCommand(query);
        verifyEntityProperties(entityType, entityName, expectedProps, false);

        expectedProps.put("testPropKey2", "testPropValue2");
        //Add another property
        query = String.format(fmtQuery, entityName, SET_OP, getSerializedProps(expectedProps));
        runCommand(query);
        verifyEntityProperties(entityType, entityName, expectedProps, false);

        if (entityType != Entity.Type.DATABASE) {
            //Database unset properties doesnt work strangely - alter database %s unset DBPROPERTIES doesnt work
            //Unset all the props
            StringBuilder sb = new StringBuilder("'");
            query = String.format(fmtQuery, entityName, UNSET_OP, Joiner.on("','").skipNulls().appendTo(sb, expectedProps.keySet()).append('\''));
            runCommand(query);

            verifyEntityProperties(entityType, entityName, expectedProps, true);
        }
    }

    @Test
    public void testAlterViewProperties() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        final String fmtQuery = "alter view %s %s TBLPROPERTIES (%s)";
        testAlterProperties(Entity.Type.TABLE, viewName, fmtQuery);
    }

    private void verifyEntityProperties(Entity.Type type, String entityName, final Map<String, String> expectedProps,
                                        final boolean checkIfNotExists) throws Exception {
        switch(type) {
        case TABLE:
            assertTableIsRegistered(DEFAULT_DB, entityName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    verifyProperties(entity, expectedProps, checkIfNotExists);
                }
            });
            break;
        case DATABASE:
            assertDatabaseIsRegistered(entityName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    verifyProperties(entity, expectedProps, checkIfNotExists);
                }
            });
            break;
        }
    }

    private void verifyTableSdProperties(String tableName, final String serdeLib, final Map<String, String> expectedProps) throws Exception {
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
                Assert.assertEquals(serdeInfo.get("serializationLib"), serdeLib);
                verifyProperties(serdeInfo, expectedProps, false);
            }
        });
    }

    private void verifyProperties(Struct referenceable, Map<String, String> expectedProps, boolean checkIfNotExists) {
        Map<String, String> parameters = (Map<String, String>) referenceable.get(HiveDataModelGenerator.PARAMETERS);

        if (checkIfNotExists == false) {
            //Check if properties exist
            Assert.assertNotNull(parameters);
            for (String propKey : expectedProps.keySet()) {
                Assert.assertEquals(parameters.get(propKey), expectedProps.get(propKey));
            }
        } else {
            //Check if properties dont exist
            if (expectedProps != null && parameters != null) {
                for (String propKey : expectedProps.keySet()) {
                    Assert.assertFalse(parameters.containsKey(propKey));
                }
            }
        }
    }

    private String assertProcessIsRegistered(final String queryStr, HiveOperation op, final List<Entity> inputTbls, final List<Entity> outputTbls) throws Exception {
        String processQFName = getProcessQualifiedName(op, getSortedProcessDataSets(inputTbls), getSortedProcessDataSets(outputTbls));
        LOG.debug("Searching for process with query {}", processQFName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processQFName, new AssertPredicate() {
            @Override
            public void assertOnEntity(final Referenceable entity) throws Exception {
                List<String> recentQueries = (List<String>) entity.get("recentQueries");
                Assert.assertEquals(recentQueries.get(0), lower(queryStr));
            }
        });
    }

    private String getDSTypeName(Entity entity) {
        return Entity.Type.TABLE.equals(entity.getType()) ? HiveDataTypes.HIVE_TABLE.name() : FSDataTypes.HDFS_PATH().toString();
    }

    private SortedMap<Entity, Referenceable> getSortedProcessDataSets(List<Entity> inputTbls) {
        SortedMap<Entity, Referenceable> inputs = new TreeMap<Entity, Referenceable>(entityComparator);
        if (inputTbls != null) {
            for (final Entity tbl : inputTbls) {
                Referenceable inputTableRef = new Referenceable(getDSTypeName(tbl), new HashMap<String, Object>() {{
                    put(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tbl.getName());
                }});
                inputs.put(tbl, inputTableRef);
            }
        }
        return inputs;
    }

    private void assertProcessIsNotRegistered(String queryStr, HiveOperation op, final List<Entity> inputTbls, final List<Entity> outputTbls) throws Exception {
        String processQFName = getProcessQualifiedName(op, getSortedProcessDataSets(inputTbls), getSortedProcessDataSets(outputTbls));
        LOG.debug("Searching for process with query {}", processQFName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processQFName);
    }

    private void assertTableIsNotRegistered(String dbName, String tableName, boolean isTemporaryTable) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName, isTemporaryTable);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName);
    }

    private void assertTableIsNotRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName, false);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName);
    }

    private void assertDBIsNotRegistered(String dbName) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(CLUSTER_NAME, dbName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_DB.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbQualifiedName);
    }

    private String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        return assertTableIsRegistered(dbName, tableName, null, false);
    }


    private String assertTableIsRegistered(String dbName, String tableName, AssertPredicate assertPredicate, boolean isTemporary) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName, isTemporary);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName,
            assertPredicate);
    }

    private String assertTableIsRegistered(String dbName, String tableName, AssertPredicate assertPredicate) throws Exception {
        return assertTableIsRegistered(dbName, tableName, assertPredicate, false);
    }

    private String assertDatabaseIsRegistered(String dbName) throws Exception {
        return assertDatabaseIsRegistered(dbName, null);
    }

    private String assertDatabaseIsRegistered(String dbName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(CLUSTER_NAME, dbName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_DB.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                dbQualifiedName, assertPredicate);
    }

    private String assertEntityIsRegistered(final String typeName, final String property, final String value,
                                            final AssertPredicate assertPredicate) throws Exception {
        waitFor(80000, new Predicate() {
            @Override
            public void evaluate() throws Exception {
                Referenceable entity = atlasClient.getEntity(typeName, property, value);
                assertNotNull(entity);
                if (assertPredicate != null) {
                    assertPredicate.assertOnEntity(entity);
                }
            }
        });
        Referenceable entity = atlasClient.getEntity(typeName, property, value);
        return entity.getId()._getId();
    }

    private void assertEntityIsNotRegistered(final String typeName, final String property, final String value) throws Exception {
        waitFor(80000, new Predicate() {
            @Override
            public void evaluate() throws Exception {
                try {
                    atlasClient.getEntity(typeName, property, value);
                } catch (AtlasServiceException e) {
                    if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                        return;
                    }
                }
                fail(String.format("Entity was not supposed to exist for typeName = %s, attributeName = %s, "
                    + "attributeValue = %s", typeName, property, value));
            }
        });
    }

    @Test
    public void testLineage() throws Exception {
        String table1 = createTable(false);

        String db2 = createDatabase();
        String table2 = tableName();

        String query = String.format("create table %s.%s as select * from %s", db2, table2, table1);
        runCommand(query);
        String table1Id = assertTableIsRegistered(DEFAULT_DB, table1);
        String table2Id = assertTableIsRegistered(db2, table2);

        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, db2, table2);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        assertTrue(vertices.has(table1Id));
        assertTrue(vertices.has(table2Id));

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, table1);
        response = atlasClient.getOutputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        assertTrue(vertices.has(table1Id));
        assertTrue(vertices.has(table2Id));
    }

    //For ATLAS-448
    @Test
    public void testNoopOperation() throws Exception {
        runCommand("show compactions");
        runCommand("show transactions");
    }

    public interface AssertPredicate {
        void assertOnEntity(Referenceable entity) throws Exception;
    }

    public interface Predicate {
        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        void evaluate() throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     */
    protected void waitFor(int timeout, Predicate predicate) throws Exception {
        ParamChecker.notNull(predicate, "predicate");
        long mustEnd = System.currentTimeMillis() + timeout;

        while (true) {
            try {
                predicate.evaluate();
                return;
            } catch(Error | Exception e) {
                if (System.currentTimeMillis() >= mustEnd) {
                    fail("Assertions failed. Failing after waiting for timeout " + timeout + " msecs", e);
                }
                LOG.debug("Waiting up to " + (mustEnd - System.currentTimeMillis()) + " msec as assertion failed", e);
                Thread.sleep(400);
            }
        }
    }
}
