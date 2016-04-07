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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class HiveHookIT {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HiveHookIT.class);

    private static final String DGI_URL = "http://localhost:21000/";
    private static final String CLUSTER_NAME = "test";
    public static final String DEFAULT_DB = "default";
    private Driver driver;
    private AtlasClient dgiCLient;
    private SessionState ss;
    
    private static final String INPUTS = AtlasClient.PROCESS_ATTRIBUTE_INPUTS;
    private static final String OUTPUTS = AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS;

    private enum QUERY_TYPE {
        GREMLIN,
        DSL
    }

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        HiveConf conf = new HiveConf();
        //Run in local mode
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.default.name", "file:///'");
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        driver = new Driver(conf);
        ss = new SessionState(conf, System.getProperty("user.name"));
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);

        Configuration configuration = ApplicationProperties.get();
        dgiCLient = new AtlasClient(configuration.getString(HiveMetaStoreBridge.ATLAS_ENDPOINT, DGI_URL));

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(conf, configuration);
        hiveMetaStoreBridge.registerHiveDataModel();

    }

    private void runCommand(String cmd) throws Exception {
        LOG.debug("Running command '{}'", cmd);
        ss.setCommandType(null);
        CommandProcessorResponse response = driver.run(cmd);
        assertEquals(response.getResponseCode(), 0);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1', 'p2'='v2')");
        String dbId = assertDatabaseIsRegistered(dbName);

        Referenceable definition = dgiCLient.getEntity(dbId);
        Map params = (Map) definition.get(HiveDataModelGenerator.PARAMETERS);
        Assert.assertNotNull(params);
        Assert.assertEquals(params.size(), 2);
        Assert.assertEquals(params.get("p1"), "v1");

        //There should be just one entity per dbname
        runCommand("drop database " + dbName);
        runCommand("create database " + dbName);
        String dbid = assertDatabaseIsRegistered(dbName);

        //assert on qualified name
        Referenceable dbEntity = dgiCLient.getEntity(dbid);
        Assert.assertEquals(dbEntity.get("qualifiedName"), dbName.toLowerCase() + "@" + CLUSTER_NAME);

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
        assertTableIsRegistered(dbName, tableName);

        //there is only one instance of column registered
        String colId = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName), colName));
        Referenceable colEntity = dgiCLient.getEntity(colId);
        Assert.assertEquals(colEntity.get("qualifiedName"), String.format("%s.%s.%s@%s", dbName.toLowerCase(),
                tableName.toLowerCase(), colName.toLowerCase(), CLUSTER_NAME));

        tableName = createTable();
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Assert.assertEquals(tableRef.get("tableType"), TableType.MANAGED_TABLE.name());
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.COMMENT), "table comment");
        String entityName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.NAME), entityName);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.NAME), "default." + tableName.toLowerCase() + "@" + CLUSTER_NAME);

        final Referenceable sdRef = (Referenceable) tableRef.get("sd");
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_IS_STORED_AS_SUB_DIRS), false);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered(DEFAULT_DB);
    }

    @Test
    public void testCreateExternalTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        String colName = columnName();

        String pFile = createTestDFSPath("parentPath");
        final String query = String.format("create EXTERNAL table %s.%s( %s, %s) location '%s'", dbName , tableName , colName + " int", "name string",  pFile);
        runCommand(query);
        String tableId = assertTableIsRegistered(dbName, tableName);

        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, pFile, INPUTS);
        validateOutputTables(processReference, tableId);
    }

    private void validateOutputTables(Referenceable processReference, String... expectedTableGuids) throws Exception {
       validateTables(processReference, OUTPUTS, expectedTableGuids);
    }

    private void validateInputTables(Referenceable processReference, String... expectedTableGuids) throws Exception {
        validateTables(processReference, INPUTS, expectedTableGuids);
    }

    private void validateTables(Referenceable processReference, String attrName, String... expectedTableGuids) throws Exception {
        List<Id> tableRef = (List<Id>) processReference.get(attrName);
        for(int i = 0; i < expectedTableGuids.length; i++) {
            Assert.assertEquals(tableRef.get(i)._getId(), expectedTableGuids[i]);
        }
    }

    private String assertColumnIsRegistered(String colName) throws Exception {
        LOG.debug("Searching for column {}", colName.toLowerCase());
        String query =
                String.format("%s where qualifiedName = '%s'", HiveDataTypes.HIVE_COLUMN.getName(), colName.toLowerCase());
        return assertEntityIsRegistered(query);
    }

    private void assertColumnIsNotRegistered(String colName) throws Exception {
        LOG.debug("Searching for column {}", colName);
        String query =
            String.format("%s where qualifiedName = '%s'", HiveDataTypes.HIVE_COLUMN.getName(), colName.toLowerCase());
        assertEntityIsNotRegistered(QUERY_TYPE.DSL, query);
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = createTable();
        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
        assertTableIsRegistered(DEFAULT_DB, ctasTableName);
    }

    @Test
    public void testCreateView() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
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
        assertProcessIsRegistered(query);
        String viewId = assertTableIsRegistered(DEFAULT_DB, viewName);

        //Check lineage which includes table1
        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        JSONObject response = dgiCLient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(viewId));
        Assert.assertTrue(vertices.has(table1Id));

        //Alter the view from table2
        String table2Name = createTable();
        query = "alter view " + viewName + " as select * from " + table2Name;
        runCommand(query);

        //Check if alter view process is reqistered
        assertProcessIsRegistered(query);
        String table2Id = assertTableIsRegistered(DEFAULT_DB, table2Name);
        Assert.assertEquals(assertTableIsRegistered(DEFAULT_DB, viewName), viewId);

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        response = dgiCLient.getInputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(viewId));

        //This is through the alter view process
        Assert.assertTrue(vertices.has(table2Id));

        //This is through the Create view process
        Assert.assertTrue(vertices.has(table1Id));

        //Outputs dont exist
        response = dgiCLient.getOutputGraph(datasetName);
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

        assertProcessIsRegistered(query);
    }

    @Test
    public void testLoadLocalPathIntoPartition() throws Exception {
        String tableName = createTable(true);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName +  " partition(dt = '2015-01-01')";
        runCommand(query);

        validateProcess(query, 0, 1);
    }

    @Test
    public void testLoadDFSPath() throws Exception {
        String tableName = createTable(true, true, false);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String loadFile = createTestDFSFile("loadDFSFile");
        String query = "load data inpath '" + loadFile + "' into table " + tableName + " partition(dt = '2015-01-01')";
        runCommand(query);

        Referenceable processReference = validateProcess(query, 1, 1);

        validateHDFSPaths(processReference, loadFile, INPUTS);

        validateOutputTables(processReference, tableId);
    }

    private Referenceable validateProcess(String query, int numInputs, int numOutputs) throws Exception {
        String processId = assertProcessIsRegistered(query);
        Referenceable process = dgiCLient.getEntity(processId);
        if (numInputs == 0) {
            Assert.assertNull(process.get(INPUTS));
        } else {
            Assert.assertEquals(((List<Referenceable>) process.get(INPUTS)).size(), numInputs);
        }

        if (numOutputs == 0) {
            Assert.assertNull(process.get(OUTPUTS));
        } else {
            Assert.assertEquals(((List<Id>) process.get(OUTPUTS)).size(), numOutputs);
        }

        return process;
    }

    private Referenceable validateProcess(String query, String[] inputs, String[] outputs) throws Exception {
        String processId = assertProcessIsRegistered(query);
        Referenceable process = dgiCLient.getEntity(processId);
        if (inputs == null) {
            Assert.assertNull(process.get(INPUTS));
        } else {
            Assert.assertEquals(((List<Referenceable>) process.get(INPUTS)).size(), inputs.length);
            validateInputTables(process, inputs);
        }

        if (outputs == null) {
            Assert.assertNull(process.get(OUTPUTS));
        } else {
            Assert.assertEquals(((List<Id>) process.get(OUTPUTS)).size(), outputs.length);
            validateOutputTables(process, outputs);
        }

        return process;
    }

    @Test
    public void testInsertIntoTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable();
        String query =
                "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);

        String inputTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);

        validateProcess(query, new String[] {inputTableId}, new String[] {opTableId});
    }

    @Test
    public void testInsertIntoLocalDir() throws Exception {
        String tableName = createTable();
        File randomLocalPath = File.createTempFile("hiverandom", ".tmp");
        String query =
            "insert overwrite LOCAL DIRECTORY '" + randomLocalPath.getAbsolutePath() + "' select id, name from " + tableName;

        runCommand(query);
        validateProcess(query, 1, 0);

        assertTableIsRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testInsertIntoDFSDir() throws Exception {
        String tableName = createTable();
        String pFile = createTestDFSPath("somedfspath");
        String query =
            "insert overwrite DIRECTORY '" + pFile  + "' select id, name from " + tableName;

        runCommand(query);
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, pFile, OUTPUTS);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        validateInputTables(processReference, tableId);
    }

    @Test
    public void testInsertIntoTempTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable(false, false, true);
        String query =
            "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);
        validateProcess(query, 1, 1);

        String ipTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);
        validateProcess(query, new String[] {ipTableId}, new String[] {opTableId});
    }

    @Test
    public void testInsertIntoPartition() throws Exception {
        String tableName = createTable(true);
        String insertTableName = createTable(true);
        String query =
            "insert into " + insertTableName + " partition(dt = '2015-01-01') select id, name from " + tableName
                + " where dt = '2015-01-01'";
        runCommand(query);
        validateProcess(query, 1, 1);

        String ipTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);
        validateProcess(query, new String[] {ipTableId}, new String[] {opTableId});
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

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String filename = "pfile://" + mkdir("export");
        String query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, OUTPUTS);

        validateInputTables(processReference, tableId);

        //Import
        tableName = createTable(false);
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, INPUTS);

        validateOutputTables(processReference, tableId);

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
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, OUTPUTS);

        validateInputTables(processReference, tableId);

        //Import
        tableName = createTable(true);
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, INPUTS);

        validateOutputTables(processReference, tableId);


    }

    @Test
    public void testIgnoreSelect() throws Exception {
        String tableName = createTable();
        String query = "select * from " + tableName;
        runCommand(query);
        assertProcessIsNotRegistered(query);

        //check with uppercase table name
        query = "SELECT * from " + tableName.toUpperCase();
        runCommand(query);
        assertProcessIsNotRegistered(query);
    }

    @Test
    public void testAlterTableRename() throws Exception {
        String tableName = createTable();
        String newName = tableName();
        String query = "alter table " + tableName + " rename to " + newName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, newName);
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    private List<Referenceable> getColumns(String dbName, String tableName) throws Exception {
        String tableId = assertTableIsRegistered(dbName, tableName);
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        return ((List<Referenceable>)tableRef.get(HiveDataModelGenerator.COLUMNS));
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        String tableName = createTable();
        String column = columnName();
        String query = "alter table " + tableName + " add columns (" + column + " string)";
        runCommand(query);

        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), column));

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
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), colDropped));

        //Verify the number of columns present in the table
        final List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 1);

        Assert.assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), "name");
    }

    @Test
    public void testAlterTableChangeColumn() throws Exception {
        //Change name
        String oldColName = "name";
        String newColName = "name1";
        String tableName = createTable();
        String query = String.format("alter table %s change %s %s string", tableName, oldColName, newColName);
        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName));

        //Verify the number of columns present in the table
        List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);
        //Change column type
        oldColName = "name1";
        newColName = "name2";
        final String newColType = "int";
        query = String.format("alter table %s change column %s %s %s", tableName, oldColName, newColName, newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        String newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        Assert.assertEquals(columns.get(1).get("type"), "int");

        //Change name and add comment
        oldColName = "name2";
        newColName = "name3";
        final String comment = "added comment";
        query = String.format("alter table %s change column %s %s %s COMMENT '%s' after id", tableName, oldColName, newColName, newColType, comment);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        Assert.assertEquals(columns.get(1).get(HiveDataModelGenerator.COMMENT), comment);

        //Change column position
        oldColName = "name3";
        newColName = "name4";
        query = String.format("alter table %s change column %s %s %s first", tableName, oldColName, newColName, newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        //Change col position again
        Assert.assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), newColName);
        Assert.assertEquals(columns.get(1).get(HiveDataModelGenerator.NAME), "id");

        oldColName = "name4";
        newColName = "name5";
        query = String.format("alter table %s change column %s %s %s after id", tableName, oldColName, newColName, newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        //Check col position
        Assert.assertEquals(columns.get(1).get(HiveDataModelGenerator.NAME), newColName);
        Assert.assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), "id");
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

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        //Verify the number of columns present in the table
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Referenceable sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(sdRef.get("location"), testPath);

        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, testPath, INPUTS);

        validateOutputTables(processReference, tableId);

    }

    private String validateHDFSPaths(Referenceable processReference, String testPath, String attributeName) throws Exception {
        List<Id> hdfsPathRefs = (List<Id>) processReference.get(attributeName);

        final String testPathNormed = normalize(new Path(testPath).toString());
        String hdfsPathId = assertHDFSPathIsRegistered(testPathNormed);
        Assert.assertEquals(hdfsPathRefs.get(0)._getId(), hdfsPathId);

        Referenceable hdfsPathRef = dgiCLient.getEntity(hdfsPathId);
        Assert.assertEquals(hdfsPathRef.get("path"), testPathNormed);
        Assert.assertEquals(hdfsPathRef.get("name"), testPathNormed);
//        Assert.assertEquals(hdfsPathRef.get("name"), new Path(testPath).getName());
        Assert.assertEquals(hdfsPathRef.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), testPathNormed);

        return hdfsPathRef.getId()._getId();
    }


    private String assertHDFSPathIsRegistered(String path) throws Exception {
        final String typeName = FSDataTypes.HDFS_PATH().toString();
        final String parentTypeName = FSDataTypes.FS_PATH().toString();
        String gremlinQuery =
            String.format("g.V.has('__typeName', '%s').has('%s.path', \"%s\").toList()", typeName, parentTypeName,
                normalize(path));
        return assertEntityIsRegistered(gremlinQuery);
    }

    @Test
    public void testAlterTableFileFormat() throws Exception {
        String tableName = createTable();
        final String testFormat = "orc";
        String query = "alter table " + tableName + " set FILEFORMAT " + testFormat;
        runCommand(query);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Referenceable sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
        Assert.assertNotNull(sdRef.get("serdeInfo"));

        Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
        Assert.assertEquals(serdeInfo.get("serializationLib"), "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        Assert.assertNotNull(serdeInfo.get(HiveDataModelGenerator.PARAMETERS));
        Assert.assertEquals(((Map<String, String>)serdeInfo.get(HiveDataModelGenerator.PARAMETERS)).get("serialization.format"), "1");


        /**
         * Hive 'alter table stored as' is not supported - See https://issues.apache.org/jira/browse/HIVE-9576
         * query = "alter table " + tableName + " STORED AS " + testFormat.toUpperCase();
         * runCommand(query);

         * tableRef = dgiCLient.getEntity(tableId);
         * sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
         * Assert.assertEquals(((Map) sdRef.get(HiveDataModelGenerator.PARAMETERS)).get("orc.compress"), "ZLIB");
         */
    }

    @Test
    public void testAlterTableBucketingClusterSort() throws Exception {
        String tableName = createTable();
        ImmutableList<String> cols = ImmutableList.<String>of("id");
        runBucketSortQuery(tableName, 5, cols, cols);

        cols = ImmutableList.<String>of("id", "name");
        runBucketSortQuery(tableName, 2, cols, cols);
    }

    private void runBucketSortQuery(String tableName, int numBuckets,  ImmutableList<String> bucketCols,ImmutableList<String> sortCols) throws Exception {
        final String fmtQuery = "alter table %s CLUSTERED BY (%s) SORTED BY (%s) INTO %s BUCKETS";
        String query = String.format(fmtQuery, tableName, stripListBrackets(bucketCols.toString()), stripListBrackets(sortCols.toString()), numBuckets);
        runCommand(query);
        verifyBucketSortingProperties(tableName, numBuckets, bucketCols, sortCols);
    }

    private String stripListBrackets(String listElements) {
        return StringUtils.strip(StringUtils.strip(listElements, "["), "]");
    }

    private void verifyBucketSortingProperties(String tableName, int numBuckets, ImmutableList<String> bucketColNames, ImmutableList<String>  sortcolNames) throws Exception {

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Referenceable sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(((scala.math.BigInt) sdRef.get(HiveDataModelGenerator.STORAGE_NUM_BUCKETS)).intValue(), numBuckets);
        Assert.assertEquals(sdRef.get("bucketCols"), bucketColNames);

        List<Struct> hiveOrderStructList = (List<Struct>) sdRef.get("sortCols");
        Assert.assertNotNull(hiveOrderStructList);
        Assert.assertEquals(hiveOrderStructList.size(), sortcolNames.size());

        for (int i = 0; i < sortcolNames.size(); i++) {
            Assert.assertEquals(hiveOrderStructList.get(i).get("col"), sortcolNames.get(i));
            Assert.assertEquals(((scala.math.BigInt)hiveOrderStructList.get(i).get("order")).intValue(), 1);
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
        final String owner = "testOwner";
        String dbId = assertDatabaseIsRegistered(dbName);
        final String fmtQuery = "alter database %s set OWNER %s %s";
        String query = String.format(fmtQuery, dbName, "USER", owner);

        runCommand(query);

        assertDatabaseIsRegistered(dbName);
        Referenceable entity = dgiCLient.getEntity(dbId);
        Assert.assertEquals(entity.get(HiveDataModelGenerator.OWNER), owner);
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

    private void verifyEntityProperties(Entity.Type type, String entityName, Map<String, String> expectedProps, boolean checkIfNotExists) throws Exception {

        String entityId = null;

        switch(type) {
        case TABLE:
            entityId = assertTableIsRegistered(DEFAULT_DB, entityName);
            break;
        case DATABASE:
            entityId = assertDatabaseIsRegistered(entityName);
            break;
        }

        Referenceable ref = dgiCLient.getEntity(entityId);
        verifyProperties(ref, expectedProps, checkIfNotExists);
    }

    private void verifyTableSdProperties(String tableName, String serdeLib, Map<String, String> expectedProps) throws Exception {
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
        Assert.assertEquals(serdeInfo.get("serializationLib"), serdeLib);
        verifyProperties(serdeInfo, expectedProps, false);
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

    private String assertProcessIsRegistered(String queryStr) throws Exception {
        //        String dslQuery = String.format("%s where queryText = \"%s\"", HiveDataTypes.HIVE_PROCESS.getName(),
        //                normalize(queryStr));
        //        assertEntityIsRegistered(dslQuery, true);
        //todo replace with DSL
        String typeName = HiveDataTypes.HIVE_PROCESS.getName();
        String gremlinQuery =
                String.format("g.V.has('__typeName', '%s').has('%s.queryText', \"%s\").toList()", typeName, typeName,
                        normalize(queryStr));
        return assertEntityIsRegistered(gremlinQuery);
    }

    private void assertProcessIsNotRegistered(String queryStr) throws Exception {
        //        String dslQuery = String.format("%s where queryText = \"%s\"", HiveDataTypes.HIVE_PROCESS.getName(),
        //                normalize(queryStr));
        //        assertEntityIsRegistered(dslQuery, true);
        //todo replace with DSL
        String typeName = HiveDataTypes.HIVE_PROCESS.getName();
        String gremlinQuery =
            String.format("g.V.has('__typeName', '%s').has('%s.queryText', \"%s\").toList()", typeName, typeName,
                normalize(queryStr));
        assertEntityIsNotRegistered(QUERY_TYPE.GREMLIN, gremlinQuery);
    }

    private String normalize(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return StringEscapeUtils.escapeJava(str.toLowerCase());
    }

    private void assertTableIsNotRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
                "%s as t where tableName = '%s', db where name = '%s' and clusterName = '%s'" + " select t",
                HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);
        assertEntityIsNotRegistered(QUERY_TYPE.DSL, query);
    }

    private String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
                "%s as t where tableName = '%s', db where name = '%s' and clusterName = '%s'" + " select t",
                HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query, "t");
    }

    private String getTableEntity(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
            "%s as t where tableName = '%s', db where name = '%s' and clusterName = '%s'" + " select t",
            HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query, "t");
    }

    private String assertDatabaseIsRegistered(String dbName) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String query = String.format("%s where name = '%s' and clusterName = '%s'", HiveDataTypes.HIVE_DB.getName(),
                dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query);
    }

    private String assertEntityIsRegistered(final String query, String... arg) throws Exception {
        waitFor(60000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = dgiCLient.search(query);
                return results.length() == 1;
            }
        });

        String column = (arg.length > 0) ? arg[0] : "_col_0";

        JSONArray results = dgiCLient.search(query);
        JSONObject row = results.getJSONObject(0);
        if (row.has("__guid")) {
            return row.getString("__guid");
        } else if (row.has("$id$")) {
            return row.getJSONObject("$id$").getString("id");
        } else {
            return row.getJSONObject(column).getString("id");
        }
    }

    private void assertEntityIsNotRegistered(QUERY_TYPE queryType, String query) throws Exception {
        JSONArray results = null;
        switch(queryType) {
        case DSL :
            results = dgiCLient.searchByDSL(query);
            break;
        case GREMLIN :
            results = dgiCLient.searchByGremlin(query);
            break;
        }
        Assert.assertEquals(results.length(), 0);
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
        JSONObject response = dgiCLient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(table1Id));
        Assert.assertTrue(vertices.has(table2Id));

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, table1);
        response = dgiCLient.getOutputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(table1Id));
        Assert.assertTrue(vertices.has(table2Id));
    }

    //For ATLAS-448
    @Test
    public void testNoopOperation() throws Exception {
        runCommand("show compactions");
        runCommand("show transactions");
    }

    public interface Predicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        boolean evaluate() throws Exception;
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

        boolean eval;
        while (!(eval = predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
            LOG.info("Waiting up to {} msec", mustEnd - System.currentTimeMillis());
            Thread.sleep(100);
        }
        if (!eval) {
            throw new Exception("Waiting timed out after " + timeout + " msec");
        }
    }
}
