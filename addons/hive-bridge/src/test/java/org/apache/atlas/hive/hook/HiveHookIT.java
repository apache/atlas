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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.ParamChecker;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class HiveHookIT {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HiveHookIT.class);

    private static final String DGI_URL = "http://localhost:21000/";
    private static final String CLUSTER_NAME = "test";
    public static final String DEFAULT_DB = "default";
    private Driver driver;
    private AtlasClient dgiCLient;
    private SessionState ss;

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        HiveConf conf = new HiveConf();
        driver = new Driver(conf);
        ss = new SessionState(conf, System.getProperty("user.name"));
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);

        dgiCLient = new AtlasClient(DGI_URL);
    }

    private void runCommand(String cmd) throws Exception {
        ss.setCommandType(null);
        driver.run(cmd);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1', 'p2'='v2')");
        String dbId = assertDatabaseIsRegistered(dbName);

        Referenceable definition = dgiCLient.getEntity(dbId);
        Map params = (Map) definition.get("parameters");
        Assert.assertNotNull(params);
        Assert.assertEquals(params.size(), 2);
        Assert.assertEquals(params.get("p1"), "v1");

        //There should be just one entity per dbname
        runCommand("drop database " + dbName);
        runCommand("create database " + dbName);
        assertDatabaseIsRegistered(dbName);
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

    private String createTable() throws Exception {
        return createTable(true);
    }

    private String createTable(boolean partition) throws Exception {
        String tableName = tableName();
        runCommand("create table " + tableName + "(id int, name string) comment 'table comment' " + (partition ?
                " partitioned by(dt string)" : ""));
        return tableName;
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        String colName = "col" + random();
        runCommand("create table " + dbName + "." + tableName + "(" + colName + " int, name string)");
        assertTableIsRegistered(dbName, tableName);
        //there is only one instance of column registered
        assertColumnIsRegistered(colName);

        tableName = createTable();
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Assert.assertEquals(tableRef.get("tableType"), TableType.MANAGED_TABLE.name());
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.COMMENT), "table comment");
        String entityName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.NAME), entityName);

        final Referenceable sdRef = (Referenceable) tableRef.get("sd");
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_IS_STORED_AS_SUB_DIRS), false);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered(DEFAULT_DB);
    }

    private String assertColumnIsRegistered(String colName) throws Exception {
        LOG.debug("Searching for column {}", colName);
        String query =
                String.format("%s where name = '%s'", HiveDataTypes.HIVE_COLUMN.getName(), colName.toLowerCase());
        return assertEntityIsRegistered(query);
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
    public void testLoadData() throws Exception {
        String tableName = createTable(false);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
    }

    @Test
    public void testInsert() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable();
        String query =
                "insert into " + insertTableName + " partition(dt = '2015-01-01') select id, name from " + tableName
                        + " where dt = '2015-01-01'";

        runCommand(query);
        assertProcessIsRegistered(query);
        assertPartitionIsRegistered(DEFAULT_DB, insertTableName, "2015-01-01");
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
    public void testExportImport() throws Exception {
        String tableName = createTable(false);

        String filename = "pfile://" + mkdir("export");
        String query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);
        assertProcessIsRegistered(query);

        tableName = createTable(false);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        assertProcessIsRegistered(query);
    }

    @Test
    public void testSelect() throws Exception {
        String tableName = createTable();
        String query = "select * from " + tableName;
        runCommand(query);
        assertProcessIsRegistered(query);

        //single entity per query
        query = "SELECT * from " + tableName.toUpperCase();
        runCommand(query);
        assertProcessIsRegistered(query);
    }

    @Test(enabled = false)
    public void testAlterTable() throws Exception {
        String tableName = createTable();
        String newName = tableName();
        String query = "alter table " + tableName + " rename to " + newName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, newName);
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    @Test(enabled = false)
    public void testAlterView() throws Exception {
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

    private void assertProcessIsRegistered(String queryStr) throws Exception {
        //        String dslQuery = String.format("%s where queryText = \"%s\"", HiveDataTypes.HIVE_PROCESS.getName(),
        //                normalize(queryStr));
        //        assertEntityIsRegistered(dslQuery, true);
        //todo replace with DSL
        String typeName = HiveDataTypes.HIVE_PROCESS.getName();
        String gremlinQuery =
                String.format("g.V.has('__typeName', '%s').has('%s.queryText', \"%s\").toList()", typeName, typeName,
                        normalize(queryStr));
        assertEntityIsRegistered(gremlinQuery);
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
        assertEntityIsNotRegistered(query);
    }

    private String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
                "%s as t where tableName = '%s', db where name = '%s' and clusterName = '%s'" + " select t",
                HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query);
    }

    private String assertDatabaseIsRegistered(String dbName) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String query = String.format("%s where name = '%s' and clusterName = '%s'", HiveDataTypes.HIVE_DB.getName(),
                dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query);
    }

    private void assertPartitionIsRegistered(String dbName, String tableName, String value) throws Exception {
        String typeName = HiveDataTypes.HIVE_PARTITION.getName();
        String dbType = HiveDataTypes.HIVE_DB.getName();
        String tableType = HiveDataTypes.HIVE_TABLE.getName();

        LOG.debug("Searching for partition of {}.{} with values {}", dbName, tableName, value);
       /*  gremlinQuery = String.format("g.V.has('__typeName', '%s').has('%s.values', ['%s']).as('p')."
                        + "out('__%s.table').has('%s.tableName', '%s').out('__%s.db').has('%s.name', '%s')"
                        + ".has('%s.clusterName', '%s').back('p').toList()", typeName, typeName, value, typeName,
                 tableType, tableName.toLowerCase(), tableType, dbType, dbName.toLowerCase(), dbType, CLUSTER_NAME);
         */
        String dslQuery = String.format("%s as p where values = ['%s'], table where tableName = '%s', "
                               + "db where name = '%s' and clusterName = '%s' select p", typeName, value,
                            tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);

        assertEntityIsRegistered(dslQuery);
    }

    private String assertEntityIsRegistered(final String query) throws Exception {
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = dgiCLient.search(query);
                return results.length() == 1;
            }
        });

        JSONArray results = dgiCLient.search(query);
        JSONObject row = results.getJSONObject(0);
        if (row.has("__guid")) {
            return row.getString("__guid");
        } else if (row.has("$id$")) {
            return row.getJSONObject("$id$").getString("id");
        } else {
            return row.getJSONObject("_col_0").getString("id");
        }
    }

    private void assertEntityIsNotRegistered(String dslQuery) throws Exception {
        JSONArray results = dgiCLient.searchByDSL(dslQuery);
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
