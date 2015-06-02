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

package org.apache.hadoop.metadata.hive.hook;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.bridge.HiveMetaStoreBridge;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.log4j.spi.LoggerFactory;
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
    private MetadataServiceClient dgiCLient;
    private SessionState ss;

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        HiveConf conf = getHiveConf();
        driver = new Driver(conf);
        ss = new SessionState(conf, System.getProperty("user.name"));
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);

        dgiCLient = new MetadataServiceClient(DGI_URL);
    }

    private HiveConf getHiveConf() {
        HiveConf hiveConf = new HiveConf(this.getClass());
        hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
        hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, HiveHook.class.getName());
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, System.getProperty("user.dir") + "/target/metastore");
        hiveConf.set(HiveMetaStoreBridge.DGI_URL_PROPERTY, DGI_URL);
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:./target/metastore_db;create=true");
        hiveConf.set("hive.hook.dgi.synchronous", "true");
        hiveConf.set(HiveMetaStoreBridge.HIVE_CLUSTER_NAME, CLUSTER_NAME);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODE, true);  //to not use hdfs
        hiveConf.setVar(HiveConf.ConfVars.HIVETESTMODEPREFIX, "");
        hiveConf.set("fs.pfile.impl", "org.apache.hadoop.fs.ProxyLocalFileSystem");
        return hiveConf;
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
        runCommand("create table " + tableName + "(id int, name string)" + (partition ? " partitioned by(dt string)"
                : ""));
        return tableName;
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        runCommand("create table " + dbName + "." + tableName + "(id int, name string)");
        assertTableIsRegistered(dbName, tableName);

        tableName = createTable();
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableRef = dgiCLient.getEntity(tableId);
        Assert.assertEquals(tableRef.get("tableType"), TableType.MANAGED_TABLE.name());

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered(DEFAULT_DB);
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = createTable();
        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, ctasTableName);
        assertProcessIsRegistered(query);
    }

    @Test
    public void testCreateView() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, viewName);
        assertProcessIsRegistered(query);
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
        String query = "insert into " + insertTableName + " partition(dt = '2015-01-01') select id, name from "
                + tableName + " where dt = '2015-01-01'";

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
        String query = "export table " + tableName + " to '" + filename + "'";
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
    }

    @Test
    public void testAlterTable() throws Exception {
        String tableName = createTable();
        String newName = tableName();
        String query = "alter table " + tableName + " rename to " + newName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, newName);
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    @Test
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
        String dslQuery = String.format("%s where queryText = \"%s\"", HiveDataTypes.HIVE_PROCESS.getName(), queryStr);
        assertEntityIsRegistered(dslQuery, true);
    }

    private String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        return assertTableIsRegistered(dbName, tableName, true);
    }

    private String assertTableIsNotRegistered(String dbName, String tableName) throws Exception {
        return assertTableIsRegistered(dbName, tableName, false);
    }

    private String assertTableIsRegistered(String dbName, String tableName, boolean registered) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format("%s as t where name = '%s', dbName where name = '%s' and clusterName = '%s'"
                + " select t", HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(),
                CLUSTER_NAME);
        return assertEntityIsRegistered(query, registered);
    }

    private String assertDatabaseIsRegistered(String dbName) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String query = String.format("%s where name = '%s' and clusterName = '%s'", HiveDataTypes.HIVE_DB.getName(),
                dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query, true);
    }

    private void assertPartitionIsRegistered(String dbName, String tableName, String value) throws Exception {
        String typeName = HiveDataTypes.HIVE_PARTITION.getName();
        String dbType = HiveDataTypes.HIVE_DB.getName();
        String tableType = HiveDataTypes.HIVE_TABLE.getName();

        LOG.debug("Searching for partition of {}.{} with values {}", dbName, tableName, value);
        String gremlinQuery = String.format("g.V.has('__typeName', '%s').has('%s.values', ['%s']).as('p')."
                        + "out('__%s.tableName').has('%s.name', '%s').out('__%s.dbName').has('%s.name', '%s')"
                        + ".has('%s.clusterName', '%s').back('p').toList()", typeName, typeName, value, typeName,
                tableType, tableName.toLowerCase(), tableType, dbType, dbName.toLowerCase(), dbType, CLUSTER_NAME);
        JSONObject response = dgiCLient.searchByGremlin(gremlinQuery);
        JSONArray results = response.getJSONArray(MetadataServiceClient.RESULTS);
        Assert.assertEquals(results.length(), 1);
    }

    private String assertEntityIsRegistered(String dslQuery, boolean registered) throws Exception{
        JSONArray results = dgiCLient.searchByDSL(dslQuery);
        if (registered) {
            Assert.assertEquals(results.length(), 1);
            JSONObject row = results.getJSONObject(0);
            if (row.has("$id$")) {
                return row.getJSONObject("$id$").getString("id");
            } else {
                return row.getJSONObject("_col_0").getString("id");
            }
        } else {
            Assert.assertEquals(results.length(), 0);
            return null;
        }
    }
}
