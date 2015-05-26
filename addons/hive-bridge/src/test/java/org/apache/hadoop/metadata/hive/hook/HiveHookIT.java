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
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.bridge.HiveMetaStoreBridge;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

public class HiveHookIT {
    private static final String DGI_URL = "http://localhost:21000/";
    private static final String CLUSTER_NAME = "test";
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
        //weird, hive prepends test_ to table name
        hiveConf.set("hive.test.mode", "true");
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
        runCommand("create database " + dbName);

        assertDatabaseIsRegistered(dbName);
    }

    @Test
    public void testCreateTable() throws Exception {
        String dbName = "db" + random();
        runCommand("create database " + dbName);

        String tableName = "table" + random();
        runCommand("create table " + dbName + "." + tableName + "(id int, name string)");
        assertTableIsRegistered(dbName, tableName);

        tableName = "table" + random();
        runCommand("create table " + tableName + "(id int, name string) partitioned by(dt string)");
        assertTableIsRegistered("default", tableName);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered("default");
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = "table" + random();
        runCommand("create table " + tableName + "(id int, name string)");

        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered("default", ctasTableName);
        assertProcessIsRegistered(query);
    }

    @Test
    public void testCreateView() throws Exception {
        String tableName = "table" + random();
        runCommand("create table " + tableName + "(id int, name string)");

        String viewName = "table" + random();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered("default", viewName);
        assertProcessIsRegistered(query);
    }

    @Test
    public void testLoadData() throws Exception {
        String tableName = "table" + random();
        runCommand("create table test_" + tableName + "(id int, name string)");

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
    }

    @Test
    public void testInsert() throws Exception {
        String tableName = "table" + random();
        runCommand("create table " + tableName + "(id int, name string) partitioned by(dt string)");

        String insertTableName = "table" + random();
        runCommand("create table test_" + insertTableName + "(name string) partitioned by(dt string)");

        String query = "insert into " + insertTableName + " partition(dt = '2015-01-01') select name from "
                + tableName + " where dt = '2015-01-01'";

        runCommand(query);
        assertProcessIsRegistered(query);
        assertPartitionIsRegistered("default", "test_" + insertTableName, "2015-01-01");
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(5).toLowerCase();
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
        String tableName = "table" + random();
        runCommand("create table test_" + tableName + "(name string)");

        String filename = "pfile://" + mkdir("export");
        String query = "export table " + tableName + " to '" + filename + "'";
        runCommand(query);
        assertProcessIsRegistered(query);

        tableName = "table" + random();
        runCommand("create table " + tableName + "(name string)");

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        assertProcessIsRegistered(query);
    }

    @Test
    public void testSelect() throws Exception {
        String tableName = "table" + random();
        runCommand("create table " + tableName + "(id int, name string)");

        String query = "select * from " + tableName;
        runCommand(query);
        assertProcessIsRegistered(query);
    }

    private void assertProcessIsRegistered(String queryStr) throws Exception {
        String dslQuery = String.format("%s where queryText = \"%s\"", HiveDataTypes.HIVE_PROCESS.getName(), queryStr);
        assertEntityIsRegistered(dslQuery);
    }

    private void assertTableIsRegistered(String dbName, String tableName) throws Exception {
        String query = String.format("%s where name = '%s', dbName where name = '%s' and clusterName = '%s'",
                HiveDataTypes.HIVE_TABLE.getName(), tableName, dbName, CLUSTER_NAME);
        assertEntityIsRegistered(query);
    }

    private void assertDatabaseIsRegistered(String dbName) throws Exception {
        String query = String.format("%s where name = '%s' and clusterName = '%s'", HiveDataTypes.HIVE_DB.getName(),
                dbName, CLUSTER_NAME);
        assertEntityIsRegistered(query);
    }

    private void assertPartitionIsRegistered(String dbName, String tableName, String value) throws Exception {
        String typeName = HiveDataTypes.HIVE_PARTITION.getName();

        String dbType = HiveDataTypes.HIVE_DB.getName();
        String tableType = HiveDataTypes.HIVE_TABLE.getName();
        String gremlinQuery = String.format("g.V.has('__typeName', '%s').has('%s.values', ['%s']).as('p')."
                        + "out('__%s.tableName').has('%s.name', '%s').out('__%s.dbName').has('%s.name', '%s')"
                        + ".has('%s.clusterName', '%s').back('p').toList()", typeName, typeName, value, typeName,
                tableType, tableName, tableType, dbType, dbName, dbType, CLUSTER_NAME);
        JSONObject response = dgiCLient.searchByGremlin(gremlinQuery);
        JSONArray results = response.getJSONArray(MetadataServiceClient.RESULTS);
        Assert.assertEquals(results.length(), 1);
    }

    private void assertEntityIsRegistered(String dslQuery) throws Exception{
        JSONArray results = dgiCLient.searchByDSL(dslQuery);
        Assert.assertEquals(results.length(), 1);
    }
}
