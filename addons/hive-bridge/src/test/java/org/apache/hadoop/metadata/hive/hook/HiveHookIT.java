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
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.bridge.HiveMetaStoreBridge;
import org.apache.hadoop.metadata.hive.model.HiveDataModelGenerator;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HiveHookIT {
    private static final String DGI_URL = "http://localhost:21000/";
    private Driver driver;
    private MetadataServiceClient dgiCLient;
    private SessionState ss;

    @BeforeClass
    public void setUp() throws Exception {
        //Register hive types
        HiveDataModelGenerator hiveModel = new HiveDataModelGenerator();
        hiveModel.createDataModel();
        TypesDef typesDef = hiveModel.getTypesDef();
        String typesAsJson = TypesSerialization.toJson(typesDef);
        MetadataServiceClient dgiClient = new MetadataServiceClient(DGI_URL);
        try {
            dgiClient.createType(typesAsJson);
        } catch (Exception e) {
            //ignore if types are already defined
        }

        //Set-up hive session
        HiveConf conf = getHiveConf();
        driver = new Driver(conf);
        ss = new SessionState(conf);
        ss = ss.start(conf);
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
        hiveConf.set("debug", "true");
        return hiveConf;
    }

    private void runCommand(String cmd) throws Exception {
        ss.setCommandType(null);
        driver.run(cmd);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create database " + dbName);

        String typeName = HiveDataTypes.HIVE_DB.getName();
        JSONObject result = dgiCLient.search(typeName, "name", dbName);
        JSONArray results = (JSONArray) result.get("results");
        Assert.assertEquals(results.length(), 1);
        JSONObject resultRow = (JSONObject) results.get(0);
        Assert.assertEquals(resultRow.get(typeName + ".name"), dbName);
    }

    @Test(enabled = false)
    public void testCreateTable() throws Exception {
        String dbName = "db" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        runCommand("create database " + dbName);

        String tableName = "table" + RandomStringUtils.randomAlphanumeric(5);
        String queryStr = String.format("create table %s.%s(id int, name string)", dbName, tableName);
        runCommand(queryStr);

        String defaultTableName = "table" + RandomStringUtils.randomAlphanumeric(5);
        runCommand("create table " + defaultTableName + "(id int, name string)");

        runCommand("select * from " + defaultTableName);

        runCommand("select * from " + dbName + "." + tableName);

        String newTableName = "table" + RandomStringUtils.randomAlphanumeric(5);

        runCommand("create table " + newTableName + " as select * from " + defaultTableName);

        runCommand("create table " + dbName + "." + newTableName + " as select * from " + dbName + "." + tableName);

        newTableName = "table" + RandomStringUtils.randomAlphanumeric(5);
        runCommand("create table " + newTableName + " as select count(*) from " + defaultTableName);
    }
}
