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

package org.apache.atlas.sqoop.hook;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.sqoop.model.SqoopDataModelGenerator;
import org.apache.atlas.sqoop.model.SqoopDataTypes;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.sqoop.SqoopJobDataPublisher;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

public class SqoopHookIT {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(SqoopHookIT.class);
    private static final String CLUSTER_NAME = "primary";
    public static final String DEFAULT_DB = "default";
    private static final int MAX_WAIT_TIME = 2000;
    private AtlasClient atlasClient;

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up sqoop session
        Configuration configuration = ApplicationProperties.get();
        atlasClient = new AtlasClient(configuration.getString("atlas.rest.address"));
        registerDataModels(atlasClient);
    }

    private void registerDataModels(AtlasClient client) throws Exception {
        // Make sure hive model exists
        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(new HiveConf(), atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();
        SqoopDataModelGenerator dataModelGenerator = new SqoopDataModelGenerator();

        //Register sqoop data model if its not already registered
        try {
            client.getType(SqoopDataTypes.SQOOP_PROCESS.getName());
            LOG.info("Sqoop data model is already registered!");
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                //Expected in case types do not exist
                LOG.info("Registering Sqoop data model");
                client.createType(dataModelGenerator.getModelAsJson());
            } else {
                throw ase;
            }
        }
    }

    @Test
    public void testSqoopImport() throws Exception {
        SqoopJobDataPublisher.Data d = new SqoopJobDataPublisher.Data("import", "jdbc:mysql:///localhost/db",
                "mysqluser", "mysql", "myTable", null, "default", "hiveTable", new Properties(),
                System.currentTimeMillis() - 100, System.currentTimeMillis());
        SqoopHook hook = new SqoopHook();
        hook.publish(d);
        Thread.sleep(1000);
        String storeName  = SqoopHook.getSqoopDBStoreName(d);
        assertDBStoreIsRegistered(storeName);
        String name = SqoopHook.getSqoopProcessName(d, CLUSTER_NAME);
        assertSqoopProcessIsRegistered(name);
        assertHiveTableIsRegistered(DEFAULT_DB, "hiveTable");
    }

    private String assertDBStoreIsRegistered(String storeName) throws Exception {
        LOG.debug("Searching for db store {}",  storeName);
        String query = String.format(
                "%s as t where name = '%s'" + " select t",
                SqoopDataTypes.SQOOP_DBDATASTORE.getName(), storeName);
        return assertEntityIsRegistered(query);
    }

    private String assertHiveTableIsRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
                "%s as t where tableName = '%s',  db where name = '%s' and clusterName = '%s'" + " select t",
                HiveDataTypes.HIVE_TABLE.getName(), tableName.toLowerCase(), dbName.toLowerCase(), CLUSTER_NAME);
        return assertEntityIsRegistered(query);
    }

    private String assertSqoopProcessIsRegistered(String processName) throws Exception {
        LOG.debug("Searching for sqoop process {}",  processName);
        String query = String.format(
                "%s as t where %s = '%s' select t",
                SqoopDataTypes.SQOOP_PROCESS.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processName);
        return assertEntityIsRegistered(query);
    }

    private String assertEntityIsRegistered(final String query) throws Exception {
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = atlasClient.search(query);
                return results.length() > 0;
            }
        });

        JSONArray results = atlasClient.search(query);
        JSONObject row = results.getJSONObject(0).getJSONObject("t");

        return row.getString("id");
    }

    protected void waitFor(int timeout, Predicate predicate) throws Exception {
        long mustEnd = System.currentTimeMillis() + timeout;

        boolean eval;
        while (!(eval = predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
            LOG.info("Waiting up to {} msec", mustEnd - System.currentTimeMillis());
            Thread.sleep(1000);
        }
        if (!eval) {
            throw new Exception("Waiting timed out after " + timeout + " msec");
        }
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

}
