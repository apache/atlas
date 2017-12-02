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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.sqoop.model.SqoopDataTypes;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.sqoop.SqoopJobDataPublisher;
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
        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClient = new AtlasClient(configuration.getStringArray(HiveMetaStoreBridge.ATLAS_ENDPOINT), new String[]{"admin", "admin"});
        } else {
            atlasClient = new AtlasClient(configuration.getStringArray(HiveMetaStoreBridge.ATLAS_ENDPOINT));
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

    @Test
    public void testSqoopExport() throws Exception {
        SqoopJobDataPublisher.Data d = new SqoopJobDataPublisher.Data("export", "jdbc:mysql:///localhost/db",
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
                "%s as t where " + AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME + " = '%s'" + " select t",
                SqoopDataTypes.SQOOP_DBDATASTORE.getName(), storeName);
        return assertEntityIsRegistered(query);
    }

    private String assertHiveTableIsRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String query = String.format(
                "%s as t where " + AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME + " = '%s',  db where " + AtlasClient.NAME + " = '%s' and clusterName = '%s'" + " select t",
                HiveDataTypes.HIVE_TABLE.getName(), HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName), dbName.toLowerCase(), CLUSTER_NAME);
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
                ArrayNode results = atlasClient.search(query, 10, 0);
                return results.size() > 0;
            }
        });

        ArrayNode results = atlasClient.search(query, 10, 0);
        JsonNode row = results.get(0).get("t");

        return row.get("id").asText();
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
