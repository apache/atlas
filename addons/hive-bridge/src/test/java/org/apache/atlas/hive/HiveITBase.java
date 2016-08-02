/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.hook.HiveHookIT;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.util.List;

import static org.apache.atlas.AtlasClient.NAME;
import static org.apache.atlas.hive.hook.HiveHook.lower;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class HiveITBase {
    private static final Logger LOG = LoggerFactory.getLogger(HiveITBase.class);

    protected static final String DGI_URL = "http://localhost:21000/";
    protected static final String CLUSTER_NAME = "primary";
    public static final String DEFAULT_DB = "default";

    protected static final String PART_FILE = "2015-01-01";
    protected Driver driver;
    protected AtlasClient atlasClient;
    protected HiveMetaStoreBridge hiveMetaStoreBridge;
    protected SessionState ss;

    protected HiveConf conf;

    protected static final String INPUTS = AtlasClient.PROCESS_ATTRIBUTE_INPUTS;
    protected static final String OUTPUTS = AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS;
    protected Driver driverWithoutContext;

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

        hiveMetaStoreBridge = new HiveMetaStoreBridge(configuration, conf, atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();

        HiveConf conf = new HiveConf();
        conf.set("hive.exec.post.hooks", "");
        SessionState ss = new SessionState(conf);
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);
        driverWithoutContext = new Driver(conf);
    }

    protected void runCommand(String cmd) throws Exception {
        runCommandWithDelay(cmd, 0);
    }

    protected void runCommand(Driver driver, String cmd) throws Exception {
        runCommandWithDelay(driver, cmd, 0);
    }

    protected void runCommandWithDelay(String cmd, int sleepMs) throws Exception {
        runCommandWithDelay(driver, cmd, sleepMs);
    }

    protected void runCommandWithDelay(Driver driver, String cmd, int sleepMs) throws Exception {
        LOG.debug("Running command '{}'", cmd);
        ss.setCommandType(null);
        CommandProcessorResponse response = driver.run(cmd);
        assertEquals(response.getResponseCode(), 0);
        if (sleepMs != 0) {
            Thread.sleep(sleepMs);
        }
    }

    protected String createTestDFSPath(String path) throws Exception {
        return "pfile://" + mkdir(path);
    }

    protected String mkdir(String tag) throws Exception {
        String filename = "./target/" + tag + "-data-" + random();
        File file = new File(filename);
        file.mkdirs();
        return file.getAbsolutePath();
    }

    protected String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    protected String tableName() {
        return "table" + random();
    }

    protected String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        return assertTableIsRegistered(dbName, tableName, null, false);
    }

    protected String assertTableIsRegistered(String dbName, String tableName, HiveHookIT.AssertPredicate assertPredicate, boolean isTemporary) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName, isTemporary);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName,
                assertPredicate);
    }

    protected String assertEntityIsRegistered(final String typeName, final String property, final String value,
                                            final HiveHookIT.AssertPredicate assertPredicate) throws Exception {
        waitFor(80000, new HiveHookIT.Predicate() {
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
                Thread.sleep(5000);
            }
        }
    }

    protected String getTableProcessQualifiedName(String dbName, String tableName) throws Exception {
        return HiveMetaStoreBridge.getTableProcessQualifiedName(CLUSTER_NAME,
                hiveMetaStoreBridge.hiveClient.getTable(dbName, tableName));
    }

    protected void validateHDFSPaths(Referenceable processReference, String attributeName, String... testPaths) throws Exception {
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

    protected String assertDatabaseIsRegistered(String dbName) throws Exception {
        return assertDatabaseIsRegistered(dbName, null);
    }

    protected String assertDatabaseIsRegistered(String dbName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(CLUSTER_NAME, dbName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_DB.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                dbQualifiedName, assertPredicate);
    }
}
