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

package org.apache.atlas.impala;

import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_QUALIFIED_NAME;
import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_RECENT_QUERIES;
import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.HIVE_TYPE_DB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.List;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.impala.hook.AtlasImpalaHookContext;
import org.apache.atlas.impala.hook.ImpalaLineageHook;
import org.apache.atlas.impala.model.ImpalaDataType;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class ImpalaLineageITBase {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaLineageITBase.class);

    public static final String DEFAULT_DB = "default";
    public static final String SEP = ":".intern();
    public static final String IO_SEP = "->".intern();
    protected static final String DGI_URL = "http://localhost:21000/";
    protected static final String CLUSTER_NAME = "primary";
    protected static final String PART_FILE = "2015-01-01";
    protected static final String INPUTS = "inputs";
    protected static final String OUTPUTS = "outputs";
    protected static AtlasClientV2 atlasClientV2;

    private static final String REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";
    private static final String ATTR_NAME = "name";

    // to push entity creation/update to HMS, so HMS hook can push the metadata notification
    // to Atlas, then the lineage notification from this tool can be created at Atlas
    protected static Driver              driverWithoutContext;
    protected static SessionState        ss;
    protected static HiveConf            conf;


    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        conf = new HiveConf();
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        HiveConf conf = new HiveConf();
        SessionState ss = new SessionState(conf);
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);
        driverWithoutContext = new Driver(conf);

        Configuration configuration = ApplicationProperties.get();

        String[] atlasEndPoint = configuration.getStringArray(ImpalaLineageHook.ATLAS_ENDPOINT);
        if (atlasEndPoint == null || atlasEndPoint.length == 0) {
            atlasEndPoint = new String[]{DGI_URL};
        }

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClientV2 = new AtlasClientV2(atlasEndPoint, new String[]{"admin", "admin"});
        } else {
            atlasClientV2 = new AtlasClientV2(atlasEndPoint);
        }

    }

    protected String assertEntityIsRegistered(final String typeName, final String property, final String value,
        final AssertPredicate assertPredicate) throws Exception {
        waitFor(80000, new Predicate() {
            @Override
            public void evaluate() throws Exception {
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = atlasClientV2.getEntityByAttribute(typeName, Collections
                    .singletonMap(property,value));
                AtlasEntity entity = atlasEntityWithExtInfo.getEntity();
                assertNotNull(entity);
                if (assertPredicate != null) {
                    assertPredicate.assertOnEntity(entity);
                }
            }
        });
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = atlasClientV2.getEntityByAttribute(typeName, Collections.singletonMap(property,value));
        AtlasEntity entity = atlasEntityWithExtInfo.getEntity();
        return (String) entity.getGuid();
    }

    protected String assertProcessIsRegistered(String processQFName, String queryString) throws Exception {
        try {
            LOG.debug("Searching for process with query {}", processQFName);

            return assertEntityIsRegistered(ImpalaDataType.IMPALA_PROCESS.getName(), ATTRIBUTE_QUALIFIED_NAME, processQFName, new AssertPredicate() {
                @Override
                public void assertOnEntity(final AtlasEntity entity) throws Exception {
                    List<String> recentQueries = (List<String>) entity.getAttribute(ATTRIBUTE_RECENT_QUERIES);

                    Assert.assertEquals(recentQueries.get(0), lower(queryString));
                }
            });
        } catch(Exception e) {
            LOG.error("Exception : ", e);
            throw e;
        }
    }

    protected String assertDatabaseIsRegistered(String dbName) throws Exception {
        return assertDatabaseIsRegistered(dbName, null);
    }

    protected String assertDatabaseIsRegistered(String dbName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for database: {}", dbName);

        String dbQualifiedName = dbName + AtlasImpalaHookContext.QNAME_SEP_CLUSTER_NAME +
            CLUSTER_NAME;

        dbQualifiedName = dbQualifiedName.toLowerCase();

        return assertEntityIsRegistered(HIVE_TYPE_DB, REFERENCEABLE_ATTRIBUTE_NAME, dbQualifiedName, assertPredicate);
    }

    protected String createDatabase() throws Exception {
        String dbName = dbName();

        return createDatabase(dbName);
    }

    protected  String createDatabase(String dbName) throws Exception {
        runCommand("CREATE DATABASE IF NOT EXISTS " + dbName);

        return dbName;
    }

    protected String createTable(String dbName, String columnsString) throws Exception {
        return createTable(dbName, columnsString, false);
    }

    protected String createTable(String dbName, String columnsString, boolean isPartitioned) throws Exception {
        String tableName = tableName();
        return createTable(dbName, tableName, columnsString, isPartitioned);
    }

    protected String createTable(String dbName, String tableName, String columnsString, boolean isPartitioned) throws Exception {
        runCommand("CREATE TABLE IF NOT EXISTS " + dbName + "." + tableName + " " + columnsString + " comment 'table comment' " + (isPartitioned ? " partitioned by(dt string)" : ""));

        return dbName + "." + tableName;
    }

    public interface AssertPredicate {
        void assertOnEntity(AtlasEntity entity) throws Exception;
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
                LOG.debug("Waiting up to {} msec as assertion failed", mustEnd - System.currentTimeMillis(), e);
                Thread.sleep(5000);
            }
        }
    }

    public static String lower(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return str.toLowerCase().trim();
    }

    protected void runCommand(String cmd) throws Exception {
        runCommandWithDelay(cmd, 0);
    }

    protected void runCommandWithDelay(String cmd, int sleepMs) throws Exception {
        runCommandWithDelay(driverWithoutContext, cmd, sleepMs);
    }

    protected void runCommandWithDelay(Driver driver, String cmd, int sleepMs) throws Exception {
        LOG.debug("Running command '{}'", cmd);
        CommandProcessorResponse response = driver.run(cmd);
        assertEquals(response.getResponseCode(), 0);
        if (sleepMs != 0) {
            Thread.sleep(sleepMs);
        }
    }

    protected String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    protected String tableName() {
        return "table_" + random();
    }
    protected String dbName() {return "db_" + random();}
}
