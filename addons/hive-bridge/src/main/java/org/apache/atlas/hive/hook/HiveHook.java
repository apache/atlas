/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * AtlasHook sends lineage information to the AtlasSever.
 */
public class HiveHook implements ExecuteWithHookContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);

    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor;

    public static final String CONF_PREFIX = "atlas.hook.hive.";

    private static final String MIN_THREADS = CONF_PREFIX + "minThreads";
    private static final String MAX_THREADS = CONF_PREFIX + "maxThreads";
    private static final String KEEP_ALIVE_TIME = CONF_PREFIX + "keepAliveTime";
    public static final String CONF_SYNC = CONF_PREFIX + "synchronous";

    private static final int minThreadsDefault = 5;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;
    private static boolean typesRegistered = false;

    static {
        // anything shared should be initialized here and destroyed in the
        // shutdown hook The hook contract is weird in that it creates a
        // boatload of hooks.

        // initialize the async facility to process hook calls. We don't
        // want to do this inline since it adds plenty of overhead for the
        // query.
        HiveConf hiveConf = new HiveConf();
        int minThreads = hiveConf.getInt(MIN_THREADS, minThreadsDefault);
        int maxThreads = hiveConf.getInt(MAX_THREADS, maxThreadsDefault);
        long keepAliveTime = hiveConf.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);

        executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Atlas Logger %d").build());

        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        executor.shutdown();
                        executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                        executor = null;
                    } catch (InterruptedException ie) {
                        LOG.info("Interrupt received in shutdown.");
                    }
                    // shutdown client
                }
            });
        } catch (IllegalStateException is) {
            LOG.info("Attempting to send msg while shutdown in progress.");
        }

        LOG.info("Created Atlas Hook");
    }

    class HiveEvent {
        public HiveConf conf;

        public Set<ReadEntity> inputs;
        public Set<WriteEntity> outputs;

        public String user;
        public UserGroupInformation ugi;
        public HiveOperation operation;
        public QueryPlan queryPlan;
        public HookContext.HookType hookType;
        public JSONObject jsonPlan;
    }

    @Override
    public void run(final HookContext hookContext) throws Exception {
        if (executor == null) {
            LOG.info("No executor running. Bail.");
            return;
        }

        // clone to avoid concurrent access
        final HiveEvent event = new HiveEvent();
        final HiveConf conf = new HiveConf(hookContext.getConf());
        boolean debug = conf.get(CONF_SYNC, "false").equals("true");

        event.conf = conf;
        event.inputs = hookContext.getInputs();
        event.outputs = hookContext.getOutputs();

        event.user = hookContext.getUserName() == null ? hookContext.getUgi().getUserName() : hookContext.getUserName();
        event.ugi = hookContext.getUgi();
        event.operation = HiveOperation.valueOf(hookContext.getOperationName());
        event.queryPlan = hookContext.getQueryPlan();
        event.hookType = hookContext.getHookType();

        event.jsonPlan = getQueryPlan(event);

        if (debug) {
            fireAndForget(event);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        fireAndForget(event);
                    } catch (Throwable e) {
                        LOG.info("Atlas hook failed", e);
                    }
                }
            });
        }
    }

    private void fireAndForget(HiveEvent event) throws Exception {
        assert event.hookType == HookContext.HookType.POST_EXEC_HOOK : "Non-POST_EXEC_HOOK not supported!";

        LOG.info("Entered Atlas hook for hook type {} operation {}", event.hookType, event.operation);
        HiveMetaStoreBridge dgiBridge = new HiveMetaStoreBridge(event.conf, event.user, event.ugi);

        if (!typesRegistered) {
            dgiBridge.registerHiveDataModel();
            typesRegistered = true;
        }

        switch (event.operation) {
        case CREATEDATABASE:
            handleCreateDB(dgiBridge, event);
            break;

        case CREATETABLE:
            handleCreateTable(dgiBridge, event);
            break;

        case CREATETABLE_AS_SELECT:
        case CREATEVIEW:
        case LOAD:
        case EXPORT:
        case IMPORT:
        case QUERY:
            registerProcess(dgiBridge, event);
            break;

        case ALTERTABLE_RENAME:
        case ALTERVIEW_RENAME:
            renameTable(dgiBridge, event);
            break;

        case ALTERVIEW_AS:
            //update inputs/outputs?
            break;

        case ALTERTABLE_ADDCOLS:
        case ALTERTABLE_REPLACECOLS:
        case ALTERTABLE_RENAMECOL:
            break;

        default:
        }
    }

    private void renameTable(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        //crappy, no easy of getting new name
        assert event.inputs != null && event.inputs.size() == 1;
        assert event.outputs != null && event.outputs.size() > 0;

        Table oldTable = event.inputs.iterator().next().getTable();
        Table newTable = null;
        for (WriteEntity writeEntity : event.outputs) {
            if (writeEntity.getType() == Entity.Type.TABLE) {
                Table table = writeEntity.getTable();
                if (table.getDbName().equals(oldTable.getDbName()) && !table.getTableName()
                        .equals(oldTable.getTableName())) {
                    newTable = table;
                    break;
                }
            }
        }
        if (newTable == null) {
            LOG.warn("Failed to deduct new name for " + event.queryPlan.getQueryStr());
            return;
        }

        Referenceable dbReferenceable = dgiBridge.registerDatabase(oldTable.getDbName());
        Referenceable tableReferenceable =
                dgiBridge.registerTable(dbReferenceable, oldTable.getDbName(), oldTable.getTableName());
        LOG.info("Updating entity name {}.{} to {}", oldTable.getDbName(), oldTable.getTableName(),
                newTable.getTableName());
        dgiBridge.updateTable(tableReferenceable, newTable);
    }

    private void handleCreateTable(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        for (WriteEntity entity : event.outputs) {
            if (entity.getType() == Entity.Type.TABLE) {

                Table table = entity.getTable();
                Referenceable dbReferenceable = dgiBridge.registerDatabase(table.getDbName());
                dgiBridge.registerTable(dbReferenceable, table.getDbName(), table.getTableName());
            }
        }
    }

    private void handleCreateDB(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        for (WriteEntity entity : event.outputs) {
            if (entity.getType() == Entity.Type.DATABASE) {
                dgiBridge.registerDatabase(entity.getDatabase().getName());
            }
        }
    }

    private String normalize(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return str.toLowerCase().trim();
    }

    private void registerProcess(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        Set<ReadEntity> inputs = event.inputs;
        Set<WriteEntity> outputs = event.outputs;

        //Even explain CTAS has operation name as CREATETABLE_AS_SELECT
        if (inputs.isEmpty() && outputs.isEmpty()) {
            LOG.info("Explain statement. Skipping...");
        }

        if (event.queryPlan == null) {
            LOG.info("Query plan is missing. Skipping...");
        }

        String queryId = event.queryPlan.getQueryId();
        String queryStr = normalize(event.queryPlan.getQueryStr());
        long queryStartTime = event.queryPlan.getQueryStartTime();

        LOG.debug("Registering CTAS query: {}", queryStr);
        Referenceable processReferenceable = dgiBridge.getProcessReference(queryStr);
        if (processReferenceable == null) {
            processReferenceable = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());
            processReferenceable.set("name", event.operation.getOperationName());
            processReferenceable.set("startTime", queryStartTime);
            processReferenceable.set("userName", event.user);

            List<Referenceable> source = new ArrayList<>();
            for (ReadEntity readEntity : inputs) {
                if (readEntity.getType() == Entity.Type.TABLE) {
                    Table table = readEntity.getTable();
                    String dbName = table.getDbName();
                    source.add(dgiBridge.registerTable(dbName, table.getTableName()));
                }
                if (readEntity.getType() == Entity.Type.PARTITION) {
                    dgiBridge.registerPartition(readEntity.getPartition());
                }
            }
            processReferenceable.set("inputs", source);

            List<Referenceable> target = new ArrayList<>();
            for (WriteEntity writeEntity : outputs) {
                if (writeEntity.getType() == Entity.Type.TABLE || writeEntity.getType() == Entity.Type.PARTITION) {
                    Table table = writeEntity.getTable();
                    String dbName = table.getDbName();
                    target.add(dgiBridge.registerTable(dbName, table.getTableName()));
                }
                if (writeEntity.getType() == Entity.Type.PARTITION) {
                    dgiBridge.registerPartition(writeEntity.getPartition());
                }
            }
            processReferenceable.set("outputs", target);
            processReferenceable.set("queryText", queryStr);
            processReferenceable.set("queryId", queryId);
            processReferenceable.set("queryPlan", event.jsonPlan.toString());
            processReferenceable.set("endTime", System.currentTimeMillis());

            //TODO set
            processReferenceable.set("queryGraph", "queryGraph");
            dgiBridge.createInstance(processReferenceable);
        } else {
            LOG.debug("Query {} is already registered", queryStr);
        }
    }


    private JSONObject getQueryPlan(HiveEvent event) throws Exception {
        try {
            ExplainTask explain = new ExplainTask();
            explain.initialize(event.conf, event.queryPlan, null);
            List<Task<?>> rootTasks = event.queryPlan.getRootTasks();
            return explain.getJSONPlan(null, null, rootTasks, event.queryPlan.getFetchTask(), true, false, false);
        } catch (Exception e) {
            LOG.warn("Failed to get queryplan", e);
            return new JSONObject();
        }
    }
}
