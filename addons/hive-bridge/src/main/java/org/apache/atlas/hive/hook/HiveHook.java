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
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * AtlasHook sends lineage information to the AtlasSever.
 */
public class HiveHook extends AtlasHook implements ExecuteWithHookContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);

    public static final String CONF_PREFIX = "atlas.hook.hive.";
    private static final String MIN_THREADS = CONF_PREFIX + "minThreads";
    private static final String MAX_THREADS = CONF_PREFIX + "maxThreads";
    private static final String KEEP_ALIVE_TIME = CONF_PREFIX + "keepAliveTime";
    public static final String CONF_SYNC = CONF_PREFIX + "synchronous";
    public static final String QUEUE_SIZE = CONF_PREFIX + "queueSize";

    public static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();

    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor;

    private static final int minThreadsDefault = 1;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;
    private static final int queueSizeDefault = 10000;

    class HiveEvent {
        public Set<ReadEntity> inputs;
        public Set<WriteEntity> outputs;

        public String user;
        public UserGroupInformation ugi;
        public HiveOperation operation;
        public HookContext.HookType hookType;
        public JSONObject jsonPlan;
        public String queryId;
        public String queryStr;
        public Long queryStartTime;
    }

    private List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

    private static final HiveConf hiveConf;

    static {
        try {
            // initialize the async facility to process hook calls. We don't
            // want to do this inline since it adds plenty of overhead for the query.
            int minThreads = atlasProperties.getInt(MIN_THREADS, minThreadsDefault);
            int maxThreads = atlasProperties.getInt(MAX_THREADS, maxThreadsDefault);
            long keepAliveTime = atlasProperties.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);
            int queueSize = atlasProperties.getInt(QUEUE_SIZE, queueSizeDefault);

            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(queueSize),
                    new ThreadFactoryBuilder().setNameFormat("Atlas Logger %d").build());

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

            setupOperationMap();
        } catch (Exception e) {
            LOG.info("Attempting to send msg while shutdown in progress.", e);
        }

        hiveConf = new HiveConf();

        LOG.info("Created Atlas Hook");
    }

    private static void setupOperationMap() {
        //Populate OPERATION_MAP - string to HiveOperation mapping
        for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }
    }

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    @Override
    public void run(final HookContext hookContext) throws Exception {
        // clone to avoid concurrent access
        final HiveEvent event = new HiveEvent();
        final HiveConf conf = new HiveConf(hookContext.getConf());

        event.inputs = hookContext.getInputs();
        event.outputs = hookContext.getOutputs();

        event.user = getUser(hookContext.getUserName(), hookContext.getUgi());
        event.ugi = hookContext.getUgi();
        event.operation = OPERATION_MAP.get(hookContext.getOperationName());
        event.hookType = hookContext.getHookType();
        event.queryId = hookContext.getQueryPlan().getQueryId();
        event.queryStr = hookContext.getQueryPlan().getQueryStr();
        event.queryStartTime = hookContext.getQueryPlan().getQueryStartTime();

        event.jsonPlan = getQueryPlan(hookContext.getConf(), hookContext.getQueryPlan());

        boolean sync = conf.get(CONF_SYNC, "false").equals("true");
        if (sync) {
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

        HiveMetaStoreBridge dgiBridge = new HiveMetaStoreBridge(hiveConf, atlasProperties, event.user, event.ugi);

        switch (event.operation) {
        case CREATEDATABASE:
            handleEventOutputs(dgiBridge, event, Type.DATABASE);
            break;

        case CREATETABLE:
            handleEventOutputs(dgiBridge, event, Type.TABLE);
            break;

        case CREATETABLE_AS_SELECT:
        case CREATEVIEW:
        case ALTERVIEW_AS:
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

        case ALTERTABLE_FILEFORMAT:
        case ALTERTABLE_LOCATION:
        case ALTERTABLE_CLUSTER_SORT:
        case ALTERTABLE_BUCKETNUM:
        case ALTERTABLE_PROPERTIES:
        case ALTERVIEW_PROPERTIES:
        case ALTERTABLE_SERDEPROPERTIES:
        case ALTERTABLE_SERIALIZER:
            alterTable(dgiBridge, event);
            break;

        case ALTERTABLE_ADDCOLS:
        case ALTERTABLE_REPLACECOLS:
        case ALTERTABLE_RENAMECOL:
            alterTable(dgiBridge, event);
            break;

        case ALTERDATABASE:
        case ALTERDATABASE_OWNER:
            alterDatabase(dgiBridge, event);
            break;

        default:
        }

        notifyEntities(messages);
    }

    private void alterDatabase(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        assert event.outputs != null && event.outputs.size() > 0;

        for (WriteEntity writeEntity : event.outputs) {
            if (writeEntity.getType() == Type.DATABASE) {
                //Create/update table entity
                createOrUpdateEntities(dgiBridge, event.user, writeEntity);
            }
        }
    }

    private void alterTable(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        assert event.inputs != null && event.inputs.size() == 1;
        assert event.outputs != null && event.outputs.size() > 0;

        for (WriteEntity writeEntity : event.outputs) {
           //Below check should  filter out partition related ddls
           if (writeEntity.getType() == Entity.Type.TABLE) {
               //Create/update table entity
               createOrUpdateEntities(dgiBridge, event.user, writeEntity);
           }
        }
    }

    private void renameTable(HiveMetaStoreBridge dgiBridge, HiveEvent event) throws Exception {
        //crappy, no easy of getting new name
        assert event.inputs != null && event.inputs.size() == 1;
        assert event.outputs != null && event.outputs.size() > 0;

        //Update entity if not exists
        ReadEntity oldEntity = event.inputs.iterator().next();
        Table oldTable = oldEntity.getTable();

        for (WriteEntity writeEntity : event.outputs) {
            if (writeEntity.getType() == Entity.Type.TABLE) {
                Table newTable = writeEntity.getTable();
                if (newTable.getDbName().equals(oldTable.getDbName()) && !newTable.getTableName()
                    .equals(oldTable.getTableName())) {

                    //Create/update old table entity - create new entity and replace id
                    Referenceable tableEntity = createOrUpdateEntities(dgiBridge, event.user, writeEntity);
                    String oldQualifiedName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                            oldTable.getDbName(), oldTable.getTableName());
                    tableEntity.set(HiveDataModelGenerator.NAME, oldQualifiedName);
                    tableEntity.set(HiveDataModelGenerator.TABLE_NAME, oldTable.getTableName().toLowerCase());

                    String newQualifiedName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                            newTable.getDbName(), newTable.getTableName());

                    Referenceable newEntity = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
                    newEntity.set(HiveDataModelGenerator.NAME, newQualifiedName);
                    newEntity.set(HiveDataModelGenerator.TABLE_NAME, newTable.getTableName().toLowerCase());
                    messages.add(new HookNotification.EntityPartialUpdateRequest(event.user,
                            HiveDataTypes.HIVE_TABLE.getName(), HiveDataModelGenerator.NAME,
                            oldQualifiedName, newEntity));
                }
            }
        }
    }

    private Referenceable createOrUpdateEntities(HiveMetaStoreBridge dgiBridge, String user, Entity entity) throws Exception {
        Database db = null;
        Table table = null;
        Partition partition = null;
        List<Referenceable> entities = new ArrayList<>();

        switch (entity.getType()) {
        case DATABASE:
            db = entity.getDatabase();
            break;

        case TABLE:
            table = entity.getTable();
            db = dgiBridge.hiveClient.getDatabase(table.getDbName());
            break;

        case PARTITION:
            partition = entity.getPartition();
            table = partition.getTable();
            db = dgiBridge.hiveClient.getDatabase(table.getDbName());
            break;
        }

        db = dgiBridge.hiveClient.getDatabase(db.getName());
        Referenceable dbEntity = dgiBridge.createDBInstance(db);
        entities.add(dbEntity);

        Referenceable tableEntity = null;
        if (table != null) {
            table = dgiBridge.hiveClient.getTable(table.getDbName(), table.getTableName());
            tableEntity = dgiBridge.createTableInstance(dbEntity, table);
            entities.add(tableEntity);
        }

        messages.add(new HookNotification.EntityUpdateRequest(user, entities));
        return tableEntity;
    }

    private void handleEventOutputs(HiveMetaStoreBridge dgiBridge, HiveEvent event, Type entityType) throws Exception {
        for (WriteEntity entity : event.outputs) {
            if (entity.getType() == entityType) {
                createOrUpdateEntities(dgiBridge, event.user, entity);
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
            return;
        }

        if (event.queryId == null) {
            LOG.info("Query id/plan is missing for {}" , event.queryStr);
        }

        String queryStr = normalize(event.queryStr);
        LOG.debug("Registering query: {}", queryStr);

        Map<String, Referenceable>  source = new LinkedHashMap<>();
        Map<String, Referenceable> target = new LinkedHashMap<>();

        boolean isSelectQuery = isSelectQuery(event);

        // Also filter out select queries which do not modify data
        if (!isSelectQuery) {
            for (ReadEntity readEntity : inputs) {
                if (readEntity.getType() == Type.TABLE || readEntity.getType() == Type.PARTITION) {
                    final String tblQFName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(),readEntity.getTable().getDbName(), readEntity.getTable().getTableName());
                    if (!source.containsKey(tblQFName)) {
                        Referenceable inTable = createOrUpdateEntities(dgiBridge, event.user, readEntity);
                        source.put(tblQFName, inTable);
                    }
                }
            }

            for (WriteEntity writeEntity : outputs) {
                if (writeEntity.getType() == Type.TABLE || writeEntity.getType() == Type.PARTITION) {
                    Referenceable outTable = createOrUpdateEntities(dgiBridge, event.user, writeEntity);
                    final String tblQFName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(), writeEntity.getTable().getDbName(), writeEntity.getTable().getTableName());
                    if (!target.containsKey(tblQFName)) {
                        target.put(tblQFName, outTable);
                    }
                }
            }

            if (source.size() > 0 || target.size() > 0) {
                Referenceable processReferenceable = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());

                List<Referenceable> sourceList = new ArrayList<>(source.values());
                List<Referenceable> targetList = new ArrayList<>(target.values());

                //The serialization code expected a list
                processReferenceable.set("inputs", sourceList);
                processReferenceable.set("outputs", targetList);
                processReferenceable.set("name", queryStr);
                processReferenceable.set("operationType", event.operation.getOperationName());
                processReferenceable.set("startTime", event.queryStartTime);
                processReferenceable.set("userName", event.user);
                processReferenceable.set("queryText", queryStr);
                processReferenceable.set("queryId", event.queryId);
                processReferenceable.set("queryPlan", event.jsonPlan.toString());
                processReferenceable.set("endTime", System.currentTimeMillis());
                //TODO set queryGraph
                messages.add(new HookNotification.EntityCreateRequest(event.user, processReferenceable));
            } else {
                LOG.info("Skipped query {} since it has no inputs or resulting outputs", queryStr);
            }
        } else {
            LOG.info("Skipped query {} for processing since it is a select query ", queryStr);
        }
    }

    private JSONObject getQueryPlan(HiveConf hiveConf, QueryPlan queryPlan) throws Exception {
        try {
            ExplainTask explain = new ExplainTask();
            explain.initialize(hiveConf, queryPlan, null);
            List<Task<?>> rootTasks = queryPlan.getRootTasks();
            return explain.getJSONPlan(null, null, rootTasks, queryPlan.getFetchTask(), true, false, false);
        } catch (Exception e) {
            LOG.info("Failed to get queryplan", e);
            return new JSONObject();
        }
    }

    private boolean isSelectQuery(HiveEvent event) {
        if (event.operation == HiveOperation.QUERY) {
            Set<WriteEntity> outputs = event.outputs;

            //Select query has only one output
            if (outputs.size() == 1) {
                WriteEntity output = outputs.iterator().next();
                /* Strangely select queries have DFS_DIR as the type which seems like a bug in hive. Filter out by checking if the path is a temporary URI
                 * Insert into/overwrite queries onto local or dfs paths have DFS_DIR or LOCAL_DIR as the type and WriteType.PATH_WRITE and tempUri = false
                 * Insert into a temporary table has isTempURI = false. So will not skip as expected
                 */
                if (output.getType() == Type.DFS_DIR || output.getType() == Type.LOCAL_DIR) {
                    if (output.getWriteType() == WriteEntity.WriteType.PATH_WRITE &&
                        output.isTempURI()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
