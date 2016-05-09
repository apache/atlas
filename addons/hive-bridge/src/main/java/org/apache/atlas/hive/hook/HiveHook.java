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
import org.apache.atlas.AtlasClient;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    static class HiveEventContext {
        private Set<ReadEntity> inputs;
        private Set<WriteEntity> outputs;

        private String user;
        private UserGroupInformation ugi;
        private HiveOperation operation;
        private HookContext.HookType hookType;
        private org.json.JSONObject jsonPlan;
        private String queryId;
        private String queryStr;
        private Long queryStartTime;

        private String queryType;

        public void setInputs(Set<ReadEntity> inputs) {
            this.inputs = inputs;
        }

        public void setOutputs(Set<WriteEntity> outputs) {
            this.outputs = outputs;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public void setUgi(UserGroupInformation ugi) {
            this.ugi = ugi;
        }

        public void setOperation(HiveOperation operation) {
            this.operation = operation;
        }

        public void setHookType(HookContext.HookType hookType) {
            this.hookType = hookType;
        }

        public void setJsonPlan(JSONObject jsonPlan) {
            this.jsonPlan = jsonPlan;
        }

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public void setQueryStr(String queryStr) {
            this.queryStr = queryStr;
        }

        public void setQueryStartTime(Long queryStartTime) {
            this.queryStartTime = queryStartTime;
        }

        public void setQueryType(String queryType) {
            this.queryType = queryType;
        }

        public Set<ReadEntity> getInputs() {
            return inputs;
        }

        public Set<WriteEntity> getOutputs() {
            return outputs;
        }

        public String getUser() {
            return user;
        }

        public UserGroupInformation getUgi() {
            return ugi;
        }

        public HiveOperation getOperation() {
            return operation;
        }

        public HookContext.HookType getHookType() {
            return hookType;
        }

        public org.json.JSONObject getJsonPlan() {
            return jsonPlan;
        }

        public String getQueryId() {
            return queryId;
        }

        public String getQueryStr() {
            return queryStr;
        }

        public Long getQueryStartTime() {
            return queryStartTime;
        }

        public String getQueryType() {
            return queryType;
        }
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

        final HiveConf conf = new HiveConf(hookContext.getConf());

        final HiveEventContext event = new HiveEventContext();
        event.setInputs(hookContext.getInputs());
        event.setOutputs(hookContext.getOutputs());
        event.setJsonPlan(getQueryPlan(hookContext.getConf(), hookContext.getQueryPlan()));
        event.setHookType(hookContext.getHookType());
        event.setUgi(hookContext.getUgi());
        event.setUser(getUser(hookContext.getUserName()));
        event.setOperation(OPERATION_MAP.get(hookContext.getOperationName()));
        event.setQueryId(hookContext.getQueryPlan().getQueryId());
        event.setQueryStr(hookContext.getQueryPlan().getQueryStr());
        event.setQueryStartTime(hookContext.getQueryPlan().getQueryStartTime());
        event.setQueryType(hookContext.getQueryPlan().getQueryPlan().getQueryType());

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

    private void fireAndForget(HiveEventContext event) throws Exception {
        assert event.getHookType() == HookContext.HookType.POST_EXEC_HOOK : "Non-POST_EXEC_HOOK not supported!";

        LOG.info("Entered Atlas hook for hook type {} operation {}", event.getHookType(), event.getOperation());

        HiveMetaStoreBridge dgiBridge = new HiveMetaStoreBridge(hiveConf);

        switch (event.getOperation()) {
        case CREATEDATABASE:
            handleEventOutputs(dgiBridge, event, Type.DATABASE);
            break;

        case CREATETABLE:
            List<Pair<? extends Entity, Referenceable>> tablesCreated = handleEventOutputs(dgiBridge, event, Type.TABLE);
            if (tablesCreated.size() > 0) {
                handleExternalTables(dgiBridge, event, tablesCreated.get(0).getLeft(), tablesCreated.get(0).getRight());
            }
            break;

        case CREATETABLE_AS_SELECT:

        case CREATEVIEW:
        case ALTERVIEW_AS:
        case LOAD:
        case EXPORT:
        case IMPORT:
        case QUERY:
        case TRUNCATETABLE:
            registerProcess(dgiBridge, event);
            break;

        case ALTERTABLE_RENAME:
        case ALTERVIEW_RENAME:
            renameTable(dgiBridge, event);
            break;

        case ALTERTABLE_FILEFORMAT:
        case ALTERTABLE_CLUSTER_SORT:
        case ALTERTABLE_BUCKETNUM:
        case ALTERTABLE_PROPERTIES:
        case ALTERVIEW_PROPERTIES:
        case ALTERTABLE_SERDEPROPERTIES:
        case ALTERTABLE_SERIALIZER:
        case ALTERTABLE_ADDCOLS:
        case ALTERTABLE_REPLACECOLS:
        case ALTERTABLE_RENAMECOL:
        case ALTERTABLE_PARTCOLTYPE:
            handleEventOutputs(dgiBridge, event, Type.TABLE);
            break;
        case ALTERTABLE_LOCATION:
            List<Pair<? extends Entity, Referenceable>> tablesUpdated = handleEventOutputs(dgiBridge, event, Type.TABLE);
            if (tablesUpdated != null && tablesUpdated.size() > 0) {
                //Track altered lineage in case of external tables
                handleExternalTables(dgiBridge, event, tablesUpdated.get(0).getLeft(), tablesUpdated.get(0).getRight());
            }
            break;
        case ALTERDATABASE:
        case ALTERDATABASE_OWNER:
            handleEventOutputs(dgiBridge, event, Type.DATABASE);
            break;

        case DROPTABLE:
        case DROPVIEW:
            deleteTable(dgiBridge, event);
            break;

        case DROPDATABASE:
            deleteDatabase(dgiBridge, event);
            break;

        default:
        }

        notifyEntities(messages);
    }

    private void deleteTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event) {
        for (WriteEntity output : event.outputs) {
            if (Type.TABLE.equals(output.getType())) {
                deleteTable(dgiBridge, event, output);
            }
        }
    }

    private void deleteTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event, WriteEntity output) {
        final String tblQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(), output.getTable());
        LOG.info("Deleting table {} ", tblQualifiedName);
        messages.add(
            new HookNotification.EntityDeleteRequest(event.getUser(),
                HiveDataTypes.HIVE_TABLE.getName(),
                HiveDataModelGenerator.NAME,
                tblQualifiedName));
    }

    private void deleteDatabase(HiveMetaStoreBridge dgiBridge, HiveEventContext event) {
        if (event.outputs.size() > 1) {
            LOG.info("Starting deletion of tables and databases with cascade {} " , event.queryStr);
        }

        for (WriteEntity output : event.outputs) {
            if (Type.TABLE.equals(output.getType())) {
                deleteTable(dgiBridge, event, output);
            } else if (Type.DATABASE.equals(output.getType())) {
                final String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(dgiBridge.getClusterName(), output.getDatabase().getName());
                messages.add(
                    new HookNotification.EntityDeleteRequest(event.getUser(),
                        HiveDataTypes.HIVE_DB.getName(),
                        AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        dbQualifiedName));
            }
        }
    }

    private void renameTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event) throws Exception {
        //crappy, no easy of getting new name
        assert event.getInputs() != null && event.getInputs().size() == 1;
        assert event.getOutputs() != null && event.getOutputs().size() > 0;

        //Update entity if not exists
        ReadEntity oldEntity = event.getInputs().iterator().next();
        Table oldTable = oldEntity.getTable();

        for (WriteEntity writeEntity : event.getOutputs()) {
            if (writeEntity.getType() == Entity.Type.TABLE) {
                Table newTable = writeEntity.getTable();
                //Hive sends with both old and new table names in the outputs which is weird. So skipping that with the below check
                if (!newTable.getDbName().equals(oldTable.getDbName()) || !newTable.getTableName().equals(oldTable.getTableName())) {
                    final String oldQualifiedName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                        oldTable);
                    final String newQualifiedName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                        newTable);

                    //Create/update old table entity - create entity with oldQFNme and old tableName if it doesnt exist. If exists, will update
                    //We always use the new entity while creating the table since some flags, attributes of the table are not set in inputEntity and Hive.getTable(oldTableName) also fails since the table doesnt exist in hive anymore
                    final Referenceable tableEntity = createOrUpdateEntities(dgiBridge, event.getUser(), writeEntity, true);

                    //Reset regular column QF Name to old Name and create a new partial notification request to replace old column QFName to newName to retain any existing traits
                    replaceColumnQFName(event, (List<Referenceable>) tableEntity.get(HiveDataModelGenerator.COLUMNS), oldQualifiedName, newQualifiedName);

                    //Reset partition key column QF Name to old Name and create a new partial notification request to replace old column QFName to newName to retain any existing traits
                    replaceColumnQFName(event, (List<Referenceable>) tableEntity.get(HiveDataModelGenerator.PART_COLS), oldQualifiedName, newQualifiedName);

                    //Reset SD QF Name to old Name and create a new partial notification request to replace old SD QFName to newName to retain any existing traits
                    replaceSDQFName(event, tableEntity, oldQualifiedName, newQualifiedName);

                    //Reset Table QF Name to old Name and create a new partial notification request to replace old Table QFName to newName
                    replaceTableQFName(dgiBridge, event, oldTable, newTable, tableEntity, oldQualifiedName, newQualifiedName);
                }
            }
        }
    }

    private Referenceable replaceTableQFName(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Table oldTable, Table newTable, final Referenceable tableEntity, final String oldTableQFName, final String newTableQFName) throws HiveException {
        tableEntity.set(HiveDataModelGenerator.NAME, oldTableQFName);
        tableEntity.set(HiveDataModelGenerator.TABLE_NAME, oldTable.getTableName().toLowerCase());
        final Referenceable newDbInstance = (Referenceable) tableEntity.get(HiveDataModelGenerator.DB);
        tableEntity.set(HiveDataModelGenerator.DB, dgiBridge.createDBInstance(dgiBridge.hiveClient.getDatabase(oldTable.getDbName())));

        //Replace table entity with new name
        final Referenceable newEntity = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        newEntity.set(HiveDataModelGenerator.NAME, newTableQFName);
        newEntity.set(HiveDataModelGenerator.TABLE_NAME, newTable.getTableName().toLowerCase());
        newEntity.set(HiveDataModelGenerator.DB, newDbInstance);

        messages.add(new HookNotification.EntityPartialUpdateRequest(event.getUser(),
            HiveDataTypes.HIVE_TABLE.getName(), HiveDataModelGenerator.NAME,
            oldTableQFName, newEntity));

        return newEntity;
    }

    private List<Referenceable> replaceColumnQFName(final HiveEventContext event, final List<Referenceable> cols, final String oldTableQFName, final String newTableQFName) {
        List<Referenceable> newColEntities = new ArrayList<>();
        for (Referenceable col : cols) {
            final String colName = (String) col.get(HiveDataModelGenerator.NAME);
            String oldColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(oldTableQFName, colName);
            String newColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(newTableQFName, colName);
            col.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, oldColumnQFName);

            Referenceable newColEntity = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            ///Only QF Name changes
            newColEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newColumnQFName);
            messages.add(new HookNotification.EntityPartialUpdateRequest(event.getUser(),
                HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                oldColumnQFName, newColEntity));
            newColEntities.add(newColEntity);
        }
        return newColEntities;
    }

    private Referenceable replaceSDQFName(final HiveEventContext event, Referenceable tableEntity, final String oldTblQFName, final String newTblQFName) {
        //Reset storage desc QF Name to old Name
        final Referenceable sdRef = ((Referenceable) tableEntity.get(HiveDataModelGenerator.STORAGE_DESC));
        sdRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, HiveMetaStoreBridge.getStorageDescQFName(oldTblQFName));

        //Replace SD QF name first to retain tags
        final String oldSDQFName = HiveMetaStoreBridge.getStorageDescQFName(oldTblQFName);
        final String newSDQFName = HiveMetaStoreBridge.getStorageDescQFName(newTblQFName);

        final Referenceable newSDEntity = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());
        newSDEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newSDQFName);
        messages.add(new HookNotification.EntityPartialUpdateRequest(event.getUser(),
            HiveDataTypes.HIVE_STORAGEDESC.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            oldSDQFName, newSDEntity));

        return newSDEntity;
    }

    private Referenceable createOrUpdateEntities(HiveMetaStoreBridge dgiBridge, String user, Entity entity, boolean skipTempTables) throws Exception {
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
            //If its an external table, even though the temp table skip flag is on,
            // we create the table since we need the HDFS path to temp table lineage.
            if (skipTempTables &&
                table.isTemporary() &&
                !TableType.EXTERNAL_TABLE.equals(table.getTableType())) {

               LOG.debug("Skipping temporary table registration {} since it is not an external table {} ", table.getTableName(), table.getTableType().name());

            } else {
                tableEntity = dgiBridge.createTableInstance(dbEntity, table);
                entities.add(tableEntity);
            }
        }

        messages.add(new HookNotification.EntityUpdateRequest(user, entities));
        return tableEntity;
    }

    private List<Pair<? extends Entity, Referenceable>> handleEventOutputs(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Type entityType) throws Exception {
        List<Pair<? extends Entity, Referenceable>> entitiesCreatedOrUpdated = new ArrayList<>();
        for (Entity entity : event.getOutputs()) {
            if (entity.getType() == entityType) {
                Referenceable entityCreatedOrUpdated = createOrUpdateEntities(dgiBridge, event.getUser(), entity, true);
                if (entitiesCreatedOrUpdated != null) {
                    entitiesCreatedOrUpdated.add(Pair.of(entity, entityCreatedOrUpdated));
                }
            }
        }
        return entitiesCreatedOrUpdated;
    }

    public static String normalize(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return str.toLowerCase().trim();
    }

    private void registerProcess(HiveMetaStoreBridge dgiBridge, HiveEventContext event) throws Exception {
        Set<ReadEntity> inputs = event.getInputs();
        Set<WriteEntity> outputs = event.getOutputs();

        //Even explain CTAS has operation name as CREATETABLE_AS_SELECT
        if (inputs.isEmpty() && outputs.isEmpty()) {
            LOG.info("Explain statement. Skipping...");
            return;
        }

        if (event.getQueryId() == null) {
            LOG.info("Query id/plan is missing for {}", event.getQueryStr());
        }

        final Map<String, Referenceable> source = new LinkedHashMap<>();
        final Map<String, Referenceable> target = new LinkedHashMap<>();

        boolean isSelectQuery = isSelectQuery(event);

        // filter out select queries which do not modify data
        if (!isSelectQuery) {
            for (ReadEntity readEntity : event.getInputs()) {
                processHiveEntity(dgiBridge, event, readEntity, source);
            }

            for (WriteEntity writeEntity : event.getOutputs()) {
                processHiveEntity(dgiBridge, event, writeEntity, target);
            }

            if (source.size() > 0 || target.size() > 0) {
                Referenceable processReferenceable = getProcessReferenceable(event,
                    new ArrayList<Referenceable>() {{
                        addAll(source.values());
                    }},
                    new ArrayList<Referenceable>() {{
                        addAll(target.values());
                    }});
                messages.add(new HookNotification.EntityCreateRequest(event.getUser(), processReferenceable));
            } else {
                LOG.info("Skipped query {} since it has no getInputs() or resulting getOutputs()", event.getQueryStr());
            }
        } else {
            LOG.info("Skipped query {} for processing since it is a select query ", event.getQueryStr());
        }
    }

    private void processHiveEntity(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Entity entity, Map<String, Referenceable> dataSets) throws Exception {
        if (entity.getType() == Type.TABLE || entity.getType() == Type.PARTITION) {
            final String tblQFName = dgiBridge.getTableQualifiedName(dgiBridge.getClusterName(), entity.getTable());
            if (!dataSets.containsKey(tblQFName)) {
                Referenceable inTable = createOrUpdateEntities(dgiBridge, event.getUser(), entity, false);
                dataSets.put(tblQFName, inTable);
            }
        } else if (entity.getType() == Type.DFS_DIR) {
            final String pathUri = normalize(new Path(entity.getLocation()).toString());
            LOG.info("Registering DFS Path {} ", pathUri);
            Referenceable hdfsPath = dgiBridge.fillHDFSDataSet(pathUri);
            dataSets.put(pathUri, hdfsPath);
        }
    }

    private JSONObject getQueryPlan(HiveConf hiveConf, QueryPlan queryPlan) throws Exception {
        try {
            ExplainTask explain = new ExplainTask();
            explain.initialize(hiveConf, queryPlan, null);
            List<Task<?>> rootTasks = queryPlan.getRootTasks();
            return explain.getJSONPlan(null, null, rootTasks, queryPlan.getFetchTask(), true, false, false);
        } catch (Throwable e) {
            LOG.info("Failed to get queryplan", e);
            return new JSONObject();
        }
    }

    private boolean isSelectQuery(HiveEventContext event) {
        if (event.getOperation() == HiveOperation.QUERY) {
            //Select query has only one output
            if (event.getOutputs().size() == 1) {
                WriteEntity output = event.getOutputs().iterator().next();
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

    private void handleExternalTables(final HiveMetaStoreBridge dgiBridge, final HiveEventContext event, final Entity entity, final Referenceable tblRef) throws HiveException, MalformedURLException {
        Table hiveTable = entity.getTable();
        //Refresh to get the correct location
        hiveTable = dgiBridge.hiveClient.getTable(hiveTable.getDbName(), hiveTable.getTableName());

        final String location = normalize(hiveTable.getDataLocation().toString());
        if (hiveTable != null && TableType.EXTERNAL_TABLE.equals(hiveTable.getTableType())) {
            LOG.info("Registering external table process {} ", event.getQueryStr());
            List<Referenceable> inputs = new ArrayList<Referenceable>() {{
                add(dgiBridge.fillHDFSDataSet(location));
            }};

            List<Referenceable> outputs = new ArrayList<Referenceable>() {{
                add(tblRef);
            }};

            Referenceable processReferenceable = getProcessReferenceable(event, inputs, outputs);
            messages.add(new HookNotification.EntityCreateRequest(event.getUser(), processReferenceable));
        }
    }

    private Referenceable getProcessReferenceable(HiveEventContext hiveEvent, List<Referenceable> sourceList, List<Referenceable> targetList) {
        Referenceable processReferenceable = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());

        String queryStr = normalize(hiveEvent.getQueryStr());
        LOG.debug("Registering query: {}", queryStr);

        //The serialization code expected a list
        if (sourceList != null || !sourceList.isEmpty()) {
            processReferenceable.set("inputs", sourceList);
        }
        if (targetList != null || !targetList.isEmpty()) {
            processReferenceable.set("outputs", targetList);
        }
        processReferenceable.set("name", queryStr);
        processReferenceable.set("operationType", hiveEvent.getOperation().getOperationName());
        processReferenceable.set("startTime", new Date(hiveEvent.getQueryStartTime()));
        processReferenceable.set("userName", hiveEvent.getUser());
        processReferenceable.set("queryText", queryStr);
        processReferenceable.set("queryId", hiveEvent.getQueryId());
        processReferenceable.set("queryPlan", hiveEvent.getJsonPlan());
        processReferenceable.set("endTime", new Date(System.currentTimeMillis()));
        //TODO set queryGraph
        return processReferenceable;
    }
}
