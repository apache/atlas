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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.hive.bridge.ColumnLineageUtils;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
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
    public static final String SEP = ":".intern();
    static final String IO_SEP = "->".intern();

    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();

    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor = null;

    private static final int minThreadsDefault = 1;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;
    private static final int queueSizeDefault = 10000;

    private static final HiveConf hiveConf;

    static {
        try {
            // initialize the async facility to process hook calls. We don't
            // want to do this inline since it adds plenty of overhead for the query.
            boolean isSync = atlasProperties.getBoolean(CONF_SYNC, Boolean.FALSE);

            if(!isSync) {
                int minThreads = atlasProperties.getInt(MIN_THREADS, minThreadsDefault);
                int maxThreads = atlasProperties.getInt(MAX_THREADS, maxThreadsDefault);
                long keepAliveTime = atlasProperties.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);
                int queueSize = atlasProperties.getInt(QUEUE_SIZE, queueSizeDefault);

                executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(queueSize),
                        new ThreadFactoryBuilder().setNameFormat("Atlas Logger %d").build());

                ShutdownHookManager.get().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            LOG.info("==> Shutdown of Atlas Hive Hook");

                            executor.shutdown();
                            executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                            executor = null;
                        } catch (InterruptedException ie) {
                            LOG.info("Interrupt received in shutdown.");
                        } finally {
                            LOG.info("<== Shutdown of Atlas Hive Hook");
                        }
                        // shutdown client
                    }
                }, AtlasConstants.ATLAS_SHUTDOWN_HOOK_PRIORITY);
            }

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
        try {
            final HiveEventContext event = new HiveEventContext();

            event.setInputs(hookContext.getInputs());
            event.setOutputs(hookContext.getOutputs());
            event.setHookType(hookContext.getHookType());

            final UserGroupInformation ugi       = hookContext.getUgi() == null ? Utils.getUGI() : hookContext.getUgi();
            final QueryPlan            queryPlan = hookContext.getQueryPlan();

            event.setUgi(ugi);
            event.setUser(getUser(hookContext.getUserName(), hookContext.getUgi()));
            event.setOperation(OPERATION_MAP.get(hookContext.getOperationName()));
            event.setQueryId(queryPlan.getQueryId());
            event.setQueryStr(queryPlan.getQueryStr());
            event.setQueryStartTime(queryPlan.getQueryStartTime());
            event.setLineageInfo(hookContext.getLinfo());

            if (executor == null) {
                collect(event);
                notifyAsPrivilegedAction(event);
            } else {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ugi.doAs(new PrivilegedExceptionAction<Object>() {
                                @Override
                                public Object run() throws Exception {
                                    collect(event);
                                    return event;
                                }
                            });

                            notifyAsPrivilegedAction(event);
                        } catch (Throwable e) {
                            LOG.error("Atlas hook failed due to error ", e);
                        }
                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("Submitting to thread pool failed due to error ", t);
        }
    }

    void notifyAsPrivilegedAction(final HiveEventContext event) {

        try {
            PrivilegedExceptionAction<Object> privilegedNotify = new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    notifyEntities(event.getMessages());
                    return event;
                }
            };

            //Notify as 'hive' service user in doAs mode
            UserGroupInformation realUser = event.getUgi().getRealUser();
            if (realUser != null) {
                LOG.info("Sending notification for event {} as service user {} #messages {} ", event.getOperation(), realUser.getShortUserName(), event.getMessages().size());
                realUser.doAs(privilegedNotify);
            } else {
                LOG.info("Sending notification for event {} as current user {} #messages {} ", event.getOperation(), event.getUgi().getShortUserName(), event.getMessages().size());
                event.getUgi().doAs(privilegedNotify);
            }
        } catch(Throwable e) {
            LOG.error("Error during notify {} ", event.getOperation(), e);
        }
    }

    private void collect(HiveEventContext event) throws Exception {

        assert event.getHookType() == HookContext.HookType.POST_EXEC_HOOK : "Non-POST_EXEC_HOOK not supported!";

        LOG.info("Entered Atlas hook for hook type {}, operation {} , user {} as {}", event.getHookType(), event.getOperation(), event.getUgi().getRealUser(), event.getUgi().getShortUserName());

        HiveMetaStoreBridge dgiBridge = new HiveMetaStoreBridge(atlasProperties, hiveConf);

        switch (event.getOperation()) {
        case CREATEDATABASE:
            handleEventOutputs(dgiBridge, event, Type.DATABASE);
            break;

        case CREATETABLE:
            LinkedHashMap<Type, Referenceable> tablesCreated = handleEventOutputs(dgiBridge, event, Type.TABLE);
            if (tablesCreated != null && tablesCreated.size() > 0) {
                handleExternalTables(dgiBridge, event, tablesCreated);
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
        case ALTERTABLE_PARTCOLTYPE:
            handleEventOutputs(dgiBridge, event, Type.TABLE);
            break;

        case ALTERTABLE_RENAMECOL:
            renameColumn(dgiBridge, event);
            break;

        case ALTERTABLE_LOCATION:
            LinkedHashMap<Type, Referenceable> tablesUpdated = handleEventOutputs(dgiBridge, event, Type.TABLE);
            if (tablesUpdated != null && tablesUpdated.size() > 0) {
                //Track altered lineage in case of external tables
                handleExternalTables(dgiBridge, event, tablesUpdated);
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
    }

    private void deleteTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event) {
        for (WriteEntity output : event.getOutputs()) {
            if (Type.TABLE.equals(output.getType())) {
                deleteTable(dgiBridge, event, output);
            }
        }
    }

    private void deleteTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event, WriteEntity output) {
        final String tblQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(), output.getTable());
        LOG.info("Deleting table {} ", tblQualifiedName);
        event.addMessage(
            new EntityDeleteRequest(event.getUser(),
                HiveDataTypes.HIVE_TABLE.getName(),
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                tblQualifiedName));
    }

    private void deleteDatabase(HiveMetaStoreBridge dgiBridge, HiveEventContext event) {
        if (event.getOutputs().size() > 1) {
            LOG.info("Starting deletion of tables and databases with cascade {} ", event.getQueryStr());
        } else {
            LOG.info("Starting deletion of database {} ", event.getQueryStr());
        }

        for (WriteEntity output : event.getOutputs()) {
            if (Type.TABLE.equals(output.getType())) {
                deleteTable(dgiBridge, event, output);
            } else if (Type.DATABASE.equals(output.getType())) {
                final String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(dgiBridge.getClusterName(), output.getDatabase().getName());
                event.addMessage(
                    new EntityDeleteRequest(event.getUser(),
                        HiveDataTypes.HIVE_DB.getName(),
                        AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        dbQualifiedName));
            }
        }
    }

    private Pair<String, String> findChangedColNames(List<FieldSchema> oldColList, List<FieldSchema> newColList) {
        HashMap<FieldSchema, Integer> oldColHashMap = new HashMap<>();
        HashMap<FieldSchema, Integer> newColHashMap = new HashMap<>();
        for (int i = 0; i < oldColList.size(); i++) {
            oldColHashMap.put(oldColList.get(i), i);
            newColHashMap.put(newColList.get(i), i);
        }

        String changedColStringOldName = oldColList.get(0).getName();
        String changedColStringNewName = changedColStringOldName;

        for (FieldSchema oldCol : oldColList) {
            if (!newColHashMap.containsKey(oldCol)) {
                changedColStringOldName = oldCol.getName();
                break;
            }
        }

        for (FieldSchema newCol : newColList) {
            if (!oldColHashMap.containsKey(newCol)) {
                changedColStringNewName = newCol.getName();
                break;
            }
        }

        return Pair.of(changedColStringOldName, changedColStringNewName);
    }

    private void renameColumn(HiveMetaStoreBridge dgiBridge, HiveEventContext event) throws AtlasHookException {
        try {
            assert event.getInputs() != null && event.getInputs().size() == 1;
            assert event.getOutputs() != null && event.getOutputs().size() > 0;

            Table oldTable = event.getInputs().iterator().next().getTable();
            List<FieldSchema> oldColList = oldTable.getAllCols();
            Table outputTbl = event.getOutputs().iterator().next().getTable();
            outputTbl = dgiBridge.hiveClient.getTable(outputTbl.getDbName(), outputTbl.getTableName());
            List<FieldSchema> newColList = outputTbl.getAllCols();
            assert oldColList.size() == newColList.size();

            Pair<String, String> changedColNamePair = findChangedColNames(oldColList, newColList);
            String oldColName = changedColNamePair.getLeft();
            String newColName = changedColNamePair.getRight();
            for (WriteEntity writeEntity : event.getOutputs()) {
                if (writeEntity.getType() == Type.TABLE) {
                    Table newTable = writeEntity.getTable();
                    createOrUpdateEntities(dgiBridge, event, writeEntity, true, oldTable);
                    final String newQualifiedTableName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                            newTable);
                    String oldColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(newQualifiedTableName, oldColName);
                    String newColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(newQualifiedTableName, newColName);
                    Referenceable newColEntity = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
                    newColEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newColumnQFName);

                    event.addMessage(new EntityPartialUpdateRequest(event.getUser(),
                            HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                            oldColumnQFName, newColEntity));
                }
            }
            handleEventOutputs(dgiBridge, event, Type.TABLE);
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.renameColumn() failed.", e);
        }
    }

    private void renameTable(HiveMetaStoreBridge dgiBridge, HiveEventContext event) throws AtlasHookException {
        try {
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
                        final String oldQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                                oldTable);
                        final String newQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(),
                                newTable);

                        //Create/update old table entity - create entity with oldQFNme and old tableName if it doesnt exist. If exists, will update
                        //We always use the new entity while creating the table since some flags, attributes of the table are not set in inputEntity and Hive.getTable(oldTableName) also fails since the table doesnt exist in hive anymore
                        final LinkedHashMap<Type, Referenceable> tables = createOrUpdateEntities(dgiBridge, event, writeEntity, true);
                        Referenceable tableEntity = tables.get(Type.TABLE);

                        //Reset regular column QF Name to old Name and create a new partial notification request to replace old column QFName to newName to retain any existing traits
                        replaceColumnQFName(event, (List<Referenceable>) tableEntity.get(HiveMetaStoreBridge.COLUMNS), oldQualifiedName, newQualifiedName);

                        //Reset partition key column QF Name to old Name and create a new partial notification request to replace old column QFName to newName to retain any existing traits
                        replaceColumnQFName(event, (List<Referenceable>) tableEntity.get(HiveMetaStoreBridge.PART_COLS), oldQualifiedName, newQualifiedName);

                        //Reset SD QF Name to old Name and create a new partial notification request to replace old SD QFName to newName to retain any existing traits
                        replaceSDQFName(event, tableEntity, oldQualifiedName, newQualifiedName);

                        //Reset Table QF Name to old Name and create a new partial notification request to replace old Table QFName to newName
                        replaceTableQFName(event, oldTable, newTable, tableEntity, oldQualifiedName, newQualifiedName);
                    }
                }
            }
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.renameTable() failed.", e);
        }
    }

    private Referenceable replaceTableQFName(HiveEventContext event, Table oldTable, Table newTable, final Referenceable tableEntity, final String oldTableQFName, final String newTableQFName) throws HiveException {
        tableEntity.set(AtlasClient.NAME,  oldTable.getTableName().toLowerCase());
        tableEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, oldTableQFName);

        //Replace table entity with new name
        final Referenceable newEntity = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        newEntity.set(AtlasClient.NAME, newTable.getTableName().toLowerCase());
        newEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newTableQFName);

        ArrayList<String> alias_list = new ArrayList<>();
        alias_list.add(oldTable.getTableName().toLowerCase());
        newEntity.set(HiveMetaStoreBridge.TABLE_ALIAS_LIST, alias_list);
        event.addMessage(new EntityPartialUpdateRequest(event.getUser(),
            HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            oldTableQFName, newEntity));

        return newEntity;
    }

    private List<Referenceable> replaceColumnQFName(final HiveEventContext event, final List<Referenceable> cols, final String oldTableQFName, final String newTableQFName) {
        List<Referenceable> newColEntities = new ArrayList<>();
        for (Referenceable col : cols) {
            final String colName = (String) col.get(AtlasClient.NAME);
            String oldColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(oldTableQFName, colName);
            String newColumnQFName = HiveMetaStoreBridge.getColumnQualifiedName(newTableQFName, colName);
            col.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, oldColumnQFName);

            Referenceable newColEntity = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            ///Only QF Name changes
            newColEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newColumnQFName);
            event.addMessage(new EntityPartialUpdateRequest(event.getUser(),
                HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                oldColumnQFName, newColEntity));
            newColEntities.add(newColEntity);
        }
        return newColEntities;
    }

    private Referenceable replaceSDQFName(final HiveEventContext event, Referenceable tableEntity, final String oldTblQFName, final String newTblQFName) {
        //Reset storage desc QF Name to old Name
        final Referenceable sdRef = ((Referenceable) tableEntity.get(HiveMetaStoreBridge.STORAGE_DESC));
        sdRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, HiveMetaStoreBridge.getStorageDescQFName(oldTblQFName));

        //Replace SD QF name fir    st to retain tags
        final String oldSDQFName = HiveMetaStoreBridge.getStorageDescQFName(oldTblQFName);
        final String newSDQFName = HiveMetaStoreBridge.getStorageDescQFName(newTblQFName);

        final Referenceable newSDEntity = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());
        newSDEntity.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, newSDQFName);
        event.addMessage(new EntityPartialUpdateRequest(event.getUser(),
            HiveDataTypes.HIVE_STORAGEDESC.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            oldSDQFName, newSDEntity));

        return newSDEntity;
    }

    private LinkedHashMap<Type, Referenceable> createOrUpdateEntities(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Entity entity, boolean skipTempTables, Table existTable) throws AtlasHookException {
        try {
            Database db = null;
            Table table = null;
            Partition partition = null;

            switch (entity.getType()) {
                case DATABASE:
                    db = entity.getDatabase();

                    if (db != null) {
                        db = dgiBridge.hiveClient.getDatabase(db.getName());
                    }
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

                default:
                    LOG.info("{}: entity-type not handled by Atlas hook. Ignored", entity.getType());
            }

            Referenceable dbEntity    = null;
            Referenceable tableEntity = null;

            if (db != null) {
                dbEntity = dgiBridge.createDBInstance(db);
            }

            if (db != null && table != null) {
                if (existTable != null) {
                    table = existTable;
                } else {
                    table = refreshTable(dgiBridge, table.getDbName(), table.getTableName());
                }

                if (table != null) {
                    // If its an external table, even though the temp table skip flag is on, we create the table since we need the HDFS path to temp table lineage.
                    if (skipTempTables && table.isTemporary() && !TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                        LOG.warn("Skipping temporary table registration {} since it is not an external table {} ", table.getTableName(), table.getTableType().name());
                    } else {
                        tableEntity = dgiBridge.createTableInstance(dbEntity, table);
                    }
                }
            }

            LinkedHashMap<Type, Referenceable> result   = new LinkedHashMap<>();
            List<Referenceable>                entities = new ArrayList<>();

            if (dbEntity != null) {
                result.put(Type.DATABASE, dbEntity);
                entities.add(dbEntity);
            }

            if (tableEntity != null) {
                result.put(Type.TABLE, tableEntity);
                entities.add(tableEntity);
            }

            if (!entities.isEmpty()) {
                event.addMessage(new EntityUpdateRequest(event.getUser(), entities));
            }

            return result;
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.createOrUpdateEntities() failed.", e);
        }
    }

    private LinkedHashMap<Type, Referenceable> createOrUpdateEntities(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Entity entity, boolean skipTempTables) throws AtlasHookException {
        try {
            return createOrUpdateEntities(dgiBridge, event, entity, skipTempTables, null);
        } catch (Exception e) {
            throw new AtlasHookException("HiveHook.createOrUpdateEntities() failed.", e);
        }
    }

    private LinkedHashMap<Type, Referenceable> handleEventOutputs(HiveMetaStoreBridge dgiBridge, HiveEventContext event, Type entityType) throws AtlasHookException {
        try {
            for (Entity entity : event.getOutputs()) {
                if (entity.getType() == entityType) {
                    return createOrUpdateEntities(dgiBridge, event, entity, true);
                }
            }
            return null;
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.handleEventOutputs() failed.", e);
        }
    }

    private static Entity getEntityByType(Set<? extends Entity> entities, Type entityType) {
        for (Entity entity : entities) {
            if (entity.getType() == entityType) {
                return entity;
            }
        }
        return null;
    }

    public static String lower(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return str.toLowerCase().trim();
    }

    private void registerProcess(HiveMetaStoreBridge dgiBridge, HiveEventContext event) throws AtlasHookException {
        try {
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

            final SortedMap<ReadEntity, Referenceable> source = new TreeMap<>(entityComparator);
            final SortedMap<WriteEntity, Referenceable> target = new TreeMap<>(entityComparator);

            final Set<String> dataSets = new HashSet<>();
            final Set<Referenceable> entities = new LinkedHashSet<>();

            boolean isSelectQuery = isSelectQuery(event);

            // filter out select queries which do not modify data
            if (!isSelectQuery) {

                SortedSet<ReadEntity> sortedHiveInputs = new TreeSet<>(entityComparator);
                if (event.getInputs() != null) {
                    sortedHiveInputs.addAll(event.getInputs());
                }

                SortedSet<WriteEntity> sortedHiveOutputs = new TreeSet<>(entityComparator);
                if (event.getOutputs() != null) {
                    sortedHiveOutputs.addAll(event.getOutputs());
                }

                for (ReadEntity readEntity : sortedHiveInputs) {
                    processHiveEntity(dgiBridge, event, readEntity, dataSets, source, entities);
                }

                for (WriteEntity writeEntity : sortedHiveOutputs) {
                    processHiveEntity(dgiBridge, event, writeEntity, dataSets, target, entities);
                }

                if (source.size() > 0 || target.size() > 0) {
                    Referenceable processReferenceable = getProcessReferenceable(dgiBridge, event, sortedHiveInputs, sortedHiveOutputs, source, target);
                    // setup Column Lineage
                    List<Referenceable> sourceList = new ArrayList<>(source.values());
                    List<Referenceable> targetList = new ArrayList<>(target.values());
                    List<Referenceable> colLineageProcessInstances = new ArrayList<>();
                    try {
                        Map<String, Referenceable> columnQNameToRef =
                                ColumnLineageUtils.buildColumnReferenceableMap(sourceList, targetList);
                        colLineageProcessInstances = createColumnLineageProcessInstances(processReferenceable,
                                event.lineageInfo,
                                columnQNameToRef);
                    } catch (Exception e) {
                        LOG.warn("Column lineage process setup failed with exception {}", e);
                    }
                    colLineageProcessInstances.add(0, processReferenceable);
                    entities.addAll(colLineageProcessInstances);

                    addEntityUpdateNotificationMessagess(event, entities);
                } else {
                    LOG.info("Skipped query {} since it has no getInputs() or resulting getOutputs()", event.getQueryStr());
                }
            } else {
                LOG.info("Skipped query {} for processing since it is a select query ", event.getQueryStr());
            }
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.registerProcess() failed.", e);
        }
    }

    private void addEntityUpdateNotificationMessagess(final HiveEventContext event, final Collection<Referenceable> entities) {
        // process each entity as separate message to avoid running into OOM errors
        for (Referenceable entity : entities) {
            event.addMessage(new EntityUpdateRequest(event.getUser(), entity));
        }
    }

    private  <T extends Entity> void processHiveEntity(HiveMetaStoreBridge dgiBridge, HiveEventContext event, T entity, Set<String> dataSetsProcessed,
        SortedMap<T, Referenceable> dataSets, Set<Referenceable> entities) throws AtlasHookException {
        try {
            if (entity.getType() == Type.TABLE || entity.getType() == Type.PARTITION) {
                final String tblQFName = HiveMetaStoreBridge.getTableQualifiedName(dgiBridge.getClusterName(), entity.getTable());
                if (!dataSetsProcessed.contains(tblQFName)) {
                    LinkedHashMap<Type, Referenceable> result = createOrUpdateEntities(dgiBridge, event, entity, false);

                    if (result.get(Type.TABLE) != null) {
                        dataSets.put(entity, result.get(Type.TABLE));
                    }

                    dataSetsProcessed.add(tblQFName);
                    entities.addAll(result.values());
                }
            } else if (entity.getType() == Type.DFS_DIR) {
                URI location = entity.getLocation();
                if (location != null) {
                    final String pathUri = dgiBridge.isConvertHdfsPathToLowerCase() ? lower(new Path(location).toString()) : new Path(location).toString();
                    LOG.debug("Registering DFS Path {} ", pathUri);
                    if (!dataSetsProcessed.contains(pathUri)) {
                        Referenceable hdfsPath = dgiBridge.fillHDFSDataSet(pathUri);
                        dataSets.put(entity, hdfsPath);
                        dataSetsProcessed.add(pathUri);
                        entities.add(hdfsPath);
                    }
                }
            }
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveHook.processHiveEntity() failed.", e);
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

    private void handleExternalTables(final HiveMetaStoreBridge dgiBridge, final HiveEventContext event, final LinkedHashMap<Type, Referenceable> tables) throws HiveException, MalformedURLException {
        List<Referenceable> entities = new ArrayList<>();
        final WriteEntity hiveEntity = (WriteEntity) getEntityByType(event.getOutputs(), Type.TABLE);

        Table hiveTable = hiveEntity == null ? null : hiveEntity.getTable();

        //Refresh to get the correct location
        if(hiveTable != null) {
            hiveTable = refreshTable(dgiBridge, hiveTable.getDbName(), hiveTable.getTableName());
        }

        if (hiveTable != null && TableType.EXTERNAL_TABLE.equals(hiveTable.getTableType())) {
            LOG.info("Registering external table process {} ", event.getQueryStr());
            final String location = dgiBridge.isConvertHdfsPathToLowerCase() ? lower(hiveTable.getDataLocation().toString()) : hiveTable.getDataLocation().toString();
            final ReadEntity dfsEntity = new ReadEntity();
            dfsEntity.setTyp(Type.DFS_DIR);
            dfsEntity.setD(new Path(location));

            SortedMap<ReadEntity, Referenceable> hiveInputsMap = new TreeMap<ReadEntity, Referenceable>(entityComparator) {{
                put(dfsEntity, dgiBridge.fillHDFSDataSet(location));
            }};

            SortedMap<WriteEntity, Referenceable> hiveOutputsMap = new TreeMap<WriteEntity, Referenceable>(entityComparator) {{
                put(hiveEntity, tables.get(Type.TABLE));
            }};

            SortedSet<ReadEntity> sortedIps = new TreeSet<>(entityComparator);
            sortedIps.addAll(hiveInputsMap.keySet());
            SortedSet<WriteEntity> sortedOps = new TreeSet<>(entityComparator);
            sortedOps.addAll(hiveOutputsMap.keySet());

            Referenceable processReferenceable = getProcessReferenceable(dgiBridge, event,
                sortedIps, sortedOps, hiveInputsMap, hiveOutputsMap);

            entities.addAll(tables.values());
            entities.add(processReferenceable);

            addEntityUpdateNotificationMessagess(event, entities);
        }
    }

    private static boolean isCreateOp(HiveEventContext hiveEvent) {
        return HiveOperation.CREATETABLE.equals(hiveEvent.getOperation())
                || HiveOperation.CREATEVIEW.equals(hiveEvent.getOperation())
                || HiveOperation.ALTERVIEW_AS.equals(hiveEvent.getOperation())
                || HiveOperation.ALTERTABLE_LOCATION.equals(hiveEvent.getOperation())
                || HiveOperation.CREATETABLE_AS_SELECT.equals(hiveEvent.getOperation());
    }

    private Referenceable getProcessReferenceable(HiveMetaStoreBridge dgiBridge, HiveEventContext hiveEvent,
        final SortedSet<ReadEntity> sortedHiveInputs, final SortedSet<WriteEntity> sortedHiveOutputs, SortedMap<ReadEntity, Referenceable> source, SortedMap<WriteEntity, Referenceable> target)
            throws HiveException {
        Referenceable processReferenceable = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());

        String queryStr = lower(hiveEvent.getQueryStr());
        processReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                getProcessQualifiedName(dgiBridge, hiveEvent, sortedHiveInputs, sortedHiveOutputs, source, target));

        LOG.debug("Registering query: {}", queryStr);
        List<Referenceable> sourceList = new ArrayList<>(source.values());
        List<Referenceable> targetList = new ArrayList<>(target.values());

        //The serialization code expected a list
        if (sourceList != null && !sourceList.isEmpty()) {
            processReferenceable.set("inputs", sourceList);
        }
        if (targetList != null && !targetList.isEmpty()) {
            processReferenceable.set("outputs", targetList);
        }
        processReferenceable.set(AtlasClient.NAME, queryStr);

        processReferenceable.set("operationType", hiveEvent.getOperation().getOperationName());
        processReferenceable.set("startTime", new Date(hiveEvent.getQueryStartTime()));
        processReferenceable.set("userName", hiveEvent.getUser());
        processReferenceable.set("queryText", queryStr);
        processReferenceable.set("queryId", hiveEvent.getQueryId());
        processReferenceable.set("queryPlan", "Not Supported");
        processReferenceable.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, dgiBridge.getClusterName());

        List<String> recentQueries = new ArrayList<>(1);
        recentQueries.add(queryStr);
        processReferenceable.set("recentQueries", recentQueries);

        processReferenceable.set("endTime", new Date(System.currentTimeMillis()));
        //TODO set queryGraph
        return processReferenceable;
    }


    private List<Referenceable> createColumnLineageProcessInstances(
            Referenceable processRefObj,
            Map<String, List<ColumnLineageUtils.HiveColumnLineageInfo>> lineageInfo,
            Map<String, Referenceable> columnQNameToRef
    ) {
        List<Referenceable> l = new ArrayList<>();
        for(Map.Entry<String, List<ColumnLineageUtils.HiveColumnLineageInfo>> e :
                lineageInfo.entrySet()) {
            Referenceable destCol = columnQNameToRef.get(e.getKey());
            if (destCol == null ) {
                LOG.debug("Couldn't find output Column {}", e.getKey());
                continue;
            }
            List<Referenceable> outRef = new ArrayList<>();
            outRef.add(destCol);
            List<Referenceable> inputRefs = new ArrayList<>();
            for(ColumnLineageUtils.HiveColumnLineageInfo cLI : e.getValue()) {
                Referenceable srcCol = columnQNameToRef.get(cLI.inputColumn);
                if (srcCol == null ) {
                    LOG.debug("Couldn't find input Column {}", cLI.inputColumn);
                    continue;
                }
                inputRefs.add(srcCol);
            }

            if (inputRefs.size() > 0 ) {
                Referenceable r = new Referenceable(HiveDataTypes.HIVE_COLUMN_LINEAGE.getName());
                r.set("name", processRefObj.get(AtlasClient.NAME) + ":" + outRef.get(0).get(AtlasClient.NAME));
                r.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processRefObj.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) + ":" + outRef.get(0).get(AtlasClient.NAME));
                r.set("inputs", inputRefs);
                r.set("outputs", outRef);
                r.set("query", processRefObj);
                r.set("depenendencyType", e.getValue().get(0).depenendencyType);
                r.set("expression", e.getValue().get(0).expr);
                l.add(r);
            }
            else{
                LOG.debug("No input references found for lineage of column {}", destCol.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME));
            }
        }

        return l;
    }

    @VisibleForTesting
    static String getProcessQualifiedName(HiveMetaStoreBridge dgiBridge, HiveEventContext eventContext,
                                          final SortedSet<ReadEntity> sortedHiveInputs,
                                          final SortedSet<WriteEntity> sortedHiveOutputs,
                                          SortedMap<ReadEntity, Referenceable> hiveInputsMap,
                                          SortedMap<WriteEntity, Referenceable> hiveOutputsMap) throws HiveException {
        HiveOperation op = eventContext.getOperation();
        if (isCreateOp(eventContext)) {
            Entity entity = getEntityByType(sortedHiveOutputs, Type.TABLE);

            if (entity != null) {
                Table outTable = entity.getTable();
                //refresh table
                outTable = dgiBridge.hiveClient.getTable(outTable.getDbName(), outTable.getTableName());
                return HiveMetaStoreBridge.getTableProcessQualifiedName(dgiBridge.getClusterName(), outTable);
            }
        }

        StringBuilder buffer = new StringBuilder(op.getOperationName());

        boolean ignoreHDFSPathsinQFName = ignoreHDFSPathsinQFName(op, sortedHiveInputs, sortedHiveOutputs);
        if ( ignoreHDFSPathsinQFName && LOG.isDebugEnabled()) {
            LOG.debug("Ignoring HDFS paths in qualifiedName for {} {} ", op, eventContext.getQueryStr());
        }

        addInputs(dgiBridge, op, sortedHiveInputs, buffer, hiveInputsMap, ignoreHDFSPathsinQFName);
        buffer.append(IO_SEP);
        addOutputs(dgiBridge, op, sortedHiveOutputs, buffer, hiveOutputsMap, ignoreHDFSPathsinQFName);
        LOG.info("Setting process qualified name to {}", buffer);
        return buffer.toString();
    }

    private static boolean ignoreHDFSPathsinQFName(final HiveOperation op, final Set<ReadEntity> inputs, final Set<WriteEntity> outputs) {
        switch (op) {
        case LOAD:
        case IMPORT:
            return isPartitionBasedQuery(outputs);
        case EXPORT:
            return isPartitionBasedQuery(inputs);
        case QUERY:
            return true;
        }
        return false;
    }

    private static boolean isPartitionBasedQuery(Set<? extends Entity> entities) {
        for (Entity entity : entities) {
            if (Type.PARTITION.equals(entity.getType())) {
                return true;
            }
        }
        return false;
    }

    private static void addInputs(HiveMetaStoreBridge hiveBridge, HiveOperation op, SortedSet<ReadEntity> sortedInputs, StringBuilder buffer, final Map<ReadEntity, Referenceable> refs, final boolean ignoreHDFSPathsInQFName) throws HiveException {
        if (refs != null) {
            if (sortedInputs != null) {
                Set<String> dataSetsProcessed = new LinkedHashSet<>();
                for (Entity input : sortedInputs) {

                    if (!dataSetsProcessed.contains(input.getName().toLowerCase())) {
                        //HiveOperation.QUERY type encompasses INSERT, INSERT_OVERWRITE, UPDATE, DELETE, PATH_WRITE operations
                        if (ignoreHDFSPathsInQFName &&
                            (Type.DFS_DIR.equals(input.getType()) || Type.LOCAL_DIR.equals(input.getType()))) {
                            LOG.debug("Skipping dfs dir input addition to process qualified name {} ", input.getName());
                        } else if (refs.containsKey(input)) {
                            if ( input.getType() == Type.PARTITION || input.getType() == Type.TABLE) {
                                Table inputTable = refreshTable(hiveBridge, input.getTable().getDbName(), input.getTable().getTableName());

                                if (inputTable != null) {
                                    final Date createTime = HiveMetaStoreBridge.getTableCreatedTime(inputTable);
                                    addDataset(buffer, refs.get(input), createTime.getTime());
                                }
                            } else {
                                addDataset(buffer, refs.get(input));
                            }
                        }

                        dataSetsProcessed.add(input.getName().toLowerCase());
                    }
                }

            }
        }
    }

    private static void addDataset(StringBuilder buffer, Referenceable ref, final long createTime) {
        addDataset(buffer, ref);
        buffer.append(SEP);
        buffer.append(createTime);
    }

    private static void addDataset(StringBuilder buffer, Referenceable ref) {
        buffer.append(SEP);
        String dataSetQlfdName = (String) ref.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME);
        // '/' breaks query parsing on ATLAS
        buffer.append(dataSetQlfdName.toLowerCase().replaceAll("/", ""));
    }

    private static void addOutputs(HiveMetaStoreBridge hiveBridge, HiveOperation op, SortedSet<WriteEntity> sortedOutputs, StringBuilder buffer, final Map<WriteEntity, Referenceable> refs, final boolean ignoreHDFSPathsInQFName) throws HiveException {
        if (refs != null) {
            Set<String> dataSetsProcessed = new LinkedHashSet<>();
            if (sortedOutputs != null) {
                for (WriteEntity output : sortedOutputs) {
                    final Entity entity = output;
                    if (!dataSetsProcessed.contains(output.getName().toLowerCase())) {
                        //HiveOperation.QUERY type encompasses INSERT, INSERT_OVERWRITE, UPDATE, DELETE, PATH_WRITE operations
                        if (addQueryType(op, (WriteEntity) entity)) {
                            buffer.append(SEP);
                            buffer.append(((WriteEntity) entity).getWriteType().name());
                        }
                        if (ignoreHDFSPathsInQFName &&
                            (Type.DFS_DIR.equals(output.getType()) || Type.LOCAL_DIR.equals(output.getType()))) {
                            LOG.debug("Skipping dfs dir output addition to process qualified name {} ", output.getName());
                        } else if (refs.containsKey(output)) {
                            if ( output.getType() == Type.PARTITION || output.getType() == Type.TABLE) {
                                Table outputTable = refreshTable(hiveBridge, output.getTable().getDbName(), output.getTable().getTableName());

                                if (outputTable != null) {
                                    final Date createTime = HiveMetaStoreBridge.getTableCreatedTime(outputTable);
                                    addDataset(buffer, refs.get(output), createTime.getTime());
                                }
                            } else {
                                addDataset(buffer, refs.get(output));
                            }
                        }

                        dataSetsProcessed.add(output.getName().toLowerCase());
                    }
                }
            }
        }
    }

    private static Table refreshTable(HiveMetaStoreBridge dgiBridge, String dbName, String tableName) {
        try {
            return dgiBridge.hiveClient.getTable(dbName, tableName);
        } catch (HiveException excp) { // this might be the case for temp tables
            LOG.warn("failed to get details for table {}.{}. Ignoring. {}: {}", dbName, tableName, excp.getClass().getCanonicalName(), excp.getMessage());
        }

        return null;
    }

    private static boolean addQueryType(HiveOperation op, WriteEntity entity) {
        if (entity.getWriteType() != null && HiveOperation.QUERY.equals(op)) {
            switch (entity.getWriteType()) {
            case INSERT:
            case INSERT_OVERWRITE:
            case UPDATE:
            case DELETE:
                return true;
            case PATH_WRITE:
                //Add query type only for DFS paths and ignore local paths since they are not added as outputs
                if ( !Type.LOCAL_DIR.equals(entity.getType())) {
                    return true;
                }
                break;
            default:
            }
        }
        return false;
    }

    public static class HiveEventContext {
        private Set<ReadEntity> inputs;
        private Set<WriteEntity> outputs;

        private String user;
        private UserGroupInformation ugi;
        private HiveOperation operation;
        private HookContext.HookType hookType;
        private JSONObject jsonPlan;
        private String queryId;
        private String queryStr;
        private Long queryStartTime;

        public Map<String, List<ColumnLineageUtils.HiveColumnLineageInfo>> lineageInfo;

        private List<HookNotification> messages = new ArrayList<>();

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

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public void setQueryStr(String queryStr) {
            this.queryStr = queryStr;
        }

        public void setQueryStartTime(Long queryStartTime) {
            this.queryStartTime = queryStartTime;
        }

        public void setLineageInfo(LineageInfo lineageInfo){
            try {
                this.lineageInfo = ColumnLineageUtils.buildLineageMap(lineageInfo);
                LOG.debug("Column Lineage Map => {} ", this.lineageInfo.entrySet());
            }catch (Throwable e){
                LOG.warn("Column Lineage Map build failed with exception {}", e);
            }
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

        public String getQueryId() {
            return queryId;
        }

        public String getQueryStr() {
            return queryStr;
        }

        public Long getQueryStartTime() {
            return queryStartTime;
        }

        public void addMessage(HookNotification message) {
            messages.add(message);
        }

        public List<HookNotification> getMessages() {
            return messages;
        }
    }

    @VisibleForTesting
    static final class EntityComparator implements Comparator<Entity> {
        @Override
        public int compare(Entity o1, Entity o2) {
            String s1 = o1.getName();
            String s2 = o2.getName();
            if (s1 == null || s2 == null){
                s1 = o1.getD().toString();
                s2 = o2.getD().toString();
            }
            return s1.toLowerCase().compareTo(s2.toLowerCase());
        }
    }

    @VisibleForTesting
    static final Comparator<Entity> entityComparator = new EntityComparator();
}
