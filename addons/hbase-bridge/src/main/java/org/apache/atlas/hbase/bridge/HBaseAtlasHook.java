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

package org.apache.atlas.hbase.bridge;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.hbase.model.HBaseOperationContext;
import org.apache.atlas.hbase.model.HBaseDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// This will register Hbase entities into Atlas
public class HBaseAtlasHook extends AtlasHook {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseAtlasHook.class);

    public static final  String CONF_PREFIX      = "atlas.hook.hbase.";
    public static final  String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";
    public static final  String QUEUE_SIZE       = CONF_PREFIX + "queueSize";
    public static final  String CONF_SYNC        = CONF_PREFIX + "synchronous";
    private static final String MIN_THREADS      = CONF_PREFIX + "minThreads";
    private static final String MAX_THREADS      = CONF_PREFIX + "maxThreads";
    private static final String KEEP_ALIVE_TIME  = CONF_PREFIX + "keepAliveTime";

    private static final int  minThreadsDefault    = 5;
    private static final int  maxThreadsDefault    = 5;
    private static final int  queueSizeDefault     = 10000;
    private static final long keepAliveTimeDefault = 10;
    // wait time determines how long we wait before we exit the jvm on shutdown. Pending requests after that will not be sent.
    private static final int  WAIT_TIME            = 3;
    private static boolean         sync;
    private static ExecutorService executor;

    public static final String HBASE_CLUSTER_NAME   = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";
    public static final String ATTR_DESCRIPTION     = "description";
    public static final String ATTR_ATLAS_ENDPOINT  = "atlas.rest.address";
    public static final String ATTR_COMMENT         = "comment";
    public static final String ATTR_PARAMETERS      = "parameters";
    public static final String ATTR_URI             = "uri";
    public static final String ATTR_NAMESPACE       = "namespace";
    public static final String ATTR_TABLE           = "table";
    public static final String ATTR_COLUMNFAMILIES  = "column_families";
    public static final String ATTR_CREATE_TIME     = "createTime";
    public static final String ATTR_MODIFIED_TIME   = "modifiedTime";
    public static final String ATTR_OWNER           = "owner";
    public static final String ATTR_NAME            = "name";

    private static final String REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";
    private              String clusterName                  = null;

    private static volatile HBaseAtlasHook me;

    public enum OPERATION {
        CREATE_NAMESPACE("create_namespace"),
        ALTER_NAMESPACE("alter_namespace"),
        DELETE_NAMESPACE("delete_namespace"),
        CREATE_TABLE("create_table"),
        ALTER_TABLE("alter_table"),
        DELETE_TABLE("delete_table"),
        CREATE_COLUMN_FAMILY("create_column_Family"),
        ALTER_COLUMN_FAMILY("alter_column_Family"),
        DELETE_COLUMN_FAMILY("delete_column_Family");

        private final String name;

        OPERATION(String s) {
            name = s;
        }

        public String getName() {
            return name;
        }
    }

    static {
        try {
            // initialize the async facility to process hook calls. We don't
            // want to do this inline since it adds plenty of overhead for the query.
            int  minThreads    = atlasProperties.getInt(MIN_THREADS, minThreadsDefault);
            int  maxThreads    = atlasProperties.getInt(MAX_THREADS, maxThreadsDefault);
            int  queueSize     = atlasProperties.getInt(QUEUE_SIZE, queueSizeDefault);
            long keepAliveTime = atlasProperties.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);

            sync = atlasProperties.getBoolean(CONF_SYNC, false);
            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                                              new LinkedBlockingQueue<Runnable>(queueSize), 
                                              new ThreadFactoryBuilder().setNameFormat("Atlas Logger %d").build());

            ShutdownHookManager.get().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        LOG.info("==> Shutdown of Atlas HBase Hook");
                        executor.shutdown();
                        executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                        executor = null;
                    } catch (InterruptedException ie) {
                        LOG.info("Interrupt received in shutdown.", ie);
                    } finally {
                        LOG.info("<== Shutdown of Atlas HBase Hook");
                    }
                    // shutdown client
                }
            }, AtlasConstants.ATLAS_SHUTDOWN_HOOK_PRIORITY);
        } catch (Exception e) {
            LOG.error("Caught exception initializing the Atlas HBase hook.", e);
        }

        LOG.info("Created Atlas Hook for HBase");
    }

    public static HBaseAtlasHook getInstance() {
        HBaseAtlasHook ret = me;

        if (ret == null) {
            try {
                synchronized (HBaseAtlasHook.class) {
                    ret = me;

                    if (ret == null) {
                        me = ret = new HBaseAtlasHook(atlasProperties);
                    }
                }
            } catch (Exception e) {
                LOG.error("Caught exception instantiating the Atlas HBase hook.", e);
            }
        }

        return ret;
    }

    public HBaseAtlasHook(Configuration atlasProperties) {
        this(atlasProperties.getString(HBASE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME));
    }

    public HBaseAtlasHook(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }


    public void createAtlasInstances(HBaseOperationContext hbaseOperationContext) {
        HBaseAtlasHook.OPERATION operation = hbaseOperationContext.getOperation();

        LOG.info("HBaseAtlasHook(operation={})", operation);

        switch (operation) {
            case CREATE_NAMESPACE:
            case ALTER_NAMESPACE:
                createOrUpdateNamespaceInstance(hbaseOperationContext);
                break;
            case DELETE_NAMESPACE:
                deleteNameSpaceInstance(hbaseOperationContext);
                break;
            case CREATE_TABLE:
            case ALTER_TABLE:
                createOrUpdateTableInstance(hbaseOperationContext);
                break;
            case DELETE_TABLE:
                deleteTableInstance(hbaseOperationContext);
                break;
            case CREATE_COLUMN_FAMILY:
            case ALTER_COLUMN_FAMILY:
                createOrUpdateColumnFamilyInstance(hbaseOperationContext);
                break;
            case DELETE_COLUMN_FAMILY:
                deleteColumnFamilyInstance(hbaseOperationContext);
                break;
        }
    }

    private void createOrUpdateNamespaceInstance(HBaseOperationContext hbaseOperationContext) {
        Referenceable nameSpaceRef = buildNameSpaceRef(hbaseOperationContext);

        switch (hbaseOperationContext.getOperation()) {
            case CREATE_NAMESPACE:
                LOG.info("Create NameSpace {}", nameSpaceRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityCreateRequest(hbaseOperationContext.getUser(), nameSpaceRef));
                break;

            case ALTER_NAMESPACE:
                LOG.info("Modify NameSpace {}", nameSpaceRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityUpdateRequest(hbaseOperationContext.getUser(), nameSpaceRef));
                break;
        }
    }

    private void deleteNameSpaceInstance(HBaseOperationContext hbaseOperationContext) {
        String nameSpaceQualifiedName = getNameSpaceQualifiedName(clusterName, hbaseOperationContext.getNameSpace());

        LOG.info("Delete NameSpace {}", nameSpaceQualifiedName);

        hbaseOperationContext.addMessage(new EntityDeleteRequest(hbaseOperationContext.getUser(),
                                                                 HBaseDataTypes.HBASE_NAMESPACE.getName(),
                                                                 REFERENCEABLE_ATTRIBUTE_NAME,
                                                                 nameSpaceQualifiedName));
    }

    private void createOrUpdateTableInstance(HBaseOperationContext hbaseOperationContext) {
        Referenceable       nameSpaceRef    = buildNameSpaceRef(hbaseOperationContext);
        Referenceable       tableRef        = buildTableRef(hbaseOperationContext, nameSpaceRef);
        List<Referenceable> columnFamilyRef = buildColumnFamiliesRef(hbaseOperationContext, nameSpaceRef, tableRef);

        tableRef.set(ATTR_COLUMNFAMILIES, columnFamilyRef);

        switch (hbaseOperationContext.getOperation()) {
            case CREATE_TABLE:
                LOG.info("Create Table {}", tableRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityCreateRequest(hbaseOperationContext.getUser(), nameSpaceRef, tableRef));
                break;

            case ALTER_TABLE:
                LOG.info("Modify Table {}", tableRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityUpdateRequest(hbaseOperationContext.getUser(), nameSpaceRef, tableRef));
                break;
        }
    }

    private void deleteTableInstance(HBaseOperationContext hbaseOperationContext) {
        TableName tableName      = hbaseOperationContext.getTableName();
        String    tableNameSpace = tableName.getNamespaceAsString();

        if (tableNameSpace == null) {
            tableNameSpace = tableName.getNameWithNamespaceInclAsString();
        }

        String tableNameStr       = tableName.getNameAsString();
        String tableQualifiedName = getTableQualifiedName(clusterName, tableNameSpace, tableNameStr);

        LOG.info("Delete Table {}", tableQualifiedName);

        hbaseOperationContext.addMessage(new EntityDeleteRequest(hbaseOperationContext.getUser(),
                                                                 HBaseDataTypes.HBASE_TABLE.getName(),
                                                                 REFERENCEABLE_ATTRIBUTE_NAME,
                                                                 tableQualifiedName));
    }

    private void createOrUpdateColumnFamilyInstance(HBaseOperationContext hbaseOperationContext) {
        Referenceable nameSpaceRef    = buildNameSpaceRef(hbaseOperationContext);
        Referenceable tableRef        = buildTableRef(hbaseOperationContext, nameSpaceRef);
        Referenceable columnFamilyRef = buildColumnFamilyRef(hbaseOperationContext, hbaseOperationContext.gethColumnDescriptor(), nameSpaceRef, tableRef);

        switch (hbaseOperationContext.getOperation()) {
            case CREATE_COLUMN_FAMILY:
                LOG.info("Create ColumnFamily {}", columnFamilyRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityCreateRequest(hbaseOperationContext.getUser(), nameSpaceRef, tableRef, columnFamilyRef));
                break;

            case ALTER_COLUMN_FAMILY:
                LOG.info("Alter ColumnFamily {}", columnFamilyRef.get(REFERENCEABLE_ATTRIBUTE_NAME));

                hbaseOperationContext.addMessage(new EntityUpdateRequest(hbaseOperationContext.getUser(), nameSpaceRef, tableRef, columnFamilyRef));
                break;
        }
    }

    private void deleteColumnFamilyInstance(HBaseOperationContext hbaseOperationContext) {
        TableName tableName      = hbaseOperationContext.getTableName();
        String    tableNameSpace = tableName.getNamespaceAsString();

        if (tableNameSpace == null) {
            tableNameSpace = tableName.getNameWithNamespaceInclAsString();
        }

        String tableNameStr              = tableName.getNameAsString();
        String columnFamilyName          = hbaseOperationContext.getColummFamily();
        String columnFamilyQualifiedName = getColumnFamilyQualifiedName(clusterName, tableNameSpace, tableNameStr, columnFamilyName);

        LOG.info("Delete ColumnFamily {}", columnFamilyQualifiedName);

        hbaseOperationContext.addMessage(new EntityDeleteRequest(hbaseOperationContext.getUser(),
                                                                 HBaseDataTypes.HBASE_COLUMN_FAMILY.getName(),
                                                                 REFERENCEABLE_ATTRIBUTE_NAME,
                                                                 columnFamilyQualifiedName));
    }


    /**
     * Construct the qualified name used to uniquely identify a ColumnFamily instance in Atlas.
     *
     * @param clusterName  Name of the cluster to which the HBase component belongs
     * @param nameSpace    Name of the HBase database to which the Table belongs
     * @param tableName    Name of the HBase table
     * @param columnFamily Name of the ColumnFamily
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getColumnFamilyQualifiedName(String clusterName, String nameSpace, String tableName, String columnFamily) {
        return String.format("%s.%s.%s@%s", nameSpace.toLowerCase(), stripNameSpace(tableName.toLowerCase()), columnFamily.toLowerCase(), clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     *
     * @param clusterName Name of the cluster to which the HBase component belongs
     * @param nameSpace   Name of the HBase database to which the Table belongs
     * @param tableName   Name of the HBase table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String clusterName, String nameSpace, String tableName) {
        return String.format("%s.%s@%s", nameSpace.toLowerCase(), stripNameSpace(tableName.toLowerCase()), clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a HBase NameSpace instance in Atlas.
     *
     * @param clusterName Name of the cluster to which the HBase component belongs
     * @param nameSpace
     * @return Unique qualified name to identify the HBase NameSpace instance in Atlas.
     */
    public static String getNameSpaceQualifiedName(String clusterName, String nameSpace) {
        return String.format("%s@%s", nameSpace.toLowerCase(), clusterName);
    }

    private static String stripNameSpace(String tableName) {
        return tableName.substring(tableName.indexOf(":") + 1);
    }

    private Referenceable buildNameSpaceRef(HBaseOperationContext hbaseOperationContext) {
        Referenceable nameSpaceRef = new Referenceable(HBaseDataTypes.HBASE_NAMESPACE.getName());

        String nameSpace = null;

        NamespaceDescriptor nameSpaceDesc = hbaseOperationContext.getNamespaceDescriptor();

        if (nameSpaceDesc != null) {
            nameSpace = hbaseOperationContext.getNamespaceDescriptor().getName();
        }

        if (nameSpace == null) {
            nameSpace = hbaseOperationContext.getNameSpace();
        }

        nameSpaceRef.set(ATTR_NAME, nameSpace);
        nameSpaceRef.set(REFERENCEABLE_ATTRIBUTE_NAME, getNameSpaceQualifiedName(clusterName, nameSpace));
        nameSpaceRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        nameSpaceRef.set(ATTR_DESCRIPTION, nameSpace);
        nameSpaceRef.set(ATTR_PARAMETERS, hbaseOperationContext.getHbaseConf());
        nameSpaceRef.set(ATTR_OWNER, hbaseOperationContext.getOwner());

        Date now = new Date(System.currentTimeMillis());

        if (OPERATION.CREATE_NAMESPACE.equals(hbaseOperationContext.getOperation())) {
            nameSpaceRef.set(ATTR_CREATE_TIME, now);
            nameSpaceRef.set(ATTR_MODIFIED_TIME, now);
        } else {
            nameSpaceRef.set(ATTR_MODIFIED_TIME, now);
        }

        return nameSpaceRef;
    }

    private Referenceable buildTableRef(HBaseOperationContext hbaseOperationContext, Referenceable nameSpaceRef) {
        Referenceable tableRef  = new Referenceable(HBaseDataTypes.HBASE_TABLE.getName());
        String        tableName = getTableName(hbaseOperationContext);
        String    tableNameSpace     = hbaseOperationContext.getNameSpace();
        String    tableQualifiedName = getTableQualifiedName(clusterName, tableNameSpace, tableName);
        OPERATION operation          = hbaseOperationContext.getOperation();
        Date      now                = new Date(System.currentTimeMillis());

        tableRef.set(REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName);
        tableRef.set(ATTR_NAME, tableName);
        tableRef.set(ATTR_URI, tableName);
        tableRef.set(ATTR_OWNER, hbaseOperationContext.getOwner());
        tableRef.set(ATTR_DESCRIPTION, tableName);
        tableRef.set(ATTR_PARAMETERS, hbaseOperationContext.getHbaseConf());

        switch (operation) {
            case CREATE_TABLE:
                tableRef.set(ATTR_NAMESPACE, nameSpaceRef);
                tableRef.set(ATTR_CREATE_TIME, now);
                tableRef.set(ATTR_MODIFIED_TIME, now);
                break;
            case ALTER_TABLE:
                tableRef.set(ATTR_NAMESPACE, nameSpaceRef);
                tableRef.set(ATTR_MODIFIED_TIME, now);
                break;
            default:
                tableRef.set(ATTR_NAMESPACE, nameSpaceRef.getId());
                break;
        }

        return tableRef;
    }

    private List<Referenceable> buildColumnFamiliesRef(HBaseOperationContext hbaseOperationContext, Referenceable nameSpaceRef, Referenceable tableRef) {
        List<Referenceable> entities = new ArrayList<>();

        HColumnDescriptor[] hColumnDescriptors = hbaseOperationContext.gethColumnDescriptors();

        if (hColumnDescriptors != null) {
            for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
                Referenceable columnFamilyRef = buildColumnFamilyRef(hbaseOperationContext, hColumnDescriptor, nameSpaceRef, tableRef);

                entities.add(columnFamilyRef);
            }
        }

        return entities;
    }

    private Referenceable buildColumnFamilyRef(HBaseOperationContext hbaseOperationContext, HColumnDescriptor hColumnDescriptor, Referenceable nameSpaceRef, Referenceable tableReference) {
        Referenceable columnFamilyRef  = new Referenceable(HBaseDataTypes.HBASE_COLUMN_FAMILY.getName());
        String        columnFamilyName = hColumnDescriptor.getNameAsString();
        String        tableName        = (String) tableReference.get(ATTR_NAME);
        String        namespace        = (String) nameSpaceRef.get(ATTR_NAME);

        String columnFamilyQualifiedName = getColumnFamilyQualifiedName(clusterName, namespace, tableName, columnFamilyName);

        columnFamilyRef.set(ATTR_NAME, columnFamilyName);
        columnFamilyRef.set(ATTR_DESCRIPTION, columnFamilyName);
        columnFamilyRef.set(REFERENCEABLE_ATTRIBUTE_NAME, columnFamilyQualifiedName);
        columnFamilyRef.set(ATTR_OWNER, hbaseOperationContext.getOwner());

        Date now = new Date(System.currentTimeMillis());

        switch (hbaseOperationContext.getOperation()) {
            case CREATE_COLUMN_FAMILY:
                columnFamilyRef.set(ATTR_TABLE, tableReference);
                columnFamilyRef.set(ATTR_CREATE_TIME, now);
                columnFamilyRef.set(ATTR_MODIFIED_TIME, now);
                break;

            case ALTER_COLUMN_FAMILY:
                columnFamilyRef.set(ATTR_TABLE, tableReference);
                columnFamilyRef.set(ATTR_MODIFIED_TIME, now);
                break;

            default:
                columnFamilyRef.set(ATTR_TABLE, tableReference.getId());
        }

        return columnFamilyRef;
    }

    private String getTableName(HBaseOperationContext hbaseOperationContext) {
        HTableDescriptor tableDescriptor = hbaseOperationContext.gethTableDescriptor();

        return (tableDescriptor != null) ? tableDescriptor.getNameAsString() : null;
    }

    private void notifyAsPrivilegedAction(final HBaseOperationContext hbaseOperationContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.notifyAsPrivilegedAction({})", hbaseOperationContext);
        }

        final List<HookNotification> messages = hbaseOperationContext.getMessages();


        try {
            PrivilegedExceptionAction<Object> privilegedNotify = new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() {
                    notifyEntities(messages);
                    return hbaseOperationContext;
                }
            };

            //Notify as 'hbase' service user in doAs mode
            UserGroupInformation realUser         = hbaseOperationContext.getUgi().getRealUser();
            String               numberOfMessages = Integer.toString(messages.size());
            String               operation        = hbaseOperationContext.getOperation().toString();
            String               user             = hbaseOperationContext.getUgi().getShortUserName();

            if (realUser != null) {
                LOG.info("Sending notification for event {} as service user {} #messages {}", operation, realUser.getShortUserName(), numberOfMessages);

                realUser.doAs(privilegedNotify);
            } else {
                LOG.info("Sending notification for event {} as service user {} #messages {}", operation, user, numberOfMessages);

                hbaseOperationContext.getUgi().doAs(privilegedNotify);
            }
        } catch (Throwable e) {
            LOG.error("Error during notify {} ", hbaseOperationContext.getOperation(), e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasHook.notifyAsPrivilegedAction()");
        }
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages hook notification messages
     */
    protected void notifyEntities(List<HookNotification> messages) {
        final int maxRetries = atlasProperties.getInt(HOOK_NUM_RETRIES, 3);
        notifyEntities(messages, maxRetries);
    }

    public void sendHBaseNameSpaceOperation(final NamespaceDescriptor namespaceDescriptor, final String nameSpace, final OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.sendHBaseNameSpaceOperation()");
        }
        try {
            final UserGroupInformation ugi                   = getUGI();
            HBaseOperationContext      hbaseOperationContext = null;
            if (executor == null) {
                hbaseOperationContext = handleHBaseNameSpaceOperation(namespaceDescriptor, nameSpace, operation);
                if (hbaseOperationContext != null) {
                    notifyAsPrivilegedAction(hbaseOperationContext);
                }
            } else {
                executor.submit(new Runnable() {
                    HBaseOperationContext hbaseOperationContext = null;

                    @Override
                    public void run() {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> HBaseAtlasHook.sendHBaseNameSpaceOperation():executor.submit()");
                        }
                        if (ugi != null) {
                            try {
                                ugi.doAs(new PrivilegedExceptionAction<Object>() {
                                    @Override
                                    public Object run() {
                                        hbaseOperationContext = handleHBaseNameSpaceOperation(namespaceDescriptor, nameSpace, operation);
                                        return hbaseOperationContext;
                                    }
                                });
                                notifyAsPrivilegedAction(hbaseOperationContext);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("<== HBaseAtlasHook.sendHBaseNameSpaceOperation(){}",  hbaseOperationContext);
                                }
                            } catch (Throwable e) {
                                LOG.error("<== HBaseAtlasHook.sendHBaseNameSpaceOperation(): Atlas hook failed due to error ", e);
                            }
                        } else {
                            LOG.error("<== HBaseAtlasHook.sendHBaseNameSpaceOperation(): Atlas hook failed, UserGroupInformation cannot be NULL!");
                        }
                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("<== HBaseAtlasHook.sendHBaseNameSpaceOperation(): Submitting to thread pool failed due to error ", t);
        }
    }

    public void sendHBaseTableOperation(final HTableDescriptor hTableDescriptor, final TableName tableName, final OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.sendHBaseTableOperation()");
        }
        try {
            final UserGroupInformation ugi                   = getUGI();
            HBaseOperationContext      hbaseOperationContext = null;
            if (executor == null) {
                hbaseOperationContext = handleHBaseTableOperation(hTableDescriptor, tableName, operation);
                if (hbaseOperationContext != null) {
                    notifyAsPrivilegedAction(hbaseOperationContext);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== HBaseAtlasHook.sendHBaseTableOperation(){}",  hbaseOperationContext);
                }
            } else {
                executor.submit(new Runnable() {
                    HBaseOperationContext hbaseOperationContext = null;

                    @Override
                    public void run() {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> HBaseAtlasHook.sendHBaseTableOperation():executor.submit()");
                        }
                        if (ugi != null) {
                            try {
                                ugi.doAs(new PrivilegedExceptionAction<Object>() {
                                    @Override
                                    public Object run() {
                                        hbaseOperationContext = handleHBaseTableOperation(hTableDescriptor, tableName, operation);
                                        return hbaseOperationContext;
                                    }
                                });
                                notifyAsPrivilegedAction(hbaseOperationContext);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("<== HBaseAtlasHook.sendHBaseTableOperation(){}",  hbaseOperationContext);
                                }
                            } catch (Throwable e) {
                                LOG.error("<== HBaseAtlasHook.sendHBaseTableOperation(): Atlas hook failed due to error ", e);
                            }
                        } else {
                            LOG.error("<== HBaseAtlasHook.sendHBasecolumnFamilyOperation(): Atlas hook failed, UserGroupInformation cannot be NULL!");
                        }
                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("<== HBaseAtlasHook.sendHBaseTableOperation(): Submitting to thread pool failed due to error ", t);
        }
    }

    public void sendHBaseColumnFamilyOperation(final HColumnDescriptor hColumnDescriptor, final TableName tableName, final String columnFamily, final OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.sendHBaseColumnFamilyOperation()");
        }
        try {
            final UserGroupInformation ugi                   = getUGI();
            HBaseOperationContext      hbaseOperationContext = null;
            if (executor == null) {
                hbaseOperationContext = handleHBaseColumnFamilyOperation(hColumnDescriptor, tableName, columnFamily, operation);
                if (hbaseOperationContext != null) {
                    notifyAsPrivilegedAction(hbaseOperationContext);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== HBaseAtlasHook.sendHBaseColumnFamilyOperation(){}",  hbaseOperationContext);
                }
            } else {
                executor.submit(new Runnable() {
                    HBaseOperationContext hbaseOperationContext = null;

                    @Override
                    public void run() {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> HBaseAtlasHook.sendHBaseColumnFamilyOperation():executor.submit()");
                        }
                        if (ugi != null) {
                            try {
                                ugi.doAs(new PrivilegedExceptionAction<Object>() {
                                    @Override
                                    public Object run() {
                                        hbaseOperationContext = handleHBaseColumnFamilyOperation(hColumnDescriptor, tableName, columnFamily, operation);
                                        return hbaseOperationContext;
                                    }
                                });
                                notifyAsPrivilegedAction(hbaseOperationContext);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("<== HBaseAtlasHook.sendHBaseColumnFamilyOperation(){}",  hbaseOperationContext);
                                }
                            } catch (Throwable e) {
                                LOG.error("<== HBaseAtlasHook.sendHBaseColumnFamilyOperation(): Atlas hook failed due to error ", e);
                            }
                        } else {
                            LOG.error("<== HBaseAtlasHook.sendHBaseColumnFamilyOperation(): Atlas hook failed, UserGroupInformation cannot be NULL!");
                        }

                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("<== HBaseAtlasHook.sendHBaseColumnFamilyOperation(): Submitting to thread pool failed due to error ", t);
        }
    }

    private HBaseOperationContext handleHBaseNameSpaceOperation(NamespaceDescriptor namespaceDescriptor, String nameSpace, OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.handleHBaseNameSpaceOperation()");
        }

        UserGroupInformation ugi      = getUGI();
        User                 user     = getActiveUser();
        String               userName = user.getShortName();

        HBaseOperationContext hbaseOperationContext = new HBaseOperationContext(namespaceDescriptor, nameSpace, operation, ugi, userName, userName);
        createAtlasInstances(hbaseOperationContext);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasHook.handleHBaseNameSpaceOperation(): {}",  hbaseOperationContext);
        }

        return hbaseOperationContext;
    }

    private HBaseOperationContext handleHBaseTableOperation(HTableDescriptor hTableDescriptor, TableName tableName, OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.handleHBaseTableOperation()");
        }

        UserGroupInformation ugi                = getUGI();
        User                 user               = getActiveUser();
        String               userName           = user.getShortName();
        Map<String, String>  hbaseConf          = null;
        String               owner              = null;
        String               tableNameSpace     = null;
        TableName            hbaseTableName     = null;
        HColumnDescriptor[]  hColumnDescriptors = null;

        if (hTableDescriptor != null) {
            owner = hTableDescriptor.getOwnerString();
            hbaseConf = hTableDescriptor.getConfiguration();
            hbaseTableName = hTableDescriptor.getTableName();
            if (hbaseTableName != null) {
                tableNameSpace = hbaseTableName.getNamespaceAsString();
                if (tableNameSpace == null) {
                    tableNameSpace = hbaseTableName.getNameWithNamespaceInclAsString();
                }
            }
        }

        if (owner == null) {
            owner = userName;
        }

        if (hTableDescriptor != null) {
            hColumnDescriptors = hTableDescriptor.getColumnFamilies();
        }

        HBaseOperationContext hbaseOperationContext = new HBaseOperationContext(tableNameSpace, hTableDescriptor, tableName, hColumnDescriptors, operation, ugi, userName, owner, hbaseConf);
        createAtlasInstances(hbaseOperationContext);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasHook.handleHBaseTableOperation(): {}",  hbaseOperationContext);
        }
        return hbaseOperationContext;
    }

    private HBaseOperationContext handleHBaseColumnFamilyOperation(HColumnDescriptor hColumnDescriptor, TableName tableName, String columnFamily, OPERATION operation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasHook.handleHBaseColumnFamilyOperation()");
        }

        UserGroupInformation ugi       = getUGI();
        User                 user      = getActiveUser();
        String               userName  = user.getShortName();
        String               owner     = userName;
        Map<String, String>  hbaseConf = null;

        String tableNameSpace = tableName.getNamespaceAsString();
        if (tableNameSpace == null) {
            tableNameSpace = tableName.getNameWithNamespaceInclAsString();
        }

        if (hColumnDescriptor != null) {
            hbaseConf = hColumnDescriptor.getConfiguration();
        }

        HBaseOperationContext hbaseOperationContext = new HBaseOperationContext(tableNameSpace, tableName, hColumnDescriptor, columnFamily, operation, ugi, userName, owner, hbaseConf);
        createAtlasInstances(hbaseOperationContext);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasHook.handleHBaseColumnFamilyOperation(): {}",  hbaseOperationContext);
        }
        return hbaseOperationContext;
    }

    private User getActiveUser() {
        User user = RpcServer.getRequestUser();
        if (user == null) {
            // for non-rpc handling, fallback to system user
            try {
                user = User.getCurrent();
            } catch (IOException e) {
                LOG.error("Unable to find the current user");
                user = null;
            }
        }
        return user;
    }

    private UserGroupInformation getUGI() {
        UserGroupInformation ugi  = null;
        User                 user = getActiveUser();

        try {
            ugi = UserGroupInformation.getLoginUser();
        } catch (Exception e) {
            // not setting the UGI here
        }

        if (ugi == null) {
            if (user != null) {
                ugi = user.getUGI();
            }
        }

        LOG.info("HBaseAtlasHook: UGI: {}",  ugi);
        return ugi;
    }
}
