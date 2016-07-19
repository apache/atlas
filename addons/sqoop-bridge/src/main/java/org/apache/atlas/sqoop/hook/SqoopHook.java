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

package org.apache.atlas.sqoop.hook;


import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.sqoop.model.SqoopDataModelGenerator;
import org.apache.atlas.sqoop.model.SqoopDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.SqoopJobDataPublisher;
import org.apache.sqoop.util.ImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * AtlasHook sends lineage information to the AtlasSever.
 */
public class SqoopHook extends SqoopJobDataPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(SqoopHook.class);
    public static final String CONF_PREFIX = "atlas.hook.sqoop.";
    public static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    public static final String ATLAS_CLUSTER_NAME = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";

    static {
        org.apache.hadoop.conf.Configuration.addDefaultResource("sqoop-site.xml");
    }

    public Referenceable createHiveDatabaseInstance(String clusterName, String dbName)
            throws Exception {
        Referenceable dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
        dbRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        dbRef.set(AtlasClient.NAME, dbName);
        dbRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                HiveMetaStoreBridge.getDBQualifiedName(clusterName, dbName));
        return dbRef;
    }

    public Referenceable createHiveTableInstance(String clusterName, Referenceable dbRef,
                                             String tableName, String dbName) throws Exception {
        Referenceable tableRef = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        tableRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                HiveMetaStoreBridge.getTableQualifiedName(clusterName, dbName, tableName));
        tableRef.set(AtlasClient.NAME, tableName.toLowerCase());
        tableRef.set(HiveDataModelGenerator.DB, dbRef);
        return tableRef;
    }

    private Referenceable createDBStoreInstance(SqoopJobDataPublisher.Data data)
            throws ImportException {

        Referenceable storeRef = new Referenceable(SqoopDataTypes.SQOOP_DBDATASTORE.getName());
        String table = data.getStoreTable();
        String query = data.getStoreQuery();
        if (StringUtils.isBlank(table) && StringUtils.isBlank(query)) {
            throw new ImportException("Both table and query cannot be empty for DBStoreInstance");
        }

        String usage = table != null ? "TABLE" : "QUERY";
        String source = table != null ? table : query;
        String name = getSqoopDBStoreName(data);
        storeRef.set(AtlasClient.NAME, name);
        storeRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        storeRef.set(SqoopDataModelGenerator.DB_STORE_TYPE, data.getStoreType());
        storeRef.set(SqoopDataModelGenerator.DB_STORE_USAGE, usage);
        storeRef.set(SqoopDataModelGenerator.STORE_URI, data.getUrl());
        storeRef.set(SqoopDataModelGenerator.SOURCE, source);
        storeRef.set(SqoopDataModelGenerator.DESCRIPTION, "");
        storeRef.set(AtlasClient.OWNER, data.getUser());
        return storeRef;
    }

    private Referenceable createSqoopProcessInstance(Referenceable dbStoreRef, Referenceable hiveTableRef,
                                                     SqoopJobDataPublisher.Data data, String clusterName) {
        Referenceable procRef = new Referenceable(SqoopDataTypes.SQOOP_PROCESS.getName());
        final String sqoopProcessName = getSqoopProcessName(data, clusterName);
        procRef.set(AtlasClient.NAME, sqoopProcessName);
        procRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sqoopProcessName);
        procRef.set(SqoopDataModelGenerator.OPERATION, data.getOperation());
        if (isImportOperation(data)) {
            procRef.set(SqoopDataModelGenerator.INPUTS, dbStoreRef);
            procRef.set(SqoopDataModelGenerator.OUTPUTS, hiveTableRef);
        } else {
            procRef.set(SqoopDataModelGenerator.INPUTS, hiveTableRef);
            procRef.set(SqoopDataModelGenerator.OUTPUTS, dbStoreRef);
        }
        procRef.set(SqoopDataModelGenerator.USER, data.getUser());
        procRef.set(SqoopDataModelGenerator.START_TIME, new Date(data.getStartTime()));
        procRef.set(SqoopDataModelGenerator.END_TIME, new Date(data.getEndTime()));

        Map<String, String> sqoopOptionsMap = new HashMap<>();
        Properties options = data.getOptions();
        for (Object k : options.keySet()) {
            sqoopOptionsMap.put((String)k, (String) options.get(k));
        }
        procRef.set(SqoopDataModelGenerator.CMD_LINE_OPTS, sqoopOptionsMap);

        return procRef;
    }

    static String getSqoopProcessName(Data data, String clusterName) {
        StringBuilder name = new StringBuilder(String.format("sqoop %s --connect %s", data.getOperation(),
                data.getUrl()));
        if (StringUtils.isNotEmpty(data.getStoreTable())) {
            name.append(" --table ").append(data.getStoreTable());
        }
        if (StringUtils.isNotEmpty(data.getStoreQuery())) {
            name.append(" --query ").append(data.getStoreQuery());
        }
        name.append(String.format(" --hive-%s --hive-database %s --hive-table %s --hive-cluster %s",
                data.getOperation(), data.getHiveDB().toLowerCase(), data.getHiveTable().toLowerCase(), clusterName));
        return name.toString();
    }

    static String getSqoopDBStoreName(SqoopJobDataPublisher.Data data)  {
        StringBuilder name = new StringBuilder(String.format("%s --url %s", data.getStoreType(), data.getUrl()));
        if (StringUtils.isNotEmpty(data.getStoreTable())) {
            name.append(" --table ").append(data.getStoreTable());
        }
        if (StringUtils.isNotEmpty(data.getStoreQuery())) {
            name.append(" --query ").append(data.getStoreQuery());
        }
        return name.toString();
    }

    static boolean isImportOperation(SqoopJobDataPublisher.Data data) {
        return data.getOperation().toLowerCase().equals("import");
    }

    @Override
    public void publish(SqoopJobDataPublisher.Data data) throws Exception {
        Configuration atlasProperties = ApplicationProperties.get();
        String clusterName = atlasProperties.getString(ATLAS_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);

        Referenceable dbStoreRef = createDBStoreInstance(data);
        Referenceable dbRef = createHiveDatabaseInstance(clusterName, data.getHiveDB());
        Referenceable hiveTableRef = createHiveTableInstance(clusterName, dbRef,
                data.getHiveTable(), data.getHiveDB());
        Referenceable procRef = createSqoopProcessInstance(dbStoreRef, hiveTableRef, data, clusterName);

        int maxRetries = atlasProperties.getInt(HOOK_NUM_RETRIES, 3);
        HookNotification.HookNotificationMessage message =
                new HookNotification.EntityCreateRequest(AtlasHook.getUser(), dbStoreRef, dbRef, hiveTableRef, procRef);
        AtlasHook.notifyEntities(Arrays.asList(message), maxRetries);
    }
}
