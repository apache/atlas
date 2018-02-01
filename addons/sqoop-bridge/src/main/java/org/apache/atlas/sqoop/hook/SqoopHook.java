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
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.notification.hook.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.atlas.sqoop.model.SqoopDataTypes;
import org.apache.atlas.type.AtlasTypeUtil;
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

    public static final String CONF_PREFIX          = "atlas.hook.sqoop.";
    public static final String HOOK_NUM_RETRIES     = CONF_PREFIX + "numRetries";
    public static final String ATLAS_CLUSTER_NAME   = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";

    public static final String USER           = "userName";
    public static final String DB_STORE_TYPE  = "dbStoreType";
    public static final String DB_STORE_USAGE = "storeUse";
    public static final String SOURCE         = "source";
    public static final String DESCRIPTION    = "description";
    public static final String STORE_URI      = "storeUri";
    public static final String OPERATION      = "operation";
    public static final String START_TIME     = "startTime";
    public static final String END_TIME       = "endTime";
    public static final String CMD_LINE_OPTS  = "commandlineOpts";
    public static final String INPUTS         = "inputs";
    public static final String OUTPUTS        = "outputs";

    static {
        org.apache.hadoop.conf.Configuration.addDefaultResource("sqoop-site.xml");
    }

    @Override
    public void publish(SqoopJobDataPublisher.Data data) throws AtlasHookException {
        try {
            Configuration atlasProperties = ApplicationProperties.get();
            String        clusterName     = atlasProperties.getString(ATLAS_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
            AtlasEntity   entDbStore      = createDBStoreInstance(data);
            AtlasEntity   entHiveDb       = createHiveDatabaseInstance(clusterName, data.getHiveDB());
            AtlasEntity   entHiveTable    = createHiveTableInstance(entHiveDb, data.getHiveTable());
            AtlasEntity   entProcess      = createSqoopProcessInstance(entDbStore, entHiveTable, data, clusterName);

            AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo(entProcess);

            entities.addReferredEntity(entDbStore);
            entities.addReferredEntity(entHiveDb);
            entities.addReferredEntity(entHiveTable);

            HookNotificationMessage message  = new EntityUpdateRequestV2(AtlasHook.getUser(), entities);

            AtlasHook.notifyEntities(Arrays.asList(message), atlasProperties.getInt(HOOK_NUM_RETRIES, 3));
        } catch(Exception e) {
            throw new AtlasHookException("SqoopHook.publish() failed.", e);
        }
    }

    private AtlasEntity createHiveDatabaseInstance(String clusterName, String dbName) {
        AtlasEntity entHiveDb     = new AtlasEntity(HiveDataTypes.HIVE_DB.getName());
        String      qualifiedName = HiveMetaStoreBridge.getDBQualifiedName(clusterName, dbName);

        entHiveDb.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        entHiveDb.setAttribute(AtlasClient.NAME, dbName);
        entHiveDb.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName);

        return entHiveDb;
    }

    private AtlasEntity createHiveTableInstance(AtlasEntity entHiveDb, String tableName) {
        AtlasEntity entHiveTable  = new AtlasEntity(HiveDataTypes.HIVE_TABLE.getName());
        String      qualifiedName = HiveMetaStoreBridge.getTableQualifiedName((String)entHiveDb.getAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE), (String)entHiveDb.getAttribute(AtlasClient.NAME), tableName);

        entHiveTable.setAttribute(AtlasClient.NAME, tableName.toLowerCase());
        entHiveTable.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName);
        entHiveTable.setAttribute(HiveMetaStoreBridge.DB, AtlasTypeUtil.getAtlasObjectId(entHiveDb));

        return entHiveTable;
    }

    private AtlasEntity createDBStoreInstance(SqoopJobDataPublisher.Data data) throws ImportException {
        String table = data.getStoreTable();
        String query = data.getStoreQuery();

        if (StringUtils.isBlank(table) && StringUtils.isBlank(query)) {
            throw new ImportException("Both table and query cannot be empty for DBStoreInstance");
        }

        String usage  = table != null ? "TABLE" : "QUERY";
        String source = table != null ? table : query;
        String name   = getSqoopDBStoreName(data);

        AtlasEntity entDbStore = new AtlasEntity(SqoopDataTypes.SQOOP_DBDATASTORE.getName());

        entDbStore.setAttribute(AtlasClient.NAME, name);
        entDbStore.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        entDbStore.setAttribute(SqoopHook.DB_STORE_TYPE, data.getStoreType());
        entDbStore.setAttribute(SqoopHook.DB_STORE_USAGE, usage);
        entDbStore.setAttribute(SqoopHook.STORE_URI, data.getUrl());
        entDbStore.setAttribute(SqoopHook.SOURCE, source);
        entDbStore.setAttribute(SqoopHook.DESCRIPTION, "");
        entDbStore.setAttribute(AtlasClient.OWNER, data.getUser());

        return entDbStore;
    }

    private AtlasEntity createSqoopProcessInstance(AtlasEntity entDbStore, AtlasEntity entHiveTable, SqoopJobDataPublisher.Data data, String clusterName) {
        AtlasEntity         entProcess       = new AtlasEntity(SqoopDataTypes.SQOOP_PROCESS.getName());
        String              sqoopProcessName = getSqoopProcessName(data, clusterName);
        Map<String, String> sqoopOptionsMap  = new HashMap<>();
        Properties          options          = data.getOptions();

        for (Object k : options.keySet()) {
            sqoopOptionsMap.put((String)k, (String) options.get(k));
        }

        entProcess.setAttribute(AtlasClient.NAME, sqoopProcessName);
        entProcess.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sqoopProcessName);
        entProcess.setAttribute(SqoopHook.OPERATION, data.getOperation());

        if (isImportOperation(data)) {
            entProcess.setAttribute(SqoopHook.INPUTS, AtlasTypeUtil.getAtlasObjectId(entDbStore));
            entProcess.setAttribute(SqoopHook.OUTPUTS, AtlasTypeUtil.getAtlasObjectId(entHiveTable));
        } else {
            entProcess.setAttribute(SqoopHook.INPUTS, AtlasTypeUtil.getAtlasObjectId(entHiveTable));
            entProcess.setAttribute(SqoopHook.OUTPUTS, AtlasTypeUtil.getAtlasObjectId(entDbStore));
        }

        entProcess.setAttribute(SqoopHook.USER, data.getUser());
        entProcess.setAttribute(SqoopHook.START_TIME, new Date(data.getStartTime()));
        entProcess.setAttribute(SqoopHook.END_TIME, new Date(data.getEndTime()));
        entProcess.setAttribute(SqoopHook.CMD_LINE_OPTS, sqoopOptionsMap);

        return entProcess;
    }

    private boolean isImportOperation(SqoopJobDataPublisher.Data data) {
        return data.getOperation().toLowerCase().equals("import");
    }

    static String getSqoopProcessName(Data data, String clusterName) {
        StringBuilder name = new StringBuilder(String.format("sqoop %s --connect %s", data.getOperation(), data.getUrl()));

        if (StringUtils.isNotEmpty(data.getStoreTable())) {
            name.append(" --table ").append(data.getStoreTable());
        }

        if (StringUtils.isNotEmpty(data.getStoreQuery())) {
            name.append(" --query ").append(data.getStoreQuery());
        }

        name.append(String.format(" --hive-%s --hive-database %s --hive-table %s --hive-cluster %s", data.getOperation(), data.getHiveDB().toLowerCase(), data.getHiveTable().toLowerCase(), clusterName));

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
}
