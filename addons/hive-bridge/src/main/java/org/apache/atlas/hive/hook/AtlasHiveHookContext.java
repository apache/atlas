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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class AtlasHiveHookContext {
    public static final char   QNAME_SEP_CLUSTER_NAME = '@';
    public static final char   QNAME_SEP_ENTITY_NAME  = '.';
    public static final char   QNAME_SEP_PROCESS      = ':';
    public static final String TEMP_TABLE_PREFIX      = "_temp-";

    private final HiveHook                 hook;
    private final HiveOperation            hiveOperation;
    private final HookContext              hiveContext;
    private final Hive                     hive;
    private final Map<String, AtlasEntity> qNameEntityMap = new HashMap<>();

    public AtlasHiveHookContext(HiveHook hook, HiveOperation hiveOperation, HookContext hiveContext) throws Exception {
        this.hook          = hook;
        this.hiveOperation = hiveOperation;
        this.hiveContext   = hiveContext;
        this.hive          = Hive.get(hiveContext.getConf());

        init();
    }

    public HookContext getHiveContext() {
        return hiveContext;
    }

    public Hive getHive() {
        return hive;
    }

    public HiveOperation getHiveOperation() {
        return hiveOperation;
    }

    public void putEntity(String qualifiedName, AtlasEntity entity) {
        qNameEntityMap.put(qualifiedName, entity);
    }

    public AtlasEntity getEntity(String qualifiedName) {
        return qNameEntityMap.get(qualifiedName);
    }

    public Collection<AtlasEntity> getEntities() { return qNameEntityMap.values(); }


    public String getClusterName() {
        return hook.getClusterName();
    }

    public String getQualifiedName(Database db) {
        return (db.getName() + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
    }

    public String getQualifiedName(Table table) {
        String tableName = table.getTableName();

        if (table.isTemporary()) {
            if (SessionState.get() != null && SessionState.get().getSessionId() != null) {
                tableName = tableName + TEMP_TABLE_PREFIX + SessionState.get().getSessionId();
            } else {
                tableName = tableName + TEMP_TABLE_PREFIX + RandomStringUtils.random(10);
            }
        }

        return (table.getDbName() + QNAME_SEP_ENTITY_NAME + tableName + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
    }

    public boolean isKnownDatabase(String dbQualifiedName) {
        return hook.isKnownDatabase(dbQualifiedName);
    }

    public boolean isKnownTable(String tblQualifiedName) {
        return hook.isKnownTable(tblQualifiedName);
    }

    public void addToKnownEntities(Collection<AtlasEntity> entities) {
        hook.addToKnownEntities(entities);
    }

    public void removeFromKnownDatabase(String dbQualifiedName) {
        hook.removeFromKnownDatabase(dbQualifiedName);
    }

    public void removeFromKnownTable(String tblQualifiedName) {
        hook.removeFromKnownTable(tblQualifiedName);
    }

    private void init() {
        // for create and alter operations, remove output entities from 'known' entity cache
        String operationName = hiveContext.getOperationName();

        if (operationName != null && operationName.startsWith("CREATE") || operationName.startsWith("ALTER")) {
            if (CollectionUtils.isNotEmpty(hiveContext.getOutputs())) {
                for (WriteEntity output : hiveContext.getOutputs()) {
                    switch (output.getType()) {
                        case DATABASE:
                            hook.removeFromKnownDatabase(getQualifiedName(output.getDatabase()));
                            break;

                        case TABLE:
                            hook.removeFromKnownTable(getQualifiedName(output.getTable()));
                            break;
                    }
                }
            }
        }
    }
}
