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

import org.apache.atlas.hive.hook.events.*;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.LruCache;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.hive.hook.events.BaseHiveEvent.ATTRIBUTE_QUALIFIED_NAME;
import static org.apache.atlas.hive.hook.events.BaseHiveEvent.HIVE_TYPE_DB;
import static org.apache.atlas.hive.hook.events.BaseHiveEvent.HIVE_TYPE_TABLE;


public class HiveHook extends AtlasHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);

    public static final String CONF_PREFIX                    = "atlas.hook.hive.";
    public static final String HOOK_DATABASE_NAME_CACHE_COUNT = CONF_PREFIX + "database.name.cache.count";
    public static final String HOOK_TABLE_NAME_CACHE_COUNT    = CONF_PREFIX + "table.name.cache.count";
    public static final String CONF_CLUSTER_NAME              = "atlas.cluster.name";

    public static final String DEFAULT_CLUSTER_NAME = "primary";

    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();

    private static final String            clusterName;
    private static final Map<String, Long> knownDatabases;
    private static final Map<String, Long> knownTables;

    static {
        for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }

        int dbNameCacheCount  = atlasProperties.getInt(HOOK_DATABASE_NAME_CACHE_COUNT, 10000);
        int tblNameCacheCount = atlasProperties.getInt(HOOK_TABLE_NAME_CACHE_COUNT, 10000);

        clusterName    = atlasProperties.getString(CONF_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        knownDatabases = dbNameCacheCount > 0 ? Collections.synchronizedMap(new LruCache<String, Long>(dbNameCacheCount, 0)) : null;
        knownTables    = tblNameCacheCount > 0 ? Collections.synchronizedMap(new LruCache<String, Long>(tblNameCacheCount, 0)) : null;
    }

    public HiveHook() {
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.run({})", hookContext.getOperationName());
        }

        try {
            HiveOperation        oper    = OPERATION_MAP.get(hookContext.getOperationName());
            AtlasHiveHookContext context = new AtlasHiveHookContext(this, oper, hookContext);

            BaseHiveEvent event = null;

            switch (oper) {
                case CREATEDATABASE:
                    event = new CreateDatabase(context);
                break;

                case DROPDATABASE:
                    event = new DropDatabase(context);
                break;

                case ALTERDATABASE:
                case ALTERDATABASE_OWNER:
                    event = new AlterDatabase(context);
                break;

                case CREATETABLE:
                    event = new CreateTable(context, true);
                break;

                case DROPTABLE:
                case DROPVIEW:
                    event = new DropTable(context);
                break;

                case CREATETABLE_AS_SELECT:
                case CREATEVIEW:
                case ALTERVIEW_AS:
                case LOAD:
                case EXPORT:
                case IMPORT:
                case QUERY:
                case TRUNCATETABLE:
                    event = new CreateHiveProcess(context);
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
                case ALTERTABLE_LOCATION:
                    event = new AlterTable(context);
                break;

                case ALTERTABLE_RENAME:
                case ALTERVIEW_RENAME:
                    event = new AlterTableRename(context);
                break;

                case ALTERTABLE_RENAMECOL:
                    event = new AlterTableRenameCol(context);
                break;

                default:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("HiveHook.run({}): operation ignored", hookContext.getOperationName());
                    }
                break;
            }

            if (event != null) {
                final UserGroupInformation ugi = hookContext.getUgi() == null ? Utils.getUGI() : hookContext.getUgi();

                super.notifyEntities(event.getNotificationMessages(), ugi);
            }
        } catch (Throwable t) {
            LOG.error("HiveHook.run(): failed to process operation {}", hookContext.getOperationName(), t);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.run({})", hookContext.getOperationName());
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isKnownDatabase(String dbQualifiedName) {
        return knownDatabases != null && dbQualifiedName != null ? knownDatabases.containsKey(dbQualifiedName) : false;
    }

    public boolean isKnownTable(String tblQualifiedName) {
        return knownTables != null && tblQualifiedName != null ? knownTables.containsKey(tblQualifiedName) : false;
    }

    public void addToKnownEntities(Collection<AtlasEntity> entities) {
        if (knownDatabases != null || knownTables != null) { // caching should be enabled at least for one
            if (entities != null) {
                for (AtlasEntity entity : entities) {
                    if (StringUtils.equalsIgnoreCase(entity.getTypeName(), HIVE_TYPE_DB)) {
                        addToKnownDatabase((String) entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
                    } else if (StringUtils.equalsIgnoreCase(entity.getTypeName(), HIVE_TYPE_TABLE)) {
                        addToKnownTable((String) entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
                    }
                }
            }
        }
    }

    public void addToKnownDatabase(String dbQualifiedName) {
        if (knownDatabases != null && dbQualifiedName != null) {
            knownDatabases.put(dbQualifiedName, System.currentTimeMillis());
        }
    }

    public void addToKnownTable(String tblQualifiedName) {
        if (knownTables != null && tblQualifiedName != null) {
            knownTables.put(tblQualifiedName, System.currentTimeMillis());
        }
    }

    public void removeFromKnownDatabase(String dbQualifiedName) {
        if (knownDatabases != null && dbQualifiedName != null) {
            knownDatabases.remove(dbQualifiedName);
        }
    }

    public void removeFromKnownTable(String tblQualifiedName) {
        if (knownTables != null && tblQualifiedName != null) {
            knownTables.remove(tblQualifiedName);
        }
    }
}
