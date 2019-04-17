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

package org.apache.atlas.hive.hook.events;

import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.*;

public class CreateTable extends BaseHiveEvent {
    private final boolean skipTempTables;

    public CreateTable(AtlasHiveHookContext context, boolean skipTempTables) {
        super(context);

        this.skipTempTables = skipTempTables;
    }

    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        List<HookNotification>   ret      = null;
        AtlasEntitiesWithExtInfo entities = context.isMetastoreHook() ? getHiveMetastoreEntities() : getHiveEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getHiveMetastoreEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        ListenerEvent            event = context.getMetastoreEvent();
        HiveOperation            oper  = context.getHiveOperation();
        Table                    table;

        if (isAlterTable(oper)) {
            table = toTable(((AlterTableEvent) event).getNewTable());
        } else {
            table = toTable(((CreateTableEvent) event).getTable());
        }

        if (skipTemporaryTable(table)) {
            table = null;
        }

        processTable(table, ret);

        addProcessedEntities(ret);

        return ret;
    }

    public AtlasEntitiesWithExtInfo getHiveEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        Table                    table = null;

        if (CollectionUtils.isNotEmpty(getOutputs())) {
            for (Entity entity : getOutputs()) {
                if (entity.getType() == Entity.Type.TABLE) {
                    table = entity.getTable();

                    if (table != null) {
                        table = getHive().getTable(table.getDbName(), table.getTableName());

                        if (table != null) {
                            if (skipTemporaryTable(table)) {
                                table = null;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }

        processTable(table, ret);

        addProcessedEntities(ret);

        return ret;
    }

    // create process entities for lineages from HBase/HDFS to hive table
    private void processTable(Table table, AtlasEntitiesWithExtInfo ret) throws Exception {
        if (table != null) {
            AtlasEntity tblEntity = toTableEntity(table, ret);

            if (tblEntity != null) {
                if (isHBaseStore(table)) {
                    // This create lineage to HBase table in case of Hive on HBase
                    AtlasEntity hbaseTableEntity = toReferencedHBaseTable(table, ret);

                    if (hbaseTableEntity != null) {
                        final AtlasEntity processEntity;

                        if (EXTERNAL_TABLE.equals(table.getTableType())) {
                            processEntity = getHiveProcessEntity(Collections.singletonList(hbaseTableEntity), Collections.singletonList(tblEntity));
                        } else {
                            processEntity = getHiveProcessEntity(Collections.singletonList(tblEntity), Collections.singletonList(hbaseTableEntity));
                        }

                        ret.addEntity(processEntity);
                    }
                } else {
                    if (EXTERNAL_TABLE.equals(table.getTableType())) {
                        AtlasEntity hdfsPathEntity = getPathEntity(table.getDataLocation(), ret);
                        AtlasEntity processEntity  = getHiveProcessEntity(Collections.singletonList(hdfsPathEntity), Collections.singletonList(tblEntity));

                        ret.addEntity(processEntity);
                        ret.addReferredEntity(hdfsPathEntity);
                    }
                }
            }
        }
    }

    private static boolean isAlterTable(HiveOperation oper) {
        return (oper == ALTERTABLE_PROPERTIES || oper == ALTERTABLE_RENAME || oper == ALTERTABLE_RENAMECOL);
    }

    private boolean skipTemporaryTable(Table table) {
        // If its an external table, even though the temp table skip flag is on, we create the table since we need the HDFS path to temp table lineage.
        return table != null && skipTempTables && table.isTemporary() && !EXTERNAL_TABLE.equals(table.getTableType());
    }
}
