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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.Collections;
import java.util.List;

public class CreateTable extends BaseHiveEvent {
    private final boolean skipTempTables;

    public CreateTable(AtlasHiveHookContext context, boolean skipTempTables) {
        super(context);

        this.skipTempTables = skipTempTables;
    }

    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        List<HookNotification>   ret      = null;
        AtlasEntitiesWithExtInfo entities = getEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
            ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getEntities() throws Exception {
        AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        Database                 db    = null;
        Table                    table = null;

        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                table = entity.getTable();

                if (table != null) {
                    db    = getHive().getDatabase(table.getDbName());
                    table = getHive().getTable(table.getDbName(), table.getTableName());

                    if (table != null) {
                        // If its an external table, even though the temp table skip flag is on, we create the table since we need the HDFS path to temp table lineage.
                        if (skipTempTables && table.isTemporary() && !TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                            table = null;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        if (table != null) {
            AtlasEntity tblEntity = toTableEntity(table, ret);

            if (tblEntity != null) {
                if (isHBaseStore(table)) {
                    // This create lineage to HBase table in case of Hive on HBase
                    AtlasEntity hbaseTableEntity = toReferencedHBaseTable(table, ret);

                    if (hbaseTableEntity != null) {
                        final AtlasEntity processEntity;

                        if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                            processEntity = getHiveProcessEntity(Collections.singletonList(hbaseTableEntity), Collections.singletonList(tblEntity));
                        } else {
                            processEntity = getHiveProcessEntity(Collections.singletonList(tblEntity), Collections.singletonList(hbaseTableEntity));
                        }

                        ret.addEntity(processEntity);
                    }
                } else {
                    if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                        AtlasEntity hdfsPathEntity = getPathEntity(table.getDataLocation(), ret);
                        AtlasEntity processEntity  = getHiveProcessEntity(Collections.singletonList(hdfsPathEntity), Collections.singletonList(tblEntity));

                        ret.addEntity(processEntity);
                        ret.addReferredEntity(hdfsPathEntity);
                    }
                }
            }
        }

        addProcessedEntities(ret);

        return ret;
    }
}
