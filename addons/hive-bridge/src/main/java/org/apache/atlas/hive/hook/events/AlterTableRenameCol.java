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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AlterTableRenameCol extends AlterTable {
    private static final Logger LOG = LoggerFactory.getLogger(AlterTableRenameCol.class);

    public AlterTableRenameCol(AtlasHiveHookContext context) {
        super(context);
    }

    @Override
    public List<HookNotification> getNotificationMessages() throws Exception {
        if (CollectionUtils.isEmpty(getHiveContext().getInputs())) {
            LOG.error("AlterTableRenameCol: old-table not found in inputs list");

            return null;
        }

        if (CollectionUtils.isEmpty(getHiveContext().getOutputs())) {
            LOG.error("AlterTableRenameCol: new-table not found in outputs list");

            return null;
        }

        List<HookNotification> baseMsgs = super.getNotificationMessages();

        if (CollectionUtils.isEmpty(baseMsgs)) {
            LOG.debug("Skipped processing of column-rename (on a temporary table?)");

            return null;
        }

        List<HookNotification> ret      = new ArrayList<>(baseMsgs);
        Table                  oldTable = getHiveContext().getInputs().iterator().next().getTable();
        Table                  newTable = getHiveContext().getOutputs().iterator().next().getTable();

        newTable = getHive().getTable(newTable.getDbName(), newTable.getTableName());

        List<FieldSchema> oldColumns       = oldTable.getCols();
        List<FieldSchema> newColumns       = newTable.getCols();
        FieldSchema       changedColumnOld = null;
        FieldSchema       changedColumnNew = null;

        for (FieldSchema oldColumn : oldColumns) {
            if (!newColumns.contains(oldColumn)) {
                changedColumnOld = oldColumn;

                break;
            }
        }

        for (FieldSchema newColumn : newColumns) {
            if (!oldColumns.contains(newColumn)) {
                changedColumnNew = newColumn;

                break;
            }
        }

        if (changedColumnOld != null && changedColumnNew != null) {
            AtlasObjectId oldColumnId = new AtlasObjectId(HIVE_TYPE_COLUMN, ATTRIBUTE_QUALIFIED_NAME, getQualifiedName(oldTable, changedColumnOld));
            AtlasEntity   newColumn   = new AtlasEntity(HIVE_TYPE_COLUMN);

            newColumn.setAttribute(ATTRIBUTE_NAME, changedColumnNew.getName());
            newColumn.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getQualifiedName(newTable, changedColumnNew));

            ret.add(0, new EntityPartialUpdateRequestV2(getUserName(), oldColumnId, new AtlasEntityWithExtInfo(newColumn)));
        } else {
            LOG.error("AlterTableRenameCol: no renamed column detected");
        }

        return ret;
    }
}
