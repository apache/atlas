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
package org.apache.atlas.trino.connector;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveEntityConnector extends AtlasEntityConnector {
    public static final String HIVE_DB                               = "hive_db";
    public static final String HIVE_TABLE                            = "hive_table";
    public static final String HIVE_COLUMN                           = "hive_column";
    public static final String TRINO_SCHEMA_HIVE_DB_RELATIONSHIP     = "trino_schema_hive_db";
    public static final String TRINO_TABLE_HIVE_TABLE_RELATIONSHIP   = "trino_table_hive_table";
    public static final String TRINO_COLUMN_HIVE_COLUMN_RELATIONSHIP = "trino_column_hive_column";
    public static final String TRINO_SCHEMA_HIVE_DB_ATTRIBUTE        = "hive_db";
    public static final String TRINO_TABLE_HIVE_TABLE_ATTRIBUTE      = "hive_table";
    public static final String TRINO_COLUMN_HIVE_COLUMN_ATTRIBUTE    = "hive_column";
    private static final Logger LOG = LoggerFactory.getLogger(HiveEntityConnector.class);

    @Override
    public void connectTrinoCatalog(AtlasClientHelper atlasClient, String instanceName, String catalogName, AtlasEntity entity, AtlasEntityWithExtInfo entityWithExtInfo) {
    }

    @Override
    public void connectTrinoSchema(AtlasClientHelper atlasClient, String instanceName, String catalogName, String schemaName, AtlasEntity dbEntity, AtlasEntityWithExtInfo entityWithExtInfo) {
        if (instanceName == null) {
            LOG.warn("Failed attempting to connect entity since hook namespace is empty, Please configure in properties");
        } else {
            try {
                AtlasEntity hiveDb = toDbEntity(atlasClient, instanceName, schemaName);

                if (hiveDb != null) {
                    dbEntity.setRelationshipAttribute(TRINO_SCHEMA_HIVE_DB_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(hiveDb, TRINO_SCHEMA_HIVE_DB_RELATIONSHIP));
                }
            } catch (AtlasServiceException e) {
                LOG.error("Error encountered: ", e);
            }
        }
    }

    @Override
    public void connectTrinoTable(AtlasClientHelper atlasClient, String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity trinoTable, List<AtlasEntity> columnEntities, AtlasEntityWithExtInfo entityWithExtInfo) {
        if (instanceName == null) {
            LOG.warn("Failed attempting to connect entity since hook namespace is empty, Please configure in properties");
        } else {
            try {
                AtlasEntity hiveTable = toTableEntity(atlasClient, instanceName, schemaName, tableName);

                if (hiveTable != null) {
                    trinoTable.setRelationshipAttribute(TRINO_TABLE_HIVE_TABLE_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(hiveTable, TRINO_TABLE_HIVE_TABLE_RELATIONSHIP));

                    for (AtlasEntity columnEntity : columnEntities) {
                        connectTrinoColumn(atlasClient, instanceName, schemaName, tableName, columnEntity);
                    }
                }
            } catch (AtlasServiceException e) {
                LOG.error("Error encountered: ", e);
            }
        }
    }

    private void connectTrinoColumn(AtlasClientHelper atlasClient, String instanceName, String schemaName, String tableName, AtlasEntity trinoColumn) throws AtlasServiceException {
        if (instanceName != null) {
            try {
                AtlasEntity hiveColumn = toColumnEntity(atlasClient, instanceName, schemaName, tableName, trinoColumn.getAttribute("name").toString());

                if (hiveColumn != null) {
                    trinoColumn.setRelationshipAttribute(TRINO_COLUMN_HIVE_COLUMN_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(hiveColumn, TRINO_COLUMN_HIVE_COLUMN_RELATIONSHIP));
                }
            } catch (AtlasServiceException e) {
                throw new AtlasServiceException(e);
            }
        }
    }

    private AtlasEntity toDbEntity(AtlasClientHelper atlasClient, String instanceName, String schemaName) throws AtlasServiceException {
        String                 dbQualifiedName = schemaName + "@" + instanceName;
        AtlasEntityWithExtInfo ret             = atlasClient.findEntity(HIVE_DB, dbQualifiedName, true, true);

        return ret != null ? ret.getEntity() : null;
    }

    private AtlasEntity toTableEntity(AtlasClientHelper atlasClient, String instanceName, String schemaName, String tableName) throws AtlasServiceException {
        String                 tableQualifiedName = schemaName + "." + tableName + "@" + instanceName;
        AtlasEntityWithExtInfo ret                = atlasClient.findEntity(HIVE_TABLE, tableQualifiedName, true, true);

        return ret != null ? ret.getEntity() : null;
    }

    private AtlasEntity toColumnEntity(AtlasClientHelper atlasClient, String instanceName, String schemaName, String tableName, String columnName) throws AtlasServiceException {
        String                 columnQualifiedName = schemaName + "." + tableName + "." + columnName + "@" + instanceName;
        AtlasEntityWithExtInfo ret                 = atlasClient.findEntity(HIVE_COLUMN, columnQualifiedName, true, true);

        return ret != null ? ret.getEntity() : null;
    }
}
