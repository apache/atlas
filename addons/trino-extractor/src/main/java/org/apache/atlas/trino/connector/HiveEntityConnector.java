/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.trino.connector;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveEntityConnector extends AtlasEntityConnector {
    private static final Logger LOG = LoggerFactory.getLogger(HiveEntityConnector.class);

    public static final String HIVE_INSTANCE = "hms_instance";
    public static final String HIVE_DB       = "hive_db";
    public static final String HIVE_TABLE    = "hive_table";
    public static final String HIVE_COLUMN   = "hive_column";

    @Override
    public void connectTrinoCatalog(String instanceName, String catalogName, AtlasEntity entity, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {

    }

    @Override
    public void connectTrinoSchema(String instanceName, String catalogName, String schemaName, AtlasEntity entity, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        AtlasEntity hiveDb = null;
        try {
            hiveDb = toDbEntity(instanceName, schemaName, entity, entityWithExtInfo);
        } catch (AtlasServiceException e) {
            LOG.error("Error encountered: ", e);
        }

        if (hiveDb != null) {
            entity.setRelationshipAttribute("hive_db", AtlasTypeUtil.getAtlasRelatedObjectId(hiveDb, "trino_schema_hive_db"));
        }
    }

    @Override
    public void connectTrinoTable(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity trinoTable, List<AtlasEntity> columnEntities, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        AtlasEntity hiveTable;
        try {
            hiveTable = toTableEntity(instanceName, schemaName, tableName, trinoTable, entityWithExtInfo);

            if (hiveTable != null) {
                trinoTable.setRelationshipAttribute("hive_table", AtlasTypeUtil.getAtlasRelatedObjectId(hiveTable, "trino_schema_hive_table"));

                for (AtlasEntity columnEntity : columnEntities) {
                    connectTrinoColumn(instanceName, catalogName, schemaName, tableName, hiveTable, columnEntity, entityWithExtInfo);
                }
            }
        } catch (AtlasServiceException e) {
            LOG.error("Error encountered: ", e);
        }
    }

    public void connectTrinoColumn(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity hiveTable, AtlasEntity trinoColumn, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasServiceException {
        AtlasEntity hiveColumn;
        try {
            hiveColumn = toColumnEntity(instanceName, schemaName, tableName, trinoColumn.getAttribute("name").toString(), hiveTable, trinoColumn, entityWithExtInfo);
        } catch (AtlasServiceException e) {
            throw new AtlasServiceException(e);
        }
        if (hiveColumn != null) {
            trinoColumn.setRelationshipAttribute("hive_column", AtlasTypeUtil.getAtlasRelatedObjectId(hiveColumn, "trino_schema_hive_column"));
        }
    }

    private AtlasEntity toDbEntity(String instanceName, String schemaName, AtlasEntity trinodschema, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasServiceException {
        String                             dbName          = schemaName;
        String                             dbQualifiedName = schemaName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret             = AtlasClientHelper.findEntity(HIVE_DB, dbQualifiedName, true, true);
        /*if (ret == null || ret.getEntity() == null) {
            AtlasEntity hiveDb = new AtlasEntity(HIVE_DB);

            hiveDb.setAttribute(ATTRIBUTE_QUALIFIED_NAME, dbQualifiedName);
            hiveDb.setAttribute("name", dbName);
            hiveDb.setAttribute("clusterName", instanceName);
            List<AtlasEntity> trinoSchemas = new ArrayList<>();
            trinoSchemas.add(trinodschema);
            hiveDb.setRelationshipAttribute("trino_schema", AtlasTypeUtil.getAtlasRelatedObjectIds(trinoSchemas, "trino_schema_hive_db"));
            entityWithExtInfo.addReferredEntity(hiveDb);
            return hiveDb;
        }*/

        return ret != null ? ret.getEntity() : null;
    }

    private AtlasEntity toTableEntity(String instanceName, String schemaName, String tableName, AtlasEntity trinoTable, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasServiceException {
        String                             tableQualifiedName = schemaName + "." + tableName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret                = AtlasClientHelper.findEntity(HIVE_TABLE, tableQualifiedName, true, true);
        /*if (ret == null || ret.getEntity() == null) {
            AtlasEntity hiveTable = new AtlasEntity(HIVE_TABLE);

            hiveTable.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName);
            hiveTable.setAttribute("name", tableName);
            List<AtlasEntity> trinotabless = new ArrayList<>();
            trinotabless.add(trinoTable);
            hiveTable.setRelationshipAttribute("trino_table", AtlasTypeUtil.getAtlasRelatedObjectIds(trinotabless, "trino_schema_hive_table"));
            entityWithExtInfo.addReferredEntity(hiveTable);

            return hiveTable;
        }*/
        return ret != null ? ret.getEntity() : null;
    }

    private AtlasEntity toColumnEntity(String instanceName, String schemaName, String tableName, String columnName, AtlasEntity hiveTable, AtlasEntity trinoColumn, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasServiceException {
        String                             columnQualifiedName = schemaName + "." + tableName + "." + columnName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret                 = AtlasClientHelper.findEntity(HIVE_COLUMN, columnQualifiedName, true, true);
        /*if (ret == null || ret.getEntity() == null) {
            AtlasEntity hiveColumn = new AtlasEntity(HIVE_COLUMN);

            hiveColumn.setAttribute(ATTRIBUTE_QUALIFIED_NAME, columnQualifiedName);
            hiveColumn.setAttribute("name", columnName);
            hiveColumn.setAttribute("type", "temp");
            hiveColumn.setRelationshipAttribute("table", AtlasTypeUtil.getAtlasRelatedObjectId(hiveTable, "hive_table_columns"));
            List<AtlasEntity> trinoColumns = new ArrayList<>();
            trinoColumns.add(trinoColumn);
            hiveColumn.setRelationshipAttribute("trino_column",  AtlasTypeUtil.getAtlasRelatedObjectIds(trinoColumns, "trino_schema_hive_column"));
            entityWithExtInfo.addReferredEntity(hiveColumn);

            return hiveColumn;
        }*/

        return ret != null ? ret.getEntity() : null;
    }
}
