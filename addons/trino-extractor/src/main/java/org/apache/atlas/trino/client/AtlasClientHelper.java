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
package org.apache.atlas.trino.client;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.trino.model.Catalog;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.type.AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME;

public class AtlasClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClientHelper.class);

    public static final String TRINO_INSTANCE                   = "trino_instance";
    public static final String TRINO_CATALOG                    = "trino_catalog";
    public static final String TRINO_SCHEMA                     = "trino_schema";
    public static final String TRINO_TABLE                      = "trino_table";
    public static final String TRINO_COLUMN                     = "trino_column";
    public static final String TRINO_INSTANCE_CATALOG_ATTRIBUTE = "catalogs";
    public static final String TRINO_CATALOG_SCHEMA_ATTRIBUTE   = "schemas";
    public static final String TRINO_SCHEMA_TABLE_ATTRIBUTE     = "tables";
    public static final String QUALIFIED_NAME_ATTRIBUTE         = "qualifiedName";
    public static final String NAME_ATTRIBUTE                   = "name";
    public static final  int    pageLimit                                = 10000;
    private static final String DEFAULT_ATLAS_URL                        = "http://localhost:21000/";
    private static final String APPLICATION_PROPERTY_ATLAS_ENDPOINT      = "atlas.rest.address";
    private static final String TRINO_CATALOG_CONNECTOR_TYPE_ATTRIBUTE   = "connectorType";
    private static final String TRINO_CATALOG_INSTANCE_ATTRIBUTE         = "instance";
    private static final String TRINO_CATALOG_INSTANCE_RELATIONSHIP      = "trino_instance_catalog";
    private static final String TRINO_SCHEMA_CATALOG_ATTRIBUTE           = "catalog";
    private static final String TRINO_SCHEMA_CATALOG_RELATIONSHIP        = "trino_schema_catalog";
    private static final String TRINO_COLUMN_DATA_TYPE_ATTRIBUTE         = "data_type";
    private static final String TRINO_COLUMN_ORIDINAL_POSITION_ATTRIBUTE = "ordinal_position";
    private static final String TRINO_COLUMN_COLUMN_DEFAULT_ATTRIBUTE    = "column_default";
    private static final String TRINO_COLUMN_IS_NULLABLE_ATTRIBUTE       = "is_nullable";
    private static final String TRINO_COLUMN_TABLE_ATTRIBUTE             = "table";
    private static final String TRINO_TABLE_TYPE                         = "table_type";
    private static final String TRINO_TABLE_COLUMN_RELATIONSHIP          = "trino_table_columns";
    private static final String TRINO_TABLE_SCHEMA_RELATIONSHIP          = "trino_table_schema";
    private static final String TRINO_TABLE_SCHEMA_ATTRIBUTE             = "trinoschema";
    private static final String TRINO_TABLE_COLUMN_ATTRIBUTE             = "columns";

    private static AtlasClientV2 atlasClientV2;

    public AtlasClientHelper(Configuration atlasConf) throws IOException {
        atlasClientV2 = getAtlasClientV2Instance(atlasConf);
    }

    public static synchronized AtlasClientV2 getAtlasClientV2Instance(Configuration atlasConf) throws IOException {
        if (atlasClientV2 == null) {
            String[] atlasEndpoint = new String[] {DEFAULT_ATLAS_URL};

            if (atlasConf != null && ArrayUtils.isNotEmpty(atlasConf.getStringArray(APPLICATION_PROPERTY_ATLAS_ENDPOINT))) {
                atlasEndpoint = atlasConf.getStringArray(APPLICATION_PROPERTY_ATLAS_ENDPOINT);
            }

            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
                atlasClientV2 = new AtlasClientV2(atlasEndpoint, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), atlasEndpoint);
            }
        }
        return atlasClientV2;
    }

    public static List<AtlasEntityHeader> getAllCatalogsInInstance(String instanceGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(instanceGuid, TRINO_INSTANCE_CATALOG_ATTRIBUTE);
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.debug("Retrieved {} catalogs of {} trino instance", entities.size(), instanceGuid);
            return entities;
        } else {
            LOG.debug("No catalog found under {} trino instance", instanceGuid);
            return null;
        }
    }

    public static List<AtlasEntityHeader> getAllSchemasInCatalog(String catalogGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(catalogGuid, TRINO_CATALOG_SCHEMA_ATTRIBUTE);
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.debug("Retrieved {} schemas of {} trino catalog", entities.size(), catalogGuid);
            return entities;
        } else {
            LOG.debug("No schema found under {} trino catalog", catalogGuid);
            return null;
        }
    }

    public static List<AtlasEntityHeader> getAllTablesInSchema(String schemaGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(schemaGuid, TRINO_SCHEMA_TABLE_ATTRIBUTE);
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.debug("Retrieved {} tables of {} trino schema", entities.size(), schemaGuid);
            return entities;
        } else {
            LOG.debug("No table found under {} trino schema", schemaGuid);
            return null;
        }
    }

    public static List<AtlasEntityHeader> getAllRelationshipEntities(String entityGuid, String relationshipAttributeName) throws AtlasServiceException {

        if (entityGuid == null) {
            return null;
        }
        List<AtlasEntityHeader> entities = new ArrayList<>();
        final int               pageSize = pageLimit;

        for (int i = 0; ; i++) {
            int offset = pageSize * i;
            LOG.debug("Retrieving: offset={}, pageSize={}", offset, pageSize);

            AtlasSearchResult searchResult = atlasClientV2.relationshipSearch(entityGuid, relationshipAttributeName, null, null, true, pageSize, offset);

            List<AtlasEntityHeader> entityHeaders = searchResult == null ? null : searchResult.getEntities();
            int                     count         = entityHeaders == null ? 0 : entityHeaders.size();

            if (count > 0) {
                entities.addAll(entityHeaders);
            }

            if (count < pageSize) { // last page
                break;
            }
        }

        return entities;
    }

    public static AtlasEntityHeader getTrinoInstance(String namespace)  {
        try {
            return atlasClientV2.getEntityHeaderByAttribute(TRINO_INSTANCE, Collections.singletonMap(QUALIFIED_NAME_ATTRIBUTE, namespace));
        } catch (AtlasServiceException e) {
            return null;
        }
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createOrUpdateInstanceEntity(String trinoNamespace) throws AtlasServiceException {
        String                             qualifiedName = trinoNamespace;
        AtlasEntity.AtlasEntityWithExtInfo ret           = findEntity(TRINO_INSTANCE, qualifiedName, true, true);

        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_INSTANCE);

            entity.setAttribute(QUALIFIED_NAME_ATTRIBUTE, qualifiedName);
            entity.setAttribute(NAME_ATTRIBUTE, trinoNamespace);

            ret.setEntity(entity);
            ret = createEntity(ret);
        }
        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createOrUpdateCatalogEntity(Catalog catalog) throws AtlasServiceException {
        String catalogName    = catalog.getName();
        String trinoNamespace = catalog.getInstanceName();
        String qualifiedName = String.format("%s@%s", catalogName, trinoNamespace);

        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_CATALOG, qualifiedName, true, true);
        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_CATALOG);

            entity.setAttribute(QUALIFIED_NAME_ATTRIBUTE, qualifiedName);
            entity.setAttribute(NAME_ATTRIBUTE, catalogName);
            entity.setAttribute(TRINO_CATALOG_CONNECTOR_TYPE_ATTRIBUTE, catalog.getType());
            entity.setRelationshipAttribute(TRINO_CATALOG_INSTANCE_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(catalog.getTrinoInstanceEntity().getEntity(), TRINO_CATALOG_INSTANCE_RELATIONSHIP));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoCatalog(catalog.getHookInstanceName(), catalogName, entity, ret);
            }
            ret.setEntity(entity);
            ret = createEntity(ret);
        } else {
            AtlasEntity entity = ret.getEntity();
            entity.setRelationshipAttribute(TRINO_CATALOG_INSTANCE_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(catalog.getTrinoInstanceEntity().getEntity(), TRINO_CATALOG_INSTANCE_RELATIONSHIP));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoCatalog(catalog.getHookInstanceName(), catalogName, entity, ret);
            }
            ret.setEntity(entity);
            updateEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createOrUpdateSchemaEntity(Catalog catalog, AtlasEntity catalogEntity, String schema) throws AtlasServiceException {
        String qualifiedName = String.format("%s.%s@%s", catalog.getName(), schema, catalog.getInstanceName());

        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_SCHEMA, qualifiedName, true, true);

        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_SCHEMA);

            entity.setAttribute(QUALIFIED_NAME_ATTRIBUTE, qualifiedName);
            entity.setAttribute(NAME_ATTRIBUTE, schema);
            entity.setRelationshipAttribute(TRINO_SCHEMA_CATALOG_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(catalogEntity, TRINO_SCHEMA_CATALOG_RELATIONSHIP));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoSchema(catalog.getHookInstanceName(), catalog.getName(), schema, entity, ret);
            }

            ret.setEntity(entity);
            ret = createEntity(ret);
        } else {
            AtlasEntity entity = ret.getEntity();
            entity.setRelationshipAttribute(TRINO_SCHEMA_CATALOG_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(catalogEntity, TRINO_SCHEMA_CATALOG_RELATIONSHIP));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoSchema(catalog.getHookInstanceName(), catalog.getName(), schema, entity, ret);
            }
            ret.setEntity(entity);
            updateEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createOrUpdateTableEntity(Catalog catalog, String schema, String table, Map<String, Object> tableMetadata, Map<String, Map<String, Object>> trinoColumns, AtlasEntity schemaEntity) throws AtlasServiceException {
        String qualifiedName = String.format("%s.%s.%s@%s", catalog.getName(), schema, table, catalog.getInstanceName());

        AtlasEntity.AtlasEntityWithExtInfo ret;
        AtlasEntity.AtlasEntityWithExtInfo tableEntityExt = findEntity(TRINO_TABLE, qualifiedName, true, true);

        if (tableEntityExt == null) {
            tableEntityExt = toTableEntity(catalog, schema, table, tableMetadata, trinoColumns, schemaEntity, tableEntityExt);
            ret            = createEntity(tableEntityExt);
        } else {
            ret = toTableEntity(catalog, schema, table, tableMetadata, trinoColumns, schemaEntity, tableEntityExt);
            updateEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo toTableEntity(Catalog catalog, String schema, String table, Map<String, Object> tableMetadata, Map<String, Map<String, Object>> trinoColumns, AtlasEntity schemaEntity, AtlasEntity.AtlasEntityWithExtInfo tableEntityExt)  {
        if (tableEntityExt == null) {
            tableEntityExt = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(TRINO_TABLE));
        }

        String qualifiedName = String.format("%s.%s.%s@%s", catalog.getName(), schema, table, catalog.getInstanceName());

        AtlasEntity tableEntity = tableEntityExt.getEntity();
        tableEntity.setAttribute(QUALIFIED_NAME_ATTRIBUTE, qualifiedName);
        tableEntity.setAttribute(NAME_ATTRIBUTE, table);
        tableEntity.setAttribute(TRINO_TABLE_TYPE, tableMetadata.get(TRINO_TABLE_TYPE).toString());

        List<AtlasEntity> columnEntities = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> columnEntry : trinoColumns.entrySet()) {
            AtlasEntity entity = new AtlasEntity(TRINO_COLUMN);

            String columnName       = columnEntry.getKey();
            String colQualifiedName = String.format("%s.%s.%s.%s@%s", catalog.getName(), schema, table, columnName, catalog.getInstanceName());

            entity.setAttribute(QUALIFIED_NAME_ATTRIBUTE, colQualifiedName);
            entity.setAttribute(NAME_ATTRIBUTE, columnName);
            if (MapUtils.isNotEmpty(columnEntry.getValue())) {
                Map<String, Object> columnAttr = columnEntry.getValue();
                entity.setAttribute(TRINO_COLUMN_DATA_TYPE_ATTRIBUTE, columnAttr.get(TRINO_COLUMN_DATA_TYPE_ATTRIBUTE));
                entity.setAttribute(TRINO_COLUMN_ORIDINAL_POSITION_ATTRIBUTE, columnAttr.get(TRINO_COLUMN_ORIDINAL_POSITION_ATTRIBUTE));
                entity.setAttribute(TRINO_COLUMN_COLUMN_DEFAULT_ATTRIBUTE, columnAttr.get(TRINO_COLUMN_COLUMN_DEFAULT_ATTRIBUTE));
                entity.setAttribute(TRINO_COLUMN_IS_NULLABLE_ATTRIBUTE, columnAttr.get(TRINO_COLUMN_IS_NULLABLE_ATTRIBUTE));
            }

            entity.setRelationshipAttribute(TRINO_COLUMN_TABLE_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(tableEntity, TRINO_TABLE_COLUMN_RELATIONSHIP));
            columnEntities.add(entity);
        }

        tableEntity.setRelationshipAttribute(TRINO_TABLE_SCHEMA_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectId(schemaEntity, TRINO_TABLE_SCHEMA_RELATIONSHIP));
        tableEntity.setRelationshipAttribute(TRINO_TABLE_COLUMN_ATTRIBUTE, AtlasTypeUtil.getAtlasRelatedObjectIds(columnEntities, TRINO_TABLE_COLUMN_RELATIONSHIP));

        if (catalog.getConnector() != null) {
            catalog.getConnector().connectTrinoTable(catalog.getHookInstanceName(), catalog.getName(), schema, table, tableEntity, columnEntities, tableEntityExt);
        }

        tableEntityExt.addReferredEntity(schemaEntity);
        if (columnEntities != null) {
            for (AtlasEntity column : columnEntities) {
                tableEntityExt.addReferredEntity(column);
            }
        }

        tableEntityExt.setEntity(tableEntity);
        return tableEntityExt;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo findEntity(final String typeName, final String qualifiedName, boolean minExtInfo, boolean ignoreRelationship) throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo ret = null;

        try {
            ret = atlasClientV2.getEntityByAttribute(typeName, Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName), minExtInfo, ignoreRelationship);
        } catch (AtlasServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }

            throw e;
        }
        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createEntity(AtlasEntity.AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        LOG.debug("creating {} entity: {}", entity.getEntity().getTypeName(), entity);

        AtlasEntity.AtlasEntityWithExtInfo ret             = null;
        EntityMutationResponse             response        = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader>            createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntityHeader createdEntity : createdEntities) {
                if (ret == null) {
                    ret = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    LOG.debug("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
                } else if (ret.getEntity(createdEntity.getGuid()) == null) {
                    AtlasEntity.AtlasEntityWithExtInfo newEntity = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    ret.addReferredEntity(newEntity.getEntity());

                    if (MapUtils.isNotEmpty(newEntity.getReferredEntities())) {
                        for (Map.Entry<String, AtlasEntity> entry : newEntity.getReferredEntities().entrySet()) {
                            ret.addReferredEntity(entry.getKey(), entry.getValue());
                        }
                    }

                    LOG.debug("Created {} entity: name={}, guid={}", newEntity.getEntity().getTypeName(), newEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), newEntity.getEntity().getGuid());
                }
            }
        }

        clearRelationshipAttributes(ret);

        return ret;
    }

    public static void deleteByGuid(Set<String> guidTodelete) throws AtlasServiceException {

        if (CollectionUtils.isNotEmpty(guidTodelete)) {

            for (String guid : guidTodelete) {
                EntityMutationResponse response = atlasClientV2.deleteEntityByGuid(guid);

                if (response == null || response.getDeletedEntities().size() < 1) {
                    LOG.debug("Entity with guid : {} is not deleted", guid);
                } else {
                    LOG.debug("Entity with guid : {} is deleted", guid);
                }
            }
        }
    }

    public static void close() {
        atlasClientV2.close();
    }



    private static void updateEntity(AtlasEntity.AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        LOG.debug("updating {} entity: {}", entity.getEntity().getTypeName(), entity);

        atlasClientV2.updateEntity(entity);

        LOG.debug("Updated {} entity: name={}, guid={}", entity.getEntity().getTypeName(), entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
    }

    private static void clearRelationshipAttributes(AtlasEntity.AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    private static void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    private static void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }
}
