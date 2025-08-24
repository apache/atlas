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
package org.apache.atlas.trino.cli;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.trino.model.Catalog;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExtractorService {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorService.class);

    public static final int    THREAD_POOL_SIZE          = 5;
    public static final int    CATALOG_EXECUTION_TIMEOUT = 60;
    public static final String TRINO_NAME_ATTRIBUTE      = "name";

    private static final String TRINO_CATALOG_REGISTERED          = "atlas.trino.catalogs.registered";
    private static final String TRINO_CATALOG_HOOK_ENABLED_PREFIX = "atlas.trino.catalog.hook.enabled.";
    private static final String TRINO_CATALOG_HOOK_ENABLED_SUFFIX = ".namespace";

    public boolean execute(ExtractorContext context) throws Exception {
        Map<String, String> catalogs = context.getTrinoConnector().getAllTrinoCatalogs();

        LOG.info("Found {} catalogs in Trino: {}", catalogs.size(), catalogs.keySet());

        try {
            processCatalogs(context, catalogs);

            deleteCatalogs(context, catalogs);
        } catch (AtlasServiceException e) {
            throw new AtlasServiceException(e);
        }

        return true;
    }

    private void processCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws AtlasServiceException {
        if (MapUtils.isEmpty(catalogInTrino)) {
            LOG.debug("No catalogs found under Trino");
        } else {
            List<Catalog> catalogsToProcess = new ArrayList<>();

            if (StringUtils.isEmpty(context.getCatalog())) {
                String[] registeredCatalogs = context.getAtlasConf().getStringArray(TRINO_CATALOG_REGISTERED);

                if (registeredCatalogs != null) {
                    for (String registeredCatalog : registeredCatalogs) {
                        if (catalogInTrino.containsKey(registeredCatalog)) {
                            catalogsToProcess.add(getCatalogInstance(context, registeredCatalog, catalogInTrino.get(registeredCatalog)));
                        }
                    }
                }
            } else {
                if (catalogInTrino.containsKey(context.getCatalog())) {
                    Catalog catalog = getCatalogInstance(context, context.getCatalog(), catalogInTrino.get(context.getCatalog()));

                    catalog.setSchemaToImport(context.getSchema());
                    catalog.setTableToImport(context.getTable());

                    catalogsToProcess.add(catalog);
                }
            }

            if (CollectionUtils.isEmpty(catalogsToProcess)) {
                LOG.info("No catalogs found to extract");
            } else {
                LOG.info("{} catalogs to be extracted", catalogsToProcess.stream().map(Catalog::getName).collect(Collectors.toList()));

                AtlasEntityWithExtInfo trinoInstanceEntity = context.getAtlasConnector().createOrUpdateInstanceEntity(context.getNamespace());
                ExecutorService        catalogExecutor     = Executors.newFixedThreadPool(Math.min(catalogsToProcess.size(), THREAD_POOL_SIZE));

                try {
                    for (Catalog currentCatalog : catalogsToProcess) {
                        catalogExecutor.submit(() -> {
                            try {
                                currentCatalog.setTrinoInstanceEntity(trinoInstanceEntity);
                                processCatalog(context, currentCatalog);
                            } catch (Exception e) {
                                LOG.error("Error processing catalog: {}", currentCatalog, e);
                            }
                        });
                    }
                } finally {
                    catalogExecutor.shutdown();
                }

                try {
                    if (!catalogExecutor.awaitTermination(CATALOG_EXECUTION_TIMEOUT, TimeUnit.MINUTES)) {
                        LOG.warn("Catalog processing did not complete within {} minutes", CATALOG_EXECUTION_TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    LOG.error("Catalog processing was interrupted", e);
                }

                LOG.info("Catalogs scan completed");
            }
        }
    }

    private void processCatalog(ExtractorContext context, Catalog catalog) throws AtlasServiceException, SQLException {
        if (catalog != null) {
            LOG.info("Started extracting catalog: {}", catalog.getName());

            String                 catalogName        = catalog.getName();
            AtlasEntityWithExtInfo trinoCatalogEntity = context.getAtlasConnector().createOrUpdateCatalogEntity(catalog);
            List<String>           schemas            = context.getTrinoConnector().getTrinoSchemas(catalogName, catalog.getSchemaToImport());

            LOG.info("Found {} schemas in catalog {}", schemas.size(), catalogName);

            processSchemas(context, catalog, trinoCatalogEntity.getEntity(), schemas);

            if (StringUtils.isEmpty(context.getSchema())) {
                deleteSchemas(context, schemas, trinoCatalogEntity.getEntity().getGuid());
            }
        }
    }

    private void processSchemas(ExtractorContext context, Catalog catalog, AtlasEntity trinoCatalogEntity, List<String> schemaToImport) {
        for (String schemaName : schemaToImport) {
            LOG.info("Started extracting schema: {}", schemaName);

            try {
                AtlasEntityWithExtInfo           schemaEntity = context.getAtlasConnector().createOrUpdateSchemaEntity(catalog, trinoCatalogEntity, schemaName);
                Map<String, Map<String, Object>> tables       = context.getTrinoConnector().getTrinoTables(catalog.getName(), schemaName, catalog.getTableToImport());

                LOG.info("Found {} tables in schema {}.{}", tables.size(), catalog.getName(), schemaName);

                processTables(context, catalog, schemaName, schemaEntity.getEntity(), tables);

                if (StringUtils.isEmpty(context.getTable())) {
                    deleteTables(context, new ArrayList<>(tables.keySet()), schemaEntity.getEntity().getGuid());
                }
            } catch (Exception e) {
                LOG.error("Error processing schema: {}", schemaName, e);
            }
        }
    }

    private void processTables(ExtractorContext context, Catalog catalog, String schemaName, AtlasEntity schemaEntity, Map<String, Map<String, Object>> trinoTables) {
        for (String trinoTableName : trinoTables.keySet()) {
            LOG.info("Started extracting table: {}", trinoTableName);

            try {
                Map<String, Map<String, Object>> trinoColumns = context.getTrinoConnector().getTrinoColumns(catalog.getName(), schemaName, trinoTableName);

                LOG.info("Found {} columns in table {}.{}.{}", trinoColumns.size(), catalog.getName(), schemaName, trinoTableName);

                context.getAtlasConnector().createOrUpdateTableEntity(catalog, schemaName, trinoTableName, trinoTables.get(trinoTableName), trinoColumns, schemaEntity);
            } catch (Exception e) {
                LOG.error("Error processing table: {}", trinoTableName, e);
            }
        }
    }

    private void deleteCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws AtlasServiceException {
        if (StringUtils.isEmpty(context.getCatalog())) {
            AtlasEntityHeader trinoInstance = context.getAtlasConnector().getTrinoInstance(context.getNamespace());

            if (trinoInstance != null) {
                Set<String> catalogsToDelete = getCatalogsToDelete(context, catalogInTrino, trinoInstance.getGuid());

                if (CollectionUtils.isNotEmpty(catalogsToDelete)) {
                    LOG.info("Atlas has {} catalogs in instance {} that are no more present in Trino, starting to delete", catalogsToDelete, trinoInstance.getGuid());

                    for (String catalogGuid : catalogsToDelete) {
                        try {
                            deleteSchemas(context, Collections.emptyList(), catalogGuid);

                            LOG.info("Deleting catalog: {}", catalogGuid);

                            context.getAtlasConnector().deleteByGuid(Collections.singleton(catalogGuid));
                        } catch (AtlasServiceException e) {
                            LOG.error("Error deleting catalog: {}", catalogGuid, e);
                        }
                    }
                } else {
                    LOG.info("Atlas has no catalogs to delete");
                }
            }

            LOG.info("Catalogs scanned for deletion completed");
        }
    }

    private void deleteSchemas(ExtractorContext context, List<String> schemasInTrino, String catalogGuid) {
        try {
            Set<String> schemasToDelete = getSchemasToDelete(context, schemasInTrino, catalogGuid);

            if (CollectionUtils.isNotEmpty(schemasToDelete)) {
                LOG.info("Atlas has {} schemas in catalog {} that are no more present in Trino, starting to delete", schemasToDelete.size(), catalogGuid);

                for (String schemaGuid : schemasToDelete) {
                    try {
                        deleteTables(context, Collections.emptyList(), schemaGuid);

                        LOG.info("Deleting schema: {}", schemaGuid);

                        context.getAtlasConnector().deleteByGuid(Collections.singleton(schemaGuid));
                    } catch (AtlasServiceException e) {
                        LOG.error("Error in deleting schema: {}", schemaGuid, e);
                    }
                }
            } else {
                LOG.info("Atlas has no schemas to delete in catalog {}", catalogGuid);
            }
        } catch (AtlasServiceException e) {
            LOG.error("Error deleting schemas in catalog {}", catalogGuid, e);
        }
    }

    private void deleteTables(ExtractorContext context, List<String> tablesInTrino, String schemaGuid) {
        try {
            Set<String> tablesToDelete = getTablesToDelete(context, tablesInTrino, schemaGuid);

            if (CollectionUtils.isNotEmpty(tablesToDelete)) {
                LOG.info("Atlas has {} tables in schema {} that are no more present in Trino, starting to delete", tablesToDelete.size(), schemaGuid);

                for (String tableGuid : tablesToDelete) {
                    try {
                        LOG.info("Deleting table: {}", tableGuid);

                        context.getAtlasConnector().deleteByGuid(Collections.singleton(tableGuid));
                    } catch (AtlasServiceException e) {
                        LOG.error("Error deleting table: {}", tableGuid, e);
                    }
                }
            } else {
                LOG.info("Atlas has no tables to delete in schema {}", schemaGuid);
            }
        } catch (AtlasServiceException e) {
            LOG.error("Error deleting tables in schema: {}", schemaGuid, e);
        }
    }

    private static Catalog getCatalogInstance(ExtractorContext context, String catalogName, String connectorType) {
        Catalog ret = null;

        if (catalogName != null) {
            boolean isHookEnabled = context.getAtlasConf().getBoolean(TRINO_CATALOG_HOOK_ENABLED_PREFIX + catalogName, false);
            String  hookNamespace = null;

            if (isHookEnabled) {
                String hookNamespaceProperty = TRINO_CATALOG_HOOK_ENABLED_PREFIX + catalogName + TRINO_CATALOG_HOOK_ENABLED_SUFFIX;

                hookNamespace = context.getAtlasConf().getString(hookNamespaceProperty);

                if (hookNamespace == null) {
                    LOG.warn("Atlas Hook is enabled for {}, but '{}' found empty", catalogName, hookNamespaceProperty);
                }
            }

            ret = new Catalog(catalogName, connectorType, isHookEnabled, hookNamespace, context.getNamespace());
        }

        return ret;
    }

    private static Set<String> getCatalogsToDelete(ExtractorContext context, Map<String, String> catalogInTrino, String instanceGuid) throws AtlasServiceException {
        Set<String> ret = Collections.emptySet();

        if (instanceGuid != null) {
            List<AtlasEntityHeader> catalogsInAtlas = context.getAtlasConnector().getAllCatalogsInInstance(instanceGuid);

            if (CollectionUtils.isNotEmpty(catalogsInAtlas)) {
                Map<String, String> finalCatalogInTrino = catalogInTrino == null ? Collections.emptyMap() : catalogInTrino;

                ret = catalogsInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalCatalogInTrino.containsKey(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return ret;
    }

    private static Set<String> getSchemasToDelete(ExtractorContext context, List<String> schemasInTrino, String catalogGuid) throws AtlasServiceException {
        Set<String> ret = Collections.emptySet();

        if (catalogGuid != null) {
            List<AtlasEntityHeader> schemasInAtlas = context.getAtlasConnector().getAllSchemasInCatalog(catalogGuid);

            if (CollectionUtils.isNotEmpty(schemasInAtlas)) {
                List<String> finalSchemasInTrino = schemasInTrino == null ? Collections.emptyList() : schemasInTrino;

                ret = schemasInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalSchemasInTrino.contains(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return ret;
    }

    private static Set<String> getTablesToDelete(ExtractorContext context, List<String> tablesInTrino, String schemaGuid) throws AtlasServiceException {
        Set<String> ret = Collections.emptySet();

        if (schemaGuid != null) {
            List<AtlasEntityHeader> tablesInAtlas = context.getAtlasConnector().getAllTablesInSchema(schemaGuid);

            if (CollectionUtils.isNotEmpty(tablesInAtlas)) {
                List<String> finalTablesInTrino = tablesInTrino == null ? Collections.emptyList() : tablesInTrino;

                ret = tablesInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalTablesInTrino.contains(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return ret;
    }
}
