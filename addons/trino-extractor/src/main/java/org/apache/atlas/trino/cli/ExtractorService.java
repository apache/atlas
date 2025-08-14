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
package org.apache.atlas.trino.cli;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.trino.client.TrinoClientHelper;
import org.apache.atlas.trino.model.Catalog;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExtractorService {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorService.class);

    public static final  int               THREAD_POOL_SIZE         = 5;
    public static final  int               CATALOG_EXECUTION_TIMEOUT = 60;
    public static final  String            TRINO_NAME_ATTRIBUTE = "name";
    private static final String            TRINO_CATALOG_REGISTERED = "atlas.trino.catalogs.registered";
    private static final String            TRINO_CATALOG_HOOK_ENABLED_PREFIX = "atlas.trino.catalog.hook.enabled.";
    private static final String            TRINO_CATALOG_HOOK_ENABLED_SUFFIX = ".namespace";
    private static       Configuration     atlasProperties;
    private static       TrinoClientHelper trinoClientHelper;
    private static       String            trinoNamespace;
    private              ExtractorContext  context;

    public boolean execute(ExtractorContext context) throws Exception {
        this.context      = context;
        atlasProperties   = context.getAtlasConf();
        trinoClientHelper = context.getTrinoConnector();
        trinoNamespace    = context.getNamespace();

        Map<String, String> catalogs = trinoClientHelper.getAllTrinoCatalogs();
        LOG.info("Found {} catalogs in Trino: {}", catalogs.size(), catalogs.keySet());

        try {
            processCatalogs(context, catalogs);
            deleteCatalogs(context, catalogs);
        } catch (AtlasServiceException e) {
            throw new AtlasServiceException(e);
        }
        return true;
    }

    public void processCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws AtlasServiceException {
        if (MapUtils.isEmpty(catalogInTrino)) {
            LOG.debug("No catalogs found under Trino");
            return;
        }

        List<Catalog> catalogsToProcess = new ArrayList<>();

        if (StringUtils.isEmpty(context.getCatalog())) {
            String[] registeredCatalogs = atlasProperties.getStringArray(TRINO_CATALOG_REGISTERED);

            if (registeredCatalogs != null) {
                for (String registeredCatalog : registeredCatalogs) {
                    if (catalogInTrino.containsKey(registeredCatalog)) {
                        catalogsToProcess.add(getCatalogInstance(registeredCatalog, catalogInTrino.get(registeredCatalog)));
                    }
                }
            }
        } else {
            if (catalogInTrino.containsKey(context.getCatalog())) {
                Catalog catalog = getCatalogInstance(context.getCatalog(), catalogInTrino.get(context.getCatalog()));
                catalog.setSchemaToImport(context.getSchema());
                catalog.setTableToImport(context.getTable());
                catalogsToProcess.add(catalog);
            }
        }

        if (CollectionUtils.isEmpty(catalogsToProcess)) {
            LOG.warn("No catalogs found to process");
            return;
        } else {
            LOG.info("{} catalogs to be extracted", catalogsToProcess.stream().map(Catalog::getName).collect(Collectors.toList()));
        }

        AtlasEntity.AtlasEntityWithExtInfo trinoInstanceEntity = AtlasClientHelper.createOrUpdateInstanceEntity(trinoNamespace);

        ExecutorService catalogExecutor = Executors.newFixedThreadPool(Math.min(catalogsToProcess.size(), THREAD_POOL_SIZE));
        List<Future<?>> futures         = new ArrayList<>();

        for (Catalog currentCatalog : catalogsToProcess) {
            futures.add(catalogExecutor.submit(() -> {
                try {
                    currentCatalog.setTrinoInstanceEntity(trinoInstanceEntity);
                    processCatalog(currentCatalog);
                } catch (Exception e) {
                    LOG.error("Error processing catalog: {}", currentCatalog, e);
                }
            }));
        }
        catalogExecutor.shutdown();

        try {
            if (!catalogExecutor.awaitTermination(CATALOG_EXECUTION_TIMEOUT, TimeUnit.MINUTES)) {
                LOG.warn("Catalog processing did not complete within the timeout. {} minutes", CATALOG_EXECUTION_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Catalog processing was interrupted", e);
        }

        LOG.info("Catalogs scanned for creation/updation completed");
    }

    public void processCatalog(Catalog catalog) throws AtlasServiceException, SQLException {
        if (catalog != null) {
            LOG.info("Started extracting {} catalog:", catalog.getName());
            String catalogName = catalog.getName();

            AtlasEntity.AtlasEntityWithExtInfo trinoCatalogEntity = AtlasClientHelper.createOrUpdateCatalogEntity(catalog);

            List<String> schemas = trinoClientHelper.getTrinoSchemas(catalogName, catalog.getSchemaToImport());
            LOG.info("Found {} schema under {} catalog", schemas.size(), catalogName);

            processSchemas(catalog, trinoCatalogEntity.getEntity(), schemas);

            if (StringUtils.isEmpty(context.getSchema())) {
                deleteSchemas(schemas, trinoCatalogEntity.getEntity().getGuid());
            }
        }
    }

    public void processSchemas(Catalog catalog, AtlasEntity trinoCatalogEntity, List<String> schemaToImport) {
        for (String schemaName : schemaToImport) {
            LOG.info("Started extracting {} schema:", schemaName);
            try {
                AtlasEntity.AtlasEntityWithExtInfo schemaEntity = AtlasClientHelper.createOrUpdateSchemaEntity(catalog, trinoCatalogEntity, schemaName);

                Map<String, Map<String, Object>> tables = trinoClientHelper.getTrinoTables(catalog.getName(), schemaName, catalog.getTableToImport());
                LOG.info("Found {} tables under {}.{} catalog.schema", tables.size(), catalog.getName(), schemaName);

                processTables(catalog, schemaName, schemaEntity.getEntity(), tables);

                if (StringUtils.isEmpty(context.getTable())) {
                    deleteTables(new ArrayList<>(tables.keySet()), schemaEntity.getEntity().getGuid());
                }
            } catch (Exception e) {
                LOG.error("Error processing schema: {}", schemaName);
            }
        }
    }

    public void processTables(Catalog catalog, String schemaName, AtlasEntity schemaEntity,  Map<String, Map<String, Object>> trinoTables) {
        for (String trinoTableName : trinoTables.keySet()) {
            LOG.info("Started extracting {} table:", trinoTableName);

            try {
                Map<String, Map<String, Object>> trinoColumns = trinoClientHelper.getTrinoColumns(catalog.getName(), schemaName, trinoTableName);
                LOG.info("Found {} columns under {}.{}.{} catalog.schema.table", trinoColumns.size(), catalog.getName(), schemaName, trinoTableName);

                AtlasClientHelper.createOrUpdateTableEntity(catalog, schemaName, trinoTableName, trinoTables.get(trinoTableName), trinoColumns, schemaEntity);
            } catch (Exception e) {
                LOG.error("Error processing table: {}", trinoTableName, e);
            }
        }
    }

    public void deleteCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws AtlasServiceException {
        if (StringUtils.isNotEmpty(context.getCatalog())) {
            return;
        }

        AtlasEntityHeader trinoInstance = AtlasClientHelper.getTrinoInstance(trinoNamespace);
        if (trinoInstance != null) {
            Set<String> catalogsToDelete = getCatalogsToDelete(catalogInTrino, trinoInstance.getGuid());

            if (CollectionUtils.isNotEmpty(catalogsToDelete)) {
                LOG.info("{} non existing catalogs to be deleted", catalogsToDelete, trinoInstance.getGuid());

                for (String catalogGuid : catalogsToDelete) {
                    try {
                        deleteSchemas(null, catalogGuid);
                        AtlasClientHelper.deleteByGuid(Collections.singleton(catalogGuid));
                    } catch (AtlasServiceException e) {
                        LOG.error("Error deleting catalog: {}", catalogGuid, e);
                    }
                }
            } else {
                LOG.info("No catalogs found to delete");
            }
        }

        LOG.info("Catalogs scanned for deletion completed");
    }

    public void deleteSchemas(List<String> schemasInTrino, String catalogGuid) {
        try {
            Set<String> schemasToDelete = getSchemasToDelete(schemasInTrino, catalogGuid);

            if (CollectionUtils.isNotEmpty(schemasToDelete)) {
                LOG.info("{} non existing schemas under {} catalog found, starting to delete", schemasToDelete, catalogGuid);

                for (String schemaGuid : schemasToDelete) {
                    try {
                        deleteTables(null, schemaGuid);
                        AtlasClientHelper.deleteByGuid(Collections.singleton(schemaGuid));
                    } catch (AtlasServiceException e) {
                        LOG.error("Error in deleting schema: {}", schemaGuid, e);
                    }
                }
            } else {
                LOG.info("No schemas found under {} catalog to delete", catalogGuid);
            }
        } catch (AtlasServiceException e) {
            LOG.error("Error in deleting schemas ", catalogGuid, e);
        }
    }

    private void deleteTables(List<String> tablesInTrino, String schemaGuid) {
        try {
            Set<String> tablesToDelete = getTablesToDelete(tablesInTrino, schemaGuid);

            if (CollectionUtils.isNotEmpty(tablesToDelete)) {
                LOG.info("{} non existing tables under {} schema found, starting to delete", tablesToDelete, schemaGuid);

                for (String tableGuid : tablesToDelete) {
                    try {
                        AtlasClientHelper.deleteByGuid(Collections.singleton(tableGuid));
                    } catch (AtlasServiceException e) {
                        LOG.error("Error deleting table: {}", tableGuid, e);
                    }
                }
            } else {
                LOG.info("No non existing tables found under {} schema", schemaGuid);
            }
        } catch (AtlasServiceException e) {
            LOG.error("Error deleting tables under schema: {}", schemaGuid, e);
        }
    }

    private static Catalog getCatalogInstance(String catalogName, String connectorType) {
        if (catalogName == null) {
            return null;
        }

        boolean isHookEnabled = atlasProperties.getBoolean(TRINO_CATALOG_HOOK_ENABLED_PREFIX + catalogName, false);
        String hookNamespace = null;
        final String HOOK_NAMESPACE_PROPERTY = TRINO_CATALOG_HOOK_ENABLED_PREFIX + catalogName + TRINO_CATALOG_HOOK_ENABLED_SUFFIX;
        if (isHookEnabled) {
            hookNamespace = atlasProperties.getString(HOOK_NAMESPACE_PROPERTY);

            if (hookNamespace == null) {
                LOG.warn("Atlas Hook is enabled for {}, but '{}' found empty", catalogName, HOOK_NAMESPACE_PROPERTY);
            }
        }

        Catalog catalog = new Catalog(catalogName, connectorType, isHookEnabled, hookNamespace, trinoNamespace);
        return catalog;
    }

    private static Set<String> getCatalogsToDelete(Map<String, String> catalogInTrino, String instanceGuid) throws AtlasServiceException {

        if (instanceGuid != null) {

            List<AtlasEntityHeader> catalogsInAtlas = AtlasClientHelper.getAllCatalogsInInstance(instanceGuid);
            if (catalogsInAtlas != null) {

                if (catalogInTrino == null) {
                    catalogInTrino = new HashMap<>();
                }

                Map<String, String> finalCatalogInTrino = catalogInTrino;
                return catalogsInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalCatalogInTrino.containsKey(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }

    private static Set<String> getSchemasToDelete(List<String> schemasInTrino, String catalogGuid) throws AtlasServiceException {

        if (catalogGuid != null) {

            List<AtlasEntityHeader> schemasInAtlas = AtlasClientHelper.getAllSchemasInCatalog(catalogGuid);
            if (schemasInAtlas != null) {

                if (schemasInTrino == null) {
                    schemasInTrino = new ArrayList<>();
                }

                List<String> finalSchemasInTrino = schemasInTrino;
                return schemasInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalSchemasInTrino.contains(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }

    private static Set<String> getTablesToDelete(List<String> tablesInTrino, String schemaGuid) throws AtlasServiceException {

        if (schemaGuid != null) {

            List<AtlasEntityHeader> tablesInAtlas = AtlasClientHelper.getAllTablesInSchema(schemaGuid);
            if (tablesInAtlas != null) {

                if (tablesInTrino == null) {
                    tablesInTrino = new ArrayList<>();
                }

                List<String> finalTablesInTrino = tablesInTrino;
                return tablesInAtlas.stream()
                        .filter(entity -> entity.getAttribute(TRINO_NAME_ATTRIBUTE) != null)
                        .filter(entity -> !finalTablesInTrino.contains(entity.getAttribute(TRINO_NAME_ATTRIBUTE)))
                        .map(AtlasEntityHeader::getGuid)
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }
}
