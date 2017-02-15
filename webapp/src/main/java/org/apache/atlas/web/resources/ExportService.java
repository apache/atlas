/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.resources;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.impexp.*;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.util.*;


public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    private final AtlasTypeRegistry    typeRegistry;
    private final AtlasGraph           atlasGraph;
    private final EntityGraphRetriever entityGraphRetriever;

    // query engine support
    private ScriptEngineManager scriptEngineManager;
    private ScriptEngine scriptEngine;
    private Bindings bindings;
    private final String gremlinQuery = "g.V('__guid', startGuid).bothE().bothV().has('__guid').__guid.dedup().toList()";

    public ExportService(final AtlasTypeRegistry typeRegistry) {
        this.typeRegistry         = typeRegistry;
        this.entityGraphRetriever = new EntityGraphRetriever(this.typeRegistry);
        this.atlasGraph           = AtlasGraphProvider.getGraphInstance();

        initScriptEngine();
    }

    private class ExportContext {
        final Set<String>       guidsProcessed = new HashSet<>();
        final List<String>      guidsToProcess = new ArrayList<>();
        final AtlasExportResult result;
        final ZipSink           sink;

        ExportContext(AtlasExportResult result, ZipSink sink) {
            this.result = result;
            this.sink   = sink;
        }
    }

    public AtlasExportResult run(ZipSink exportSink, AtlasExportRequest request, String userName, String hostName,
                                 String requestingIP) throws AtlasBaseException {

        ExportContext context = new ExportContext(new AtlasExportResult(request, userName, hostName, requestingIP,
                                                                        System.currentTimeMillis()), exportSink);

        try {
            LOG.info("==> export(user={}, from={})", userName, requestingIP);

            for (AtlasObjectId item : request.getItemsToExport()) {
                processObjectId(item, context);
            }

            context.sink.setExportOrder(context.result.getData().getEntityCreationOrder());
            context.sink.setTypesDef(context.result.getData().getTypesDef());
            context.result.setData(null);
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.SUCCESS);
            context.sink.setResult(context.result);
        } catch(Exception ex) {
            LOG.error("Operation failed: ", ex);
        } finally {
            LOG.info("<== export(user={}, from={}): status {}", userName, requestingIP, context.result.getOperationStatus());
        }

        return context.result;
    }

    private void processObjectId(AtlasObjectId item, ExportContext context) throws AtlasServiceException, AtlasException, AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> processObjectId({})", item);
        }

        try {
            AtlasEntity entity = entityGraphRetriever.toAtlasEntity(item);

            processEntity(entity, context);

            while (!context.guidsToProcess.isEmpty()) {
                String guid = context.guidsToProcess.remove(0);

                entity = entityGraphRetriever.toAtlasEntity(guid);

                processEntity(entity, context);
            }
        } catch (AtlasBaseException excp) {
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

            LOG.error("Fetching entity failed for: {}", item, excp);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processObjectId({})", item);
        }
    }

    private void processEntity(AtlasEntity entity, ExportContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> processEntity({})", entity.getAtlasObjectId());
        }

        if (!context.guidsProcessed.contains(entity.getGuid())) {
            addTypesAsNeeded(entity.getTypeName(), context);
            addClassificationsAsNeeded(entity, context);
            addEntity(entity, context);

            context.guidsProcessed.add(entity.getGuid());
            context.result.getData().getEntityCreationOrder().add(entity.getGuid());

            getConnectedEntityGuids(entity, context);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processEntity({})", entity.getAtlasObjectId());
        }
    }

    private void getConnectedEntityGuids(AtlasEntity entity, ExportContext context) {

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> getConnectedEntityGuids({}): guidsToProcess {}", entity.getAtlasObjectId(), context.guidsToProcess.size());
            }

            List<String> result = executeGremlinScriptFor(entity.getGuid());
            for (String guid : result) {
                if (!context.guidsProcessed.contains(guid)) {
                    context.guidsToProcess.add(guid);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== getConnectedEntityGuids({}): found {} guids; guidsToProcess {}", entity.getGuid(), result.size(), context.guidsToProcess.size());
            }
        } catch (ScriptException e) {
            LOG.error("Child entities could not be added for %s", entity.getGuid());
        }
    }

    private void addEntity(AtlasEntity entity, ExportContext context) throws AtlasBaseException {
        context.sink.add(entity);

        context.result.incrementMeticsCounter(String.format("entity:%s", entity.getTypeName()));
        context.result.incrementMeticsCounter("Entities");

        if (context.guidsProcessed.size() % 10 == 0) {
            LOG.info("export(): in progress.. number of entities exported: {}", context.guidsProcessed.size());
        }
    }

    private void addClassificationsAsNeeded(AtlasEntity entity, ExportContext context) {
        AtlasExportResult result   = context.result;
        AtlasTypesDef     typesDef = result.getData().getTypesDef();

        if(CollectionUtils.isNotEmpty(entity.getClassifications())) {
            for (AtlasClassification c : entity.getClassifications()) {
                if (typesDef.hasClassificationDef(c.getTypeName())) {
                    continue;
                }

                AtlasClassificationDef cd = typeRegistry.getClassificationDefByName(c.getTypeName());

                typesDef.getClassificationDefs().add(cd);
                result.incrementMeticsCounter("Classification");
            }
        }
    }

    private void addTypesAsNeeded(String typeName, ExportContext context) {
        AtlasExportResult result   = context.result;
        AtlasTypesDef     typesDef = result.getData().getTypesDef();

        if(!typesDef.hasEntityDef(typeName)) {
            AtlasEntityDef typeDefinition = typeRegistry.getEntityDefByName(typeName);

            typesDef.getEntityDefs().add(typeDefinition);
            result.incrementMeticsCounter("Type(s)");
        }
    }

    private List<String> executeGremlinScriptFor(String guid) throws ScriptException {

        bindings.put("startGuid", guid);
        return (List<String>) atlasGraph.executeGremlinScript(this.scriptEngine, this.bindings, this.gremlinQuery, false);
    }

    private void initScriptEngine() {
        if (scriptEngineManager != null) {
            return;
        }

        scriptEngineManager = new ScriptEngineManager();
        scriptEngine = scriptEngineManager.getEngineByName("gremlin-groovy");
        bindings = scriptEngine.createBindings();

        //Do not cache script compilations due to memory implications
        scriptEngine.getContext().setAttribute("#jsr223.groovy.engine.keep.globals", "phantom", ScriptContext.ENGINE_SCOPE);
    }
}
