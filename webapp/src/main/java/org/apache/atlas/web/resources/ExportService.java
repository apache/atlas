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

import javax.script.ScriptException;
import java.util.*;


public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    private final AtlasTypeRegistry    typeRegistry;
    private final AtlasGraph           atlasGraph;
    private final EntityGraphRetriever entityGraphRetriever;

    public ExportService(final AtlasTypeRegistry typeRegistry) {

        this.typeRegistry         = typeRegistry;
        this.entityGraphRetriever = new EntityGraphRetriever(this.typeRegistry);
        this.atlasGraph           = AtlasGraphProvider.getGraphInstance();
    }

    private class ExportContext {
        final Map<String, Boolean> entitiesToBeProcessed = new HashMap<>();
        final AtlasExportResult    result;
        final ZipSink              sink;
        long                       numOfEntitiesExported = 0;

        ExportContext(AtlasExportResult result, ZipSink sink) {
            this.result = result;
            this.sink   = sink;
        }
    }

    public AtlasExportResult run(ZipSink exportSink, AtlasExportRequest request, String userName, String hostName,
                                 String requestingIP) throws AtlasException {

        ExportContext context = new ExportContext(new AtlasExportResult(request, userName, hostName, requestingIP, System.currentTimeMillis()), exportSink);

        try {
            LOG.info("==> export(user={}, from={})", userName, requestingIP);

            int i = 0;
            for (AtlasObjectId item : request.getItemsToExport()) {
                process(Integer.toString(i++), item, context);
            }

            context.sink.setExportOrder(context.result.getData().getEntityCreationOrder());
            context.sink.setTypesDef(context.result.getData().getTypesDef());

            context.result.getData().clear();
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.SUCCESS);
            context.sink.setResult(context.result);
        }
        catch(Exception ex) {
            LOG.error("Operation failed: ", ex);
        } finally {
            LOG.info("<== export(user={}, from={}): status {}", userName, requestingIP, context.result.getOperationStatus());
        }

        return context.result;
    }

    private void process(String folder, AtlasObjectId item, ExportContext context) throws AtlasServiceException, AtlasException, AtlasBaseException {
        try {
            AtlasEntity  entity = entityGraphRetriever.toAtlasEntity(item);
            List<String> queue  = populateConnectedEntities(entity.getGuid(), context);

            process(entity, context);

            for (String guid : queue) {
                if(context.entitiesToBeProcessed.get(guid)) {
                    continue;
                }

                process(entityGraphRetriever.toAtlasEntity(guid), context);
            }

            context.result.getData().getEntityCreationOrder().put(folder, queue);
        } catch (AtlasBaseException e) {
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

            LOG.error("Fetching entity failed for: {}", item);
        }
    }

    private void process(AtlasEntity entity, ExportContext context) throws AtlasBaseException, AtlasException {
        addTypesAsNeeded(entity.getTypeName(), context);
        addClassificationsAsNeeded(entity, context);
        addEntity(entity, context);
    }

    private void addEntity(AtlasEntity entity, ExportContext context) throws AtlasException, AtlasBaseException {
        context.entitiesToBeProcessed.put(entity.getGuid(), true);
        context.sink.add(entity);

        context.result.incrementMeticsCounter(String.format("entity:%s", entity.getTypeName()));
        context.result.incrementMeticsCounter("Entities");

        context.numOfEntitiesExported++;

        if (context.numOfEntitiesExported % 10 == 0) {
            LOG.info("export(): in progress.. number of entities exported: {}", context.numOfEntitiesExported);
        }
    }

    private List<String> populateConnectedEntities(String startGuid, ExportContext context) {
        final String gremlinQuery = "g.V('__guid', '%s').bothE().bothV().has('__guid').__guid.toList()";

        Map<String, Boolean> entitiesToBeProcessed = context.entitiesToBeProcessed;

        List<String> queue = new ArrayList<>();

        entitiesToBeProcessed.put(startGuid, false);
        queue.add(startGuid);

        for (int i=0; i < queue.size(); i++) {
            String currentGuid = queue.get(i);

            try {
                List<String> result = (List<String>) atlasGraph.executeGremlinScript(
                                                        String.format(gremlinQuery, currentGuid), false);

                for (String guid : result) {
                    if (entitiesToBeProcessed.containsKey(guid)) {
                        continue;
                    }

                    entitiesToBeProcessed.put(guid, false);
                    queue.add(guid);
                }
            } catch (ScriptException e) {
                LOG.error("Child entities could not be added for %s", currentGuid);
            }
        }

        return queue;
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
}
