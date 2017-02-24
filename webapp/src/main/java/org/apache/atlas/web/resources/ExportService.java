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

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;


public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    public static final String OPTION_ATTR_MATCH_TYPE = "matchType";
    public static final String MATCH_TYPE_STARTS_WITH = "startsWith";
    public static final String MATCH_TYPE_ENDS_WITH   = "endsWith";
    public static final String MATCH_TYPE_CONTAINS    = "contains";
    public static final String MATCH_TYPE_MATCHES     = "matches";

    private final AtlasTypeRegistry    typeRegistry;
    private final AtlasGraph           atlasGraph;
    private final EntityGraphRetriever entityGraphRetriever;

    // query engine support
    private final ScriptEngine scriptEngine;
    private final Bindings     bindings;
    private final String queryByGuid          = "g.V('__guid', startGuid).bothE().bothV().has('__guid').__guid.dedup().toList()";
    final private String queryByAttrEquals    = "g.V().has('__typeName','%s').has('%s', attrValue).has('__guid').__guid.toList()";
    final private String queryByAttrStartWith = "g.V().has('__typeName','%s').filter({it.'%s'.startsWith(attrValue)}).has('__guid').__guid.toList()";
    final private String queryByAttrEndsWith  = "g.V().has('__typeName','%s').filter({it.'%s'.endsWith(attrValue)}).has('__guid').__guid.toList()";
    final private String queryByAttrContains  = "g.V().has('__typeName','%s').filter({it.'%s'.contains(attrValue)}).has('__guid').__guid.toList()";
    final private String queryByAttrMatches   = "g.V().has('__typeName','%s').filter({it.'%s'.matches(attrValue)}).has('__guid').__guid.toList()";

    public ExportService(final AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.typeRegistry         = typeRegistry;
        this.entityGraphRetriever = new EntityGraphRetriever(this.typeRegistry);
        this.atlasGraph           = AtlasGraphProvider.getGraphInstance();

        this.scriptEngine  = new GremlinGroovyScriptEngine();

        //Do not cache script compilations due to memory implications
        scriptEngine.getContext().setAttribute("#jsr223.groovy.engine.keep.globals", "phantom",  ScriptContext.ENGINE_SCOPE);

        bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
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
        long startTimestamp = System.currentTimeMillis();
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

            long endTimestamp = System.currentTimeMillis();
            context.result.incrementMeticsCounter("duration", (int) (endTimestamp - startTimestamp));
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
            List<AtlasEntity> entities = getStartingEntity(item, context);

            for (AtlasEntity entity: entities) {
                processEntity(entity, context);
            }

            while (!context.guidsToProcess.isEmpty()) {
                String guid = context.guidsToProcess.remove(0);

                AtlasEntity e = entityGraphRetriever.toAtlasEntity(guid);

                processEntity(e, context);
            }
        } catch (AtlasBaseException excp) {
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

            LOG.error("Fetching entity failed for: {}", item, excp);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processObjectId({})", item);
        }
    }

    private List<AtlasEntity> getStartingEntity(AtlasObjectId item, ExportContext context) throws AtlasBaseException {
        List<AtlasEntity> ret = new ArrayList<>();

        if (StringUtils.isNotEmpty(item.getGuid())) {
            AtlasEntity entity = entityGraphRetriever.toAtlasEntity(item);

            if (entity != null) {
                ret = Collections.singletonList(entity);
            }
        } else if (StringUtils.isNotEmpty(item.getTypeName()) && MapUtils.isNotEmpty(item.getUniqueAttributes())) {
            String          typeName   = item.getTypeName();
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
            }

            AtlasExportRequest request = context.result.getRequest();
            String matchType = null;

            if (MapUtils.isNotEmpty(request.getOptions())) {
                if (request.getOptions().get(OPTION_ATTR_MATCH_TYPE) != null) {
                    matchType = request.getOptions().get(OPTION_ATTR_MATCH_TYPE).toString();
                }
            }

            final String queryTemplate;
            if (StringUtils.equalsIgnoreCase(matchType, MATCH_TYPE_STARTS_WITH)) {
                queryTemplate = queryByAttrStartWith;
            } else if (StringUtils.equalsIgnoreCase(matchType, MATCH_TYPE_ENDS_WITH)) {
                queryTemplate = queryByAttrEndsWith;
            } else if (StringUtils.equalsIgnoreCase(matchType, MATCH_TYPE_CONTAINS)) {
                queryTemplate = queryByAttrContains;
            } else if (StringUtils.equalsIgnoreCase(matchType, MATCH_TYPE_MATCHES)) {
                queryTemplate = queryByAttrMatches;
            } else { // default
                queryTemplate = queryByAttrEquals;
            }

            for (Map.Entry<String, Object> e : item.getUniqueAttributes().entrySet()) {
                String attrName  = e.getKey();
                Object attrValue = e.getValue();

                AtlasAttribute attribute = entityType.getAttribute(attrName);

                if (attribute == null || attrValue == null) {
                    continue;
                }

                String       query = String.format(queryTemplate, typeName, attribute.getQualifiedName());
                List<String> guids = executeGremlinScriptFor(query, "attrValue", attrValue.toString());

                if (CollectionUtils.isNotEmpty(guids)) {
                    for (String guid : guids) {
                        AtlasEntity entity = entityGraphRetriever.toAtlasEntity(guid);

                        if (entity == null) {
                            continue;
                        }

                        ret.add(entity);
                    }
                }

                break;
            }

            LOG.info("export(item={}; matchType={}): found {} entities", item, matchType, ret.size());
        }

        return ret;
    }

    private void processEntity(AtlasEntity entity, ExportContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> processEntity({})", AtlasTypeUtil.getAtlasObjectId(entity));
        }

        if (!context.guidsProcessed.contains(entity.getGuid())) {
            context.guidsProcessed.add(entity.getGuid());
            context.result.getData().getEntityCreationOrder().add(entity.getGuid());

            addTypesAsNeeded(entity.getTypeName(), context);
            addClassificationsAsNeeded(entity, context);
            addEntity(entity, context);

            getConnectedEntityGuids(entity, context);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processEntity({})", AtlasTypeUtil.getAtlasObjectId(entity));
        }
    }

    private void getConnectedEntityGuids(AtlasEntity entity, ExportContext context) {

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> getConnectedEntityGuids({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
            }

            List<String> result = executeGremlinScriptForHive(entity.getGuid());
            if(result == null) {
                return;
            }

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
        context.result.incrementMeticsCounter("entities");

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
                result.incrementMeticsCounter("typedef:classification");
            }
        }
    }

    private void addTypesAsNeeded(String typeName, ExportContext context) {
        AtlasExportResult result   = context.result;
        AtlasTypesDef     typesDef = result.getData().getTypesDef();

        if(!typesDef.hasEntityDef(typeName)) {
            AtlasEntityDef typeDefinition = typeRegistry.getEntityDefByName(typeName);

            typesDef.getEntityDefs().add(typeDefinition);
            result.incrementMeticsCounter("typedef:" + typeDefinition.getName());
        }
    }

    private List<String> executeGremlinScriptForHive(String guid) throws ScriptException {
        return executeGremlinScriptFor(this.queryByGuid, "startGuid", guid);
    }

    private List<String> executeGremlinScriptFor(String query, String parameterName, String parameterValue) {
        bindings.put(parameterName, parameterValue);
        try {
            return (List<String>) atlasGraph.executeGremlinScript(this.scriptEngine,
                    this.bindings,
                    query,
                    false);
        } catch (ScriptException e) {
            LOG.error("Script execution failed for query: ", query, e);
            return null;
        }
    }
}
