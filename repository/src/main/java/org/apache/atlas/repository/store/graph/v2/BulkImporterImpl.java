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
package org.apache.atlas.repository.store.graph.v2;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.BulkImporter;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.HISTORICAL_GUID_PROPERTY_KEY;

@Component
public class BulkImporterImpl implements BulkImporter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV2.class);

    private final AtlasEntityStore entityStore;
    private final EntityGraphRetriever entityGraphRetriever;
    private final Map<AtlasObjectId, String> objectIdExistingGuidMap;
    private final StartEntityFetchByExportRequest startEntityFetchByExportRequest;

    @Inject
    public BulkImporterImpl(AtlasGraph atlasGraph, AtlasEntityStore entityStore, AtlasTypeRegistry typeRegistry) {
        this.entityStore = entityStore;
        this.entityGraphRetriever = new EntityGraphRetriever(typeRegistry);
        this.objectIdExistingGuidMap = new HashMap<>();
        this.startEntityFetchByExportRequest = new StartEntityFetchByExportRequest(atlasGraph, typeRegistry, AtlasGremlinQueryProvider.INSTANCE);
    }

    @Override
    public EntityMutationResponse bulkImport(EntityImportStream entityStream, AtlasImportResult importResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> bulkImport()");
        }

        if (entityStream == null || !entityStream.hasNext()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entities to create/update.");
        }

        fetchExistingEntitiesNotCreatedByImport(importResult.getExportResult().getRequest());
        EntityMutationResponse ret = new EntityMutationResponse();
        ret.setGuidAssignments(new HashMap<>());

        Set<String>  processedGuids = new HashSet<>();
        float        currentPercent = 0f;
        List<String> residualList   = new ArrayList<>();

        EntityImportStreamWithResidualList entityImportStreamWithResidualList = new EntityImportStreamWithResidualList(entityStream, residualList);

        while (entityImportStreamWithResidualList.hasNext()) {
            AtlasEntityWithExtInfo entityWithExtInfo = entityImportStreamWithResidualList.getNextEntityWithExtInfo();
            AtlasEntity            entity            = entityWithExtInfo != null ? entityWithExtInfo.getEntity() : null;

            if (entity == null) {
                continue;
            }

            AtlasEntityStreamForImport oneEntityStream = new AtlasEntityStreamForImport(entityWithExtInfo, entityStream);

            try {
                EntityMutationResponse resp = entityStore.createOrUpdateForImport(oneEntityStream);

                if (resp.getGuidAssignments() != null) {
                    ret.getGuidAssignments().putAll(resp.getGuidAssignments());
                }

                currentPercent = updateImportMetrics(entityWithExtInfo, resp, importResult, processedGuids, entityStream.getPosition(), entityImportStreamWithResidualList.getStreamSize(), currentPercent);

                entityStream.onImportComplete(entity.getGuid());
            } catch (AtlasBaseException e) {
                if (!updateResidualList(e, residualList, entityWithExtInfo.getEntity().getGuid())) {
                    throw e;
                }
            } catch (AtlasSchemaViolationException e) {
                AtlasObjectId objectId = entityGraphRetriever.toAtlasObjectIdWithoutGuid(entity);
                if (objectIdExistingGuidMap.containsKey(objectId)) {
                    updateVertexGuidIfImportingToNonImportedEntity(entity, objectId);
                }

                continue;
            }
            catch (Throwable e) {
                AtlasBaseException abe = new AtlasBaseException(e);

                if (!updateResidualList(abe, residualList, entityWithExtInfo.getEntity().getGuid())) {
                    throw abe;
                }
            } finally {
                RequestContext.get().clearCache();
            }
        }

        importResult.getProcessedEntities().addAll(processedGuids);
        LOG.info("bulkImport(): done. Total number of entities (including referred entities) imported: {}", processedGuids.size());

        return ret;
    }

    private void fetchExistingEntitiesNotCreatedByImport(AtlasExportRequest request) {
        List<AtlasObjectId> objectIds = startEntityFetchByExportRequest.get(request);
        if (objectIds.isEmpty()) {
            return;
        }

        for (AtlasObjectId objectId : objectIds) {
            String existingGuid = objectId.getGuid();
            objectId.setGuid(null);

            objectIdExistingGuidMap.put(objectId, existingGuid);
        }
    }

    @GraphTransaction
    public void updateVertexGuidIfImportingToNonImportedEntity(AtlasEntity entity, AtlasObjectId objectId) {
        String entityGuid = entity.getGuid();
        String vertexGuid = objectIdExistingGuidMap.get(objectId);
        if (vertexGuid.equals(entityGuid)) {
            return;
        }

        AtlasVertex v = AtlasGraphUtilsV2.findByGuid(vertexGuid);
        if (v == null) {
            return;
        }

        addHistoricalGuid(v, vertexGuid);
        AtlasGraphUtilsV2.setProperty(v, Constants.GUID_PROPERTY_KEY, entityGuid);

        LOG.warn("GUID Updated: Entity: {}: from: {}: to: {}", objectId, vertexGuid, entity.getGuid());
    }

    private void addHistoricalGuid(AtlasVertex v, String vertexGuid) {
        String existingJson = AtlasGraphUtilsV2.getProperty(v, HISTORICAL_GUID_PROPERTY_KEY, String.class);
        
        AtlasGraphUtilsV2.setProperty(v, HISTORICAL_GUID_PROPERTY_KEY, getJsonArray(existingJson, vertexGuid));
    }

    @VisibleForTesting
    static String getJsonArray(String json, String vertexGuid) {
        String quotedGuid = String.format("\"%s\"", vertexGuid);
        if (StringUtils.isEmpty(json)) {
            json = String.format("[%s]", quotedGuid);
        } else {
            json = json.replace("]", "").concat(",").concat(quotedGuid).concat("]");
        }
        return json;
    }

    private boolean updateResidualList(AtlasBaseException e, List<String> lineageList, String guid) {
        if (!e.getAtlasErrorCode().getErrorCode().equals(AtlasErrorCode.INVALID_OBJECT_ID.getErrorCode())) {
            return false;
        }

        lineageList.add(guid);

        return true;
    }

    private float updateImportMetrics(AtlasEntity.AtlasEntityWithExtInfo currentEntity,
                                      EntityMutationResponse             resp,
                                      AtlasImportResult                  importResult,
                                      Set<String>                        processedGuids,
                                      int currentIndex, int streamSize, float currentPercent) {
        updateImportMetrics("entity:%s:created", resp.getCreatedEntities(), processedGuids, importResult);
        updateImportMetrics("entity:%s:updated", resp.getUpdatedEntities(), processedGuids, importResult);
        updateImportMetrics("entity:%s:deleted", resp.getDeletedEntities(), processedGuids, importResult);

        String lastEntityImported = String.format("entity:last-imported:%s:[%s]:(%s)", currentEntity.getEntity().getTypeName(), currentIndex, currentEntity.getEntity().getGuid());

        return updateImportProgress(LOG, currentIndex, streamSize, currentPercent, lastEntityImported);
    }

    @VisibleForTesting
    static float updateImportProgress(Logger log, int currentIndex, int streamSize, float currentPercent, String additionalInfo) {
        final double tolerance   = 0.000001;
        final int    MAX_PERCENT = 100;

        int     maxSize        = (currentIndex <= streamSize) ? streamSize : currentIndex;
        float   percent        = (float) ((currentIndex * MAX_PERCENT) / maxSize);
        boolean updateLog      = Double.compare(percent, currentPercent) > tolerance;
        float   updatedPercent = (MAX_PERCENT < maxSize) ? percent : ((updateLog) ? ++currentPercent : currentPercent);

        if (updateLog) {
            log.info("bulkImport(): progress: {}% (of {}) - {}", (int) Math.ceil(percent), maxSize, additionalInfo);
        }

        return updatedPercent;
    }

    private static void updateImportMetrics(String prefix, List<AtlasEntityHeader> list, Set<String> processedGuids, AtlasImportResult importResult) {
        if (list == null) {
            return;
        }

        for (AtlasEntityHeader h : list) {
            if (processedGuids.contains(h.getGuid())) {
                continue;
            }

            processedGuids.add(h.getGuid());
            importResult.incrementMeticsCounter(String.format(prefix, h.getTypeName()));
        }
    }

    private static class EntityImportStreamWithResidualList {
        private final EntityImportStream stream;
        private final List<String>       residualList;
        private       boolean            navigateResidualList;
        private       int                currentResidualListIndex;


        public EntityImportStreamWithResidualList(EntityImportStream stream, List<String> residualList) {
            this.stream                   = stream;
            this.residualList             = residualList;
            this.navigateResidualList     = false;
            this.currentResidualListIndex = 0;
        }

        public AtlasEntity.AtlasEntityWithExtInfo getNextEntityWithExtInfo() {
            if (navigateResidualList == false) {
                return stream.getNextEntityWithExtInfo();
            } else {
                stream.setPositionUsingEntityGuid(residualList.get(currentResidualListIndex++));
                return stream.getNextEntityWithExtInfo();
            }
        }

        public boolean hasNext() {
            if (!navigateResidualList) {
                boolean streamHasNext = stream.hasNext();
                navigateResidualList = (streamHasNext == false);
                return streamHasNext ? streamHasNext : (currentResidualListIndex < residualList.size());
            } else {
                return (currentResidualListIndex < residualList.size());
            }
        }

        public int getStreamSize() {
            return stream.size() + residualList.size();
        }
    }
}
