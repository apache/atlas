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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RelationshipAttributesExtractor implements ExtractStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAttributesExtractor.class);

    private final AtlasTypeRegistry typeRegistry;

    public RelationshipAttributesExtractor(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void fullFetch(AtlasEntity entity, ExportService.ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> fullFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        List<AtlasRelatedObjectId> atlasRelatedObjectIdList = getRelatedObjectIds(entity);

        for (AtlasRelatedObjectId ar : atlasRelatedObjectIdList) {
            boolean isLineage = isLineageType(ar.getTypeName());

            if (context.skipLineage && isLineage) {
                continue;
            }
            context.addToBeProcessed(isLineage, ar.getGuid(), ExportService.TraversalDirection.BOTH);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== fullFetch({}): guidsToProcess {}", entity.getGuid(), context.guidsToProcess.size());
        }
    }

    @Override
    public void connectedFetch(AtlasEntity entity, ExportService.ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {} isSkipConnectedFetch :{}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size(), context.isSkipConnectedFetch);
        }

        List<AtlasRelatedObjectId> atlasRelatedObjectIdList = getRelatedObjectIds(entity);
        for (AtlasRelatedObjectId ar : atlasRelatedObjectIdList) {
            boolean isLineage = isLineageType(ar.getTypeName());

            if (context.skipLineage && isLineage) {
                continue;
            }
            if (!context.isSkipConnectedFetch || isLineage) {
                context.addToBeProcessed(isLineage, ar.getGuid(), ExportService.TraversalDirection.BOTH);
            }
        }

        if(isLineageType(entity.getTypeName())){
            context.isSkipConnectedFetch = false;
        }else{
            context.isSkipConnectedFetch = true;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {}, isSkipConnectedFetch :{}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size(), context.isSkipConnectedFetch);
        }
    }

    @Override
    public void close() {
    }

    private boolean isLineageType(String typeName) {
        AtlasEntityDef entityDef = typeRegistry.getEntityDefByName(typeName);
        return entityDef.getSuperTypes().contains("Process");
    }

    private List<AtlasRelatedObjectId> getRelatedObjectIds(AtlasEntity entity) {
        List<AtlasRelatedObjectId> relatedObjectIds = new ArrayList<>();

        for (Object o : entity.getRelationshipAttributes().values()) {
            if (o instanceof AtlasRelatedObjectId) {
                relatedObjectIds.add((AtlasRelatedObjectId) o);
            } else if (o instanceof Collection) {
                relatedObjectIds.addAll((List) o);
            }
        }

        return relatedObjectIds;
    }
}
