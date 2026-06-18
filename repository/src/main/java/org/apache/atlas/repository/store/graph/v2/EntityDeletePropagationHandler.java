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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.DeletePropagationTarget;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Component
public class EntityDeletePropagationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDeletePropagationHandler.class);

    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public EntityDeletePropagationHandler(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    public Set<AtlasVertex> collectPropagatedVertices(AtlasVertex sourceVertex, AtlasEntityType entityType, Set<String> visitedGuids) {
        Set<AtlasVertex> propagatedVertices = new HashSet<>();

        collectPropagatedVerticesRecursive(sourceVertex, entityType, visitedGuids, propagatedVertices);

        return propagatedVertices;
    }

    private void collectPropagatedVerticesRecursive(AtlasVertex sourceVertex, AtlasEntityType sourceEntityType,
                                                    Set<String> visitedGuids, Set<AtlasVertex> propagatedVertices) {
        for (DeletePropagationTarget propagationTarget : sourceEntityType.getDeletePropagationTargets()) {
            AtlasAttribute relAttr = propagationTarget.getRelAttr();

            if (relAttr == null) {
                continue;
            }

            for (AtlasEdge edge : toList(GraphHelper.getEdgesForLabel(sourceVertex, relAttr.getRelationshipEdgeLabel(), relAttr.getRelationshipEdgeDirection()))) {
                if (GraphHelper.getStatus(edge) == AtlasEntity.Status.DELETED) {
                    LOG.debug("collectPropagatedVertices(): skip — soft-deleted edge (label={})", relAttr.getRelationshipEdgeLabel());
                    continue;
                }

                AtlasVertex targetVertex = getOtherVertex(edge, sourceVertex);

                if (targetVertex == null) {
                    continue;
                }

                if (GraphHelper.getStatus(targetVertex) == AtlasEntity.Status.DELETED) {
                    LOG.debug("collectPropagatedVertices(): skip — already-deleted target vertex (guid={})",
                            AtlasGraphUtilsV2.getIdFromVertex(targetVertex));
                    continue;
                }

                String targetGuid = AtlasGraphUtilsV2.getIdFromVertex(targetVertex);

                if (StringUtils.isBlank(targetGuid) || !visitedGuids.add(targetGuid)) {
                    LOG.debug("collectPropagatedVertices(): skip — already visited (guid={})", targetGuid);
                    continue;
                }

                String          targetTypeName   = GraphHelper.getTypeName(targetVertex);
                AtlasEntityType targetEntityType = typeRegistry.getEntityTypeByName(targetTypeName);

                if (targetEntityType == null) {
                    LOG.debug("collectPropagatedVertices(): skip — no typedef for target type '{}' (guid={})", targetTypeName, targetGuid);
                    continue;
                }

                propagatedVertices.add(targetVertex);

                LOG.info("collectPropagatedVertices(): propagating delete from '{}' to '{}' (guid={})",
                        sourceEntityType.getTypeName(), targetTypeName, targetGuid);

                if (CollectionUtils.isNotEmpty(targetEntityType.getDeletePropagationTargets())) {
                    collectPropagatedVerticesRecursive(targetVertex, targetEntityType, visitedGuids, propagatedVertices);
                }
            }
        }
    }

    private AtlasVertex getOtherVertex(AtlasEdge edge, AtlasVertex anchor) {
        AtlasVertex out = edge.getOutVertex();
        AtlasVertex in  = edge.getInVertex();

        return out != null && out.equals(anchor) ? in : out;
    }

    private <T> List<T> toList(Iterator<T> iterator) {
        List<T> list = new ArrayList<>();

        if (iterator != null) {
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }

        return list;
    }
}
