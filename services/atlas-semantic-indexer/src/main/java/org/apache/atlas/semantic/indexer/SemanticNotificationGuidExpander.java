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
package org.apache.atlas.semantic.indexer;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.glossary.GlossaryUtils;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Expands Kafka notification guids into the guids to re-embed, using the type name from the notification: a
 * glossary term is replaced by its assigned entities (the only graph read here), any other guid is passed on.
 * {@link org.apache.atlas.semantic.SemanticEntityEmbedder} decides from the graph whether each one is embeddable.
 */
public final class SemanticNotificationGuidExpander {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticNotificationGuidExpander.class);

    private SemanticNotificationGuidExpander() {
    }

    public static Set<String> expandForIndexing(Map<String, String> guidTypes) {
        if (guidTypes == null || guidTypes.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> expanded = new LinkedHashSet<>();
        for (Map.Entry<String, String> entry : guidTypes.entrySet()) {
            String guid = entry.getKey();
            if (StringUtils.isBlank(guid)) {
                continue;
            }

            if (GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME.equals(entry.getValue())) {
                addAssignedEntityGuids(guid, expanded);
            } else {
                expanded.add(guid);
            }
        }
        return expanded;
    }

    // capped: a poll that takes longer than max.poll.interval.ms is redelivered, so an unbounded fan-out could loop
    private static void addAssignedEntityGuids(String termGuid, Set<String> expanded) {
        int maxEntities = Math.max(0, AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getInt());
        if (maxEntities == 0) {
            LOG.debug("Glossary term {} updated: fan-out disabled ({}=0)", termGuid, AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName());
            return;
        }

        AtlasVertex termVertex = AtlasGraphUtilsV2.findByGuid(termGuid);
        if (termVertex == null) {
            return;
        }

        int count = 0;
        for (AtlasEdge edge : GraphHelper.getActiveTermAssignmentEdges(termVertex, AtlasEdgeDirection.OUT, maxEntities + 1)) {
            if (edge == null) {
                continue;
            }

            if (++count > maxEntities) {
                LOG.warn("Glossary term {} has more than {} assigned entities ({}): only the first {} are re-embedded;"
                        + " run atlas_semantic_repair.sh --all to refresh the others",
                        termGuid, maxEntities, AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName(), maxEntities);
                break;
            }

            AtlasVertex entityVertex = edge.getInVertex();
            if (entityVertex == null) {
                continue;
            }

            String entityGuid = GraphHelper.getGuid(entityVertex);
            if (StringUtils.isNotBlank(entityGuid)) {
                expanded.add(entityGuid);
            }
        }
    }
}
