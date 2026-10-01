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
package org.apache.atlas.semantic;

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
import java.util.Set;

import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

/**
 * Expands Kafka notification guids into entity guids that should receive semantic embeddings.
 * Glossary term updates are mapped to their assigned entities; glossary-only types are skipped.
 */
public final class SemanticNotificationGuidExpander {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticNotificationGuidExpander.class);

    // Glossary objects are entity vertices too: their supertype is __internal, but their names lack the "__" prefix
    // that isInternalType() checks, so Atlas notifies them and the repair scan would match them like any entity.
    // Names, not the supertype, because __superTypeNames is a SET property and is not in the OpenSearch vertex index.
    private static final Set<String> NON_EMBEDDABLE_ENTITY_TYPES = Set.of(
            "AtlasGlossaryTerm",
            "AtlasGlossary",
            "AtlasGlossaryCategory");

    private SemanticNotificationGuidExpander() {
    }

    public static Set<String> expandForIndexing(Set<String> guids) {
        if (guids == null || guids.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> expanded = new LinkedHashSet<>();
        for (String guid : guids) {
            expandGuid(guid, expanded);
        }
        return expanded;
    }

    public static Set<String> getNonEmbeddableEntityTypes() {
        return NON_EMBEDDABLE_ENTITY_TYPES;
    }

    public static boolean isEmbeddableEntityType(String typeName) {
        return StringUtils.isNotBlank(typeName)
                && !GraphHelper.isInternalType(typeName)
                && !NON_EMBEDDABLE_ENTITY_TYPES.contains(typeName);
    }

    private static void expandGuid(String guid, Set<String> expanded) {
        if (StringUtils.isBlank(guid)) {
            return;
        }

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);
        if (vertex == null) {
            return;
        }

        String typeName = AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class);
        if (GraphHelper.isInternalType(typeName)) {
            return;
        }

        if ("AtlasGlossaryTerm".equals(typeName)) {
            addAssignedEntityGuids(guid, vertex, expanded);
            return;
        }

        if (isEmbeddableEntityType(typeName)) {
            expanded.add(guid);
        }
    }

    // capped: a poll that takes longer than max.poll.interval.ms is redelivered, so an unbounded fan-out could loop
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void addAssignedEntityGuids(String termGuid, AtlasVertex termVertex, Set<String> expanded) {
        int maxEntities = SemanticSearchConfiguration.getSemanticIndexerMaxTermEntities();
        if (maxEntities == 0) {
            LOG.debug("Glossary term {} updated: fan-out disabled ({}=0)", termGuid, SemanticSearchConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES_CONF);
            return;
        }

        Iterable<?> edges = termVertex.query()
                .direction(AtlasEdgeDirection.OUT)
                .label(TERM_ASSIGNMENT_LABEL)
                .edges(maxEntities + 1);
        if (edges == null) {
            return;
        }

        int count = 0;
        for (AtlasEdge edge : (Iterable<AtlasEdge>) edges) {
            if (edge == null) {
                continue;
            }

            if (++count > maxEntities) {
                LOG.warn("Glossary term {} has more than {} assigned entities ({}): only the first {} are re-embedded;"
                        + " run atlas_semantic_repair.sh --all to refresh the others",
                        termGuid, maxEntities, SemanticSearchConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES_CONF, maxEntities);
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
