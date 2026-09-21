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
            addAssignedEntityGuids(vertex, expanded);
            return;
        }

        if (isEmbeddableEntityType(typeName)) {
            expanded.add(guid);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void addAssignedEntityGuids(AtlasVertex termVertex, Set<String> expanded) {
        Iterable<?> edges = termVertex.query()
                .direction(AtlasEdgeDirection.OUT)
                .label(TERM_ASSIGNMENT_LABEL)
                .edges();
        if (edges == null) {
            return;
        }

        for (AtlasEdge edge : (Iterable<AtlasEdge>) edges) {
            if (edge == null) {
                continue;
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
