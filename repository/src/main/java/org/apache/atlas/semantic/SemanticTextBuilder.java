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

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;

/**
 * Builds embeddable text directly from JanusGraph vertex properties without typedef registry access.
 */
@Component
public class SemanticTextBuilder {
    private static final String TEXT_DELIMITER = " ";
    private static final int    MAX_TEXT_LENGTH = 8000;

    private static final int MAX_TYPE_NAME           = 100;
    private static final int MAX_CLASSIFICATION_TEXT = 2000;
    private static final int MAX_LABELS              = 500;
    private static final int MAX_CUSTOM_ATTRIBUTES   = 500;
    private static final int MAX_GLOSSARY_TERM       = 200;
    private static final int MAX_ATTRIBUTE_VALUE     = 500;
    private static final int MAX_GLOSSARY_TERMS      = 100; // assigned terms read per entity

    /** Encoded vertex properties for {@link org.apache.atlas.model.glossary.AtlasGlossaryTerm}. */
    private static final String GLOSSARY_TERM_DISPLAY_NAME_ATTR = "AtlasGlossaryTerm.name";
    private static final String GLOSSARY_TERM_ABBREVIATION_ATTR = "AtlasGlossaryTerm.abbreviation";
    private static final String GLOSSARY_TERM_DESCRIPTION_ATTR  = "AtlasGlossaryTerm.description";

    private static final Set<String> HANDLED_PROPERTY_KEYS = new HashSet<>();

    static {
        HANDLED_PROPERTY_KEYS.add(CLASSIFICATION_TEXT_KEY);
        HANDLED_PROPERTY_KEYS.add(LABELS_PROPERTY_KEY);
        HANDLED_PROPERTY_KEYS.add(CUSTOM_ATTRIBUTES_PROPERTY_KEY);
    }

    public String buildText(AtlasVertex vertex) {
        if (vertex == null) {
            return "";
        }

        // the entity's own attributes first, so classifications and terms can't push them past MAX_TEXT_LENGTH
        StringBuilder sb = new StringBuilder();
        appendToken(sb, AtlasGraphUtilsV2.getTypeName(vertex), MAX_TYPE_NAME);
        appendRemainingStringProperties(sb, vertex);
        // Atlas keeps the type names and attribute values of all classifications (own and propagated) here
        appendVertexProperty(sb, vertex, CLASSIFICATION_TEXT_KEY, MAX_CLASSIFICATION_TEXT);
        appendVertexProperty(sb, vertex, LABELS_PROPERTY_KEY, MAX_LABELS);
        appendVertexProperty(sb, vertex, CUSTOM_ATTRIBUTES_PROPERTY_KEY, MAX_CUSTOM_ATTRIBUTES);
        appendGlossaryTerms(sb, vertex);

        return truncate(StringUtils.trimToEmpty(sb.toString()));
    }

    // once the text is full, stop reading more vertices: the rest would be truncated anyway
    private static boolean isFull(StringBuilder sb) {
        return sb.length() >= MAX_TEXT_LENGTH;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void appendGlossaryTerms(StringBuilder sb, AtlasVertex entityVertex) {
        AtlasVertexQuery vertexQuery = entityVertex.query();
        if (vertexQuery == null) {
            return;
        }

        Iterable<?> edges = vertexQuery
                .direction(AtlasEdgeDirection.IN)
                .label(TERM_ASSIGNMENT_LABEL)
                .edges(MAX_GLOSSARY_TERMS);
        if (edges == null) {
            return;
        }

        for (AtlasEdge edge : (Iterable<AtlasEdge>) edges) {
            if (isFull(sb)) {
                return;
            }

            if (edge == null) {
                continue;
            }

            AtlasVertex termVertex = edge.getOutVertex();
            if (termVertex == null) {
                continue;
            }

            appendGlossaryTermText(sb, termVertex);
        }
    }

    // only the assigned term itself: terms linked to it (synonyms, related, antonyms, ...) are not followed, so a
    // change to a linked term never needs a second-level fan-out
    private static void appendGlossaryTermText(StringBuilder sb, AtlasVertex termVertex) {
        appendToken(sb, AtlasGraphUtilsV2.getEncodedProperty(termVertex, GLOSSARY_TERM_DISPLAY_NAME_ATTR, String.class),
                MAX_GLOSSARY_TERM);
        appendToken(sb, AtlasGraphUtilsV2.getEncodedProperty(termVertex, GLOSSARY_TERM_ABBREVIATION_ATTR, String.class),
                MAX_GLOSSARY_TERM);
        appendToken(sb, AtlasGraphUtilsV2.getEncodedProperty(termVertex, GLOSSARY_TERM_DESCRIPTION_ATTR, String.class),
                MAX_GLOSSARY_TERM);
    }

    private static void appendRemainingStringProperties(StringBuilder sb, AtlasVertex vertex) {
        Collection<? extends String> propertyKeys = vertex.getPropertyKeys();
        if (propertyKeys == null || propertyKeys.isEmpty()) {
            return;
        }

        for (String propertyKey : propertyKeys) {
            if (isFull(sb)) {
                return;
            }

            if (StringUtils.isBlank(propertyKey) || isInternalPropertyKey(propertyKey)) {
                continue;
            }

            if (HANDLED_PROPERTY_KEYS.contains(propertyKey)) {
                continue;
            }

            appendVertexStringProperty(sb, vertex, propertyKey);
        }
    }

    private static boolean isInternalPropertyKey(String propertyKey) {
        return propertyKey.startsWith("__");
    }

    private static void appendVertexStringProperty(StringBuilder sb, AtlasVertex vertex, String propertyKey) {
        try {
            String value = vertex.getProperty(propertyKey, String.class);
            if (StringUtils.isNotBlank(value)) {
                appendToken(sb, value, MAX_ATTRIBUTE_VALUE);
                return;
            }
        } catch (Exception ignored) {
            // not a single-valued string property
        }

        try {
            List<String> values = vertex.getListProperty(propertyKey);
            if (values == null || values.isEmpty()) {
                return;
            }

            for (String value : values) {
                appendToken(sb, value, MAX_ATTRIBUTE_VALUE);
            }
        } catch (Exception ignored) {
            // skip non-string properties (numbers, references, etc.)
        }
    }

    private static void appendVertexProperty(StringBuilder sb, AtlasVertex vertex, String propertyKey, int maxChars) {
        String value = AtlasGraphUtilsV2.getEncodedProperty(vertex, propertyKey, String.class);
        appendToken(sb, value, maxChars);
    }

    private static void appendToken(StringBuilder sb, String token, int maxTokenChars) {
        if (StringUtils.isBlank(token)) {
            return;
        }

        String trimmed = token.trim();
        int tokenLimit = Math.min(trimmed.length(), maxTokenChars);
        if (tokenLimit <= 0) {
            return;
        }

        if (sb.length() > 0) {
            sb.append(TEXT_DELIMITER);
        }
        sb.append(trimmed, 0, tokenLimit);
    }

    private static String truncate(String text) {
        if (text.length() <= MAX_TEXT_LENGTH) {
            return text;
        }
        return text.substring(0, MAX_TEXT_LENGTH);
    }
}
