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

import org.apache.atlas.glossary.GlossaryUtils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.INTERNAL_PROPERTY_KEY_PREFIX;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
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
    private static final String GLOSSARY_TERM_DISPLAY_NAME_ATTR = GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME + ".name";
    private static final String GLOSSARY_TERM_ABBREVIATION_ATTR = GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME + ".abbreviation";
    private static final String GLOSSARY_TERM_DESCRIPTION_ATTR  = GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME + ".description";

    // only these attributes are embedded, in this order: small embedding models only read the first ~256-512 tokens
    private static final List<String> ATTRIBUTE_PROPERTY_KEYS = Arrays.asList(
            "Asset.name",
            "Asset.displayName",
            "Asset.description",
            "Asset.userDescription",
            QUALIFIED_NAME);

    // type-specific free text, e.g. hive_table.comment, hive_column.comment
    private static final String COMMENT_PROPERTY_SUFFIX = ".comment";

    public String buildText(AtlasVertex vertex) {
        if (vertex == null) {
            return "";
        }

        // the entity's own attributes first, so classifications and terms can't push them past MAX_TEXT_LENGTH
        StringBuilder sb = new StringBuilder();
        appendToken(sb, AtlasGraphUtilsV2.getTypeName(vertex), MAX_TYPE_NAME);
        for (String propertyKey : ATTRIBUTE_PROPERTY_KEYS) {
            appendStringProperty(sb, vertex, propertyKey);
        }
        appendComments(sb, vertex);
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

    /**
     * Term-to-entity assignment edges of a term (OUT) or an entity (IN). With soft delete, removed assignments keep
     * their edge in DELETED state: they are skipped.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Iterable<AtlasEdge> activeTermAssignmentEdges(AtlasVertex vertex, AtlasEdgeDirection direction, int limit) {
        AtlasVertexQuery query = vertex.query();
        Iterable<AtlasEdge> edges = query == null ? null : query
                .direction(direction)
                .label(TERM_ASSIGNMENT_LABEL)
                .has(STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name())
                .edges(limit);

        return edges != null ? edges : Collections.emptyList();
    }

    private static void appendGlossaryTerms(StringBuilder sb, AtlasVertex entityVertex) {
        for (AtlasEdge edge : activeTermAssignmentEdges(entityVertex, AtlasEdgeDirection.IN, MAX_GLOSSARY_TERMS)) {
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

    private static void appendComments(StringBuilder sb, AtlasVertex vertex) {
        Collection<? extends String> propertyKeys = vertex.getPropertyKeys();
        if (propertyKeys == null) {
            return;
        }

        for (String propertyKey : propertyKeys) {
            if (propertyKey != null && propertyKey.endsWith(COMMENT_PROPERTY_SUFFIX) && !propertyKey.startsWith(INTERNAL_PROPERTY_KEY_PREFIX)) {
                appendStringProperty(sb, vertex, propertyKey);
            }
        }
    }

    // getProperty() casts unchecked: read as Object, so a non-string value is skipped instead of throwing
    private static void appendStringProperty(StringBuilder sb, AtlasVertex vertex, String propertyKey) {
        Object value = vertex.getProperty(propertyKey, Object.class);
        if (value instanceof String) {
            appendToken(sb, (String) value, MAX_ATTRIBUTE_VALUE);
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
