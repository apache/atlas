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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.FullTextMapperV2;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;

/**
 * Builds embeddable text from the graph, aligned with free-text Solr field coverage
 * ({@code SolrIndexHelper}): indexable string attributes, labels, classification text,
 * business metadata, and glossary term assignments.
 */
@Component
public class SemanticTextBuilder {
    /** Same separator as {@link org.apache.atlas.repository.graph.FullTextMapperV2}. */
    private static final String TEXT_DELIMITER = " ";
    /** Safety cap applied once on the final string (chars, not tokens). */
    private static final int MAX_TEXT_LENGTH = 8000;

    private static final int MAX_TYPE_NAME           = 100;
    private static final int MAX_CLASSIFICATION_TEXT = 2000;
    private static final int MAX_LABELS              = 500;
    private static final int MAX_CUSTOM_ATTRIBUTES   = 500;
    private static final int MAX_GLOSSARY_TERM       = 200;
    private static final int MAX_ATTRIBUTE_VALUE     = 500;

    private final FullTextMapperV2 fullTextMapperV2;
    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public SemanticTextBuilder(FullTextMapperV2 fullTextMapperV2, AtlasTypeRegistry typeRegistry) {
        this.fullTextMapperV2 = fullTextMapperV2;
        this.typeRegistry    = typeRegistry;
    }

    public String buildTextForEntity(String guid, AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity entity = fullTextMapperV2.getAndCacheEntity(guid, false);
        if (entity == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        appendToken(sb, entity.getTypeName(), MAX_TYPE_NAME);
        appendVertexProperty(sb, vertex, CLASSIFICATION_TEXT_KEY, MAX_CLASSIFICATION_TEXT);
        appendVertexProperty(sb, vertex, LABELS_PROPERTY_KEY, MAX_LABELS);
        appendVertexProperty(sb, vertex, CUSTOM_ATTRIBUTES_PROPERTY_KEY, MAX_CUSTOM_ATTRIBUTES);
        appendGlossaryTerms(sb, entity);

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        if (entityType != null) {
            appendIndexableAttributes(sb, entity, entityType.getAllAttributes());
            appendBusinessMetadata(sb, entity, entityType);
        }

        return truncate(StringUtils.trimToEmpty(sb.toString()));
    }

    private void appendBusinessMetadata(StringBuilder sb, AtlasEntity entity, AtlasEntityType entityType) {
        Map<String, Map<String, Object>> entityBusinessMetadata = entity.getBusinessAttributes();
        Map<String, Map<String, AtlasBusinessAttribute>> typeBusinessMetadata = entityType.getBusinessAttributes();

        if (MapUtils.isEmpty(typeBusinessMetadata) || MapUtils.isEmpty(entityBusinessMetadata)) {
            return;
        }

        for (Map.Entry<String, Map<String, AtlasBusinessAttribute>> bmEntry : typeBusinessMetadata.entrySet()) {
            Map<String, Object> bmValues = entityBusinessMetadata.get(bmEntry.getKey());
            if (MapUtils.isEmpty(bmValues)) {
                continue;
            }

            for (AtlasBusinessAttribute bmAttribute : bmEntry.getValue().values()) {
                if (!isIndexableStringAttribute(bmAttribute)) {
                    continue;
                }
                appendAttributeValue(sb, bmValues.get(bmAttribute.getName()), MAX_ATTRIBUTE_VALUE);
            }
        }
    }

    private void appendIndexableAttributes(StringBuilder sb,
                                           AtlasEntity entity,
                                           Map<String, AtlasAttribute> attributes) {
        if (MapUtils.isEmpty(attributes)) {
            return;
        }

        for (AtlasAttribute attribute : attributes.values()) {
            if (!isIndexableStringAttribute(attribute)) {
                continue;
            }
            appendAttributeValue(sb, entity.getAttribute(attribute.getName()), MAX_ATTRIBUTE_VALUE);
        }
    }

    /** Same rule as {@link org.apache.atlas.repository.graph.SolrIndexHelper#processAttribute}. */
    private static boolean isIndexableStringAttribute(AtlasAttribute attribute) {
        return attribute != null
                && GraphBackedSearchIndexer.isStringAttribute(attribute)
                && StringUtils.isNotEmpty(attribute.getIndexFieldName());
    }

    private static void appendGlossaryTerms(StringBuilder sb, AtlasEntity entity) {
        List<AtlasTermAssignmentHeader> meanings = entity.getMeanings();
        if (CollectionUtils.isEmpty(meanings)) {
            return;
        }

        for (AtlasTermAssignmentHeader meaning : meanings) {
            if (meaning == null) {
                continue;
            }
            appendToken(sb, meaning.getDisplayText(), MAX_GLOSSARY_TERM);
        }
    }

    private static void appendVertexProperty(StringBuilder sb, AtlasVertex vertex, String propertyKey, int maxChars) {
        String value = AtlasGraphUtilsV2.getEncodedProperty(vertex, propertyKey, String.class);
        appendToken(sb, value, maxChars);
    }

    private static void appendAttributeValue(StringBuilder sb, Object value, int maxValueChars) {
        if (value == null) {
            return;
        }

        if (value instanceof String) {
            appendToken(sb, (String) value, maxValueChars);
            return;
        }

        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                if (item != null) {
                    appendToken(sb, String.valueOf(item), maxValueChars);
                }
            }
            return;
        }

        appendToken(sb, String.valueOf(value), maxValueChars);
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
