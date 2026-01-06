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


import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v2.utils.MDCScope;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.util.FileUtils;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_NAME_TYPENAME;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GLOSSARY_TERMS_EDGE_LABEL;
import static org.apache.atlas.repository.Constants.INDEX_SEARCH_VERTEX_PREFIX_DEFAULT;
import static org.apache.atlas.repository.Constants.INDEX_SEARCH_VERTEX_PREFIX_PROPERTY;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.SUPER_TYPES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TYPENAME_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.UNIQUE_QUALIFIED_NAME;
import static org.apache.atlas.repository.graph.AtlasGraphProvider.getGraphInstance;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.SortOrder.ASC;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.SortOrder.DESC;

/**
 * Utility methods for Graph.
 */
public class AtlasGraphUtilsV2 {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasGraphUtilsV2.class);

    public static final String PROPERTY_PREFIX = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";
    public static final String ENTITYTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".entitytype";
    public static final String RELATIONSHIPTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".relationshipType";
    public static final String VERTEX_TYPE = "typeSystem";
    private static final String COMPOSITE_INDEX_QN_TYPE = "__u_qualifiedName__typeName";

    private static boolean USE_INDEX_QUERY_TO_FIND_ENTITY_BY_UNIQUE_ATTRIBUTES = false;
    private static boolean USE_UNIQUE_INDEX_PROPERTY_TO_FIND_ENTITY = true;
    private static String INDEX_SEARCH_PREFIX;

    static {
        try {
            Configuration conf = ApplicationProperties.get();

            USE_INDEX_QUERY_TO_FIND_ENTITY_BY_UNIQUE_ATTRIBUTES = conf.getBoolean("atlas.use.index.query.to.find.entity.by.unique.attributes", USE_INDEX_QUERY_TO_FIND_ENTITY_BY_UNIQUE_ATTRIBUTES);
            USE_UNIQUE_INDEX_PROPERTY_TO_FIND_ENTITY = conf.getBoolean("atlas.unique.index.property.to.find.entity", USE_UNIQUE_INDEX_PROPERTY_TO_FIND_ENTITY);
            INDEX_SEARCH_PREFIX = conf.getString(INDEX_SEARCH_VERTEX_PREFIX_PROPERTY, INDEX_SEARCH_VERTEX_PREFIX_DEFAULT);
        } catch (Exception excp) {
            LOG.error("Error reading configuration", excp);
        } finally {
            LOG.debug("atlas.use.index.query.to.find.entity.by.unique.attributes=" + USE_INDEX_QUERY_TO_FIND_ENTITY_BY_UNIQUE_ATTRIBUTES);
        }
    }

    public static String getTypeDefPropertyKey(AtlasBaseTypeDef typeDef) {
        return getTypeDefPropertyKey(typeDef.getName());
    }

    public static String getTypeDefPropertyKey(AtlasBaseTypeDef typeDef, String child) {
        return getTypeDefPropertyKey(typeDef.getName(), child);
    }

    public static String getTypeDefPropertyKey(String typeName) {
        return PROPERTY_PREFIX + typeName;
    }

    public static String getTypeDefPropertyKey(String typeName, String child) {
        return PROPERTY_PREFIX + typeName + "." + child;
    }

    public static String getIdFromVertex(AtlasVertex vertex) {
        return vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
    }

    public static String getIdFromEdge(AtlasEdge edge) {
        return edge.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
    }

    public static String getTypeName(AtlasElement element) {
        return element.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public static String getEdgeLabel(String fromNode, String toNode) {
        return PROPERTY_PREFIX + "edge." + fromNode + "." + toNode;
    }

    public static String getEdgeLabel(String property) {
        return GraphHelper.EDGE_LABEL_PREFIX + property;
    }

    public static String getQualifiedAttributePropertyKey(AtlasStructType fromType, String attributeName) throws AtlasBaseException {
        switch (fromType.getTypeCategory()) {
            case ENTITY:
            case STRUCT:
            case CLASSIFICATION:
                return fromType.getQualifiedAttributePropertyKey(attributeName);
            default:
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPE, fromType.getTypeCategory().name());
        }
    }

    public static boolean isEntityVertex(AtlasVertex vertex) {
        return StringUtils.isNotEmpty(getIdFromVertex(vertex)) && StringUtils.isNotEmpty(getTypeName(vertex));
    }

    public static boolean isTypeVertex(AtlasVertex vertex) {
        return vertex.getProperty(TYPENAME_PROPERTY_KEY, String.class) != null;
    }

    public static boolean isReference(AtlasType type) {
        return isReference(type.getTypeCategory());
    }

    public static boolean isReference(TypeCategory typeCategory) {
        return typeCategory == TypeCategory.STRUCT ||
                typeCategory == TypeCategory.ENTITY ||
                typeCategory == TypeCategory.OBJECT_ID_TYPE;
    }

    public static String encodePropertyKey(String key) {
        return AtlasAttribute.encodePropertyKey(key);
    }

    public static String decodePropertyKey(String key) {
        return AtlasAttribute.decodePropertyKey(key);
    }

    /**
     * Adds an additional value to a multi-property.
     *
     * @param propertyName
     * @param value
     */
    public static AtlasVertex addProperty(AtlasVertex vertex, String propertyName, Object value) {
        return addProperty(vertex, propertyName, value, false);
    }

    public static AtlasVertex addEncodedProperty(AtlasVertex vertex, String propertyName, Object value) {
        return addProperty(vertex, propertyName, value, true);
    }

    public static AtlasEdge addEncodedProperty(AtlasEdge edge, String propertyName, String value) {
        List<String> listPropertyValues = edge.getListProperty(propertyName);

        if (listPropertyValues == null) {
            listPropertyValues = new ArrayList<>();
        }

        listPropertyValues.add(value);

        edge.removeProperty(propertyName);

        edge.setListProperty(propertyName, listPropertyValues);

        return edge;
    }

    public static AtlasVertex addProperty(AtlasVertex vertex, String propertyName, Object value, boolean isEncoded) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addProperty({}, {}, {})", toString(vertex), propertyName, value);
        }

        if (!isEncoded) {
            propertyName = encodePropertyKey(propertyName);
        }

        vertex.addProperty(propertyName, value);

        return vertex;
    }

    public static AtlasVertex addListProperty(AtlasVertex vertex, String propertyName, Object value, boolean isEncoded) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addListProperty({}, {}, {})", toString(vertex), propertyName, value);
        }

        if (!isEncoded) {
            propertyName = encodePropertyKey(propertyName);
        }

        vertex.addListProperty(propertyName, value);

        return vertex;
    }

    public static AtlasVertex deleteProperty(AtlasVertex vertex, String encodedPropertyName, String propertyValue) {
        Collection<Object> existingValues = vertex.getPropertyValues(encodedPropertyName, Object.class);

        if (! existingValues.isEmpty() && existingValues.contains(propertyValue)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing value {} from property {} of entity {}", propertyValue, encodedPropertyName, toString(vertex));
            }
            vertex.removePropertyValue(encodedPropertyName, propertyValue);
        }

        return vertex;
    }

    public static <T extends AtlasElement> void setProperty(T element, String propertyName, Object value) {
        setProperty(element, propertyName, value, false);
    }

    public static <T extends AtlasElement> void setEncodedProperty(T element, String propertyName, Object value) {
        setProperty(element, propertyName, value, true);
    }

    public static <T extends AtlasElement> void setProperty(T element, String propertyName, Object value, boolean isEncoded) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> setProperty({}, {}, {})", toString(element), propertyName, value);
        }

        if (!isEncoded) {
            propertyName = encodePropertyKey(propertyName);
        }

        Object existingValue = element.getProperty(propertyName, Object.class);

        if (value == null) {
            if (existingValue != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removing property {} from {}", propertyName, toString(element));
                }
                element.removeProperty(propertyName);
            }
        } else {
            if (!value.equals(existingValue)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting property {} in {}", propertyName, toString(element));
                }

                if (value instanceof Date) {
                    Long encodedValue = ((Date) value).getTime();
                    element.setProperty(propertyName, encodedValue);
                } else {
                    element.setProperty(propertyName, value);
                }
            }
        }
    }

    public static <T extends AtlasElement, O> O getProperty(T element, String propertyName, Class<O> returnType) {
        return getProperty(element, propertyName, returnType, false);
    }

    public static <T extends AtlasElement, O> O getEncodedProperty(T element, String propertyName, Class<O> returnType) {
        return getProperty(element, propertyName, returnType, true);
    }

    public static <T extends AtlasElement, O> O getProperty(T element, String propertyName, Class<O> returnType, boolean isEncoded) {
        if (!isEncoded) {
            propertyName = encodePropertyKey(propertyName);
        }

        Object property = element.getProperty(propertyName, returnType);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getProperty({}, {}) ==> {}", toString(element), propertyName, returnType.cast(property));
        }

        return returnType.cast(property);
    }

    public static AtlasVertex getVertexByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> attrValues) throws AtlasBaseException {
        return getVertexByUniqueAttributes(getGraphInstance(), entityType, attrValues);
    }

    public static AtlasVertex getVertexByUniqueAttributes(AtlasGraph graph, AtlasEntityType entityType, Map<String, Object> attrValues) throws AtlasBaseException {
        AtlasVertex vertex = findByUniqueAttributes(graph, entityType, attrValues);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    attrValues.toString());
        }

        return vertex;
    }

    public static String getGuidByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> attrValues) throws AtlasBaseException {
        AtlasVertex vertexByUniqueAttributes = getVertexByUniqueAttributes(getGraphInstance(), entityType, attrValues);
        return getIdFromVertex(vertexByUniqueAttributes);
    }

    public static String getGuidByUniqueAttributes(AtlasGraph graph, AtlasEntityType entityType, Map<String, Object> attrValues) throws AtlasBaseException {
        AtlasVertex vertexByUniqueAttributes = getVertexByUniqueAttributes(graph, entityType, attrValues);
        return getIdFromVertex(vertexByUniqueAttributes);
    }

    public static AtlasVertex findByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> attrValues) {
        return findByUniqueAttributes(getGraphInstance(), entityType, attrValues);
    }

    public static AtlasVertex findByUniqueAttributes(AtlasGraph graph, AtlasEntityType entityType, Map<String, Object> attrValues) {
        MetricRecorder                    metric           = RequestContext.get().startMetricRecord("findByUniqueAttributes");
        AtlasVertex                       vertex           = null;
        final Map<String, AtlasAttribute> uniqueAttributes = entityType.getUniqAttributes();

        if (MapUtils.isNotEmpty(uniqueAttributes) && MapUtils.isNotEmpty(attrValues)) {
            Map<String, Object> uniqAttrValues = populateUniqueAttributesMap(uniqueAttributes, attrValues);
            Map<String, Object> attrNameValues = populateAttributesMap(uniqueAttributes, attrValues);
            String              typeName       = entityType.getTypeName();
            Set<String>         entitySubTypes = entityType.getAllSubTypes();

            if (USE_UNIQUE_INDEX_PROPERTY_TO_FIND_ENTITY && MapUtils.isNotEmpty(uniqAttrValues)) {
                vertex = findByTypeAndUniquePropertyName(graph, typeName, uniqAttrValues);

                // if no instance of given typeName is found, try to find an instance of type's sub-type
                // Added exception for few types to solve https://atlanhq.atlassian.net/browse/PLT-1638
                if (vertex == null && !entitySubTypes.isEmpty() && !AtlasTypeRegistry.TYPENAMES_TO_SKIP_SUPER_TYPE_CHECK.contains(typeName)) {
                    vertex = findBySuperTypeAndUniquePropertyName(graph, typeName, uniqAttrValues);
                }
            } else {
                vertex = findByTypeAndPropertyName(graph, typeName, attrNameValues);

                // if no instance of given typeName is found, try to find an instance of type's sub-type
                // Added exception for few types to solve https://atlanhq.atlassian.net/browse/PLT-1638
                if (vertex == null && !entitySubTypes.isEmpty() && !AtlasTypeRegistry.TYPENAMES_TO_SKIP_SUPER_TYPE_CHECK.contains(typeName)) {
                    vertex = findBySuperTypeAndPropertyName(graph, typeName, attrNameValues);
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return vertex;
    }

    public static String findFirstDeletedDuringSpooledByQualifiedName(String qualifiedName, long timestamp) {
        return findFirstDeletedDuringSpooledByQualifiedName(getGraphInstance(), qualifiedName, timestamp);
    }

    public static String findFirstDeletedDuringSpooledByQualifiedName(AtlasGraph graph, String qualifiedName, long timestamp) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("findDeletedDuringSpooledByQualifiedName");

        AtlasGraphQuery query = graph.query().has(STATE_PROPERTY_KEY, Status.DELETED.name())
                                             .has(Constants.ENTITY_DELETED_TIMESTAMP_PROPERTY_KEY, AtlasGraphQuery.ComparisionOperator.GREATER_THAN, timestamp)
                                             .has(Constants.QUALIFIED_NAME, qualifiedName)
                                             .orderBy(Constants.ENTITY_DELETED_TIMESTAMP_PROPERTY_KEY, ASC);

        Iterator iterator = query.vertices().iterator();

        String ret = iterator.hasNext() ? GraphHelper.getGuid((AtlasVertex) iterator.next()) : null;

        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    public static AtlasVertex findByGuid(String guid) {
        return findByGuid(getGraphInstance(), guid);
    }

    public static AtlasVertex findByGuid(AtlasGraph graph, String guid) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("findByGuid");

        if (guid == null) {
            LOG.warn("findByGuid -> called for null guid!");
            return null;
        }

        AtlasVertex ret = GraphTransactionInterceptor.getVertexFromCache(guid);

        if (ret == null) {
            AtlasGraphQuery query = graph.query().has(Constants.GUID_PROPERTY_KEY, guid);

            Iterator<AtlasVertex> results = query.vertices().iterator();

            ret = results.hasNext() ? results.next() : null;

            if (ret != null) {
                GraphTransactionInterceptor.addToVertexCache(guid, ret);
            }
        }

        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    public static AtlasVertex findDeletedByGuid(AtlasGraph graph, String guid) {
        AtlasVertex ret = GraphTransactionInterceptor.getVertexFromCache(guid);

        if (ret == null) {
            AtlasGraphQuery query = graph.query()
                    .has(Constants.GUID_PROPERTY_KEY, guid)
                    .has(STATE_PROPERTY_KEY, Status.DELETED.name());

            Iterator<AtlasVertex> results = query.vertices().iterator();

            ret = results.hasNext() ? results.next() : null;

            if (ret != null) {
                GraphTransactionInterceptor.addToVertexCache(guid, ret);
            }
        }

        return ret;
    }

    public static String getTypeNameFromGuid(AtlasGraph graph, String guid) {
        String ret = null;

        if (StringUtils.isNotEmpty(guid)) {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

            ret = (vertex != null) ? AtlasGraphUtilsV2.getTypeName(vertex) : null;
        }

        return ret;
    }

    public static boolean typeHasInstanceVertex(String typeName) throws AtlasBaseException {
        return typeHasInstanceVertex(getGraphInstance(), typeName);
    }

    public static boolean typeHasInstanceVertex(AtlasGraph graph, String typeName) throws AtlasBaseException {
        AtlasGraphQuery query = graph
                .query()
                .has(TYPE_NAME_PROPERTY_KEY, AtlasGraphQuery.ComparisionOperator.EQUAL, typeName);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        boolean hasInstanceVertex = results != null && results.hasNext();

        if (LOG.isDebugEnabled()) {
            LOG.debug("typeName {} has instance vertex {}", typeName, hasInstanceVertex);
        }

        return hasInstanceVertex;
    }

    /**
     * Check if a classification type has references using Elasticsearch.
     * This method searches for entities that have the classification attached
     * either directly or through propagation.
     * 
     * @param typeName the classification type name to check
     * @return true if any active entities have this classification, false otherwise
     *
     */
    public static boolean classificationHasReferences(String typeName) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking if classification {} has references using ES", typeName);
            }

            String indexName = Constants.getESIndex();
            AtlasIndexQuery indexQuery = getGraphInstance().elasticsearchQuery(indexName);

            String esQuery = buildClassificationReferenceQuery(typeName);
            Long count = indexQuery.countIndexQuery(esQuery);

            boolean hasReferences = count != null && count > 0;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Classification {} has references: {} (count: {})", typeName, hasReferences, count);
            }

            return hasReferences;
        } catch (Exception e) {
            LOG.error("Error checking classification references for {}: {}", typeName, e.getMessage(), e);
            // Conservative approach: if ES query fails, assume there are references to prevent accidental deletion
            return true;
        }
    }

    /**
     * Build Elasticsearch query to check for classification references.
     * Searches for entities that have the classification in either __traitNames or __propagatedTraitNames.
     * 
     * @param typeName the classification type name
     * @return JSON query string
     */
    private static String buildClassificationReferenceQuery(String typeName) {
        return String.format(
            "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"filter\": [\n" +
            "        {\n" +
            "          \"bool\": {\n" +
            "            \"should\": [\n" +
            "              {\"term\": {\"%s\": \"%s\"}},\n" +
            "              {\"term\": {\"%s\": \"%s\"}}\n" +
            "            ],\n" +
            "            \"minimum_should_match\": 1\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}",
            Constants.TRAIT_NAMES_PROPERTY_KEY, typeName,
            Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, typeName
        );
    }

    public static AtlasVertex findByTypeAndUniquePropertyName(String typeName, String propertyName, Object attrVal) {
        return findByTypeAndUniquePropertyName(getGraphInstance(), typeName, propertyName, attrVal);
    }

    public static AtlasVertex findByTypeAndUniquePropertyName(AtlasGraph graph, String typeName, String propertyName, Object attrVal) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("findByTypeAndUniquePropertyName");

        AtlasGraphQuery query = graph.query()
                .has(ENTITY_TYPE_PROPERTY_KEY, typeName)
                .has(propertyName, attrVal);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        AtlasVertex vertex = results.hasNext() ? results.next() : null;

        RequestContext.get().endMetricRecord(metric);

        return vertex;
    }

    private static Map<String, Object> populateUniqueAttributesMap(Map<String, AtlasAttribute> uniqueAttributes, Map<String, Object> attrValues) {
        return populateAttributesMap(uniqueAttributes, attrValues, true);
    }

    private static Map<String, Object> populateAttributesMap(Map<String, AtlasAttribute> uniqueAttributes, Map<String, Object> attrValues) {
        return populateAttributesMap(uniqueAttributes, attrValues, false);
    }

    private static Map<String, Object> populateAttributesMap(Map<String, AtlasAttribute> uniqueAttributes, Map<String, Object> attrValues, boolean isUnique) {
        Map<String, Object> ret = new HashMap<>();

        for (AtlasAttribute attribute : uniqueAttributes.values()) {
            String attrName  = isUnique ? attribute.getVertexUniquePropertyName() : attribute.getVertexPropertyName();
            Object attrValue = attrValues.get(attribute.getName());

            if (attrName != null && attrValue != null) {
                ret.put(attrName, attrValue);
            }
        }

        return ret;
    }

    public static AtlasVertex findByTypeAndUniquePropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues) {
        return findByTypeAndUniquePropertyName(graph, typeName, attributeValues, false);
    }

    public static AtlasVertex findBySuperTypeAndUniquePropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues) {
        return findByTypeAndUniquePropertyName(graph, typeName, attributeValues, true);
    }

    public static AtlasVertex findByTypeAndUniquePropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues, boolean isSuperType) {
        String          metricName      = isSuperType ? "findBySuperTypeAndUniquePropertyName" : "findByTypeAndUniquePropertyName";
        MetricRecorder  metric          = RequestContext.get().startMetricRecord(metricName);
        String          typePropertyKey = isSuperType ? SUPER_TYPES_PROPERTY_KEY : ENTITY_TYPE_PROPERTY_KEY;
        AtlasGraphQuery query           = graph.query().has(typePropertyKey, typeName);
        String uniqueQualifiedName = "";

        for (Map.Entry<String, Object> entry : attributeValues.entrySet()) {
            String attrName  = entry.getKey();
            Object attrValue = entry.getValue();

            if (attrName != null && attrValue != null) {
                query.has(attrName, attrValue);
                if (UNIQUE_QUALIFIED_NAME.equals(attrName)){
                    uniqueQualifiedName =  (String) attrValue;
                }
            }
        }

        Iterator<AtlasVertex> results = query.vertices().iterator();
        AtlasVertex           vertex  = results.hasNext() ? results.next() : null;

        RequestContext.get().endMetricRecord(metric);

        return logIfVertexIndexIsCorrupt(vertex, typeName, uniqueQualifiedName);
    }

    private static AtlasVertex logIfVertexIndexIsCorrupt(AtlasVertex vertex, String typeName, String uniqueQualifiedName) {
        if (vertex == null) {
            return vertex;
        }

        if (vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class) != null &&
                vertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class) != null &&
                vertex.getProperty(UNIQUE_QUALIFIED_NAME, String.class) != null) {
            return vertex;

        }

        // definitely a corrupted vertex
        if (vertex.getPropertyKeys().isEmpty() && StringUtils.isNotEmpty(uniqueQualifiedName)) {
            try (MDCScope corruptedVertexDetails = new MDCScope(Map.of("id", vertex.getIdForDisplay(), ATTRIBUTE_NAME_TYPENAME, typeName, QUALIFIED_NAME, uniqueQualifiedName))) {
                LOG.warn("findByTypeAndUniquePropertyName: vertex is corrupted {}::{}::{} ", vertex.getIdForDisplay(), typeName, uniqueQualifiedName);
            }
        }

        return vertex;
    }

    public static AtlasVertex glossaryFindByTypeAndPropertyName(AtlasEntityType entityType, String name) {
        Iterator<AtlasVertex> result = glossaryFindAllByTypeAndPropertyName(entityType, name);
        AtlasVertex           vertex = result.hasNext() ? result.next() : null;

        return vertex;
    }

    public static List<AtlasVertex> glossaryFindChildByTypeAndPropertyName(AtlasEntityType entityType, String name, String glossaryQName) {
        MetricRecorder  metric          = RequestContext.get().startMetricRecord("glossaryFindChildByTypeAndPropertyName");
        Iterator<AtlasVertex> result = glossaryFindAllByTypeAndPropertyName(entityType, name);
        String qNameKey = entityType.getAllAttributes().get("qualifiedName").getQualifiedName();

        List<AtlasVertex> vertexList = new ArrayList<>();
        while (result.hasNext()) {
            AtlasVertex v = result.next();
            String vQualifiedName = v.getProperty(qNameKey, String.class);
            if (vQualifiedName.endsWith(glossaryQName)) {
                vertexList.add(v);
            }
        }
        RequestContext.get().endMetricRecord(metric);
        return vertexList;
    }

    public static Iterator<AtlasVertex> glossaryFindAllByTypeAndPropertyName(AtlasEntityType entityType, String name) {
        MetricRecorder  metric          = RequestContext.get().startMetricRecord("glossaryFindAllByTypeAndPropertyName");
        AtlasGraph graph                = getGraphInstance();
        AtlasGraphQuery query           = graph.query()
                                                .has(ENTITY_TYPE_PROPERTY_KEY, entityType.getTypeName())
                                                .has(STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name())
                                                .has(entityType.getAllAttributes().get("name").getQualifiedName(), name);


        Iterator<AtlasVertex> result = query.vertices().iterator();

        RequestContext.get().endMetricRecord(metric);
        return result;
    }

    public static boolean glossaryExists(String name) {
        MetricRecorder  metric          = RequestContext.get().startMetricRecord("AtlasGraphUtilsV2.glossaryExists");
        boolean ret = false;
        AtlasGraph graph      = getGraphInstance();
        AtlasGraphQuery query = graph.query()
                .has(ENTITY_TYPE_PROPERTY_KEY, ATLAS_GLOSSARY_ENTITY_TYPE);

        Iterator<AtlasVertex> result = query.vertices().iterator();

        while (result.hasNext()) {
            AtlasVertex glossaryVertex = result.next();
            if (getState(glossaryVertex) == Status.ACTIVE) {
                String existingGlossaryName = glossaryVertex.getProperty(NAME, String.class);
                if (name.equals(existingGlossaryName)) {
                    ret = true;
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    public static boolean termExists(String name, String glossaryQName) {
        MetricRecorder  metric = RequestContext.get().startMetricRecord("AtlasGraphUtilsV2.termExists");

        AtlasGraphQuery query = getGraphInstance().query()
                .has(ENTITY_TYPE_PROPERTY_KEY, ATLAS_GLOSSARY_ENTITY_TYPE)
                .has(QUALIFIED_NAME, glossaryQName);

        Iterator<AtlasVertex> results         = query.vertices().iterator();
        AtlasVertex           glossaryVertex  = results.hasNext() ? results.next() : null;

        if (glossaryVertex != null) {
            Iterator<AtlasEdge> termEdges = glossaryVertex.getEdges(AtlasEdgeDirection.OUT, GLOSSARY_TERMS_EDGE_LABEL).iterator();

            while (termEdges.hasNext()) {
                AtlasVertex term = termEdges.next().getInVertex();
                if (getState(term) == Status.ACTIVE) {
                    String termName = term.getProperty(NAME, String.class);
                    if (name.equals(termName)) {
                        return true;
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);
        return false;
    }

    public static AtlasVertex findByTypeAndPropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues) {
        return findByTypeAndPropertyName(graph, typeName, attributeValues, false);
    }

    public static AtlasVertex findBySuperTypeAndPropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues) {
        return findByTypeAndPropertyName(graph, typeName, attributeValues, true);
    }

    public static AtlasVertex findByTypeAndPropertyName(AtlasGraph graph, String typeName, Map<String, Object> attributeValues, boolean isSuperType) {
        String          metricName      = isSuperType ? "findBySuperTypeAndPropertyName" : "findByTypeAndPropertyName";
        MetricRecorder  metric          = RequestContext.get().startMetricRecord(metricName);
        String          typePropertyKey = isSuperType ? SUPER_TYPES_PROPERTY_KEY : ENTITY_TYPE_PROPERTY_KEY;
        AtlasGraphQuery query           = graph.query()
                                               .has(typePropertyKey, typeName)
                                               .has(STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());

        for (Map.Entry<String, Object> entry : attributeValues.entrySet()) {
            String attrName  = entry.getKey();
            Object attrValue = entry.getValue();

            if (attrName != null && attrValue != null) {
                query.has(attrName, attrValue);
            }
        }

        Iterator<AtlasVertex> results = query.vertices().iterator();
        AtlasVertex           vertex  = results.hasNext() ? results.next() : null;

        RequestContext.get().endMetricRecord(metric);

        return vertex;
    }

    public static List<String> findEntityGUIDsByType(String typename, SortOrder sortOrder) {
        return findEntityGUIDsByType(getGraphInstance(), typename, sortOrder);
    }

    public static List<String> findEntityGUIDsByType(AtlasGraph graph, String typename, SortOrder sortOrder) {
        AtlasGraphQuery query = graph.query()
                .has(ENTITY_TYPE_PROPERTY_KEY, typename);
        if (sortOrder != null) {
            AtlasGraphQuery.SortOrder qrySortOrder = sortOrder == SortOrder.ASCENDING ? ASC : DESC;
            query.orderBy(Constants.QUALIFIED_NAME, qrySortOrder);
        }

        Iterator<AtlasVertex> results = query.vertices().iterator();
        ArrayList<String> ret = new ArrayList<>();

        if (!results.hasNext()) {
            return Collections.emptyList();
        }

        while (results.hasNext()) {
            ret.add(getIdFromVertex(results.next()));
        }

        return ret;
    }

    public static List<String> findEntityGUIDsByType(AtlasGraph graph, String typename) {
        return findEntityGUIDsByType(graph, typename, null);
    }

    public static Iterator<AtlasVertex> findActiveEntityVerticesByType(AtlasGraph graph, String typename) {
        AtlasGraphQuery query = graph.query()
                .has(ENTITY_TYPE_PROPERTY_KEY, typename)
                .has(STATE_PROPERTY_KEY, Status.ACTIVE.name());

        return query.vertices().iterator();
    }

    public static boolean relationshipTypeHasInstanceEdges(String typeName) throws AtlasBaseException {
        return relationshipTypeHasInstanceEdges(getGraphInstance(), typeName);
    }

    public static boolean relationshipTypeHasInstanceEdges(AtlasGraph graph, String typeName) throws AtlasBaseException {
        AtlasGraphQuery query = graph
                .query()
                .has(TYPE_NAME_PROPERTY_KEY, AtlasGraphQuery.ComparisionOperator.EQUAL, typeName);

        Iterator<AtlasEdge> results = query.edges().iterator();

        boolean hasInstanceEdges = results != null && results.hasNext();

        if (LOG.isDebugEnabled()) {
            LOG.debug("relationshipType {} has instance edges {}", typeName, hasInstanceEdges);
        }

        return hasInstanceEdges;
    }

    private static String toString(AtlasElement element) {
        if (element instanceof AtlasVertex) {
            return toString((AtlasVertex) element);
        } else if (element instanceof AtlasEdge) {
            return toString((AtlasEdge) element);
        }

        return element.toString();
    }

    public static String toString(AtlasVertex vertex) {
        if (vertex == null) {
            return "vertex[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getVertexDetails(vertex);
            } else {
                return String.format("vertex[id=%s]", vertex.getId().toString());
            }
        }
    }


    public static String toString(AtlasEdge edge) {
        if (edge == null) {
            return "edge[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getEdgeDetails(edge);
            } else {
                return String.format("edge[id=%s]", edge.getId().toString());
            }
        }
    }

    public static String getVertexDetails(AtlasVertex vertex) {
        return String.format("vertex[id=%s type=%s guid=%s]",
                vertex.getId().toString(), getTypeName(vertex), getIdFromVertex(vertex));
    }

    public static String getEdgeDetails(AtlasEdge edge) {
        return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getId(), edge.getLabel(),
                toString(edge.getOutVertex()), toString(edge.getInVertex()));
    }

    public static AtlasEntity.Status getState(AtlasElement element) {
        String state = getStateAsString(element);
        return state == null ? null : AtlasEntity.Status.valueOf(state);
    }

    public static String getStateAsString(AtlasElement element) {
        return element.getProperty(STATE_PROPERTY_KEY, String.class);
    }

    private static AtlasIndexQuery getIndexQuery(AtlasGraph graph, AtlasEntityType entityType, String propertyName, String value) {
        StringBuilder sb = new StringBuilder();

        sb.append(INDEX_SEARCH_PREFIX + "\"").append(TYPE_NAME_PROPERTY_KEY).append("\":").append(entityType.getTypeAndAllSubTypesQryStr())
                .append(" AND ")
                .append(INDEX_SEARCH_PREFIX + "\"").append(propertyName).append("\":").append(AtlasAttribute.escapeIndexQueryValue(value))
                .append(" AND ")
                .append(INDEX_SEARCH_PREFIX + "\"").append(STATE_PROPERTY_KEY).append("\":ACTIVE");

        return graph.indexQuery(Constants.VERTEX_INDEX, sb.toString());
    }

    public static String getIndexSearchPrefix() {
        return INDEX_SEARCH_PREFIX;
    }

    public static List<String> getClassificationNames(AtlasVertex entityVertex) {
        return getClassificationNamesHelper(entityVertex, CLASSIFICATION_NAMES_KEY);
    }

    public static List<String> getPropagatedClassificationNames(AtlasVertex entityVertex) {
        return getClassificationNamesHelper(entityVertex, PROPAGATED_CLASSIFICATION_NAMES_KEY);
    }

    private static List<String> getClassificationNamesHelper(AtlasVertex entityVertex, String propertyKey) {
        List<String> classificationNames = null;
        String classificationNamesString = entityVertex.getProperty(propertyKey, String.class);
        if (StringUtils.isNotEmpty(classificationNamesString)) {
            classificationNames = Arrays.asList(StringUtils.split(classificationNamesString, "\\|"));
        }
        return classificationNames;
    }

    public static List<Date> dateParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Date> ret = new ArrayList();
        for (String s : arr) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
                Date date = formatter.parse(s);
                ret.add(date);
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Date type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is not of Date type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Boolean> booleanParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Boolean> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Boolean.parseBoolean(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Boolean type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is not of Boolean type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Double> doubleParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Double> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Double.parseDouble(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Double type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is not of Double type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Short> shortParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Short> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Short.parseShort(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Short type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is not of Short type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Long> longParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Long> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Long.parseLong(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Long type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is not of Long type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Integer> intParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Integer> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Integer.parseInt(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is not of Integer type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is Integer of Long type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List<Float> floatParser(String[] arr, List failedTermMsgList, int lineIndex) {

        List<Float> ret = new ArrayList();
        for (String s : arr) {
            try {
                ret.add(Float.parseFloat(s));
            } catch (Exception e) {
                LOG.error("Provided value " + s + " is Float of Long type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is Float of Long type at line #" + lineIndex);
            }
        }
        return ret;
    }

    public static List assignEnumValues(String bmAttributeValues, AtlasEnumType enumType, List<String> failedTermMsgList, int lineIndex) {
        List<String> ret = new ArrayList<>();
        String[] arr = bmAttributeValues.split(FileUtils.ESCAPE_CHARACTER + FileUtils.PIPE_CHARACTER);
        AtlasEnumDef.AtlasEnumElementDef atlasEnumDef;
        for (String s : arr) {
            atlasEnumDef = enumType.getEnumElementDef(s);
            if (atlasEnumDef == null) {
                LOG.error("Provided value " + s + " is Enumeration of Long type at line #" + lineIndex);
                failedTermMsgList.add("Provided value " + s + " is Enumeration of Long type at line #" + lineIndex);
            } else {
                ret.add(s);
            }
        }
        return ret;
    }

    public static void addItemToListProperty(AtlasElement element, String property, String value) {
        List list = getListFromProperty(element, property);

        list.add(value);

        element.setListProperty(property, list);
    }

    public static void removeItemFromListProperty(AtlasElement element, String property, String value) {
        List list = getListFromProperty(element, property);

        list.remove(value);

        if (CollectionUtils.isEmpty(list)) {
            element.removeProperty(property);
        } else {
            element.setListProperty(property, list);
        }
    }

    public static void removeItemFromListPropertyValue(AtlasElement element, String property, String value) {
        element.removePropertyValue(property, value);
    }

    private static List getListFromProperty(AtlasElement element, String property) {
        List list = element.getListProperty(property);

        return CollectionUtils.isEmpty(list) ? new ArrayList() : list;
    }
}
