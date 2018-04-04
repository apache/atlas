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

package org.apache.atlas.repository.graphdb.janus.migration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.repository.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_EDGE_STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_GUID;
import static org.apache.atlas.repository.Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY;

class GraphSONUtility {
    private static final Logger LOG = LoggerFactory.getLogger(GraphSONUtility.class);

    private static final String EMPTY_STRING = "";

    private final RelationshipTypeCache relationshipTypeCache;

    public GraphSONUtility(final RelationshipTypeCache relationshipTypeCache) {
        this.relationshipTypeCache = relationshipTypeCache;
    }

    public Map<String, Object> vertexFromJson(Graph g, final JsonNode json) {
        final Map<String, Object> props = readProperties(json);

        if (props.containsKey(Constants.TYPENAME_PROPERTY_KEY)) {
            return null;
        }

        Map<String, Object> schemaUpdate   = null;
        VertexFeatures      vertexFeatures = g.features().vertex();
        Object              vertexId       = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));
        Vertex              vertex         = vertexFeatures.willAllowId(vertexId) ? g.addVertex(T.id, vertexId) : g.addVertex();

        props.put(Constants.VERTEX_ID_IN_IMPORT_KEY, vertexId);

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            try {
                vertex.property(vertexFeatures.getCardinality(entry.getKey()), entry.getKey(), entry.getValue());
            } catch (IllegalArgumentException ex) {
                schemaUpdate = getSchemaUpdateMap(schemaUpdate);

                if (!schemaUpdate.containsKey("id")) {
                    schemaUpdate.put("id", vertex.id());
                }

                schemaUpdate.put(entry.getKey(), entry.getValue());
            }
        }

        return schemaUpdate;
    }

    public Map<String, Object> edgeFromJson(Graph g, MappedElementCache cache, final JsonNode json) {
        final JsonNode nodeLabel = json.get(GraphSONTokensTP2._LABEL);
              String   label     = nodeLabel == null ? EMPTY_STRING : nodeLabel.textValue();

        if (label.startsWith("__type.")) {
            return null;
        }

        Map<String, Object> schemaUpdate = null;
        Object              edgeId       = null;

        try {
            final Vertex in  = getMappedVertex(g, cache, json, GraphSONTokensTP2._IN_V);
            final Vertex out = getMappedVertex(g, cache, json, GraphSONTokensTP2._OUT_V);

            if (in == null || out == null) {
                return null;
            }

            edgeId = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));

            final Map<String, Object> props = GraphSONUtility.readProperties(json);

            props.put(Constants.EDGE_ID_IN_IMPORT_KEY, edgeId.toString());

            if(addRelationshipTypeForClassification(in, out, label, props)) {
                label = Constants.CLASSIFICATION_LABEL;
            } else {
                addRelationshipTypeName(label, props);
            }

            EdgeFeatures  edgeFeatures = g.features().edge();
            final Edge    edge         = edgeFeatures.willAllowId(edgeId) ? out.addEdge(label, in, T.id, edgeId) : out.addEdge(label, in);

            addMandatoryRelationshipProperties(props);

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                try {
                    edge.property(entry.getKey(), entry.getValue());
                } catch (IllegalArgumentException ex) {
                    schemaUpdate = getSchemaUpdateMap(schemaUpdate);

                    if (!schemaUpdate.containsKey("id")) {
                        schemaUpdate.put("id", edge.id());
                    }

                    schemaUpdate.put(entry.getKey(), entry.getValue());
                }
            }
        } catch(IllegalArgumentException ex) {
            schemaUpdate = getSchemaUpdateMap(schemaUpdate);
            schemaUpdate.put("oid", edgeId);
        }

        return schemaUpdate;
    }

    private Map<String, Object> getSchemaUpdateMap(Map<String, Object> schemaUpdate) {
        if(schemaUpdate == null) {
            schemaUpdate = new HashMap<>();
        }

        return schemaUpdate;
    }

    private Vertex getMappedVertex(Graph gr, MappedElementCache cache, JsonNode json, String direction) {
        Object inVId = GraphSONUtility.getTypedValueFromJsonNode(json.get(direction));

        return cache.getMappedVertex(gr, inVId);
    }

    private boolean addRelationshipTypeForClassification(Vertex in, Vertex out, String label, Map<String, Object> props) {
        if (in.property(Constants.ENTITY_TYPE_PROPERTY_KEY).isPresent()) {
            String inTypeName  = (String) in.property(Constants.ENTITY_TYPE_PROPERTY_KEY).value();

            if (inTypeName.equals(label)) {
                if (StringUtils.isNotEmpty(inTypeName)) {
                    props.put(Constants.ENTITY_TYPE_PROPERTY_KEY, inTypeName);

                    addEntityGuidToTrait(in, out);

                    return true;
                } else {
                    LOG.info("Could not find typeName for trait: {}", label);
                }
            }
        }

        return false;
    }

    private void addEntityGuidToTrait(Vertex in, Vertex out) {
        String entityGuid = "";
        if (out.property(Constants.GUID_PROPERTY_KEY).isPresent()) {
            entityGuid = (String) out.property(Constants.GUID_PROPERTY_KEY).value();
        }

        if(StringUtils.isNotEmpty(entityGuid)) {
            in.property(CLASSIFICATION_ENTITY_GUID, entityGuid);
        }
    }

    private void addRelationshipTypeName(String edgeLabel, Map<String, Object> props) {
        String typeName = relationshipTypeCache.get(edgeLabel);

        if (StringUtils.isNotEmpty(typeName)) {
            props.put(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);
        } else {
            LOG.debug("Could not find relationship type for: {}", edgeLabel);
        }
    }

    private void addMandatoryRelationshipProperties(Map<String, Object> props) {
        props.put(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, UUID.randomUUID().toString());
        props.put(RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, "NONE");
        props.put(CLASSIFICATION_EDGE_STATE_PROPERTY_KEY, "ACTIVE");
    }

    public void replaceReferencedEdgeIdForList(Graph g, MappedElementCache cache, Vertex v, String propertyName) {
        try {
            if (v.property(Constants.TYPENAME_PROPERTY_KEY).isPresent() || !v.property(propertyName).isPresent()) {
                return;
            }

            List list = (List) v.property(propertyName).value();
            for (int i = 0; i < list.size(); i++) {
                String id    = list.get(i).toString();
                Object newId = cache.getMappedEdge(g, id);

                if (newId == null) {
                    continue;
                }

                list.set(i, newId.toString());
            }

            v.property(propertyName, list);
        } catch (IllegalArgumentException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("processItem: IllegalArgumentException: v[{}] error!", v.id(), ex);
            }
        }
    }

    static Map<String, Object> readProperties(final JsonNode node) {
        final Map<String, Object>                   map      = new HashMap<>();
        final Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

        while (iterator.hasNext()) {
            final Map.Entry<String, JsonNode> entry = iterator.next();

            if (!isReservedKey(entry.getKey())) {
                // it generally shouldn't be as such but graphson containing null values can't be shoved into
                // element property keys or it will result in error
                final Object o = readProperty(entry.getValue());

                if (o != null) {
                    map.put(entry.getKey(), o);
                }
            }
        }

        return map;
    }

    private static boolean isReservedKey(final String key) {
        return key.equals(GraphSONTokensTP2._ID) || key.equals(GraphSONTokensTP2._TYPE) || key.equals(GraphSONTokensTP2._LABEL)
                || key.equals(GraphSONTokensTP2._OUT_V) || key.equals(GraphSONTokensTP2._IN_V);
    }

    private static Object readProperty(final JsonNode node) {
        final Object propertyValue;

        if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_UNKNOWN)) {
            propertyValue = null;
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BOOLEAN)) {
            propertyValue = node.get(GraphSONTokensTP2.VALUE).booleanValue();
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_FLOAT)) {
            propertyValue = Float.parseFloat(node.get(GraphSONTokensTP2.VALUE).asText());
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BYTE)) {
            propertyValue = Byte.parseByte(node.get(GraphSONTokensTP2.VALUE).asText());
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_SHORT)) {
            propertyValue = Short.parseShort(node.get(GraphSONTokensTP2.VALUE).asText());
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_DOUBLE)) {
            propertyValue = node.get(GraphSONTokensTP2.VALUE).doubleValue();
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_INTEGER)) {
            propertyValue = node.get(GraphSONTokensTP2.VALUE).intValue();
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LONG)) {
            propertyValue = node.get(GraphSONTokensTP2.VALUE).longValue();
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_STRING)) {
            propertyValue = node.get(GraphSONTokensTP2.VALUE).textValue();
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LIST)) {
            propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE).elements());
        } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_MAP)) {
            propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE));
        } else {
            propertyValue = node.textValue();
        }

        return propertyValue;
    }

    private static List readProperties(final Iterator<JsonNode> listOfNodes) {
        final List<Object> array = new ArrayList<>();

        while (listOfNodes.hasNext()) {
            array.add(readProperty(listOfNodes.next()));
        }

        return array;
    }

    @VisibleForTesting
    static Object getTypedValueFromJsonNode(final JsonNode node) {
        Object theValue = null;

        if (node != null && !node.isNull()) {
            if (node.isBoolean()) {
                theValue = node.booleanValue();
            } else if (node.isDouble()) {
                theValue = node.doubleValue();
            } else if (node.isFloatingPointNumber()) {
                theValue = node.floatValue();
            } else if (node.isInt()) {
                theValue = node.intValue();
            } else if (node.isLong()) {
                theValue = node.longValue();
            } else if (node.isTextual()) {
                theValue = node.textValue();
            } else if (node.isArray()) {
                // this is an array so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else if (node.isObject()) {
                // this is an object so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else {
                theValue = node.textValue();
            }
        }

        return theValue;
    }
}
