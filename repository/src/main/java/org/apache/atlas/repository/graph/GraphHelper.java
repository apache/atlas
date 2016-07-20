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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Utility class for graph operations.
 */
public final class GraphHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphHelper.class);
    public static final String EDGE_LABEL_PREFIX = "__";

    private static final TypeSystem typeSystem = TypeSystem.getInstance();

    private static final GraphHelper INSTANCE = new GraphHelper(TitanGraphProvider.getGraphInstance());

    private TitanGraph titanGraph;

    private GraphHelper(TitanGraph titanGraph) {
        this.titanGraph = titanGraph;
    }

    public static GraphHelper getInstance() {
        return INSTANCE;
    }

    public Vertex createVertexWithIdentity(ITypedReferenceableInstance typedInstance, Set<String> superTypeNames) {
        final String guid = UUID.randomUUID().toString();

        final Vertex vertexWithIdentity = createVertexWithoutIdentity(typedInstance.getTypeName(),
                new Id(guid, 0, typedInstance.getTypeName()), superTypeNames);

        // add identity
        setProperty(vertexWithIdentity, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        setProperty(vertexWithIdentity, Constants.VERSION_PROPERTY_KEY, typedInstance.getId().version);

        return vertexWithIdentity;
    }

    public Vertex createVertexWithoutIdentity(String typeName, Id typedInstanceId, Set<String> superTypeNames) {
        LOG.debug("Creating vertex for type {} id {}", typeName,
                typedInstanceId != null ? typedInstanceId._getId() : null);
        final Vertex vertexWithoutIdentity = titanGraph.addVertex(null);

        // add type information
        setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);


        // add super types
        for (String superTypeName : superTypeNames) {
            addProperty(vertexWithoutIdentity, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        // add state information
        setProperty(vertexWithoutIdentity, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

        // add timestamp information
        setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());

        return vertexWithoutIdentity;
    }

    private Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> label {} -> {}", string(fromVertex), edgeLabel, string(toVertex));
        Edge edge = titanGraph.addEdge(null, fromVertex, toVertex, edgeLabel);

        setProperty(edge, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        setProperty(edge, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(edge, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());

        LOG.debug("Added {}", string(edge));
        return edge;
    }

    public Edge getOrCreateEdge(Vertex outVertex, Vertex inVertex, String edgeLabel) {
        Iterable<Edge> edges = inVertex.getEdges(Direction.IN, edgeLabel);
        for (Edge edge : edges) {
            if (edge.getVertex(Direction.OUT).getId().toString().equals(outVertex.getId().toString())) {
                Id.EntityState edgeState = getState(edge);
                if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                    return edge;
                }
            }
        }
        return addEdge(outVertex, inVertex, edgeLabel);
    }


    public Edge getEdgeByEdgeId(Vertex outVertex, String edgeLabel, String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return titanGraph.getEdge(edgeId);

        //TODO get edge id is expensive. Use this logic. But doesn't work for now
        /**
        Iterable<Edge> edges = outVertex.getEdges(Direction.OUT, edgeLabel);
        for (Edge edge : edges) {
            if (edge.getId().toString().equals(edgeId)) {
                return edge;
            }
        }
        return null;
         **/
    }

    /**
     * Args of the format prop1, key1, prop2, key2...
     * Searches for a vertex with prop1=key1 && prop2=key2
     * @param args
     * @return vertex with the given property keys
     * @throws EntityNotFoundException
     */
    public Vertex findVertex(Object... args) throws EntityNotFoundException {
        StringBuilder condition = new StringBuilder();
        GraphQuery query = titanGraph.query();
        for (int i = 0 ; i < args.length; i+=2) {
            query = query.has((String) args[i], args[i+1]);
            condition.append(args[i]).append(" = ").append(args[i+1]).append(", ");
        }
        String conditionStr = condition.toString();
        LOG.debug("Finding vertex with {}", conditionStr);

        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since entityType, qualifiedName should be unique
        Vertex vertex = results.hasNext() ? results.next() : null;

        if (vertex == null) {
            LOG.debug("Could not find a vertex with {}", condition.toString());
            throw new EntityNotFoundException("Could not find an entity in the repository with " + conditionStr);
        } else {
            LOG.debug("Found a vertex {} with {}", string(vertex), conditionStr);
        }

        return vertex;
    }

    public static Iterator<Edge> getOutGoingEdgesByLabel(Vertex instanceVertex, String edgeLabel) {
        LOG.debug("Finding edges for {} with label {}", string(instanceVertex), edgeLabel);
        if(instanceVertex != null && edgeLabel != null) {
            return instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator();
        }
        return null;
    }

    /**
     * Returns the active edge for the given edge label.
     * If the vertex is deleted and there is no active edge, it returns the latest deleted edge
     * @param vertex
     * @param edgeLabel
     * @return
     */
    public static Edge getEdgeForLabel(Vertex vertex, String edgeLabel) {
        Iterator<Edge> iterator = GraphHelper.getOutGoingEdgesByLabel(vertex, edgeLabel);
        Edge latestDeletedEdge = null;
        long latestDeletedEdgeTime = Long.MIN_VALUE;

        while (iterator != null && iterator.hasNext()) {
            Edge edge = iterator.next();
            Id.EntityState edgeState = getState(edge);
            if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                LOG.debug("Found {}", string(edge));
                return edge;
            } else {
                Long modificationTime = getProperty(edge, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
                if (modificationTime != null && modificationTime >= latestDeletedEdgeTime) {
                    latestDeletedEdgeTime = modificationTime;
                    latestDeletedEdge = edge;
                }
            }
        }

        //If the vertex is deleted, return latest deleted edge
        LOG.debug("Found {}", latestDeletedEdge == null ? "null" : string(latestDeletedEdge));
        return latestDeletedEdge;
    }

    public static String vertexString(final Vertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            properties.append(propertyKey).append("=").append(vertex.getProperty(propertyKey).toString()).append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], [" + edge.getVertex(Direction.OUT) + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN) + "]";
    }

    public static <T extends Element> void setProperty(T element, String propertyName, Object value) {
        String elementStr = string(element);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Setting property {} = \"{}\" to {}", actualPropertyName, value, elementStr);
        Object existValue = element.getProperty(actualPropertyName);
        if(value == null || (value instanceof Collection && ((Collection) value).isEmpty())) {
            if(existValue != null) {
                LOG.info("Removing property - {} value from {}", actualPropertyName, elementStr);
                element.removeProperty(actualPropertyName);
            }
        } else {
            if (!value.equals(existValue)) {
                element.setProperty(actualPropertyName, value);
                LOG.debug("Set property {} = \"{}\" to {}", actualPropertyName, value, elementStr);
            }
        }
    }

    public static <T extends Element, O> O getProperty(T element, String propertyName) {
        String elementStr = string(element);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Reading property {} from {}", actualPropertyName, elementStr);
        return element.getProperty(actualPropertyName);
    }

    public static Iterable<TitanProperty> getProperties(TitanVertex vertex, String propertyName) {
        String elementStr = string(vertex);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Reading property {} from {}", actualPropertyName, elementStr);
        return vertex.getProperties(actualPropertyName);
    }

    private static <T extends Element> String string(T element) {
        if (element instanceof Vertex) {
            return string((Vertex) element);
        } else if (element instanceof Edge) {
            return string((Edge)element);
        }
        return element.toString();
    }

    public static void addProperty(Vertex vertex, String propertyName, Object value) {
        LOG.debug("Adding property {} = \"{}\" to vertex {}", propertyName, value, string(vertex));
        ((TitanVertex)vertex).addProperty(propertyName, value);
    }

    /**
     * Remove the specified edge from the graph.
     * 
     * @param edge
     */
    public void removeEdge(Edge edge) {
        String edgeString = string(edge);
        LOG.debug("Removing {}", edgeString);
        titanGraph.removeEdge(edge);
        LOG.info("Removed {}", edgeString);
    }
    
    /**
     * Remove the specified vertex from the graph.
     * 
     * @param vertex
     */
    public void removeVertex(Vertex vertex) {
        String vertexString = string(vertex);
        LOG.debug("Removing {}", vertexString);
        titanGraph.removeVertex(vertex);
        LOG.info("Removed {}", vertexString);
    }

    public Vertex getVertexForGUID(String guid) throws EntityNotFoundException {
        return findVertex(Constants.GUID_PROPERTY_KEY, guid);
    }

    public static String getQualifiedNameForMapKey(String prefix, String key) {
        return prefix + "." + key;
    }

    public static String getQualifiedFieldName(ITypedInstance typedInstance, AttributeInfo attributeInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getQualifiedFieldName(dataType, attributeInfo.name);
    }

    public static String getQualifiedFieldName(IDataType dataType, String attributeName) throws AtlasException {
        return dataType.getTypeCategory() == DataTypes.TypeCategory.STRUCT ? dataType.getName() + "." + attributeName
            // else class or trait
            : ((HierarchicalType) dataType).getQualifiedName(attributeName);
    }

    public static String getTraitLabel(String typeName, String attrName) {
        return typeName + "." + attrName;
    }

    public static List<String> getTraitNames(Vertex entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        for (TitanProperty property : ((TitanVertex) entityVertex).getProperties(Constants.TRAIT_NAMES_PROPERTY_KEY)) {
            traits.add((String) property.getValue());
        }

        return traits;
    }

    public static String getEdgeLabel(ITypedInstance typedInstance, AttributeInfo aInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getEdgeLabel(dataType, aInfo);
    }

    public static String getEdgeLabel(IDataType dataType, AttributeInfo aInfo) throws AtlasException {
        return GraphHelper.EDGE_LABEL_PREFIX + getQualifiedFieldName(dataType, aInfo.name);
    }

    public static Id getIdFromVertex(String dataTypeName, Vertex vertex) {
        return new Id(getIdFromVertex(vertex),
            vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), dataTypeName, getStateAsString(vertex));
    }

    public static String getIdFromVertex(Vertex vertex) {
        return vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY);
    }

    public static String getTypeName(Vertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    public static Id.EntityState getState(Element element) {
        String state = getStateAsString(element);
        return state == null ? null : Id.EntityState.valueOf(state);
    }

    public static String getStateAsString(Element element) {
        return element.getProperty(Constants.STATE_PROPERTY_KEY);
    }

    /**
     * For the given type, finds an unique attribute and checks if there is an existing instance with the same
     * unique value
     *
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    public Vertex getVertexForInstanceByUniqueAttribute(ClassType classType, IReferenceableInstance instance)
        throws AtlasException {
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        Vertex result = null;
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    result = findVertex(propertyKey, instance.get(attributeInfo.name),
                            Constants.ENTITY_TYPE_PROPERTY_KEY, classType.getName(),
                            Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
                    LOG.debug("Found vertex by unique attribute : " + propertyKey + "=" + instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }

        return result;
    }

    public static void dumpToLog(final Graph graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (Vertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (Edge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }

    public static String string(ITypedReferenceableInstance instance) {
        return String.format("entity[type=%s guid=%]", instance.getTypeName(), instance.getId()._getId());
    }

    public static String string(Vertex vertex) {
        if(vertex == null) {
            return "vertex[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return String.format("vertex[id=%s type=%s guid=%s]", vertex.getId().toString(), getTypeName(vertex),
                        getIdFromVertex(vertex));
            } else {
                return String.format("vertex[id=%s]", vertex.getId().toString());
            }
        }
    }

    public static String string(Edge edge) {
        if(edge == null) {
            return "edge[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getId().toString(), edge.getLabel(),
                        string(edge.getVertex(Direction.OUT)), string(edge.getVertex(Direction.IN)));
            } else {
                return String.format("edge[id=%s]", edge.getId().toString());
            }
        }
    }

    @VisibleForTesting
    //Keys copied from com.thinkaurelius.titan.graphdb.types.StandardRelationTypeMaker
    //Titan checks that these chars are not part of any keys. So, encoding...
    public static BiMap<String, String> RESERVED_CHARS_ENCODE_MAP =
            HashBiMap.create(new HashMap<String, String>() {{
                put("{", "_o");
                put("}", "_c");
                put("\"", "_q");
                put("$", "_d");
                put("%", "_p");
            }});


    public static String encodePropertyKey(String key) {
        if (StringUtils.isBlank(key)) {
            return key;
        }

        for (String str : RESERVED_CHARS_ENCODE_MAP.keySet()) {
            key = key.replace(str, RESERVED_CHARS_ENCODE_MAP.get(str));
        }
        return key;
    }

    public static String decodePropertyKey(String key) {
        if (StringUtils.isBlank(key)) {
            return key;
        }

        for (String encodedStr : RESERVED_CHARS_ENCODE_MAP.values()) {
            key = key.replace(encodedStr, RESERVED_CHARS_ENCODE_MAP.inverse().get(encodedStr));
        }
        return key;
    }
}