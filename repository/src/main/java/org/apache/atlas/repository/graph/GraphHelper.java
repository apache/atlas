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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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

    public Vertex createVertexWithIdentity(ITypedReferenceableInstance typedInstance,
                                                  Set<String> superTypeNames) {
        final Vertex vertexWithIdentity = createVertexWithoutIdentity(typedInstance.getTypeName(),
                typedInstance.getId(), superTypeNames);

        // add identity
        final String guid = UUID.randomUUID().toString();
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

        // add timestamp information
        setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());

        return vertexWithoutIdentity;
    }

    public Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> label {} -> {}", fromVertex, edgeLabel, toVertex);

        return titanGraph.addEdge(null, fromVertex, toVertex, edgeLabel);
    }

    public Vertex findVertex(String propertyKey, Object value) {
        LOG.debug("Finding vertex for {}={}", propertyKey, value);

        GraphQuery query = titanGraph.query().has(propertyKey, value);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since entityType, qualifiedName should be unique
        return results.hasNext() ? results.next() : null;
    }

    public static Iterable<Edge> getOutGoingEdgesByLabel(Vertex instanceVertex, String edgeLabel) {
        if(instanceVertex != null && edgeLabel != null) {
            return instanceVertex.getEdges(Direction.OUT, edgeLabel);
        }
        return null;
    }

    public Edge getOutGoingEdgeById(String edgeId) {
        if(edgeId != null) {
            return titanGraph.getEdge(edgeId);
        }
        return null;
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

    public static void setProperty(Vertex vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        Object existValue = vertex.getProperty(propertyName);
        if(value == null || (value instanceof Collection && ((Collection) value).isEmpty())) {
            if(existValue != null) {
                LOG.info("Removing property - {} value from vertex {}", propertyName, vertex);
                vertex.removeProperty(propertyName);
            }
        } else {
            if (!value.equals(existValue)) {
                vertex.setProperty(propertyName, value);
            }
        }
    }

    public static void addProperty(Vertex vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        ((TitanVertex)vertex).addProperty(propertyName, value);
    }

    public Edge removeRelation(String edgeId, boolean cascade) {
        LOG.debug("Removing edge with id {}", edgeId);
        final Edge edge = titanGraph.getEdge(edgeId);
        titanGraph.removeEdge(edge);
        LOG.info("Removed edge {}", edge);
        if (cascade) {
           Vertex referredVertex = edge.getVertex(Direction.IN);
           titanGraph.removeVertex(referredVertex);
           LOG.info("Removed vertex {}", referredVertex);
        }
        return edge;
    }

    public Vertex getVertexForGUID(String guid) throws EntityNotFoundException {
        return getVertexForProperty(Constants.GUID_PROPERTY_KEY, guid);
    }


    public Vertex getVertexForProperty(String propertyKey, Object value) throws EntityNotFoundException {
        Vertex instanceVertex = findVertex(propertyKey, value);
        if (instanceVertex == null) {
            LOG.debug("Could not find a vertex with {}={}", propertyKey, value);
            throw new EntityNotFoundException("Could not find an entity in the repository with " + propertyKey + "="
                + value);
        } else {
            LOG.debug("Found a vertex {} with {}={}", instanceVertex, propertyKey, value);
        }

        return instanceVertex;
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
        return new Id(vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY),
            vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), dataTypeName);
    }

    public static String getTypeName(Vertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
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

        Vertex result = null;
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    result = getVertexForProperty(propertyKey, instance.get(attributeInfo.name));
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
}