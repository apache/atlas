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

import com.google.inject.Singleton;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.graph.GraphHelper.string;

@Singleton
public final class GraphToTypedInstanceMapper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphToTypedInstanceMapper.class);
    private static TypeSystem typeSystem = TypeSystem.getInstance();
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private TitanGraph titanGraph;

    public GraphToTypedInstanceMapper(TitanGraph titanGraph) {
        this.titanGraph = titanGraph;
    }

    public ITypedReferenceableInstance mapGraphToTypedInstance(String guid, Vertex instanceVertex)
        throws AtlasException {

        LOG.debug("Mapping graph root vertex {} to typed instance for guid {}", instanceVertex, guid);
        String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
        List<String> traits = GraphHelper.getTraitNames(instanceVertex);

        Id id = new Id(guid, instanceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), typeName,
                instanceVertex.<String>getProperty(Constants.STATE_PROPERTY_KEY));
        LOG.debug("Created id {} for instance type {}", id, typeName);

        ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance typedInstance =
            classType.createInstance(id, traits.toArray(new String[traits.size()]));

        mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
        mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);

        return typedInstance;
    }

    private void mapVertexToInstanceTraits(Vertex instanceVertex, ITypedReferenceableInstance typedInstance,
        List<String> traits) throws AtlasException {
        for (String traitName : traits) {
            LOG.debug("mapping trait {} to instance", traitName);
            TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
            mapVertexToTraitInstance(instanceVertex, typedInstance, traitName, traitType);
        }
    }

    public void mapVertexToInstance(Vertex instanceVertex, ITypedInstance typedInstance,
        Map<String, AttributeInfo> fields) throws AtlasException {

        LOG.debug("Mapping vertex {} to instance {} for fields", instanceVertex, typedInstance.getTypeName(),
            fields);
        for (AttributeInfo attributeInfo : fields.values()) {
            mapVertexToAttribute(instanceVertex, typedInstance, attributeInfo);
        }
    }

    private void mapVertexToAttribute(Vertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Mapping attributeInfo {}", attributeInfo.name);
        final IDataType dataType = attributeInfo.dataType();
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String relationshipLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapVertexToPrimitive(instanceVertex, typedInstance, attributeInfo);
            break;  // add only if vertex has this attribute

        case ENUM:
            if (instanceVertex.getProperty(vertexPropertyName) == null) {
                return;
            }

            typedInstance.set(attributeInfo.name,
                dataType.convert(instanceVertex.<String>getProperty(vertexPropertyName),
                    Multiplicity.REQUIRED));
            break;

        case ARRAY:
            mapVertexToArrayInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case MAP:
            mapVertexToMapInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case STRUCT:
            ITypedStruct structInstance = mapVertexToStructInstance(instanceVertex,
                    (StructType) attributeInfo.dataType(), relationshipLabel, null);
            typedInstance.set(attributeInfo.name, structInstance);
            break;

        case TRAIT:
            // do NOTHING - handled in class
            break;

        case CLASS:
            Object idOrInstance = mapVertexToClassReference(instanceVertex, attributeInfo, relationshipLabel,
                attributeInfo.dataType(), null);
            if (idOrInstance != null) {
                typedInstance.set(attributeInfo.name, idOrInstance);
            }
            break;

        default:
            break;
        }
    }

    private Object mapVertexToClassReference(Vertex instanceVertex, AttributeInfo attributeInfo,
        String relationshipLabel, IDataType dataType, String edgeId) throws AtlasException {
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);

        Edge edge;
        if (edgeId == null) {
            edge = GraphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);;
        } else {
            edge = graphHelper.getEdgeById(edgeId);
        }

        if (edge != null) {
            final Vertex referenceVertex = edge.getVertex(Direction.IN);
            final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
            LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel, guid);
            if (attributeInfo.isComposite) {
                //Also, when you retrieve a type's instance, you get the complete object graph of the composites
                LOG.debug("Found composite, mapping vertex to instance");
                return mapGraphToTypedInstance(guid, referenceVertex);
            } else {
                Id referenceId =
                        new Id(guid, referenceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                                dataType.getName());
                LOG.debug("Found non-composite, adding id {} ", referenceId);
                return referenceId;
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToArrayInstance(Vertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List list = instanceVertex.getProperty(propertyName);
        if (list == null || list.size() == 0) {
            return;
        }
        DataTypes.ArrayType arrayType = (DataTypes.ArrayType) attributeInfo.dataType();
        final IDataType elementType = arrayType.getElemType();

        String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        ArrayList values = new ArrayList();
        for (int index = 0; index < list.size(); index++) {
            values.add(mapVertexToCollectionEntry(instanceVertex, attributeInfo, elementType, list.get(index),
                edgeLabel));
        }

        if (values.size() > 0) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private Object mapVertexToCollectionEntry(Vertex instanceVertex, AttributeInfo attributeInfo,
        IDataType elementType, Object value, String edgeLabel) throws AtlasException {
        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return value;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            break;

        case STRUCT:
            return mapVertexToStructInstance(instanceVertex, (StructType) elementType, edgeLabel, (String) value);

        case CLASS:
            return mapVertexToClassReference(instanceVertex, attributeInfo, edgeLabel, elementType, (String) value);

        default:
            break;
        }

        throw new IllegalArgumentException();
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToMapInstance(Vertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, final String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List<String> keys = instanceVertex.getProperty(propertyName);
        if (keys == null || keys.size() == 0) {
            return;
        }
        DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
        final IDataType valueType = mapType.getValueType();

        HashMap values = new HashMap();
        for (String key : keys) {
            final String keyPropertyName = propertyName + "." + key;
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + keyPropertyName;
            final Object keyValue = instanceVertex.getProperty(keyPropertyName);
            Object mapValue = mapVertexToCollectionEntry(instanceVertex, attributeInfo, valueType, keyValue, edgeLabel);
            if (mapValue != null) {
                values.put(key, mapValue);
            }
        }

        if (!values.isEmpty()) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private ITypedStruct mapVertexToStructInstance(Vertex instanceVertex, StructType structType,
                                                   String relationshipLabel, String edgeId) throws AtlasException {
        LOG.debug("mapping {} to struct {}", string(instanceVertex), relationshipLabel);
        ITypedStruct structInstance = null;

        Edge edge;
        if (edgeId == null) {
            edge = GraphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        } else {
            edge = graphHelper.getEdgeById(edgeId);
        }

        if (edge != null) {
            structInstance = structType.createInstance();
            Vertex structInstanceVertex = edge.getVertex(Direction.IN);
            LOG.debug("Found struct instance {}, mapping to instance {} ", string(structInstanceVertex),
                    structInstance.getTypeName());
            mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);

        }
        return structInstance;
    }

    private void mapVertexToTraitInstance(Vertex instanceVertex, ITypedReferenceableInstance typedInstance,
        String traitName, TraitType traitType) throws AtlasException {
        ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

        mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(), traitName, traitType, traitInstance);
    }

    private void mapVertexToTraitInstance(Vertex instanceVertex, String typedInstanceTypeName, String traitName,
        TraitType traitType, ITypedStruct traitInstance) throws AtlasException {
        String relationshipLabel = GraphHelper.getTraitLabel(typedInstanceTypeName, traitName);
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
            final Vertex traitInstanceVertex = edge.getVertex(Direction.IN);
            if (traitInstanceVertex != null) {
                LOG.debug("Found trait instance vertex {}, mapping to instance {} ", traitInstanceVertex,
                    traitInstance.getTypeName());
                mapVertexToInstance(traitInstanceVertex, traitInstance, traitType.fieldMapping().fields);
                break;
            }
        }
    }

    private void mapVertexToPrimitive(Vertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        if (instanceVertex.getProperty(vertexPropertyName) == null) {
            return;
        }

        if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            typedInstance.setString(attributeInfo.name, instanceVertex.<String>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            typedInstance.setShort(attributeInfo.name, instanceVertex.<Short>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            typedInstance.setInt(attributeInfo.name, instanceVertex.<Integer>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            typedInstance.setBigInt(attributeInfo.name, instanceVertex.<BigInteger>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            typedInstance.setBoolean(attributeInfo.name, instanceVertex.<Boolean>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            typedInstance.setByte(attributeInfo.name, instanceVertex.<Byte>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            typedInstance.setLong(attributeInfo.name, instanceVertex.<Long>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            typedInstance.setFloat(attributeInfo.name, instanceVertex.<Float>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            typedInstance.setDouble(attributeInfo.name, instanceVertex.<Double>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            typedInstance
                .setBigDecimal(attributeInfo.name, instanceVertex.<BigDecimal>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Long dateVal = instanceVertex.<Long>getProperty(vertexPropertyName);
            typedInstance.setDate(attributeInfo.name, new Date(dateVal));
        }
    }

    public ITypedInstance getReferredEntity(String edgeId, IDataType<?> referredType) throws AtlasException {
        final Edge edge = titanGraph.getEdge(edgeId);
        if (edge != null) {
            final Vertex referredVertex = edge.getVertex(Direction.IN);
            if (referredVertex != null) {
                switch (referredType.getTypeCategory()) {
                case STRUCT:
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ", referredVertex,
                        referredType.getName());
                    StructType structType = (StructType) referredType;
                    ITypedStruct instance = structType.createInstance();
                    Map<String, AttributeInfo> fields = structType.fieldMapping().fields;
                    mapVertexToInstance(referredVertex, instance, fields);
                    return instance;
                case CLASS:
                    //TODO isComposite handling for class loads
                    final String guid = referredVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    Id referenceId =
                        new Id(guid, referredVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                            referredType.getName());
                    return referenceId;
                default:
                    throw new UnsupportedOperationException("Loading " + referredType.getTypeCategory() + " is not supported");
                }
            }
        }
        return null;
    }
}

