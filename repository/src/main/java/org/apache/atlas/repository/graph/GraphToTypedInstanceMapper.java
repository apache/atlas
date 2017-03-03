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

import static org.apache.atlas.repository.graph.GraphHelper.string;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.AtlasSystemAttributes;
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

import com.google.inject.Singleton;

@Singleton
public final class GraphToTypedInstanceMapper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphToTypedInstanceMapper.class);
    private static TypeSystem typeSystem = TypeSystem.getInstance();
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final IAtlasGraphProvider graphProvider;

    public GraphToTypedInstanceMapper(IAtlasGraphProvider graphProvider) {
        this.graphProvider = graphProvider;
    }

    public ITypedReferenceableInstance mapGraphToTypedInstance(String guid, AtlasVertex instanceVertex)
        throws AtlasException {

        if(LOG.isDebugEnabled()) {
            //We don't do a cache check here since we want that to be at a higher level
            //where the vertex lookup can also be avoided.  However, this is a convenient
            //place to add a check to see if there are any places that were missed.
            if(RequestContext.get().getInstanceV1(guid) != null) {
                LOG.warn("Looking up previously cached guid at: ", new Exception());
            }

            LOG.debug("Mapping graph root vertex {} to typed instance for guid {}", instanceVertex, guid);
        }

        String typeName = GraphHelper.getSingleValuedProperty(instanceVertex, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
        List<String> traits = GraphHelper.getTraitNames(instanceVertex);
        String state = GraphHelper.getStateAsString(instanceVertex);
        String createdBy = GraphHelper.getCreatedByAsString(instanceVertex);
        String modifiedBy = GraphHelper.getModifiedByAsString(instanceVertex);
        Date createdTime = new Date(GraphHelper.getCreatedTime(instanceVertex));
        Date modifiedTime = new Date(GraphHelper.getModifiedTime(instanceVertex));
        AtlasSystemAttributes systemAttributes = new AtlasSystemAttributes(createdBy, modifiedBy, createdTime, modifiedTime);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Found createdBy : {} modifiedBy : {} createdTime: {} modifedTime: {}", createdBy, modifiedBy, createdTime, modifiedTime);
        }

        Id id = new Id(guid, Integer.parseInt(String.valueOf(GraphHelper.getProperty(instanceVertex, Constants.VERSION_PROPERTY_KEY))),
                typeName, state);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created id {} for instance type {}", id, typeName);
        }

        ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance typedInstance =
            classType.createInstance(id, systemAttributes, traits.toArray(new String[traits.size()]));

        mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
        mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);
        RequestContext.get().cache(typedInstance);
        return typedInstance;
    }

    private void mapVertexToInstanceTraits(AtlasVertex instanceVertex, ITypedReferenceableInstance typedInstance,
        List<String> traits) throws AtlasException {
        for (String traitName : traits) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("mapping trait {} to instance", traitName);
            }

            TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
            mapVertexToTraitInstance(instanceVertex, typedInstance, traitName, traitType);
        }
    }

    public void mapVertexToInstance(AtlasVertex instanceVertex, ITypedInstance typedInstance,
        Map<String, AttributeInfo> fields) throws AtlasException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping vertex {} to instance {} for fields", instanceVertex, typedInstance.getTypeName(),
                    fields);
        }

        for (AttributeInfo attributeInfo : fields.values()) {
            mapVertexToAttribute(instanceVertex, typedInstance, attributeInfo);
        }
    }

    private void mapVertexToAttribute(AtlasVertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping attributeInfo {}", attributeInfo.name);
        }

        final IDataType dataType = attributeInfo.dataType();
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String relationshipLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapVertexToPrimitive(instanceVertex, typedInstance, attributeInfo);
            break;  // add only if vertex has this attribute

        case ENUM:
            Object propertyValue = GraphHelper.getProperty(instanceVertex, vertexPropertyName);
            if (propertyValue == null) {
                return;
            }

            typedInstance.set(attributeInfo.name, dataType.convert(propertyValue, Multiplicity.REQUIRED));
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
            AtlasEdge nullEdge = null;
            Object idOrInstance = mapVertexToClassReference(instanceVertex, attributeInfo, relationshipLabel,
                attributeInfo.dataType(), nullEdge);
            if (idOrInstance != null) {
                typedInstance.set(attributeInfo.name, idOrInstance);
            }
            break;

        default:
            break;
        }
    }

    private Object mapVertexToClassReference(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
            String relationshipLabel, IDataType dataType, AtlasEdge optionalEdge) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        }

        AtlasEdge edge = null;
        if (optionalEdge == null) {
            edge = graphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        } else {
            edge = optionalEdge;
        }

        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex referenceVertex = edge.getInVertex();
            final String guid = GraphHelper.getSingleValuedProperty(referenceVertex, Constants.GUID_PROPERTY_KEY, String.class);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel, guid);
            }

            if (attributeInfo.isComposite) {
                //Also, when you retrieve a type's instance, you get the complete object graph of the composites
                LOG.debug("Found composite, mapping vertex to instance");
                ITypedReferenceableInstance cached = RequestContext.get().getInstanceV1(guid);
                if(cached != null) {
                    return cached;
                }
                return mapGraphToTypedInstance(guid, referenceVertex);
            } else {
                String state = GraphHelper.getStateAsString(referenceVertex);
                Id referenceId =
                        new Id(guid, GraphHelper.getSingleValuedProperty(referenceVertex, Constants.VERSION_PROPERTY_KEY, Integer.class),
                                dataType.getName(), state);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found non-composite, adding id {} ", referenceId);
                }

                return referenceId;
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToArrayInstance(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
            AttributeInfo attributeInfo, String propertyName) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        }

        final DataTypes.ArrayType arrayType = (DataTypes.ArrayType) attributeInfo.dataType();
        final IDataType elementType = arrayType.getElemType();

        List<Object> list = GraphHelper.getArrayElementsProperty(elementType, instanceVertex, propertyName);

        if (list == null || list.size() == 0) {
            return;
        }

        String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        ArrayList values = new ArrayList();
        for (Object aList : list) {
            values.add(mapVertexToCollectionEntry(instanceVertex, attributeInfo, elementType, aList,
                    edgeLabel));
        }

        if (values.size() > 0) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private Object mapVertexToCollectionEntry(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
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
            return mapVertexToStructInstance(instanceVertex, (StructType) elementType, edgeLabel, (AtlasEdge) value);

        case CLASS:
            return mapVertexToClassReference(instanceVertex, attributeInfo, edgeLabel, elementType, (AtlasEdge) value);

        default:
            break;
        }

        throw new IllegalArgumentException();
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToMapInstance(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
            AttributeInfo attributeInfo, final String propertyName) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        }

        List<String> keys = GraphHelper.getListProperty(instanceVertex, propertyName);
        if (keys == null || keys.size() == 0) {
            return;
        }
        DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
        final IDataType valueType = mapType.getValueType();

        HashMap<String,Object> values = new HashMap<>();
        for (String key : keys) {
            final String keyPropertyName = propertyName + "." + key;
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + keyPropertyName;
            final Object keyValue = GraphHelper.getMapValueProperty(valueType, instanceVertex, keyPropertyName);
            Object mapValue = mapVertexToCollectionEntry(instanceVertex, attributeInfo, valueType, keyValue, edgeLabel);
            if (mapValue != null) {
                values.put(key, mapValue);
            }
        }

        if (!values.isEmpty()) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private  ITypedStruct mapVertexToStructInstance(AtlasVertex instanceVertex, StructType structType,
            String relationshipLabel, AtlasEdge optionalEdge) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mapping {} to struct {}", string(instanceVertex), relationshipLabel);
        }

        ITypedStruct structInstance = null;

        AtlasEdge edge;
        if (optionalEdge == null) {
            edge = graphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        } else {
            edge = optionalEdge;
        }

        if (GraphHelper.elementExists(edge)) {
            structInstance = structType.createInstance();
            AtlasVertex structInstanceVertex = edge.getInVertex();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found struct instance {}, mapping to instance {} ", string(structInstanceVertex),
                        structInstance.getTypeName());
            }

            mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);
        }
        return structInstance;
    }

    private void mapVertexToTraitInstance(AtlasVertex instanceVertex, ITypedReferenceableInstance typedInstance,
        String traitName, TraitType traitType) throws AtlasException {
        ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

        mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(), traitName, traitType, traitInstance);
    }

    private void mapVertexToTraitInstance(AtlasVertex<?,?> instanceVertex, String typedInstanceTypeName, String traitName,
            TraitType traitType, ITypedStruct traitInstance) throws AtlasException {
        String relationshipLabel = GraphHelper.getTraitLabel(typedInstanceTypeName, traitName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        }

        for (AtlasEdge<?,?> edge : instanceVertex.getEdges(AtlasEdgeDirection.OUT, relationshipLabel)) {
            final AtlasVertex<?,?> traitInstanceVertex = edge.getInVertex();
            if (traitInstanceVertex != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found trait instance vertex {}, mapping to instance {} ", traitInstanceVertex,
                            traitInstance.getTypeName());
                }

                mapVertexToInstance(traitInstanceVertex, traitInstance, traitType.fieldMapping().fields);
                break;
            }
        }
    }

    private void mapVertexToPrimitive(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
            AttributeInfo attributeInfo) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
        }

        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        if (GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Object.class) == null) {
            return;
        }

        if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            typedInstance.setString(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, String.class));
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            typedInstance.setShort(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Short.class));
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            typedInstance.setInt(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Integer.class));
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            typedInstance.setBigInt(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, BigInteger.class));
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            typedInstance.setBoolean(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Boolean.class));
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            typedInstance.setByte(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Byte.class));
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            typedInstance.setLong(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Long.class));
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            typedInstance.setFloat(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Float.class));
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            typedInstance.setDouble(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Double.class));
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            typedInstance
            .setBigDecimal(attributeInfo.name, GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, BigDecimal.class));
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Long dateVal = GraphHelper.getSingleValuedProperty(instanceVertex, vertexPropertyName, Long.class);
            typedInstance.setDate(attributeInfo.name, new Date(dateVal));
        }
    }


    public ITypedInstance getReferredEntity(String edgeId, IDataType<?> referredType) throws AtlasException {
        final AtlasEdge edge = getGraph().getEdge(edgeId);
        if (edge != null) {
            final AtlasVertex referredVertex = edge.getInVertex();
            if (referredVertex != null) {
                switch (referredType.getTypeCategory()) {
                case STRUCT:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found struct instance vertex {}, mapping to instance {} ", referredVertex,
                                referredType.getName());
                    }

                    StructType structType = (StructType) referredType;
                    ITypedStruct instance = structType.createInstance();
                    Map<String, AttributeInfo> fields = structType.fieldMapping().fields;
                    mapVertexToInstance(referredVertex, instance, fields);
                    return instance;
                case CLASS:
                    //TODO isComposite handling for class loads
                    return GraphHelper.getIdFromVertex(referredType.getName(), referredVertex);
                default:
                    throw new UnsupportedOperationException("Loading " + referredType.getTypeCategory() + " is not supported");
                }
            }
        }
        return null;
    }

    private AtlasGraph getGraph() throws RepositoryException {
        return graphProvider.get();
    }
}

