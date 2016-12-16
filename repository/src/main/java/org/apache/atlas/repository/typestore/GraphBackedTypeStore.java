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

package org.apache.atlas.repository.typestore;

import static org.apache.atlas.repository.graph.GraphHelper.setProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class GraphBackedTypeStore implements ITypeStore {
    public static final String VERTEX_TYPE = "typeSystem";
    private static final String PROPERTY_PREFIX = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";

    private static Logger LOG = LoggerFactory.getLogger(GraphBackedTypeStore.class);

    private final AtlasGraph graph;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    @Inject
    public GraphBackedTypeStore() {
        graph = AtlasGraphProvider.getGraphInstance();
    }

    @Override
    @GraphTransaction
    public void store(TypeSystem typeSystem, ImmutableList<String> typeNames) throws AtlasException {
        for (String typeName : typeNames) {
            IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
            LOG.debug("Processing {}.{}.{} in type store", dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
            switch (dataType.getTypeCategory()) {
            case ENUM:
                storeInGraph((EnumType) dataType);
                break;

            case STRUCT:
                StructType structType = (StructType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(), dataType.getDescription(),
                        ImmutableList.copyOf(structType.infoToNameMap.keySet()), ImmutableSet.<String>of());
                break;

            case TRAIT:
            case CLASS:
                HierarchicalType type = (HierarchicalType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(), type.getDescription(), type.immediateAttrs,
                        type.superTypes);
                break;

            default:    //Ignore primitive/collection types as they are covered under references
                break;
            }
        }
    }

    private void storeInGraph(EnumType dataType) {
        AtlasVertex AtlasVertex = createVertex(dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
        List<String> values = new ArrayList<>(dataType.values().size());
        for (EnumValue enumValue : dataType.values()) {
            String key = getPropertyKey(dataType.getName(), enumValue.value);
            setProperty(AtlasVertex, key, enumValue.ordinal);
            values.add(enumValue.value);
        }
        setProperty(AtlasVertex, getPropertyKey(dataType.getName()), values);
    }

    private String getPropertyKey(String name) {
        return PROPERTY_PREFIX + name;
    }

    private String getPropertyKey(String parent, String child) {
        return PROPERTY_PREFIX + parent + "." + child;
    }

    String getEdgeLabel(String parent, String child) {
        return PROPERTY_PREFIX + "edge." + parent + "." + child;
    }

    private void storeInGraph(TypeSystem typeSystem, DataTypes.TypeCategory category, String typeName, String typeDescription,
            ImmutableList<AttributeInfo> attributes, ImmutableSet<String> superTypes) throws AtlasException {
        AtlasVertex vertex = createVertex(category, typeName, typeDescription);
        List<String> attrNames = new ArrayList<>();
        if (attributes != null) {
            for (AttributeInfo attribute : attributes) {
                String propertyKey = getPropertyKey(typeName, attribute.name);
                try {
                    setProperty(vertex, propertyKey, attribute.toJson());
                } catch (JSONException e) {
                    throw new StorageException(typeName, e);
                }
                attrNames.add(attribute.name);
                addReferencesForAttribute(typeSystem, vertex, attribute);
            }
        }
        setProperty(vertex, getPropertyKey(typeName), attrNames);

        //Add edges for hierarchy
        if (superTypes != null) {
            for (String superTypeName : superTypes) {
                HierarchicalType superType = typeSystem.getDataType(HierarchicalType.class, superTypeName);
                AtlasVertex superVertex = createVertex(superType.getTypeCategory(), superTypeName, superType.getDescription());
                graphHelper.getOrCreateEdge(vertex, superVertex, SUPERTYPE_EDGE_LABEL);
            }
        }
    }

    private void addReferencesForAttribute(TypeSystem typeSystem, AtlasVertex vertex, AttributeInfo attribute)
            throws AtlasException {
        ImmutableList<String> coreTypes = typeSystem.getCoreTypes();
        List<IDataType> attrDataTypes = new ArrayList<>();
        IDataType attrDataType = attribute.dataType();
        String vertexTypeName = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);

        switch (attrDataType.getTypeCategory()) {
        case ARRAY:
            String attrType = TypeUtils.parseAsArrayType(attrDataType.getName());
            if(attrType != null) {
                IDataType elementType = typeSystem.getDataType(IDataType.class, attrType);
                attrDataTypes.add(elementType);
            }
            break;

        case MAP:
            String[] attrTypes = TypeUtils.parseAsMapType(attrDataType.getName());
            if(attrTypes != null && attrTypes.length > 1) {
                IDataType keyType = typeSystem.getDataType(IDataType.class, attrTypes[0]);
                IDataType valueType = typeSystem.getDataType(IDataType.class, attrTypes[1]);
                attrDataTypes.add(keyType);
                attrDataTypes.add(valueType);
            }
            break;

        case ENUM:
        case STRUCT:
        case CLASS:
            attrDataTypes.add(attrDataType);
            break;

        case PRIMITIVE: //no vertex for primitive type, hence no edge required
            break;

        default:
            throw new IllegalArgumentException(
                    "Attribute cannot reference instances of type : " + attrDataType.getTypeCategory());
        }

        for (IDataType attrType : attrDataTypes) {
            if (!coreTypes.contains(attrType.getName())) {
                AtlasVertex attrVertex = createVertex(attrType.getTypeCategory(), attrType.getName(), attrType.getDescription());
                String label = getEdgeLabel(vertexTypeName, attribute.name);
                graphHelper.getOrCreateEdge(vertex, attrVertex, label);
            }
        }
    }

    @Override
    @GraphTransaction
    public TypesDef restore() throws AtlasException {
        //Get all vertices for type system
        Iterator vertices =
                graph.query().has(Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE).vertices().iterator();

        return getTypesFromVertices(vertices);
    }

    @Override
    @GraphTransaction
    public TypesDef restoreType(String typeName) throws AtlasException {
        // Get AtlasVertex for the specified type name.
        Iterator vertices =
            graph.query().has(Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE).has(Constants.TYPENAME_PROPERTY_KEY, typeName).vertices().iterator();

        return getTypesFromVertices(vertices);
    }

    private TypesDef getTypesFromVertices(Iterator<AtlasVertex> vertices) throws AtlasException {
        ImmutableList.Builder<EnumTypeDefinition> enums = ImmutableList.builder();
        ImmutableList.Builder<StructTypeDefinition> structs = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<ClassType>> classTypes = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.builder();

        while (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            DataTypes.TypeCategory typeCategory = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);
            String typeName = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);
            String typeDescription = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
            LOG.info("Restoring type {}.{}.{}", typeCategory, typeName, typeDescription);
            switch (typeCategory) {
            case ENUM:
                enums.add(getEnumType(vertex));
                break;

            case STRUCT:
                AttributeDefinition[] attributes = getAttributes(vertex, typeName);
                structs.add(new StructTypeDefinition(typeName, typeDescription, attributes));
                break;

            case CLASS:
                ImmutableSet<String> superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                classTypes.add(new HierarchicalTypeDefinition(ClassType.class, typeName, typeDescription, superTypes, attributes));
                break;

            case TRAIT:
                superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                traits.add(new HierarchicalTypeDefinition(TraitType.class, typeName, typeDescription, superTypes, attributes));
                break;

            default:
                throw new IllegalArgumentException("Unhandled type category " + typeCategory);
            }
        }
        return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
    }

    private EnumTypeDefinition getEnumType(AtlasVertex vertex) throws AtlasException {
        String typeName = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);
        String typeDescription = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
        List<EnumValue> enumValues = new ArrayList<>();
        List<String> values = vertex.getListProperty(getPropertyKey(typeName));
        for (String value : values) {
            String valueProperty = getPropertyKey(typeName, value);
            enumValues.add(new EnumValue(value, GraphHelper.getSingleValuedProperty(vertex, valueProperty, Integer.class)));
        }
        return new EnumTypeDefinition(typeName, typeDescription, enumValues.toArray(new EnumValue[enumValues.size()]));
    }

    private ImmutableSet<String> getSuperTypes(AtlasVertex vertex) {
        Set<String> superTypes = new HashSet<>();
        for (AtlasEdge edge : (Iterable<AtlasEdge>) vertex.getEdges(AtlasEdgeDirection.OUT, SUPERTYPE_EDGE_LABEL)) {
            superTypes.add(edge.getInVertex().getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class));
        }
        return ImmutableSet.copyOf(superTypes);
    }

    private AttributeDefinition[] getAttributes(AtlasVertex vertex, String typeName) throws AtlasException {
        List<AttributeDefinition> attributes = new ArrayList<>();
        List<String> attrNames = vertex.getListProperty(getPropertyKey(typeName));
        if (attrNames != null) {
            for (String attrName : attrNames) {
                try {
                    String propertyKey = getPropertyKey(typeName, attrName);
                    AttributeDefinition attrValue = AttributeInfo.fromJson((String) vertex.getJsonProperty(propertyKey));
                    if (attrValue != null)
                    {
                        attributes.add(attrValue);
                    }
                } catch (JSONException e) {
                    throw new AtlasException(e);
                }
            }
        }
        return attributes.toArray(new AttributeDefinition[attributes.size()]);
    }

    /**
     * Find vertex for the given type category and name, else create new vertex
     * @param category
     * @param typeName
     * @return vertex
     */
    AtlasVertex findVertex(DataTypes.TypeCategory category, String typeName) {
        LOG.debug("Finding AtlasVertex for {}.{}", category, typeName);

        Iterator results = graph.query().has(Constants.TYPENAME_PROPERTY_KEY, typeName).vertices().iterator();
        AtlasVertex vertex = null;
        if (results != null && results.hasNext()) {
            //There should be just one AtlasVertex with the given typeName
            vertex = (AtlasVertex) results.next();
        }
        return vertex;
    }

    private AtlasVertex createVertex(DataTypes.TypeCategory category, String typeName, String typeDescription) {
        AtlasVertex vertex = findVertex(category, typeName);
        if (vertex == null) {
            LOG.debug("Adding vertex {}{}", PROPERTY_PREFIX, typeName);
            vertex = graph.addVertex();
            setProperty(vertex, Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE); // Mark as type AtlasVertex
            setProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, category);
            setProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, typeName);
        }
        if (typeDescription != null) {
            String oldDescription = getPropertyKey(Constants.TYPEDESCRIPTION_PROPERTY_KEY);
            if (!typeDescription.equals(oldDescription)) {
                setProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, typeDescription);
            }
        } else {
            LOG.debug(" type description is null ");
        }
        return vertex;
    }
}
