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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Singleton
@Component
@Deprecated
public class GraphBackedTypeStore implements ITypeStore {
    public static final String VERTEX_TYPE = "typeSystem";
    private static final String PROPERTY_PREFIX = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";

    private static Logger LOG = LoggerFactory.getLogger(GraphBackedTypeStore.class);

    private final AtlasGraph graph;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    @Inject
    public GraphBackedTypeStore(AtlasGraph atlasGraph) {
        this.graph = atlasGraph;
    }

    @Override
    @GraphTransaction
    public void store(TypeSystem typeSystem, ImmutableList<String> typeNames) throws AtlasException {

        //Pre-create the vertices that are needed for the types.  This allows us to execute
        //one query to determine all of the vertices that already exist.
        Map<String, AtlasVertex> typeVertices = getOrCreateTypeVertices(typeSystem, typeNames);

        //Complete the storage process by adding properties and edges to the vertices
        //that were created.
        TypePersistenceVisitor visitor = new TypePersistenceVisitor(this, typeVertices, typeSystem);
        processTypes(typeNames, typeSystem, visitor);
    }

    private void processTypes(ImmutableList<String> typeNames, TypeSystem typeSystem, TypeVisitor visitor) throws AtlasException {
        for (String typeName : typeNames) {
            IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
            LOG.debug("Processing {}.{}.{} in type store", dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
            switch (dataType.getTypeCategory()) {
            case ENUM:
                visitor.visitEnumeration((EnumType)dataType);
                break;

            case STRUCT:
                StructType structType = (StructType) dataType;
                processType(typeSystem, dataType.getTypeCategory(), dataType.getName(), dataType.getDescription(),
                        ImmutableList.copyOf(structType.infoToNameMap.keySet()), ImmutableSet.<String>of(), visitor);
                break;

            case TRAIT:
            case CLASS:
                HierarchicalType type = (HierarchicalType) dataType;
                processType(typeSystem, dataType.getTypeCategory(), dataType.getName(), type.getDescription(), type.immediateAttrs,
                        type.superTypes, visitor);
                break;

            case RELATIONSHIP:
                // ignore
                break;

            default:    //Ignore primitive/collection types as they are covered under references
                break;
            }
        }
    }

    private Map<String, AtlasVertex> getOrCreateTypeVertices(TypeSystem typeSystem, ImmutableList<String> typeNames) throws AtlasException {

        //examine the types to determine what type vertices are needed
        TypeVertexFinder vertexFinder = new TypeVertexFinder(typeSystem);
        processTypes(typeNames, typeSystem, vertexFinder);
        List<TypeVertexInfo> typeVerticesNeeded = vertexFinder.getVerticesToCreate();

        //find or create the type vertices
        List<AtlasVertex> vertices = createVertices(typeVerticesNeeded);

        //Create a type name->AtlasVertex map with the result
        Map<String, AtlasVertex> result = new HashMap<>(typeVerticesNeeded.size());
        for(int i = 0 ; i < typeVerticesNeeded.size(); i++) {
            TypeVertexInfo createdVertexInfo = typeVerticesNeeded.get(i);
            AtlasVertex createdVertex = vertices.get(i);
            result.put(createdVertexInfo.getTypeName(), createdVertex);
        }
        return result;

    }


    static String getPropertyKey(String name) {
        return PROPERTY_PREFIX + name;
    }

    static String getPropertyKey(String parent, String child) {
        return PROPERTY_PREFIX + parent + "." + child;
    }

    static String getEdgeLabel(String parent, String child) {
        return PROPERTY_PREFIX + "edge." + parent + "." + child;
    }

    private void processType(TypeSystem typeSystem, DataTypes.TypeCategory category, String typeName, String typeDescription,
            ImmutableList<AttributeInfo> attributes, ImmutableSet<String> superTypes, TypeVisitor visitor) throws AtlasException {

        visitor.visitDataType(category, typeName, typeDescription);

        List<String> attrNames = new ArrayList<>();
        if (attributes != null) {
            for (AttributeInfo attribute : attributes) {
                visitor.visitAttribute(typeName, attribute);
                attrNames.add(attribute.name);
                processsAttribute(typeSystem,  typeName, attribute, visitor);
            }
        }
        visitor.visitAttributeNames(typeName, attrNames);

        //Add edges for hierarchy
        if (superTypes != null) {
            for (String superTypeName : superTypes) {
                visitor.visitSuperType(typeName, superTypeName);
            }
        }
    }

    private void processsAttribute(TypeSystem typeSystem, String typeName, AttributeInfo attribute, TypeVisitor visitor)
            throws AtlasException {

        ImmutableList<String> coreTypes = typeSystem.getCoreTypes();
        List<IDataType> attrDataTypes = new ArrayList<>();
        IDataType attrDataType = attribute.dataType();


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
                visitor.visitAttributeDataType(typeName, attribute, attrType);
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
            DataTypes.TypeCategory typeCategory = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);
            String typeName = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);
            String typeDescription = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
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

            case RELATIONSHIP:
                // v1 typesystem is not notified on new relation type
                break;

            default:
                throw new IllegalArgumentException("Unhandled type category " + typeCategory);
            }
        }
        return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
    }

    private EnumTypeDefinition getEnumType(AtlasVertex vertex) throws AtlasException {
        String typeName = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);
        String typeDescription = AtlasGraphUtilsV1.getEncodedProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
        List<EnumValue> enumValues = new ArrayList<>();
        List<String> values = GraphHelper.getListProperty(vertex, getPropertyKey(typeName));
        for (String value : values) {
            String valueProperty = getPropertyKey(typeName, value);
            enumValues.add(new EnumValue(value, AtlasGraphUtilsV1.getProperty(vertex, valueProperty, Integer.class)));
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
        List<String> attrNames = GraphHelper.getListProperty(vertex, getPropertyKey(typeName));
        if (attrNames != null) {
            for (String attrName : attrNames) {
                try {
                    String encodedPropertyKey = AtlasGraphUtilsV1.encodePropertyKey(getPropertyKey(typeName, attrName));
                    AttributeDefinition attrValue = AttributeInfo.fromJson((String) vertex.getJsonProperty(encodedPropertyKey));
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

  //package-private for testing
   Map<String, AtlasVertex> findVertices(List<String> typeNames) throws RepositoryException {
        LOG.debug("Finding vertices for {}", typeNames.toString());
        Map<String, AtlasVertex> foundVertices = graphHelper.getVerticesForPropertyValues(Constants.TYPENAME_PROPERTY_KEY, typeNames);
        return foundVertices;

    }


    /**
     * Finds or creates type vertices with the information specified.
     *
     * @param infoList
     * @return list with the vertices corresponding to the types in the list.
     * @throws AtlasException
     */
    private List<AtlasVertex> createVertices(List<TypeVertexInfo> infoList) throws AtlasException {

        List<AtlasVertex> result = new ArrayList<>(infoList.size());
        List<String> typeNames = Lists.transform(infoList, new Function<TypeVertexInfo,String>() {
            @Override
            public String apply(TypeVertexInfo input) {
                return input.getTypeName();
            }
        });
        Map<String, AtlasVertex> vertices = findVertices(typeNames);

        for(TypeVertexInfo info : infoList) {
            AtlasVertex vertex = vertices.get(info.getTypeName());
            if (! GraphHelper.elementExists(vertex)) {
                LOG.debug("Adding vertex {}{}", PROPERTY_PREFIX, info.getTypeName());
                vertex = graph.addVertex();
                AtlasGraphUtilsV1.setEncodedProperty(vertex, Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE); // Mark as type AtlasVertex
                AtlasGraphUtilsV1.setEncodedProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, info.getCategory());
                AtlasGraphUtilsV1.setEncodedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, info.getTypeName());
            }
            String newDescription = info.getTypeDescription();
            if (newDescription != null) {
                String oldDescription = getPropertyKey(Constants.TYPEDESCRIPTION_PROPERTY_KEY);
                if (!newDescription.equals(oldDescription)) {
                    AtlasGraphUtilsV1.setEncodedProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, newDescription);
                }
            } else {
                LOG.debug(" type description is null ");
            }
            result.add(vertex);
        }
        return result;
    }
}
