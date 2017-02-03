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
import java.util.List;
import java.util.Map;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TypeVisitor implementation that completes the type storage process by
 * adding the required properties and edges to the type vertices
 * that were created.
 */
public class TypePersistenceVisitor implements TypeVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(TypePersistenceVisitor.class);
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final GraphBackedTypeStore typeStore_;
    private final Map<String,AtlasVertex> typeVertices;
    private final TypeSystem typeSystem;

    /**
     * @param graphBackedTypeStore
     */
    public TypePersistenceVisitor(GraphBackedTypeStore graphBackedTypeStore, Map<String,AtlasVertex> typeVertices, TypeSystem typeSystem) {
        typeStore_ = graphBackedTypeStore;
        this.typeVertices = typeVertices;
        this.typeSystem = typeSystem;
    }

    @Override
    public void visitEnumeration(EnumType dataType) throws AtlasException {
        AtlasVertex vertex = typeVertices.get(dataType.getName());
        List<String> values = new ArrayList<>(dataType.values().size());
        for (EnumValue enumValue : dataType.values()) {
            String key = GraphBackedTypeStore.getPropertyKey(dataType.getName(), enumValue.value);
            setProperty(vertex, key, enumValue.ordinal);
            values.add(enumValue.value);
        }
        setProperty(vertex, GraphBackedTypeStore.getPropertyKey(dataType.getName()), values);

    }
    @Override
    public void visitAttributeDataType(String typeName, AttributeInfo attribute, IDataType attrType) throws AtlasException {
        AtlasVertex vertex = typeVertices.get(typeName);
        String vertexTypeName = GraphHelper.getSingleValuedProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, String.class);
        AtlasVertex attrVertex = typeVertices.get(attrType.getName());
        String label = GraphBackedTypeStore.getEdgeLabel(vertexTypeName, attribute.name);
        graphHelper.getOrCreateEdge(vertex, attrVertex, label);
    }
    @Override
    public void visitSuperType(String typeName, String superTypeName) throws AtlasException {
        AtlasVertex vertex = typeVertices.get(typeName);
        HierarchicalType superType = typeSystem.getDataType(HierarchicalType.class, superTypeName);
        AtlasVertex superVertex = typeVertices.get(superTypeName);
        graphHelper.getOrCreateEdge(vertex, superVertex, GraphBackedTypeStore.SUPERTYPE_EDGE_LABEL);
    }

    @Override
    public void visitAttributeNames(String typeName, List<String> attrNames) throws AtlasException {
        AtlasVertex vertex = typeVertices.get(typeName);
        setProperty(vertex, GraphBackedTypeStore.getPropertyKey(typeName), attrNames);

    }

    @Override
    public void visitAttribute(String typeName, AttributeInfo attribute) throws AtlasException {
        AtlasVertex vertex = typeVertices.get(typeName);
        String propertyKey = GraphBackedTypeStore.getPropertyKey(typeName, attribute.name);
        try {
            setProperty(vertex, propertyKey, attribute.toJson());
        } catch (JSONException e) {
            throw new StorageException(typeName, e);
        }
    }

    @Override
    public void visitDataType(TypeCategory category, String typeName, String typeDescription) {
        //nothing to do

    }
}