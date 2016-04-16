/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;
import static org.apache.atlas.repository.graph.GraphHelper.string;

public abstract class DeleteHandler {
    public static final Logger LOG = LoggerFactory.getLogger(DeleteHandler.class);

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    protected TypeSystem typeSystem;
    private boolean shouldUpdateReverseAttribute;

    public DeleteHandler(TypeSystem typeSystem, boolean shouldUpdateReverseAttribute) {
        this.typeSystem = typeSystem;
        this.shouldUpdateReverseAttribute = shouldUpdateReverseAttribute;

    }

    /**
     * Deletes the entity vertex - deletes the traits and all the references
     * @param instanceVertex
     * @throws AtlasException
     */
    public void deleteEntity(Vertex instanceVertex) throws AtlasException {
        String guid = GraphHelper.getIdFromVertex(instanceVertex);
        String typeName = GraphHelper.getTypeName(instanceVertex);
        RequestContext.get().recordDeletedEntity(guid, typeName);

        deleteAllTraits(instanceVertex);

        deleteTypeVertex(instanceVertex);
    }

    protected abstract void deleteEdge(Edge edge) throws AtlasException;

    /**
     * Deletes a type vertex - can be entity(class type) or just vertex(struct/trait type)
     * @param instanceVertex
     * @param typeCategory
     * @throws AtlasException
     */
    protected void deleteTypeVertex(Vertex instanceVertex, DataTypes.TypeCategory typeCategory) throws AtlasException {
        switch (typeCategory) {
        case STRUCT:
        case TRAIT:
            deleteTypeVertex(instanceVertex);
            break;

        case CLASS:
            deleteEntity(instanceVertex);
            break;

        default:
            throw new IllegalStateException("Type category " + typeCategory + " not handled");
        }
    }

    /**
     * Deleting any type vertex. Goes over the complex attributes and removes the references
     * @param instanceVertex
     * @throws AtlasException
     */
    protected void deleteTypeVertex(Vertex instanceVertex) throws AtlasException {
        LOG.debug("Deleting {}", string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);
        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        FieldMapping fieldMapping = getFieldMapping(type);

        for (AttributeInfo attributeInfo : fieldMapping.fields.values()) {
            LOG.debug("Deleting attribute {} for {}", attributeInfo.name, string(instanceVertex));
            String edgeLabel = GraphHelper.getEdgeLabel(type, attributeInfo);

            switch (attributeInfo.dataType().getTypeCategory()) {
            case CLASS:
                //If its class attribute, delete the reference
                deleteReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.CLASS, attributeInfo.isComposite);
                break;

            case STRUCT:
                //If its struct attribute, delete the reference
                deleteReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.STRUCT);
                break;

            case ARRAY:
                //For array attribute, if the element is struct/class, delete all the references
                IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
                DataTypes.TypeCategory elementTypeCategory = elementType.getTypeCategory();
                if (elementTypeCategory == DataTypes.TypeCategory.STRUCT ||
                        elementTypeCategory == DataTypes.TypeCategory.CLASS) {
                    Iterator<Edge> edges = GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
                    if (edges != null) {
                        while (edges.hasNext()) {
                            Edge edge = edges.next();
                            deleteReference(edge, elementType, attributeInfo);
                        }
                    }
                }
                break;

            case MAP:
                //For map attribute, if the value type is struct/class, delete all the references
                DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
                DataTypes.TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                String propertyName = GraphHelper.getQualifiedFieldName(type, attributeInfo.name);

                if (valueTypeCategory == DataTypes.TypeCategory.STRUCT ||
                        valueTypeCategory == DataTypes.TypeCategory.CLASS) {
                    List<String> keys = instanceVertex.getProperty(propertyName);
                    if (keys != null) {
                        for (String key : keys) {
                            String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                            deleteReference(instanceVertex, mapEdgeLabel, valueTypeCategory, attributeInfo.isComposite);
                        }
                    }
                }
            }
        }

        deleteVertex(instanceVertex, type.getTypeCategory());
    }

    public void deleteReference(Edge edge, IDataType dataType, AttributeInfo attributeInfo) throws AtlasException {
        deleteReference(edge, dataType.getTypeCategory(), attributeInfo.isComposite);
    }

    public void deleteReference(Edge edge, DataTypes.TypeCategory typeCategory, boolean isComposite) throws AtlasException {
        LOG.debug("Deleting {}", string(edge));
        if (typeCategory == DataTypes.TypeCategory.STRUCT || typeCategory == DataTypes.TypeCategory.TRAIT
                || (typeCategory == DataTypes.TypeCategory.CLASS && isComposite)) {
            //If the vertex is of type struct/trait, delete the edge and then the reference vertex as the vertex is not shared by any other entities.
            //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
            //through this delete, hence delete the edge and the reference vertex.
            Vertex vertexForDelete = edge.getVertex(Direction.IN);

            //If deleting the edge and then the in vertex, reverse attribute shouldn't be updated
            deleteEdge(edge, false);
            deleteTypeVertex(vertexForDelete, typeCategory);
        } else {
            //If the vertex is of type class, and its not a composite attributes, the reference vertex' lifecycle is not controlled
            //through this delete. Hence just remove the reference edge. Leave the reference vertex as is

            //If deleting just the edge, reverse attribute should be updated for any references
            //For example, for the department type system, if the person's manager edge is deleted, subordinates of manager should be updated
            deleteEdge(edge, true);
        }
    }

    public void deleteReference(Vertex instanceVertex, String edgeLabel, DataTypes.TypeCategory typeCategory)
            throws AtlasException {
        deleteReference(instanceVertex, edgeLabel, typeCategory, false);
    }

    public void deleteReference(Vertex instanceVertex, String edgeLabel, DataTypes.TypeCategory typeCategory,
                                boolean isComposite) throws AtlasException {
        Edge edge = GraphHelper.getEdgeForLabel(instanceVertex, edgeLabel);
        if (edge != null) {
            deleteReference(edge, typeCategory, isComposite);
        }
    }

    protected void deleteEdge(Edge edge, boolean updateReverseAttribute) throws AtlasException {
        //update reverse attribute
        if (updateReverseAttribute) {
            AttributeInfo attributeInfo = getAttributeForEdge(edge.getLabel());
            if (attributeInfo.reverseAttributeName != null) {
                deleteEdgeBetweenVertices(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT),
                        attributeInfo.reverseAttributeName);
            }
        }

        deleteEdge(edge);
    }

    protected void deleteVertex(Vertex instanceVertex, DataTypes.TypeCategory typeCategory) throws AtlasException {
        //Update external references(incoming edges) to this vertex
        LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));
        Iterator<Edge> edges = instanceVertex.getEdges(Direction.IN).iterator();

        while(edges.hasNext()) {
            Edge edge = edges.next();
            String edgeState = edge.getProperty(Constants.STATE_PROPERTY_KEY);
            if (Id.EntityState.ACTIVE.name().equals(edgeState)) {
                //Delete only the active edge references
                AttributeInfo attribute = getAttributeForEdge(edge.getLabel());
                deleteEdgeBetweenVertices(edge.getVertex(Direction.OUT), edge.getVertex(Direction.IN), attribute.name);
                deleteEdge(edge);
            }
        }
        _deleteVertex(instanceVertex);
    }

    protected abstract void _deleteVertex(Vertex instanceVertex);

    /**
     * Deletes the edge between outvertex and inVertex. The edge is for attribute attributeName of outVertex
     * @param outVertex
     * @param inVertex
     * @param attributeName
     * @throws AtlasException
     */
    protected void deleteEdgeBetweenVertices(Vertex outVertex, Vertex inVertex, String attributeName) throws AtlasException {
        LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex),
                attributeName);
        String typeName = GraphHelper.getTypeName(outVertex);
        String outId = GraphHelper.getIdFromVertex(outVertex);
        if (outId != null && RequestContext.get().getDeletedEntityIds().contains(outId)) {
            //If the reference vertex is marked for deletion, skip updating the reference
            return;
        }

        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        AttributeInfo attributeInfo = getFieldMapping(type).fields.get(attributeName);
        String propertyName = GraphHelper.getQualifiedFieldName(type, attributeName);
        String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
        Edge edge = null;

        switch (attributeInfo.dataType().getTypeCategory()) {
        case CLASS:
            //If its class attribute, its the only edge between two vertices
            //TODO need to enable this
            //            if (refAttributeInfo.multiplicity == Multiplicity.REQUIRED) {
            //                throw new AtlasException("Can't set attribute " + refAttributeName + " to null as its required attribute");
            //            }
            edge = GraphHelper.getEdgeForLabel(outVertex, edgeLabel);
            break;

        case ARRAY:
            //If its array attribute, find the right edge between the two vertices and update array property
            List<String> elements = outVertex.getProperty(propertyName);
            if (elements != null) {
                elements = new ArrayList<>(elements);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String elementEdgeId : elements) {
                    Edge elementEdge = graphHelper.getEdgeById(elementEdgeId);
                    if (elementEdge == null) {
                        continue;
                    }

                    Vertex elementVertex = elementEdge.getVertex(Direction.IN);
                    if (elementVertex.getId().toString().equals(inVertex.getId().toString())) {
                        edge = elementEdge;

                        if (shouldUpdateReverseAttribute || attributeInfo.isComposite) {
                            //if composite attribute, remove the reference as well. else, just remove the edge
                            //for example, when table is deleted, process still references the table
                            //but when column is deleted, table will not reference the deleted column
                            LOG.debug("Removing edge {} from the array attribute {}", string(elementEdge),
                                    attributeName);
                            elements.remove(elementEdge.getId().toString());
                            GraphHelper.setProperty(outVertex, propertyName, elements);
                        }
                        break;
                    }
                }
            }
            break;

        case MAP:
            //If its map attribute, find the right edge between two vertices and update map property
            List<String> keys = outVertex.getProperty(propertyName);
            if (keys != null) {
                keys = new ArrayList<>(keys);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String key : keys) {
                    String keyPropertyName = propertyName + "." + key;
                    String mapEdgeId = outVertex.getProperty(keyPropertyName);
                    Edge mapEdge = graphHelper.getEdgeById(mapEdgeId);
                    Vertex mapVertex = mapEdge.getVertex(Direction.IN);
                    if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                        edge = mapEdge;

                        if (shouldUpdateReverseAttribute || attributeInfo.isComposite) {
                            //remove this key
                            LOG.debug("Removing edge {}, key {} from the map attribute {}", string(mapEdge), key,
                                    attributeName);
                            keys.remove(key);
                            GraphHelper.setProperty(outVertex, propertyName, keys);
                            GraphHelper.setProperty(outVertex, keyPropertyName, null);
                        }
                        break;
                    }
                }
            }
            break;

        case STRUCT:
        case TRAIT:
            break;

        default:
            throw new IllegalStateException("There can't be an edge from " + string(outVertex) + " to "
                    + string(inVertex) + " with attribute name " + attributeName + " which is not class/array/map attribute");
        }

        if (edge != null) {
            deleteEdge(edge);
            GraphHelper.setProperty(outVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                    RequestContext.get().getRequestTime());
        }
    }

    protected AttributeInfo getAttributeForEdge(String edgLabel) throws AtlasException {
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(edgLabel);
        IDataType referenceType = typeSystem.getDataType(IDataType.class, atlasEdgeLabel.getTypeName());
        return getFieldMapping(referenceType).fields.get(atlasEdgeLabel.getAttributeName());
    }

    protected FieldMapping getFieldMapping(IDataType type) {
        switch (type.getTypeCategory()) {
        case CLASS:
        case TRAIT:
            return ((HierarchicalType)type).fieldMapping();

        case STRUCT:
            return ((StructType)type).fieldMapping();

        default:
            throw new IllegalStateException("Type " + type + " doesn't have any fields!");
        }
    }

    /**
     * Delete all traits from the specified vertex.
     * @param instanceVertex
     * @throws AtlasException
     */
    private void deleteAllTraits(Vertex instanceVertex) throws AtlasException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        LOG.debug("Deleting traits {} for {}", traitNames, string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);

        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(typeName, traitNameToBeDeleted);
            deleteReference(instanceVertex, relationshipLabel, DataTypes.TypeCategory.TRAIT);
        }
    }
}
