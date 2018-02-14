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

import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;
import static org.apache.atlas.repository.graph.GraphHelper.string;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper.VertexInfo;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.exception.NullRequiredAttributeException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeleteHandler {
    public static final Logger LOG = LoggerFactory.getLogger(DeleteHandler.class);

    protected static final GraphHelper graphHelper = GraphHelper.getInstance();

    protected TypeSystem typeSystem;
    private boolean shouldUpdateReverseAttribute;
    private boolean softDelete;

    public DeleteHandler(TypeSystem typeSystem, boolean shouldUpdateReverseAttribute, boolean softDelete) {
        this.typeSystem = typeSystem;
        this.shouldUpdateReverseAttribute = shouldUpdateReverseAttribute;
        this.softDelete = softDelete;
    }

    /**
     * Deletes the specified entity vertices.
     * Deletes any traits, composite entities, and structs owned by each entity.
     * Also deletes all the references from/to the entity.
     *
     * @param instanceVertices
     * @throws AtlasException
     */
    public void deleteEntities(Collection<AtlasVertex> instanceVertices) throws AtlasException {
       RequestContext requestContext = RequestContext.get();

       Set<AtlasVertex> deletionCandidateVertices = new HashSet<>();

       for (AtlasVertex instanceVertex : instanceVertices) {
            String guid = GraphHelper.getGuid(instanceVertex);
            Id.EntityState state = GraphHelper.getState(instanceVertex);
            if (requestContext.getDeletedEntityIds().contains(guid) || state == Id.EntityState.DELETED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping deletion of {} as it is already deleted", guid);
                }

                continue;
            }

           // Get GUIDs and vertices for all deletion candidates.
           Set<VertexInfo> compositeVertices = graphHelper.getCompositeVertices(instanceVertex);

           // Record all deletion candidate GUIDs in RequestContext
           // and gather deletion candidate vertices.
           for (VertexInfo vertexInfo : compositeVertices) {
               ClassType                   entityType = typeSystem.getDataType(ClassType.class, vertexInfo.getTypeName());
               ITypedReferenceableInstance entity     = entityType.createInstance(new Id(guid, 0, vertexInfo.getTypeName()));

               // populate unique attributes only
               for (AttributeInfo attributeInfo : entityType.fieldMapping().fields.values()) {
                   if (!attributeInfo.isUnique) {
                       continue;
                   }

                   DataTypes.TypeCategory attrTypeCategory = attributeInfo.dataType().getTypeCategory();

                   if (attrTypeCategory == DataTypes.TypeCategory.PRIMITIVE) {
                       GraphToTypedInstanceMapper.mapVertexToPrimitive(vertexInfo.getVertex(), entity, attributeInfo);
                   } else if (attrTypeCategory == DataTypes.TypeCategory.ENUM) {
                       GraphToTypedInstanceMapper.mapVertexToEnum(vertexInfo.getVertex(), entity, attributeInfo);
                   }
               }

               requestContext.recordEntityDelete(entity);
               deletionCandidateVertices.add(vertexInfo.getVertex());
           }
       }

       // Delete traits and vertices.
       for (AtlasVertex deletionCandidateVertex : deletionCandidateVertices) {
           deleteAllTraits(deletionCandidateVertex);
           deleteTypeVertex(deletionCandidateVertex, false);
       }
    }

    protected abstract void deleteEdge(AtlasEdge edge, boolean force) throws AtlasException;

    /**
     * Deletes a type vertex - can be entity(class type) or just vertex(struct/trait type)
     * @param instanceVertex
     * @param typeCategory
     * @throws AtlasException
     */
    protected void deleteTypeVertex(AtlasVertex instanceVertex, DataTypes.TypeCategory typeCategory, boolean force) throws AtlasException {
        switch (typeCategory) {
        case STRUCT:
        case TRAIT:
            deleteTypeVertex(instanceVertex, force);
            break;

        case CLASS:
            deleteEntities(Collections.singletonList(instanceVertex));
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
    protected void deleteTypeVertex(AtlasVertex instanceVertex, boolean force) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting {}", string(instanceVertex));
        }

        String typeName = GraphHelper.getTypeName(instanceVertex);
        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        FieldMapping fieldMapping = getFieldMapping(type);

        for (AttributeInfo attributeInfo : fieldMapping.fields.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting attribute {} for {}", attributeInfo.name, string(instanceVertex));
            }

            String edgeLabel = GraphHelper.getEdgeLabel(type, attributeInfo);

            switch (attributeInfo.dataType().getTypeCategory()) {
            case CLASS:
                //If its class attribute, delete the reference
                deleteEdgeReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.CLASS, attributeInfo.isComposite);
                break;

            case STRUCT:
                //If its struct attribute, delete the reference
                deleteEdgeReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.STRUCT, false);
                break;

            case ARRAY:
                //For array attribute, if the element is struct/class, delete all the references
                IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
                DataTypes.TypeCategory elementTypeCategory = elementType.getTypeCategory();
                if (elementTypeCategory == DataTypes.TypeCategory.STRUCT ||
                        elementTypeCategory == DataTypes.TypeCategory.CLASS) {
                    Iterator<AtlasEdge> edges = graphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
                    if (edges != null) {
                        while (edges.hasNext()) {
                            AtlasEdge edge = edges.next();
                            deleteEdgeReference(edge, elementType.getTypeCategory(), attributeInfo.isComposite, false);
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
                    List<String> keys = GraphHelper.getListProperty(instanceVertex, propertyName);
                    if (keys != null) {
                        for (String key : keys) {
                            String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                            deleteEdgeReference(instanceVertex, mapEdgeLabel, valueTypeCategory, attributeInfo.isComposite);
                        }
                    }
                }
            }
        }

        deleteVertex(instanceVertex, force);
    }

    /**
     * Force delete is used to remove struct/trait in case of entity updates
     * @param edge
     * @param typeCategory
     * @param isComposite
     * @param forceDeleteStructTrait
     * @return returns true if the edge reference is hard deleted
     * @throws AtlasException
     */
    public boolean deleteEdgeReference(AtlasEdge edge, DataTypes.TypeCategory typeCategory, boolean isComposite,
                                    boolean forceDeleteStructTrait) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting {}", string(edge));
        }

        boolean forceDelete =
                (typeCategory == DataTypes.TypeCategory.STRUCT || typeCategory == DataTypes.TypeCategory.TRAIT) && forceDeleteStructTrait;
        if (typeCategory == DataTypes.TypeCategory.STRUCT || typeCategory == DataTypes.TypeCategory.TRAIT
                || (typeCategory == DataTypes.TypeCategory.CLASS && isComposite)) {
            //If the vertex is of type struct/trait, delete the edge and then the reference vertex as the vertex is not shared by any other entities.
            //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
            //through this delete, hence delete the edge and the reference vertex.
            AtlasVertex vertexForDelete = edge.getInVertex();

            //If deleting the edge and then the in vertex, reverse attribute shouldn't be updated
            deleteEdge(edge, false, forceDelete);
            deleteTypeVertex(vertexForDelete, typeCategory, forceDelete);
        } else {
            //If the vertex is of type class, and its not a composite attributes, the reference AtlasVertex' lifecycle is not controlled
            //through this delete. Hence just remove the reference edge. Leave the reference AtlasVertex as is

            //If deleting just the edge, reverse attribute should be updated for any references
            //For example, for the department type system, if the person's manager edge is deleted, subordinates of manager should be updated
            deleteEdge(edge, true, false);
        }
        return !softDelete || forceDelete;
    }

    public void deleteEdgeReference(AtlasVertex outVertex, String edgeLabel, DataTypes.TypeCategory typeCategory,
                                    boolean isComposite) throws AtlasException {
        AtlasEdge edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);
        if (edge != null) {
            deleteEdgeReference(edge, typeCategory, isComposite, false);
        }
    }

    protected void deleteEdge(AtlasEdge edge, boolean updateReverseAttribute, boolean force) throws AtlasException {
        //update reverse attribute
        if (updateReverseAttribute) {
            AttributeInfo attributeInfo = getAttributeForEdge(edge.getLabel());
            if (attributeInfo.reverseAttributeName != null) {
                deleteEdgeBetweenVertices(edge.getInVertex(), edge.getOutVertex(),
                        attributeInfo.reverseAttributeName);
            }
        }

        deleteEdge(edge, force);
    }

    protected void deleteVertex(AtlasVertex instanceVertex, boolean force) throws AtlasException {
        //Update external references(incoming edges) to this vertex
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));
        }

        for (AtlasEdge edge : (Iterable<AtlasEdge>) instanceVertex.getEdges(AtlasEdgeDirection.IN)) {
            Id.EntityState edgeState = GraphHelper.getState(edge);
            if (edgeState == Id.EntityState.ACTIVE) {
                //Delete only the active edge references
                AttributeInfo attribute = getAttributeForEdge(edge.getLabel());
                //TODO use delete edge instead??
                deleteEdgeBetweenVertices(edge.getOutVertex(), edge.getInVertex(), attribute.name);
            }
        }
        _deleteVertex(instanceVertex, force);
    }

    protected abstract void _deleteVertex(AtlasVertex instanceVertex, boolean force);

    /**
     * Deletes the edge between outvertex and inVertex. The edge is for attribute attributeName of outVertex
     * @param outVertex
     * @param inVertex
     * @param attributeName
     * @throws AtlasException
     */
    protected void deleteEdgeBetweenVertices(AtlasVertex outVertex, AtlasVertex inVertex, String attributeName) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex),
                    attributeName);
        }

        String typeName = GraphHelper.getTypeName(outVertex);
        String outId = GraphHelper.getGuid(outVertex);
        Id.EntityState state = GraphHelper.getState(outVertex);
        if ((outId != null && RequestContext.get().isDeletedEntity(outId)) || state == Id.EntityState.DELETED) {
            //If the reference vertex is marked for deletion, skip updating the reference
            return;
        }

        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        AttributeInfo attributeInfo = getFieldMapping(type).fields.get(attributeName);
        String propertyName = GraphHelper.getQualifiedFieldName(type, attributeName);
        String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
        AtlasEdge edge = null;

        switch (attributeInfo.dataType().getTypeCategory()) {
        case CLASS:
            //If its class attribute, its the only edge between two vertices
            if (attributeInfo.multiplicity.nullAllowed()) {
                edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);
                if (shouldUpdateReverseAttribute) {
                    GraphHelper.setProperty(outVertex, propertyName, null);
                }
            } else {
                // Cannot unset a required attribute.
                throw new NullRequiredAttributeException("Cannot unset required attribute " + GraphHelper.getQualifiedFieldName(type, attributeName) +
                    " on " + GraphHelper.getVertexDetails(outVertex) + " edge = " + edgeLabel);
            }
            break;

        case ARRAY:
            //If its array attribute, find the right edge between the two vertices and update array property
            List<String> elements = GraphHelper.getListProperty(outVertex, propertyName);
            if (elements != null) {
                elements = new ArrayList<>(elements);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String elementEdgeId : elements) {
                    AtlasEdge elementEdge = graphHelper.getEdgeByEdgeId(outVertex, edgeLabel, elementEdgeId);
                    if (elementEdge == null) {
                        continue;
                    }

                    AtlasVertex elementVertex = elementEdge.getInVertex();
                    if (elementVertex.equals(inVertex)) {
                        edge = elementEdge;

                        //TODO element.size includes deleted items as well. should exclude
                        if (!attributeInfo.multiplicity.nullAllowed()
                                && elements.size() <= attributeInfo.multiplicity.lower) {
                            // Deleting this edge would violate the attribute's lower bound.
                            throw new NullRequiredAttributeException(
                                    "Cannot remove array element from required attribute " +
                                            GraphHelper.getQualifiedFieldName(type, attributeName) + " on "
                                            + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(elementEdge));
                        }

                        if (shouldUpdateReverseAttribute) {
                            //if composite attribute, remove the reference as well. else, just remove the edge
                            //for example, when table is deleted, process still references the table
                            //but when column is deleted, table will not reference the deleted column
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Removing edge {} from the array attribute {}", string(elementEdge),
                                        attributeName);
                            }

                            // Remove all occurrences of the edge ID from the list.
                            // This prevents dangling edge IDs (i.e. edge IDs for deleted edges)
                            // from the remaining in the list if there are duplicates.
                            elements.removeAll(Collections.singletonList(elementEdge.getId().toString()));
                            GraphHelper.setProperty(outVertex, propertyName, elements);
                            break;

                        }
                    }
                }
            }
            break;

        case MAP:
            //If its map attribute, find the right edge between two vertices and update map property
            List<String> keys = GraphHelper.getListProperty(outVertex, propertyName);
            if (keys != null) {
                keys = new ArrayList<>(keys);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String key : keys) {
                    String keyPropertyName = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                    String mapEdgeId = GraphHelper.getSingleValuedProperty(outVertex, keyPropertyName, String.class);
                    AtlasEdge mapEdge = graphHelper.getEdgeByEdgeId(outVertex, keyPropertyName, mapEdgeId);
                    if(mapEdge != null) {
                        AtlasVertex mapVertex = mapEdge.getInVertex();
                        if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                            //TODO keys.size includes deleted items as well. should exclude
                            if (attributeInfo.multiplicity.nullAllowed() || keys.size() > attributeInfo.multiplicity.lower) {
                                edge = mapEdge;
                            } else {
                                // Deleting this entry would violate the attribute's lower bound.
                                throw new NullRequiredAttributeException(
                                        "Cannot remove map entry " + keyPropertyName + " from required attribute " +
                                                GraphHelper.getQualifiedFieldName(type, attributeName) + " on " + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(mapEdge));
                            }

                            if (shouldUpdateReverseAttribute) {
                                //remove this key
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Removing edge {}, key {} from the map attribute {}", string(mapEdge), key,
                                            attributeName);
                                }

                                keys.remove(key);
                                GraphHelper.setProperty(outVertex, propertyName, keys);
                                GraphHelper.setProperty(outVertex, keyPropertyName, null);
                            }
                            break;
                        }
                    }
                }
            }
            break;

        case STRUCT:
        case TRAIT:
            break;

        default:
            throw new IllegalStateException("There can't be an edge from " + GraphHelper.getVertexDetails(outVertex) + " to "
                    + GraphHelper.getVertexDetails(inVertex) + " with attribute name " + attributeName + " which is not class/array/map attribute");
        }

        if (edge != null) {
            deleteEdge(edge, false);
            RequestContext requestContext = RequestContext.get();
            GraphHelper.setProperty(outVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                    requestContext.getRequestTime());
            GraphHelper.setProperty(outVertex, Constants.MODIFIED_BY_KEY, requestContext.getUser());
            requestContext.recordEntityUpdate(outId);
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
    private void deleteAllTraits(AtlasVertex instanceVertex) throws AtlasException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting traits {} for {}", traitNames, string(instanceVertex));
        }

        String typeName = GraphHelper.getTypeName(instanceVertex);

        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(typeName, traitNameToBeDeleted);
            deleteEdgeReference(instanceVertex, relationshipLabel, DataTypes.TypeCategory.TRAIT, false);
        }
    }
}
