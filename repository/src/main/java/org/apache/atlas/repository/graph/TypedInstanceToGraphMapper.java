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

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IConstructableType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.TypeUtils.Pair;
import org.apache.atlas.utils.MD5Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TypedInstanceToGraphMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    private final Map<Id, Vertex> idToVertexMap = new HashMap<>();
    //Maintains a set of Guid based Ids that are referenced/created during graph walk
    private final Set<Id> referencedIds = new HashSet<>();
    private final TypeSystem typeSystem = TypeSystem.getInstance();
    private final List<String> deletedEntityGuids = new ArrayList<>();
    private final List<ITypedReferenceableInstance> deletedEntities = new ArrayList<>();
    private final GraphToTypedInstanceMapper graphToTypedInstanceMapper;
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final String SIGNATURE_HASH_PROPERTY_KEY = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "signature";

    public enum Operation {
        CREATE,
        UPDATE_PARTIAL,
        UPDATE_FULL,
        DELETE
    }

    public TypedInstanceToGraphMapper(GraphToTypedInstanceMapper graphToTypedInstanceMapper) {
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
    }

    TypeUtils.Pair<List<String>, List<String>> mapTypedInstanceToGraph(Operation operation, ITypedReferenceableInstance... typedInstances)
        throws AtlasException {

        List<String> createdIds = new ArrayList<>();
        List<String> updatedIds = new ArrayList<>();

        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            Collection<IReferenceableInstance> newInstances = walkClassInstances(typedInstance);
            TypeUtils.Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> instancesPair =
                    createVerticesAndDiscoverInstances(newInstances);

            switch (operation) {
                case CREATE:
                    List<String> ids = addOrUpdateAttributesAndTraits(operation, instancesPair.left);
                    createdIds.addAll(ids);
                    addFullTextProperty(instancesPair.left);
                    break;

                case UPDATE_FULL:
                case UPDATE_PARTIAL:
                    ids = addOrUpdateAttributesAndTraits(Operation.CREATE, instancesPair.left);
                    createdIds.addAll(ids);
                    ids = addOrUpdateAttributesAndTraits(operation, instancesPair.right);
                    updatedIds.addAll(ids);

                    addFullTextProperty(instancesPair.left);
                    addFullTextProperty(instancesPair.right);
                    break;

                default:
                    throw new UnsupportedOperationException("Not handled - " + operation);
            }
        }
        return TypeUtils.Pair.of(createdIds, updatedIds);
    }

    private Collection<IReferenceableInstance> walkClassInstances(ITypedReferenceableInstance typedInstance)
            throws RepositoryException {

        EntityProcessor entityProcessor = new EntityProcessor();
        try {
            LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
            new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        entityProcessor.addInstanceIfNotExists(typedInstance);
        return entityProcessor.getInstances();
    }

    private List<String> addOrUpdateAttributesAndTraits(Operation operation, List<ITypedReferenceableInstance> instances) throws AtlasException {
        List<String> guids = new ArrayList<>();
        for (ITypedReferenceableInstance instance : instances) {
            try {
                //new vertex, set all the properties
                String guid = addOrUpdateAttributesAndTraits(operation, instance);
                guids.add(guid);
            } catch (SchemaViolationException e) {
                throw new EntityExistsException(instance, e);
            }
        }
        return guids;
    }

    private String addOrUpdateAttributesAndTraits(Operation operation, ITypedReferenceableInstance typedInstance)
            throws AtlasException {
        LOG.debug("Adding/Updating typed instance {}", typedInstance.getTypeName());

        Id id = typedInstance.getId();
        if (id == null) { // oops
            throw new RepositoryException("id cannot be null");
        }

        Vertex instanceVertex = idToVertexMap.get(id);

        // add the attributes for the instance
        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
        final Map<String, AttributeInfo> fields = classType.fieldMapping().fields;

        mapInstanceToVertex(typedInstance, instanceVertex, fields, false, operation);

        if (Operation.CREATE.equals(operation)) {
            //TODO - Handle Trait updates
            addTraits(typedInstance, instanceVertex, classType);
        } else if (Operation.UPDATE_FULL.equals(operation) || Operation.UPDATE_PARTIAL.equals(operation)) {
            GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.valueOf(System.currentTimeMillis()));
        }
        return getId(typedInstance)._getId();
    }

    void mapInstanceToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                             Map<String, AttributeInfo> fields, boolean mapOnlyUniqueAttributes, Operation operation)
            throws AtlasException {
        
        LOG.debug("Mapping instance {} of {} to vertex {}", typedInstance, typedInstance.getTypeName(),
                instanceVertex);
        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            mapAttributesToVertex(typedInstance, instanceVertex, attributeInfo, operation);
        }
        
        if (operation == Operation.DELETE) {
            // Remove uni-directional references to the deletion candidate.
            removeUnidirectionalReferences(instanceVertex);
            
            // Remove vertex for deletion candidate.
            graphHelper.removeVertex(instanceVertex);
        }

    }

    private String getInstanceName(Vertex referencingVertex, IConstructableType referencingType) {

        if (referencingType.getTypeCategory() == TypeCategory.CLASS) {
            Id idFromVertex = GraphHelper.getIdFromVertex(referencingType.getName(), referencingVertex);
            String instanceId = referencingType.getName() + ":" + idFromVertex._getId();
            return instanceId;
        }
        else {
            return referencingType.getName();
        }
    }

    void mapAttributesToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                               AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);
        LOG.debug("mapping attribute {} = {}", attributeInfo.name, attrValue);

        if (attrValue != null  || operation == Operation.UPDATE_FULL || operation == Operation.DELETE) {
            switch (attributeInfo.dataType().getTypeCategory()) {
                case PRIMITIVE:
                case ENUM:
                    if (operation != Operation.DELETE) {
                        mapPrimitiveOrEnumToVertex(typedInstance, instanceVertex, attributeInfo);
                    }
                    break;

                case ARRAY:
                    mapArrayCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                    break;

                case MAP:
                    mapMapCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                    break;

                case STRUCT:
                case CLASS:
                    final String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
                    String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);
                    Iterator<Edge> outGoingEdgesIterator =
                            GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel).iterator();
                    String currentEntry =
                            outGoingEdgesIterator.hasNext() ? outGoingEdgesIterator.next().getId().toString() : null;
                    addOrUpdateCollectionEntry(instanceVertex, attributeInfo, attributeInfo.dataType(), attrValue,
                            currentEntry, propertyName, operation);
                    break;

                case TRAIT:
                    // do NOTHING - this is taken care of earlier
                    break;

                default:
                    throw new IllegalArgumentException("Unknown type category: " + attributeInfo.dataType().getTypeCategory());
            }
        }
    }

    private TypeUtils.Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> createVerticesAndDiscoverInstances(
            Collection<IReferenceableInstance> instances) throws AtlasException {

        List<ITypedReferenceableInstance> instancesToCreate = new ArrayList<>();
        List<ITypedReferenceableInstance> instancesToUpdate = new ArrayList<>();

        for (IReferenceableInstance instance : instances) {
            LOG.debug("Discovering instance to create/update for {}", instance);
            ITypedReferenceableInstance newInstance;
            Id id = instance.getId();

            if (!idToVertexMap.containsKey(id)) {
                Vertex instanceVertex;
                if (id.isAssigned()) {  // has a GUID
                    LOG.debug("Instance {} has an assigned id", instance.getId()._getId());
                    instanceVertex = graphHelper.getVertexForGUID(id.id);
                    if (!(instance instanceof ReferenceableInstance)) {
                        throw new IllegalStateException(
                                String.format("%s is not of type ITypedReferenceableInstance", instance));
                    }
                    newInstance = (ITypedReferenceableInstance) instance;
                    instancesToUpdate.add(newInstance);

                } else {
                    //Check if there is already an instance with the same unique attribute value
                    ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());
                    instanceVertex = graphHelper.getVertexForInstanceByUniqueAttribute(classType, instance);

                    //no entity with the given unique attribute, create new
                    if (instanceVertex == null) {
                        LOG.debug("Creating new vertex for instance {}", instance);
                        newInstance = classType.convert(instance, Multiplicity.REQUIRED);
                        instanceVertex = graphHelper.createVertexWithIdentity(newInstance, classType.getAllSuperTypeNames());
                        instancesToCreate.add(newInstance);

                        //Map only unique attributes for cases of circular references
                        mapInstanceToVertex(newInstance, instanceVertex, classType.fieldMapping().fields, true, Operation.CREATE);

                    } else {
                        LOG.debug("Re-using existing vertex {} for instance {}", instanceVertex.getId(), instance);
                        if (!(instance instanceof ReferenceableInstance)) {
                            throw new IllegalStateException(
                                    String.format("%s is not of type ITypedReferenceableInstance", instance));
                        }
                        newInstance = (ITypedReferenceableInstance) instance;
                        instancesToUpdate.add(newInstance);
                    }

                }

                //Set the id in the new instance
                idToVertexMap.put(id, instanceVertex);
                referencedIds.add(GraphHelper.getIdFromVertex(instance.getTypeName(), instanceVertex));

            }
        }
        return TypeUtils.Pair.of(instancesToCreate, instancesToUpdate);
    }

    private void addFullTextProperty(List<ITypedReferenceableInstance> instances) throws AtlasException {
        FullTextMapper fulltextMapper = new FullTextMapper(graphToTypedInstanceMapper);
        for (ITypedReferenceableInstance typedInstance : instances) { // Traverse
            Vertex instanceVertex = getClassVertex(typedInstance);
            String fullText = fulltextMapper.mapRecursive(instanceVertex, true);
            GraphHelper.setProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
        }
    }

    private void addTraits(ITypedReferenceableInstance typedInstance, Vertex instanceVertex, ClassType classType)
            throws AtlasException {
        for (String traitName : typedInstance.getTraits()) {
            LOG.debug("mapping trait {}", traitName);
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            // add the attributes for the trait instance
            mapTraitInstanceToVertex(traitInstance, classType, instanceVertex);
        }
    }

    /******************************************** STRUCT **************************************************/

    private TypeUtils.Pair<Vertex, Edge> updateStructVertex(ITypedStruct structInstance, Edge relEdge,
                                                            Operation operation) throws AtlasException {
        //Already existing vertex. Update
        Vertex structInstanceVertex = relEdge.getVertex(Direction.IN);

        // Update attributes
        final MessageDigest digester = MD5Utils.getDigester();
        String newSignature = structInstance.getSignatureHash(digester);
        String curSignature = structInstanceVertex.getProperty(SIGNATURE_HASH_PROPERTY_KEY);

        if (!newSignature.equals(curSignature)) {
            //Update struct vertex instance only if there is a change
            LOG.debug("Updating struct {} since signature has changed {} {} ", structInstance, curSignature, newSignature);
            mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false, operation);
            GraphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(newSignature));
        }
        return TypeUtils.Pair.of(structInstanceVertex, relEdge);
    }

    private TypeUtils.Pair<Vertex, Edge> addStructVertex(ITypedStruct structInstance, Vertex instanceVertex,
                                                         AttributeInfo attributeInfo, String edgeLabel) throws AtlasException {
        // add a new vertex for the struct or trait instance
        Vertex structInstanceVertex = graphHelper.createVertexWithoutIdentity(structInstance.getTypeName(), null,
                Collections.<String>emptySet()); // no super types for struct type
        LOG.debug("created vertex {} for struct {} value {}", structInstanceVertex, attributeInfo.name, structInstance);

        // map all the attributes to this new vertex
        mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false, Operation.CREATE);
        // add an edge to the newly created vertex from the parent
        Edge relEdge = graphHelper.addEdge(instanceVertex, structInstanceVertex, edgeLabel);

        return TypeUtils.Pair.of(structInstanceVertex, relEdge);
    }

    /******************************************** ARRAY **************************************************/

    private void mapArrayCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                attributeInfo.name);
        List newElements = (List) typedInstance.get(attributeInfo.name);
        boolean empty = (newElements == null || newElements.isEmpty());
        if (!empty  || operation == Operation.UPDATE_FULL || operation == Operation.DELETE) {
            String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
            List<String> currentEntries = instanceVertex.getProperty(propertyName);

            IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
            List<String> newEntries = new ArrayList<>();
            if (newElements != null && !newElements.isEmpty()) {
                int index = 0;
                for (; index < newElements.size(); index++) {
                    String currentEntry =
                            (currentEntries != null && index < currentEntries.size()) ? currentEntries.get(index) : null;
                    String newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                            newElements.get(index), currentEntry, propertyName, operation);
                    newEntries.add(newEntry);
                }

                //Remove extra entries in the list
                if (currentEntries != null) {
                    if (index < currentEntries.size()) {
                        for (; index < currentEntries.size(); index++) {
                            if (elementType.getTypeCategory() == TypeCategory.CLASS) {
                                final String edgeId = currentEntries.get(index);
                                final Pair<Edge, Vertex> edgeAndTargetVertex = GraphHelper.getInstance().getEdgeAndTargetVertex(edgeId);
                                Id guid = GraphHelper.getIdFromVertex(elementType.getName(), edgeAndTargetVertex.right);
                                removeUnusedClassReference(edgeId, attributeInfo, elementType, !referencedIds.contains(guid));
                            } else if (elementType.getTypeCategory() == TypeCategory.STRUCT) {
                                removeUnusedStructReference(currentEntries.get(index), attributeInfo, elementType);
                            }
                        }
                    }
                }
            }
            else if (operation == Operation.UPDATE_FULL || operation == Operation.DELETE) {
                // Clear all existing entries
                if (currentEntries != null) {
                    for (String entry : currentEntries) {
                        if (elementType.getTypeCategory() == TypeCategory.CLASS) {
                            removeUnusedClassReference(entry, attributeInfo, elementType, true);
                        } else if(elementType.getTypeCategory() == TypeCategory.STRUCT) {
                            removeUnusedStructReference(entry, attributeInfo, elementType);
                        }
                    }
                }
            }

            // for dereference on way out
            GraphHelper.setProperty(instanceVertex, propertyName, newEntries);
        }
    }

    /******************************************** MAP **************************************************/

    private void mapMapCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                attributeInfo.name);
        @SuppressWarnings("unchecked") Map<Object, Object> collection =
            (Map<Object, Object>) typedInstance.get(attributeInfo.name);
        boolean empty = (collection == null || collection.isEmpty());
        if (!empty  || operation == Operation.UPDATE_FULL || operation == Operation.DELETE) {

            String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
            IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();

            if (!empty) {
                for (Map.Entry entry : collection.entrySet()) {
                    String myPropertyName = propertyName + "." + entry.getKey().toString();

                    String currentEntry = instanceVertex.getProperty(myPropertyName);
                    String newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                            entry.getValue(), currentEntry, myPropertyName, operation);

                    //Add/Update/Remove property value
                    GraphHelper.setProperty(instanceVertex, myPropertyName, newEntry);
                }
            }
            
            //Remove unused key references
            List<Object> origKeys = instanceVertex.getProperty(propertyName);
            if (origKeys != null) {
                if (collection != null) {
                    origKeys.removeAll(collection.keySet());
                }
                for (Object unusedKey : origKeys) {
                    String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo) + "." + unusedKey;
                    if (instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().hasNext()) {
                        Edge edge = instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().next();
                        if (TypeCategory.STRUCT.equals(((DataTypes.MapType) attributeInfo.dataType()).getValueType().getTypeCategory())) {
                           removeUnusedStructReference(edge.getId().toString(), attributeInfo,
                               ((DataTypes.MapType) attributeInfo.dataType()).getValueType());
                        } else if(TypeCategory.CLASS.equals(((DataTypes.MapType) attributeInfo.dataType()).getValueType().getTypeCategory())){
                            final Vertex targetVertex = edge.getVertex(Direction.OUT);
                            Id guid = GraphHelper.getIdFromVertex(elementType.getName(), targetVertex);
                            removeUnusedClassReference(edge.getId().toString(), attributeInfo, elementType, !referencedIds.contains(guid));                        }
                    }
                }
            }

            // for dereference on way out
            GraphHelper.setProperty(instanceVertex, propertyName, collection == null ? null : new ArrayList(collection.keySet()));
        }
    }

    /******************************************** ARRAY & MAP **************************************************/

    private String addOrUpdateCollectionEntry(Vertex instanceVertex, AttributeInfo attributeInfo,
                                              IDataType elementType, Object newVal, String curVal, String propertyName,
                                              Operation operation)
        throws AtlasException {

        final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return newVal != null ? newVal.toString() : null;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            return null;

        case STRUCT:
            return addOrUpdateStruct(instanceVertex, attributeInfo, elementType, (ITypedStruct) newVal, curVal, edgeLabel, operation);

        case CLASS:
            return addOrUpdateClassVertex(instanceVertex, attributeInfo, elementType,
                    (ITypedReferenceableInstance) newVal, curVal, edgeLabel, operation);

        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private String addOrUpdateStruct(Vertex instanceVertex, AttributeInfo attributeInfo, IDataType elementType,
                                     ITypedStruct structAttr, String curVal,
                                     String edgeLabel, Operation operation) throws AtlasException {
        TypeUtils.Pair<Vertex, Edge> vertexEdgePair = null;
        if (curVal != null && structAttr == null) {
            //remove edge
            removeUnusedStructReference(curVal, attributeInfo, elementType);
        } else if (curVal != null && structAttr != null) {
            //update
            Edge edge = graphHelper.getOutGoingEdgeById(curVal);
            vertexEdgePair = updateStructVertex(structAttr, edge, operation);
        } else if (structAttr != null) {
            //add
            vertexEdgePair = addStructVertex(structAttr, instanceVertex, attributeInfo, edgeLabel);
        }

        return (vertexEdgePair != null) ? vertexEdgePair.right.getId().toString() : null;
    }

    private String addOrUpdateClassVertex(Vertex instanceVertex, AttributeInfo attributeInfo, IDataType elementType,
                                          ITypedReferenceableInstance newVal, String curVal,
                                          String edgeLabel, Operation operation) throws AtlasException {
        Vertex toVertex = getClassVertex(newVal);
        if(toVertex == null && newVal != null) {
            LOG.error("Could not find vertex for Class Reference " + newVal);
            throw new EntityNotFoundException("Could not find vertex for Class Reference " + newVal);
        }

        TypeUtils.Pair<Vertex, Edge> vertexEdgePair = null;
        if (curVal != null && newVal == null) {
            //remove edge
            removeUnusedClassReference(curVal, attributeInfo, elementType, true);
        } else if (curVal != null && newVal != null) {
            Edge edge = graphHelper.getOutGoingEdgeById(curVal);
            Id classRefId = getId(newVal);
            vertexEdgePair = updateClassEdge(classRefId, newVal, instanceVertex, edge, toVertex, attributeInfo,
                    elementType, edgeLabel, operation);
        } else if (newVal != null){
            vertexEdgePair = addClassEdge(instanceVertex, toVertex, edgeLabel);
        }

        return (vertexEdgePair != null) ? vertexEdgePair.right.getId().toString() : null;
    }

    /******************************************** CLASS **************************************************/

    private TypeUtils.Pair<Vertex, Edge> addClassEdge(Vertex instanceVertex, Vertex toVertex, String edgeLabel) throws AtlasException {
            // add an edge to the class vertex from the instance
          Edge edge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
          return TypeUtils.Pair.of(toVertex, edge);
    }

    private Vertex getClassVertex(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        Vertex referenceVertex = null;
        Id id = null;
        if (typedReference != null) {
            id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
            if (id.isAssigned()) {
                referenceVertex = graphHelper.getVertexForGUID(id.id);
            } else {
                referenceVertex = idToVertexMap.get(id);
            }
        }

        return referenceVertex;
    }

    private Id getId(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        if (typedReference == null) {
            throw new IllegalArgumentException("typedReference must be non-null");
        }
        Id id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();

        if (id.isUnassigned()) {
            Vertex classVertex = idToVertexMap.get(id);
            String guid = classVertex.getProperty(Constants.GUID_PROPERTY_KEY);
            id = new Id(guid, 0, typedReference.getTypeName());
        }
        return id;
    }


    private TypeUtils.Pair<Vertex, Edge> updateClassEdge(Id id, final ITypedReferenceableInstance typedInstance,
                                               Vertex instanceVertex, Edge edge, Vertex toVertex,
                                               AttributeInfo attributeInfo, IDataType dataType,
                                               String edgeLabel, Operation operation) throws AtlasException {
        TypeUtils.Pair<Vertex, Edge> result = TypeUtils.Pair.of(toVertex, edge);
        Edge newEdge = edge;
        // Update edge if it exists
        Vertex invertex = edge.getVertex(Direction.IN);
        String currentGUID = invertex.getProperty(Constants.GUID_PROPERTY_KEY);
        Id currentId = new Id(currentGUID, 0, (String) invertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY));
        if (!currentId.equals(id)) {
            // add an edge to the class vertex from the instance
            if (toVertex != null) {
                newEdge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
                result = TypeUtils.Pair.of(toVertex, newEdge);
            }

            //Delete vertex only if the IdtoVertex map doesnt have it in future references
            removeUnusedClassReference(edge.getId().toString(), attributeInfo, dataType, !referencedIds.contains(currentId));
        }


        if (attributeInfo.isComposite) {
            //Update the attributes also if composite
            if (typedInstance.fieldMapping() != null) {
                //In case of Id instance, fieldMapping is null
                mapInstanceToVertex(typedInstance, toVertex, typedInstance.fieldMapping().fields , false, operation);
                //Update full text for the updated composite vertex
                addFullTextProperty(new ArrayList<ITypedReferenceableInstance>() {{ add(typedInstance); }});
            }
        }

        return result;
    }

    /******************************************** TRAITS ****************************************************/

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, IDataType entityType, Vertex parentInstanceVertex)
        throws AtlasException {
        // add a new vertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        Vertex traitInstanceVertex = graphHelper.createVertexWithoutIdentity(traitInstance.getTypeName(), null,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

        // map all the attributes to this newly created vertex
        mapInstanceToVertex(traitInstance, traitInstanceVertex, traitInstance.fieldMapping().fields, false, Operation.CREATE);

        // add an edge to the newly created vertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        graphHelper.addEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
    }

    /******************************************** PRIMITIVES **************************************************/

    private void mapPrimitiveOrEnumToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                            AttributeInfo attributeInfo) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);

        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        Object propertyValue = null;

        if (attrValue == null ) {
            propertyValue = null;
        } else if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            propertyValue = typedInstance.getString(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            propertyValue = typedInstance.getShort(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            propertyValue = typedInstance.getInt(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            propertyValue = typedInstance.getBigInt(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            propertyValue = typedInstance.getBoolean(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            propertyValue = typedInstance.getByte(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            propertyValue = typedInstance.getLong(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            propertyValue = typedInstance.getFloat(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            propertyValue = typedInstance.getDouble(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            propertyValue = typedInstance.getBigDecimal(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Date dateVal = typedInstance.getDate(attributeInfo.name);
            //Convert Property value to Long  while persisting
            propertyValue = dateVal.getTime();
        } else if (attributeInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            if (attrValue != null) {
                propertyValue = ((EnumValue)attrValue).value;
            }
        }


        GraphHelper.setProperty(instanceVertex, vertexPropertyName, propertyValue);
    }

    private Edge removeUnusedClassReference(String edgeId, AttributeInfo attributeInfo, IDataType<?> elementType, boolean deleteReferredVertex) throws AtlasException {
        // Remove edge to disconnect struct or class reference.
        // For struct or composite class reference, also delete the target instance.
        Edge removedRelation = null;
        TypeUtils.Pair<Edge, Vertex> edgeAndVertex = graphHelper.getEdgeAndTargetVertex(edgeId);
        if (attributeInfo.isComposite) {
            // For uni-directional reference, remove the edge.
            // For bi-directional reference, the edges are removed
            // when the composite entity is deleted.
            if (attributeInfo.reverseAttributeName == null) {
                graphHelper.removeEdge(edgeAndVertex.left);
                removedRelation = edgeAndVertex.left;
            }

            // Delete the contained entity.
            if (deleteReferredVertex) {
                if (LOG.isDebugEnabled()) {
                    Vertex sourceVertex = edgeAndVertex.left.getVertex(Direction.OUT);
                    String sourceTypeName = GraphHelper.getTypeName(sourceVertex);
                    LOG.debug("Deleting composite entity {}:{} contained by {}:{} through reference {}",
                        elementType.getName(), GraphHelper.getIdFromVertex(elementType.getName(), edgeAndVertex.right)._getId(),
                        sourceTypeName, GraphHelper.getIdFromVertex(sourceTypeName, sourceVertex)._getId(),
                        attributeInfo.name);
                }
                deleteEntity(elementType.getName(), edgeAndVertex.right);
            }
        }
        else {
            if (attributeInfo.reverseAttributeName != null) {
                // Disconnect both ends of the bi-directional reference
                removeReverseReference(edgeAndVertex, attributeInfo);
            }
            graphHelper.removeEdge(edgeAndVertex.left);
            removedRelation = edgeAndVertex.left;

            return removedRelation;
        }
        return removedRelation;

    }

    private Edge removeUnusedStructReference(String edgeId, AttributeInfo attributeInfo, IDataType<?> elementType) throws AtlasException {
        // Remove edge to disconnect struct or class reference.
        // For struct or composite class reference, also delete the target instance.
        Edge removedRelation = null;
        TypeUtils.Pair<Edge, Vertex> edgeAndVertex = graphHelper.getEdgeAndTargetVertex(edgeId);
        graphHelper.removeEdge(edgeAndVertex.left);
        removedRelation = edgeAndVertex.left;

            // Create an empty instance to use for clearing all struct attributes.
        StructType structType = (StructType) elementType;
        ITypedStruct typedInstance = structType.createInstance();
            
         //  Delete target vertex and any underlying structs and composite entities owned by this struct.
        mapInstanceToVertex(typedInstance, edgeAndVertex.right, structType.fieldMapping().fields, false, Operation.DELETE);
        return removedRelation;
    }

    /**
     * Remove the reverse reference value for the specified edge and vertex.
     * 
     * @param edgeAndVertex
     * @param attributeInfo
     * @throws AtlasException
     */
    private void removeReverseReference(TypeUtils.Pair<Edge, Vertex> edgeAndVertex, 
        AttributeInfo attributeInfo) throws AtlasException {
        
        Vertex sourceVertex = edgeAndVertex.left.getVertex(Direction.OUT);
        String inverseTypeName = GraphHelper.getTypeName(edgeAndVertex.right);
        IConstructableType inverseType = typeSystem.getDataType(IConstructableType.class, inverseTypeName);
        AttributeInfo inverseAttributeInfo = inverseType.fieldMapping().fields.get(attributeInfo.reverseAttributeName);
        String inverseEdgeLabel = GraphHelper.getEdgeLabel(inverseType, inverseAttributeInfo);
        TypeCategory inverseTypeCategory = inverseAttributeInfo.dataType().getTypeCategory();
    
        // Find and remove the edge which represents the inverse reference value.
        Iterable<Edge> inverseEdges = GraphHelper.getOutGoingEdgesByLabel(edgeAndVertex.right, inverseEdgeLabel);
        Edge removedEdge = null;
        // Search for the edge which references the source vertex.
        for (Edge edge : inverseEdges) {
            Vertex vertex = edge.getVertex(Direction.IN);
            if (vertex.equals(sourceVertex)) {
                // Found the edge which points back at source vertex.
                // Disconnect the reference by removing the edge and
                // removing the edge ID from the vertex property.
                removeReferenceValue(edge, new AtlasEdgeLabel(edge.getLabel()), edgeAndVertex.right, inverseType, inverseTypeCategory);
                removedEdge = edge;
                break;
            }
        }
        if (removedEdge != null) {
            if (LOG.isDebugEnabled()) {
                String sourceTypeName = GraphHelper.getTypeName(sourceVertex);
                LOG.debug("Removed edge {} for reverse reference {} from {}:{} to {}:{} ", removedEdge,
                    GraphHelper.getQualifiedFieldName(inverseType, inverseAttributeInfo.name), 
                    inverseTypeName, GraphHelper.getIdFromVertex(inverseTypeName, edgeAndVertex.right)._getId(), 
                    sourceTypeName, GraphHelper.getIdFromVertex(sourceTypeName, sourceVertex)._getId());
            }
        }
        else {
            // We didn't find the edge for the inverse reference.
            // Since Atlas currently does not automatically set
            // the inverse reference when a reference value is updated,
            // unbalanced references are not unexpected.
            // The presence of inverse reference values depends on
            // well behaved client applications which explicitly set
            // both ends of the reference.
            // TODO: throw an exception as it indicates a unbalanced reference?
            String sourceTypeName = GraphHelper.getTypeName(sourceVertex);
            LOG.warn("No edge found for inverse reference {} on vertex {} for entity instance {}:{} which points back to vertex {} for {}:{}",
                inverseAttributeInfo.name, edgeAndVertex.right,
                inverseTypeName, GraphHelper.getIdFromVertex(inverseTypeName, edgeAndVertex.right)._getId(), 
                sourceVertex, sourceTypeName, GraphHelper.getIdFromVertex(sourceTypeName, sourceVertex)._getId());
        }
    }

    /**
     * Remove any unidirectional map or array reference to a class, struct, or trait vertex.
     * This involves removing appropriate value from the vertex property which holds the
     * reference values.
     * 
     * @param targetVertex a vertex which represents a class, struct, or trait instance
     * @throws AtlasException
     */
    private void removeUnidirectionalReferences(Vertex targetVertex) throws AtlasException {
    
        // Search for any remaining incoming edges that represent unidirectional references
        // to the target vertex.
        Iterable<Edge> incomingEdges = targetVertex.getEdges(Direction.IN);
        for (Edge edge : incomingEdges) {
            String label = edge.getLabel();
            AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(label);
            Vertex referencingVertex = edge.getVertex(Direction.OUT);
            String typeName = atlasEdgeLabel.getTypeName();
            IConstructableType referencingType = typeSystem.getDataType(IConstructableType.class, typeName);
            
            AttributeInfo attributeInfo = referencingType.fieldMapping().fields.get(atlasEdgeLabel.getAttributeName());
            if (attributeInfo == null) {
                String instanceId = getInstanceName(referencingVertex, referencingType);
                throw new AtlasException("Outgoing edge " + edge.getId().toString()  
                    + " for " + instanceId + "(vertex " + referencingVertex + "): label " + label
                    + " has an attribute name " + atlasEdgeLabel.getAttributeName() + " that is undefined on "
                    + referencingType.getTypeCategory() + " " + typeName);
            }
            // Remove the appropriate value from the vertex property for this reference.
            removeReferenceValue(edge, atlasEdgeLabel, referencingVertex, referencingType, attributeInfo.dataType().getTypeCategory());
        }
    }

    private Pair<String, Boolean> removeReferenceValue(Edge edge, AtlasEdgeLabel atlasEdgeLabel,
            Vertex referencingVertex, IConstructableType referencingType, TypeCategory attrTypeCategory) 
        throws AtlasException {
    
        graphHelper.removeEdge(edge);
        if (attrTypeCategory != TypeCategory.ARRAY && attrTypeCategory != TypeCategory.MAP) {
            // Multiplicity-one reference is represented by the edge,
            // there is no vertex property to update. So just remove the edge.
            return new Pair<String, Boolean>(edge.getId().toString(), Boolean.TRUE);
        }
        List<String> currentRefValues = referencingVertex.getProperty(atlasEdgeLabel.getQualifiedAttributeName());
        List<String> newRefValues = new ArrayList<>(currentRefValues);
        Pair<String, Boolean> refValueRemoved = null;
        if (attrTypeCategory == TypeCategory.ARRAY) {
            refValueRemoved = removeArrayReferenceValue(atlasEdgeLabel, referencingVertex, edge, newRefValues);
        }
        else {
            refValueRemoved = removeMapReferenceValue(atlasEdgeLabel, referencingVertex, edge, newRefValues);
        }
        if (refValueRemoved.right) {
            if (LOG.isDebugEnabled()) {
                String instanceId = getInstanceName(referencingVertex, referencingType);
                LOG.debug("Reference value {} removed from reference {} on vertex {} for instance of {} {}",
                    refValueRemoved.left, atlasEdgeLabel.getAttributeName(), referencingVertex,
                    referencingType.getTypeCategory(), instanceId);
            }
            // If the referencing instance is an entity, update the modification timestamp.
            if (referencingType instanceof ClassType) {
                GraphHelper.setProperty(referencingVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());
            }
        }
        else {
            // The expected value is missing from the reference property values - log a warning.
            String instanceId = getInstanceName(referencingVertex, referencingType);
            LOG.warn("Reference value {} expected but not found in array reference {} on vertex {} for instance of {} {}",
                refValueRemoved.left, atlasEdgeLabel.getAttributeName(), referencingVertex,
                referencingType.getTypeCategory(), instanceId);
        }
        return refValueRemoved;
    }

    private TypeUtils.Pair<String, Boolean> removeArrayReferenceValue(AtlasEdgeLabel atlasEdgeLabel, Vertex referencingVertex, 
        Edge edge, List<String> newRefValues) {
        
        String refValueToRemove = edge.getId().toString();
        boolean valueRemoved = newRefValues.remove(refValueToRemove);
        if (valueRemoved) {
            GraphHelper.setProperty(referencingVertex, atlasEdgeLabel.getQualifiedAttributeName(), newRefValues);
        }
        return new TypeUtils.Pair<String, Boolean>(refValueToRemove, Boolean.valueOf(valueRemoved));
    }

    private TypeUtils.Pair<String, Boolean>  removeMapReferenceValue(AtlasEdgeLabel atlasEdgeLabel, Vertex referencingVertex, 
        Edge edge, List<String> newRefValues) throws AtlasException {
        
        String refValueToRemove = atlasEdgeLabel.getMapKey();
        if (refValueToRemove == null) {
            // Edge label is missing the map key - throw an exception.
            String typeName = atlasEdgeLabel.getTypeName();
            throw new AtlasException("Outgoing edge " + edge.getId().toString()  
                + " for vertex " + referencingVertex + "): label " + atlasEdgeLabel.getEdgeLabel()
                + " for map attribute " + atlasEdgeLabel.getAttributeName() + " on type "
                + typeName + " is missing the map key");
        }
        boolean valueRemoved = newRefValues.remove(refValueToRemove);
        if (valueRemoved) {
            GraphHelper.setProperty(referencingVertex, atlasEdgeLabel.getQualifiedAttributeName(), newRefValues);
            // For maps, also remove the key-value pair property value.
            GraphHelper.setProperty(referencingVertex, atlasEdgeLabel.getQualifiedMapKey(), null);
        }
        return new TypeUtils.Pair<String, Boolean>(refValueToRemove, Boolean.valueOf(valueRemoved));
    }

    void deleteEntity(String typeName, Vertex instanceVertex) throws AtlasException {
        // Check if this entity has already been processed.
        Id id = GraphHelper.getIdFromVertex(typeName, instanceVertex);
        if (deletedEntityGuids.contains(id._getId())) {
            return;
        }
        deletedEntityGuids.add(id._getId());
        
        // Remove traits owned by this entity.
        deleteAllTraits(instanceVertex);
        
        // Create an empty instance to use for clearing all attributes.
        ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance typedInstance = classType.createInstance(id);
        
        //  Remove any underlying structs and composite entities owned by this entity.
        mapInstanceToVertex(typedInstance, instanceVertex, classType.fieldMapping().fields, false, Operation.DELETE);
        deletedEntities.add(typedInstance);
    }

    /**
     * Delete all traits from the specified vertex.
     * 
     * @param instanceVertex
     * @throws AtlasException 
     */
    private void deleteAllTraits(Vertex instanceVertex) throws AtlasException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        final String entityTypeName = GraphHelper.getTypeName(instanceVertex);
        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(entityTypeName, traitNameToBeDeleted);
            Iterator<Edge> results = instanceVertex.getEdges(Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) { // there should only be one edge for this label
                final Edge traitEdge = results.next();
                final Vertex traitVertex = traitEdge.getVertex(Direction.IN);
    
                // remove the edge to the trait instance from the repository
                graphHelper.removeEdge(traitEdge);
    
                if (traitVertex != null) { // remove the trait instance from the repository
                    deleteTraitVertex(traitNameToBeDeleted, traitVertex);
                }
            }
        }
    }

    void deleteTraitVertex(String traitName, final Vertex traitVertex) throws AtlasException {

        TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
        ITypedStruct traitStruct = traitType.createInstance();
        
        //  Remove trait vertex along with any struct and class attributes owned by this trait.
        mapInstanceToVertex(traitStruct, traitVertex, traitType.fieldMapping().fields, false, Operation.DELETE);
    }

    
    /**
     * Get the GUIDs of entities that have been deleted.
     * 
     * @return
     */
    List<String> getDeletedEntityGuids() {
        if (deletedEntityGuids.size() == 0) {
            return Collections.emptyList();
        }
        else {
            return Collections.unmodifiableList(deletedEntityGuids);
        }
    }
    
    /**
     * Get the entities that have been deleted.
     * 
     * @return
     */
    List<ITypedReferenceableInstance> getDeletedEntities() {
        if (deletedEntities.size() == 0) {
            return Collections.emptyList();
        }
        else {
            return Collections.unmodifiableList(deletedEntities);
        }
    }

}
