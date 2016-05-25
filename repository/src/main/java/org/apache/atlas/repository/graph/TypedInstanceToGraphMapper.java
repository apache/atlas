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

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
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
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public final class TypedInstanceToGraphMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    private final Map<Id, Vertex> idToVertexMap = new HashMap<>();
    private final TypeSystem typeSystem = TypeSystem.getInstance();
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private DeleteHandler deleteHandler;
    private GraphToTypedInstanceMapper graphToTypedInstanceMapper;

    @Inject
    public TypedInstanceToGraphMapper(GraphToTypedInstanceMapper graphToTypedInstanceMapper, DeleteHandler deleteHandler) {
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
        this.deleteHandler = deleteHandler;
    }

    private final String SIGNATURE_HASH_PROPERTY_KEY = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "signature";

    public enum Operation {
        CREATE,
        UPDATE_PARTIAL,
        UPDATE_FULL
    }

    void mapTypedInstanceToGraph(Operation operation, ITypedReferenceableInstance... typedInstances)
            throws AtlasException {

        RequestContext requestContext = RequestContext.get();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            LOG.debug("Adding/updating entity {}", typedInstance);
            Collection<IReferenceableInstance> newInstances = walkClassInstances(typedInstance);
            TypeUtils.Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> instancesPair =
                    createVerticesAndDiscoverInstances(newInstances);
            List<ITypedReferenceableInstance> entitiesToCreate = instancesPair.left;
            List<ITypedReferenceableInstance> entitiesToUpdate = instancesPair.right;
            FullTextMapper fulltextMapper = new FullTextMapper(graphToTypedInstanceMapper);
            switch (operation) {
            case CREATE:
                List<String> ids = addOrUpdateAttributesAndTraits(operation, entitiesToCreate);
                addFullTextProperty(entitiesToCreate, fulltextMapper);
                requestContext.recordEntityCreate(ids);
                break;

            case UPDATE_FULL:
            case UPDATE_PARTIAL:
                ids = addOrUpdateAttributesAndTraits(Operation.CREATE, entitiesToCreate);
                requestContext.recordEntityCreate(ids);
                ids = addOrUpdateAttributesAndTraits(operation, entitiesToUpdate);
                requestContext.recordEntityUpdate(ids);

                addFullTextProperty(entitiesToCreate, fulltextMapper);
                addFullTextProperty(entitiesToUpdate, fulltextMapper);
                break;

            default:
                throw new UnsupportedOperationException("Not handled - " + operation);
            }
        }
    }

    private Collection<IReferenceableInstance> walkClassInstances(ITypedReferenceableInstance typedInstance)
            throws RepositoryException {

        EntityProcessor entityProcessor = new EntityProcessor();
        try {
            LOG.debug("Walking the object graph for instance {}", typedInstance.toShortString());
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
        LOG.debug("Adding/Updating typed instance {}", typedInstance.toShortString());

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
        }
        return getId(typedInstance)._getId();
    }

    void mapInstanceToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                             Map<String, AttributeInfo> fields, boolean mapOnlyUniqueAttributes, Operation operation)
            throws AtlasException {

        LOG.debug("Mapping instance {} to vertex {}", typedInstance.toShortString(), string(instanceVertex));
        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            mapAttributeToVertex(typedInstance, instanceVertex, attributeInfo, operation);
        }
        GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());
    }

    void mapAttributeToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                              AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);
        LOG.debug("Mapping attribute {} = {}", attributeInfo.name, attrValue);

        if (attrValue != null  || operation == Operation.UPDATE_FULL) {
            switch (attributeInfo.dataType().getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
                mapPrimitiveOrEnumToVertex(typedInstance, instanceVertex, attributeInfo);
                break;

            case ARRAY:
                mapArrayCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                break;

            case MAP:
                mapMapCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                break;

            case STRUCT:
            case CLASS:
                String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);

                Edge currentEdge = GraphHelper.getEdgeForLabel(instanceVertex, edgeLabel);
                String newEdgeId = addOrUpdateReference(instanceVertex, attributeInfo, attributeInfo.dataType(),
                        attrValue, currentEdge, edgeLabel, operation);

                if (currentEdge != null && !currentEdge.getId().toString().equals(newEdgeId)) {
                    deleteHandler.deleteEdgeReference(currentEdge, attributeInfo.dataType().getTypeCategory(),
                            attributeInfo.isComposite, true);
                }
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
            LOG.debug("Discovering instance to create/update for {}", instance.toShortString());
            ITypedReferenceableInstance newInstance;
            Id id = instance.getId();

            if (!idToVertexMap.containsKey(id)) {
                Vertex instanceVertex;
                if (id.isAssigned()) {  // has a GUID
                    LOG.debug("Instance has an assigned id {}", instance.getId()._getId());
                    instanceVertex = graphHelper.getVertexForGUID(id.id);
                    if (!(instance instanceof ReferenceableInstance)) {
                        throw new IllegalStateException(
                                String.format("%s is not of type ITypedReferenceableInstance", instance.toShortString()));
                    }
                    newInstance = (ITypedReferenceableInstance) instance;
                    instancesToUpdate.add(newInstance);

                } else {
                    //Check if there is already an instance with the same unique attribute value
                    ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());
                    instanceVertex = graphHelper.getVertexForInstanceByUniqueAttribute(classType, instance);

                    //no entity with the given unique attribute, create new
                    if (instanceVertex == null) {
                        LOG.debug("Creating new vertex for instance {}", instance.toShortString());
                        newInstance = classType.convert(instance, Multiplicity.REQUIRED);
                        instanceVertex = graphHelper.createVertexWithIdentity(newInstance, classType.getAllSuperTypeNames());
                        instancesToCreate.add(newInstance);

                        //Map only unique attributes for cases of circular references
                        mapInstanceToVertex(newInstance, instanceVertex, classType.fieldMapping().fields, true, Operation.CREATE);

                    } else {
                        LOG.debug("Re-using existing vertex {} for instance {}", string(instanceVertex), instance.toShortString());
                        if (!(instance instanceof ReferenceableInstance)) {
                            throw new IllegalStateException(
                                    String.format("%s is not of type ITypedReferenceableInstance", instance.toShortString()));
                        }
                        newInstance = (ITypedReferenceableInstance) instance;
                        instancesToUpdate.add(newInstance);
                    }
                }

                //Set the id in the new instance
                idToVertexMap.put(id, instanceVertex);
            }
        }
        return TypeUtils.Pair.of(instancesToCreate, instancesToUpdate);
    }

    private void addFullTextProperty(List<ITypedReferenceableInstance> instances, FullTextMapper fulltextMapper) throws AtlasException {
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

    /******************************************** ARRAY **************************************************/

    private void mapArrayCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                            AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} for array attribute {} vertex {}", typedInstance.toShortString(),
                attributeInfo.name, string(instanceVertex));

        List newElements = (List) typedInstance.get(attributeInfo.name);
        boolean newAttributeEmpty = (newElements == null || newElements.isEmpty());

        if (newAttributeEmpty && operation != Operation.UPDATE_FULL) {
            return;
        }

        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        List<String> currentElements = instanceVertex.getProperty(propertyName);
        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
        List<Object> newElementsCreated = new ArrayList<>();

        if (!newAttributeEmpty) {
            if (newElements != null && !newElements.isEmpty()) {
                int index = 0;
                for (; index < newElements.size(); index++) {
                    String currentElement = (currentElements != null && index < currentElements.size()) ?
                            currentElements.get(index) : null;
                    LOG.debug("Adding/updating element at position {}, current element {}, new element {}", index,
                            currentElement, newElements.get(index));
                    Object newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                            newElements.get(index), currentElement, propertyName, operation);
                    newElementsCreated.add(newEntry);
                }
            }
        }

        List<String> additionalEdges = removeUnusedEntries(instanceVertex, propertyName, currentElements,
                newElementsCreated, elementType, attributeInfo);
        newElementsCreated.addAll(additionalEdges);

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, newElementsCreated);
    }

    //Removes unused edges from the old collection, compared to the new collection
    private List<String> removeUnusedEntries(Vertex instanceVertex, String edgeLabel,
                                             Collection<String> currentEntries,
                                             Collection<Object> newEntries,
                                             IDataType entryType, AttributeInfo attributeInfo) throws AtlasException {
        if (currentEntries != null && !currentEntries.isEmpty()) {
            LOG.debug("Removing unused entries from the old collection");
            if (entryType.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                    || entryType.getTypeCategory() == DataTypes.TypeCategory.CLASS) {

                //Remove the edges for (current edges - new edges)
                List<String> cloneElements = new ArrayList<>(currentEntries);
                cloneElements.removeAll(newEntries);
                List<String> additionalElements = new ArrayList<>();
                LOG.debug("Removing unused entries from the old collection - {}", cloneElements);

                if (!cloneElements.isEmpty()) {
                    for (String edgeIdForDelete : cloneElements) {
                        Edge edge = graphHelper.getEdgeByEdgeId(instanceVertex, edgeLabel, edgeIdForDelete);
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(),
                                attributeInfo.isComposite, true);
                        if (!deleted) {
                            additionalElements.add(edgeIdForDelete);
                        }
                    }
                }
                return additionalElements;
            }
        }
        return new ArrayList<>();
    }

    /******************************************** MAP **************************************************/

    private void mapMapCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                          AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for attribute {}", typedInstance.toShortString(), string(instanceVertex),
                attributeInfo.name);
        @SuppressWarnings("unchecked") Map<Object, Object> newAttribute =
                (Map<Object, Object>) typedInstance.get(attributeInfo.name);

        boolean newAttributeEmpty = (newAttribute == null || newAttribute.isEmpty());
        if (newAttributeEmpty && operation != Operation.UPDATE_FULL) {
            return;
        }

        IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);

        Map<String, String> currentMap = new HashMap<>();
        Map<String, Object> newMap = new HashMap<>();

        List<String> currentKeys = instanceVertex.getProperty(propertyName);
        if (currentKeys != null && !currentKeys.isEmpty()) {
            for (String key : currentKeys) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                String propertyValueForKey = instanceVertex.getProperty(propertyNameForKey).toString();
                currentMap.put(key, propertyValueForKey);
            }
        }

        if (!newAttributeEmpty) {
            for (Map.Entry entry : newAttribute.entrySet()) {
                String keyStr = entry.getKey().toString();
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, keyStr);

                Object newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                        entry.getValue(), currentMap.get(keyStr), propertyNameForKey, operation);

                //Add/Update/Remove property value
                GraphHelper.setProperty(instanceVertex, propertyNameForKey, newEntry);
                newMap.put(keyStr, newEntry);
            }
        }

        Map<String, String> additionalMap =
                removeUnusedMapEntries(instanceVertex, propertyName, currentMap, newMap, elementType, attributeInfo);

        Set<String> newKeys = new HashSet<>(newMap.keySet());
        newKeys.addAll(additionalMap.keySet());

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, new ArrayList<>(newKeys));
    }

    //Remove unused entries from map
    private Map<String, String> removeUnusedMapEntries(Vertex instanceVertex, String propertyName,
                                                       Map<String, String> currentMap,
                                                       Map<String, Object> newMap, IDataType elementType,
                                                       AttributeInfo attributeInfo)
            throws AtlasException {
        boolean reference = (elementType.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                || elementType.getTypeCategory() == DataTypes.TypeCategory.CLASS);
        Map<String, String> additionalMap = new HashMap<>();

        for (String currentKey : currentMap.keySet()) {
            boolean shouldDeleteKey = !newMap.containsKey(currentKey);
            if (reference) {
                String currentEdge = currentMap.get(currentKey);
                //Delete the edge reference if its not part of new edges created/updated
                if (!newMap.values().contains(currentEdge)) {
                    String edgeLabel = GraphHelper.getQualifiedNameForMapKey(propertyName, currentKey);
                    Edge edge = graphHelper.getEdgeByEdgeId(instanceVertex, edgeLabel, currentMap.get(currentKey));
                    boolean deleted =
                            deleteHandler.deleteEdgeReference(edge, elementType.getTypeCategory(), attributeInfo.isComposite, true);
                    if (!deleted) {
                        additionalMap.put(currentKey, currentEdge);
                        shouldDeleteKey = false;
                    }
                }
            }

            if (shouldDeleteKey) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, currentKey);
                graphHelper.setProperty(instanceVertex, propertyNameForKey, null);
            }
        }
        return additionalMap;
    }

    /******************************************** ARRAY & MAP **************************************************/

    private Object addOrUpdateCollectionEntry(Vertex instanceVertex, AttributeInfo attributeInfo,
                                              IDataType elementType, Object newAttributeValue, String currentValue,
                                              String propertyName, Operation operation)
            throws AtlasException {

        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return newAttributeValue != null ? newAttributeValue : null;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            return null;

        case STRUCT:
        case CLASS:
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
            Edge currentEdge = graphHelper.getEdgeByEdgeId(instanceVertex, edgeLabel, currentValue);
            return addOrUpdateReference(instanceVertex, attributeInfo, elementType, newAttributeValue, currentEdge,
                    edgeLabel, operation);

        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private String addOrUpdateReference(Vertex instanceVertex, AttributeInfo attributeInfo,
                                        IDataType attributeType, Object newAttributeValue, Edge currentEdge,
                                        String edgeLabel, Operation operation) throws AtlasException {
        switch (attributeType.getTypeCategory()) {
        case STRUCT:
            return addOrUpdateStruct(instanceVertex, attributeInfo, (ITypedStruct) newAttributeValue, currentEdge,
                    edgeLabel, operation);

        case CLASS:
            return addOrUpdateClassVertex(instanceVertex, currentEdge,
                    (ITypedReferenceableInstance) newAttributeValue, attributeInfo, edgeLabel);

        default:
            throw new IllegalArgumentException("Unknown type category: " + attributeType.getTypeCategory());
        }
    }
    /******************************************** STRUCT **************************************************/

    private String addOrUpdateStruct(Vertex instanceVertex, AttributeInfo attributeInfo,
                                     ITypedStruct newAttributeValue, Edge currentEdge,
                                     String edgeLabel, Operation operation) throws AtlasException {
        String newEdgeId = null;
        if (currentEdge != null && newAttributeValue != null) {
            //update
            updateStructVertex(newAttributeValue, currentEdge, operation);
            newEdgeId = currentEdge.getId().toString();
        } else if (currentEdge == null && newAttributeValue != null) {
            //add
            Edge newEdge = addStructVertex(newAttributeValue, instanceVertex, attributeInfo, edgeLabel);
            newEdgeId = newEdge.getId().toString();
        }
        return newEdgeId;
    }

    private Edge addStructVertex(ITypedStruct structInstance, Vertex instanceVertex,
                                 AttributeInfo attributeInfo, String edgeLabel) throws AtlasException {
        // add a new vertex for the struct or trait instance
        Vertex structInstanceVertex = graphHelper.createVertexWithoutIdentity(structInstance.getTypeName(), null,
                Collections.<String>emptySet()); // no super types for struct type
        LOG.debug("created vertex {} for struct {} value {}", string(structInstanceVertex), attributeInfo.name,
                structInstance.toShortString());

        // map all the attributes to this new vertex
        mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false,
                Operation.CREATE);
        // add an edge to the newly created vertex from the parent
        Edge newEdge = graphHelper.getOrCreateEdge(instanceVertex, structInstanceVertex, edgeLabel);

        return newEdge;
    }

    private void updateStructVertex(ITypedStruct newAttributeValue, Edge currentEdge,
                                    Operation operation) throws AtlasException {
        //Already existing vertex. Update
        Vertex structInstanceVertex = currentEdge.getVertex(Direction.IN);

        LOG.debug("Updating struct vertex {} with struct {}", string(structInstanceVertex), newAttributeValue.toShortString());

        // Update attributes
        final MessageDigest digester = MD5Utils.getDigester();
        String newSignature = newAttributeValue.getSignatureHash(digester);
        String curSignature = structInstanceVertex.getProperty(SIGNATURE_HASH_PROPERTY_KEY);

        if (!newSignature.equals(curSignature)) {
            //Update struct vertex instance only if there is a change
            LOG.debug("Updating struct {} since signature has changed {} {} ", newAttributeValue, curSignature, newSignature);
            mapInstanceToVertex(newAttributeValue, structInstanceVertex, newAttributeValue.fieldMapping().fields, false, operation);
            GraphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(newSignature));
        }
    }

    /******************************************** CLASS **************************************************/

    private String addOrUpdateClassVertex(Vertex instanceVertex, Edge currentEdge,
                                          ITypedReferenceableInstance newAttributeValue, AttributeInfo attributeInfo,
                                          String edgeLabel) throws AtlasException {
        Vertex newReferenceVertex = getClassVertex(newAttributeValue);
        if(newReferenceVertex == null && newAttributeValue != null) {
            LOG.error("Could not find vertex for Class Reference " + newAttributeValue);
            throw new EntityNotFoundException("Could not find vertex for Class Reference " + newAttributeValue);
        }

        String newEdgeId = null;
        if (currentEdge != null && newAttributeValue != null) {
            newEdgeId = updateClassEdge(instanceVertex, currentEdge, newAttributeValue, newReferenceVertex,
                    attributeInfo, edgeLabel);
        } else if (currentEdge == null && newAttributeValue != null){
            Edge newEdge = addClassEdge(instanceVertex, newReferenceVertex, edgeLabel);
            newEdgeId = newEdge.getId().toString();
        }
        return newEdgeId;
    }

    private Edge addClassEdge(Vertex instanceVertex, Vertex toVertex, String edgeLabel) throws AtlasException {
        // add an edge to the class vertex from the instance
        return graphHelper.getOrCreateEdge(instanceVertex, toVertex, edgeLabel);
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


    private String updateClassEdge(Vertex instanceVertex, Edge currentEdge,
                                   ITypedReferenceableInstance newAttributeValue,
                                   Vertex newVertex, AttributeInfo attributeInfo,
                                   String edgeLabel) throws AtlasException {
        LOG.debug("Updating {} for reference attribute {}", string(currentEdge), attributeInfo.name);
        // Update edge if it exists
        Vertex currentVertex = currentEdge.getVertex(Direction.IN);
        String currentEntityId = GraphHelper.getIdFromVertex(currentVertex);
        String newEntityId = getId(newAttributeValue).id;
        String newEdgeId = currentEdge.getId().toString();
        if (!currentEntityId.equals(newEntityId)) {
            // add an edge to the class vertex from the instance
            if (newVertex != null) {
                Edge newEdge = graphHelper.getOrCreateEdge(instanceVertex, newVertex, edgeLabel);
                newEdgeId = newEdge.getId().toString();
            }
        }

        return newEdgeId;
    }

    /******************************************** TRAITS ****************************************************/

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, IDataType entityType, Vertex parentInstanceVertex)
            throws AtlasException {
        // add a new vertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        Vertex traitInstanceVertex = graphHelper.createVertexWithoutIdentity(traitInstance.getTypeName(), null,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        LOG.debug("created vertex {} for trait {}", string(traitInstanceVertex), traitName);

        // map all the attributes to this newly created vertex
        mapInstanceToVertex(traitInstance, traitInstanceVertex, traitInstance.fieldMapping().fields, false, Operation.CREATE);

        // add an edge to the newly created vertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        graphHelper.getOrCreateEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
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
}
