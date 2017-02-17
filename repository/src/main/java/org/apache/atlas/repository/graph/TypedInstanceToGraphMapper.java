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

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.GuidMapping;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
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
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.atlas.utils.MD5Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

public final class TypedInstanceToGraphMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    private final Map<Id, AtlasVertex> idToVertexMap = new HashMap<>();
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
        Collection<IReferenceableInstance> allNewInstances = new ArrayList<>();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            allNewInstances.addAll(walkClassInstances(typedInstance));
        }

        TypeUtils.Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> instancesPair =
                createVerticesAndDiscoverInstances(allNewInstances);

        List<ITypedReferenceableInstance> entitiesToCreate = instancesPair.left;
        List<ITypedReferenceableInstance> entitiesToUpdate = instancesPair.right;

        FullTextMapper fulltextMapper = new FullTextMapper(this, graphToTypedInstanceMapper);
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

        for(ITypedReferenceableInstance instance : typedInstances) {
            addToEntityCache(requestContext, instance);
        }
    }

    private Collection<IReferenceableInstance> walkClassInstances(ITypedReferenceableInstance typedInstance)
            throws RepositoryException {

        EntityProcessor entityProcessor = new EntityProcessor();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Walking the object graph for instance {}", typedInstance.toShortString());
            }

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
            } catch (AtlasSchemaViolationException e) {
                throw new EntityExistsException(instance, e);
            }
        }
        return guids;
    }

    private String addOrUpdateAttributesAndTraits(Operation operation, ITypedReferenceableInstance typedInstance)
            throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding/Updating typed instance {}", typedInstance.toShortString());
        }

        Id id = typedInstance.getId();
        if (id == null) { // oops
            throw new RepositoryException("id cannot be null");
        }

        AtlasVertex instanceVertex = idToVertexMap.get(id);

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

    void mapInstanceToVertex(ITypedInstance typedInstance, AtlasVertex instanceVertex,
                             Map<String, AttributeInfo> fields, boolean mapOnlyUniqueAttributes, Operation operation)
            throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance {} to vertex {}", typedInstance.toShortString(), string(instanceVertex));
        }

        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            mapAttributeToVertex(typedInstance, instanceVertex, attributeInfo, operation);
        }
        GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());
        GraphHelper.setProperty(instanceVertex, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting modifiedBy: {} and modifiedTime: {}", RequestContext.get().getUser(), RequestContext.get().getRequestTime());
        }
    }

    void mapAttributeToVertex(ITypedInstance typedInstance, AtlasVertex instanceVertex,
                              AttributeInfo attributeInfo, Operation operation) throws AtlasException {

        if ( typedInstance.isValueSet(attributeInfo.name) || operation == Operation.CREATE ) {

            Object attrValue = typedInstance.get(attributeInfo.name);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Mapping attribute {} = {}", attributeInfo.name, attrValue);
            }

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
                String edgeLabel = graphHelper.getEdgeLabel(typedInstance, attributeInfo);

                AtlasEdge currentEdge = graphHelper.getEdgeForLabel(instanceVertex, edgeLabel);
                AtlasEdge newEdge = addOrUpdateReference(instanceVertex, attributeInfo, attributeInfo.dataType(),
                        attrValue, currentEdge, edgeLabel, operation);

                if (currentEdge != null && !currentEdge.equals(newEdge)) {
                    deleteHandler.deleteEdgeReference(currentEdge, attributeInfo.dataType().getTypeCategory(),
                            attributeInfo.isComposite, true);
                }
                if (attributeInfo.reverseAttributeName != null && newEdge != null) {
                    addReverseReference(instanceVertex, attributeInfo.reverseAttributeName, newEdge);
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

        Map<Id,AtlasVertex> foundVertices = findExistingVertices(instances);
        //cache all the ids
        idToVertexMap.putAll(foundVertices);

        Set<Id> processedIds = new HashSet<>();
        for(IReferenceableInstance instance : instances) {
            Id id = instance.getId();
            if(processedIds.contains(id)) {
                continue;
            }

            AtlasVertex instanceVertex = foundVertices.get(id);
            ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());

            if(instanceVertex == null) {

                if(LOG.isDebugEnabled()) {
                    LOG.debug("Creating new vertex for instance {}", instance.toShortString());
                }

                ITypedReferenceableInstance newInstance = classType.convert(instance, Multiplicity.REQUIRED);
                instanceVertex = graphHelper.createVertexWithIdentity(newInstance, classType.getAllSuperTypeNames());
                instancesToCreate.add(newInstance);

                //Map only unique attributes for cases of circular references
                mapInstanceToVertex(newInstance, instanceVertex, classType.fieldMapping().fields, true, Operation.CREATE);
                idToVertexMap.put(id, instanceVertex);

            }
            else {

                if(LOG.isDebugEnabled()) {
                    LOG.debug("Re-using existing vertex {} for instance {}", string(instanceVertex), instance.toShortString());
                }

                if (!(instance instanceof ITypedReferenceableInstance)) {
                    throw new IllegalStateException(
                            String.format("%s is not of type ITypedReferenceableInstance", instance.toShortString()));
                }
                ITypedReferenceableInstance existingInstance = (ITypedReferenceableInstance) instance;
                instancesToUpdate.add(existingInstance);
            }
            processedIds.add(id);

        }
        return TypeUtils.Pair.of(instancesToCreate, instancesToUpdate);
    }

    private Map<Id,AtlasVertex> findExistingVertices(Collection<IReferenceableInstance> instances) throws AtlasException {

        VertexLookupContext context = new VertexLookupContext(this);
        Map<Id,AtlasVertex> result = new HashMap<>();

        for(IReferenceableInstance instance : instances) {
            context.addInstance(instance);
        }

        List<Id> instancesToLoad = new ArrayList<>(context.getInstancesToLoadByGuid());
        List<String> guidsToLoad = Lists.transform(instancesToLoad, new Function<Id,String>() {

            @Override
            public String apply(Id instance) {
                Id id = getExistingId(instance);
                return id.id;
            }

        });

        Map<String, AtlasVertex> instanceVertices = graphHelper.getVerticesForGUIDs(guidsToLoad);

        List<String> missingGuids = new ArrayList<>();
        for(int i = 0 ; i < instancesToLoad.size(); i++) {

            String guid = guidsToLoad.get(i);
            AtlasVertex instanceVertex = instanceVertices.get(guid);
            if(instanceVertex == null) {
                missingGuids.add(guid);
                continue;
            }

            Id instance = instancesToLoad.get(i);
            if(LOG.isDebugEnabled()) {
                LOG.debug("Found vertex {} for instance {}", string(instanceVertex), instance);
            }
            result.put(instance, instanceVertex);
        }

        if(missingGuids.size() > 0) {
            throw new EntityNotFoundException("Could not find entities in the repository with the following GUIDs: " + missingGuids);
        }

        for(Map.Entry<ClassType,List<IReferenceableInstance>> entry : context.getInstancesToLoadByUniqueAttribute().entrySet()) {
            ClassType type = entry.getKey();
            List<IReferenceableInstance> instancesForClass = entry.getValue();
            List<AtlasVertex> correspondingVertices = graphHelper.getVerticesForInstancesByUniqueAttribute(type, instancesForClass);
            for(int i = 0; i < instancesForClass.size(); i++) {
                IReferenceableInstance inst = instancesForClass.get(i);
                AtlasVertex vertex = correspondingVertices.get(i);
                result.put(getExistingId(inst), vertex);
            }
        }

        return result;
    }


    private void addFullTextProperty(List<ITypedReferenceableInstance> instances, FullTextMapper fulltextMapper) throws AtlasException {

        if(! AtlasRepositoryConfiguration.isFullTextSearchEnabled()) {
            return;
        }

        for (ITypedReferenceableInstance typedInstance : instances) { // Traverse
            AtlasVertex instanceVertex = getClassVertex(typedInstance);
            String fullText = fulltextMapper.mapRecursive(instanceVertex, true);
            GraphHelper.setProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
        }
    }

    private void addTraits(ITypedReferenceableInstance typedInstance, AtlasVertex instanceVertex, ClassType classType)
            throws AtlasException {
        for (String traitName : typedInstance.getTraits()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("mapping trait {}", traitName);
            }

            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            // add the attributes for the trait instance
            mapTraitInstanceToVertex(traitInstance, classType, instanceVertex);
        }
    }

    /******************************************** ARRAY **************************************************/

    private void mapArrayCollectionToVertex(ITypedInstance typedInstance, AtlasVertex instanceVertex,
                                            AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance {} for array attribute {} vertex {}", typedInstance.toShortString(),
                    attributeInfo.name, string(instanceVertex));
        }

        List newElements = (List) typedInstance.get(attributeInfo.name);
        boolean newAttributeEmpty = (newElements == null || newElements.isEmpty());

        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);

        List<Object> currentElements = GraphHelper.getArrayElementsProperty(elementType, instanceVertex, propertyName);

        List<Object> newElementsCreated = new ArrayList<>();

        if (!newAttributeEmpty) {
            int index = 0;
            for (; index < newElements.size(); index++) {
                Object currentElement = (currentElements != null && index < currentElements.size()) ?
                        currentElements.get(index) : null;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding/updating element at position {}, current element {}, new element {}", index,
                            currentElement, newElements.get(index));
                }

                Object newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                        newElements.get(index), currentElement, propertyName, operation);
                newElementsCreated.add(newEntry);
            }
        }

        if(GraphHelper.isReference(elementType)) {
            if (attributeInfo.reverseAttributeName != null && newElementsCreated.size() > 0) {
                // Set/add the new reference value(s) on the reverse reference.
                for (Object newElement : newElementsCreated) {
                    if ((newElement instanceof AtlasEdge)) {
                        AtlasEdge newEdge = (AtlasEdge) newElement;
                        addReverseReference(instanceVertex, attributeInfo.reverseAttributeName, newEdge);
                    }
                    else {
                        throw new AtlasException("Invalid array element type " + newElement.getClass().getName() + " - expected " + AtlasEdge.class.getName() +
                            " for reference " + GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo) + " on vertex " + GraphHelper.getVertexDetails(instanceVertex));
                    }
                }
            }

            List<AtlasEdge> additionalEdges = removeUnusedEntries(instanceVertex, propertyName, (List)currentElements,
                    (List)newElementsCreated, elementType, attributeInfo);
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        GraphHelper.setArrayElementsProperty(elementType, instanceVertex, propertyName, newElementsCreated);
    }

    //Removes unused edges from the old collection, compared to the new collection
    private List<AtlasEdge> removeUnusedEntries(AtlasVertex instanceVertex, String edgeLabel,
                                             Collection<AtlasEdge> currentEntries,
                                             Collection<AtlasEdge> newEntries,
                                             IDataType entryType, AttributeInfo attributeInfo) throws AtlasException {
        if (currentEntries != null && !currentEntries.isEmpty()) {
            LOG.debug("Removing unused entries from the old collection");
            if (entryType.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                    || entryType.getTypeCategory() == DataTypes.TypeCategory.CLASS) {

                //Remove the edges for (current edges - new edges)
                List<AtlasEdge> cloneElements = new ArrayList<>(currentEntries);
                cloneElements.removeAll(newEntries);
                List<AtlasEdge> additionalElements = new ArrayList<>();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removing unused entries from the old collection - {}", cloneElements);
                }

                if (!cloneElements.isEmpty()) {
                    for (AtlasEdge edge : cloneElements) {
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(),
                                attributeInfo.isComposite, true);
                        if (!deleted) {
                            additionalElements.add(edge);
                        }
                    }
                }
                return additionalElements;
            }
        }
        return new ArrayList<>();
    }

    /******************************************** MAP **************************************************/

    private void mapMapCollectionToVertex(ITypedInstance typedInstance, AtlasVertex instanceVertex,
                                          AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance {} to vertex {} for attribute {}", typedInstance.toShortString(), string(instanceVertex),
                    attributeInfo.name);
        }

        @SuppressWarnings("unchecked") Map<Object, Object> newAttribute =
                (Map<Object, Object>) typedInstance.get(attributeInfo.name);

        boolean newAttributeEmpty = (newAttribute == null || newAttribute.isEmpty());

        IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);

        Map<String, Object> currentMap = new HashMap<>();
        Map<String, Object> newMap = new HashMap<>();

        List<String> currentKeys = GraphHelper.getListProperty(instanceVertex, propertyName);
        if (currentKeys != null && !currentKeys.isEmpty()) {
            for (String key : currentKeys) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                Object propertyValueForKey = GraphHelper.getMapValueProperty(elementType, instanceVertex, propertyNameForKey);
                currentMap.put(key, propertyValueForKey);
            }
        }

        if (!newAttributeEmpty) {
            for (Map.Entry<Object,Object> entry : newAttribute.entrySet()) {
                String keyStr = entry.getKey().toString();
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, keyStr);

                Object newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                        entry.getValue(), currentMap.get(keyStr), propertyNameForKey, operation);

                //Add/Update/Remove property value
                GraphHelper.setMapValueProperty(elementType, instanceVertex, propertyNameForKey, newEntry);
                newMap.put(keyStr, newEntry);
            }
        }

        Map<String, Object> additionalMap =
                removeUnusedMapEntries(instanceVertex, propertyName, currentMap, newMap, elementType, attributeInfo);

        Set<String> newKeys = new HashSet<>(newMap.keySet());
        newKeys.addAll(additionalMap.keySet());


        // for dereference on way out
        GraphHelper.setListProperty(instanceVertex, propertyName, new ArrayList<>(newKeys));
    }

    //Remove unused entries from map
    private Map<String, Object> removeUnusedMapEntries(
            AtlasVertex instanceVertex, String propertyName,
            Map<String, Object> currentMap,
            Map<String, Object> newMap, IDataType elementType,
            AttributeInfo attributeInfo)
                    throws AtlasException {

        Map<String, Object> additionalMap = new HashMap<>();
        for (String currentKey : currentMap.keySet()) {

            boolean shouldDeleteKey = !newMap.containsKey(currentKey);
            if (GraphHelper.isReference(elementType)) {

                //Delete the edge reference if its not part of new edges created/updated
                AtlasEdge currentEdge = (AtlasEdge)currentMap.get(currentKey);

                if (!newMap.values().contains(currentEdge)) {

                    boolean deleted =
                            deleteHandler.deleteEdgeReference(currentEdge, elementType.getTypeCategory(), attributeInfo.isComposite, true);
                    if (!deleted) {
                        additionalMap.put(currentKey, currentEdge);
                        shouldDeleteKey = false;
                    }
                }
            }

            if (shouldDeleteKey) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, currentKey);
                GraphHelper.setProperty(instanceVertex, propertyNameForKey, null);
            }
        }
        return additionalMap;
    }

    /******************************************** ARRAY & MAP **************************************************/

    private Object addOrUpdateCollectionEntry(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
                                              IDataType elementType, Object newAttributeValue, Object currentValue,
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
            return addOrUpdateReference(instanceVertex, attributeInfo, elementType, newAttributeValue, (AtlasEdge)currentValue,
                    edgeLabel, operation);

        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private AtlasEdge addOrUpdateReference(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
                                        IDataType attributeType, Object newAttributeValue, AtlasEdge currentEdge,
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


    private AtlasEdge addOrUpdateStruct(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
            ITypedStruct newAttributeValue, AtlasEdge currentEdge,
            String edgeLabel, Operation operation) throws AtlasException {
        AtlasEdge newEdge = null;
        if (GraphHelper.elementExists(currentEdge) && newAttributeValue != null) {
            //update
            updateStructVertex(newAttributeValue, currentEdge, operation);
            newEdge = currentEdge;
        } else if (! GraphHelper.elementExists(currentEdge) && newAttributeValue != null) {
            //add
            newEdge = addStructVertex(newAttributeValue, instanceVertex, attributeInfo, edgeLabel);
        }
        return newEdge;
    }

    private AtlasEdge addStructVertex(ITypedStruct structInstance, AtlasVertex instanceVertex,
                                 AttributeInfo attributeInfo, String edgeLabel) throws AtlasException {
        // add a new vertex for the struct or trait instance
        AtlasVertex structInstanceVertex = graphHelper.createVertexWithoutIdentity(structInstance.getTypeName(), null,
                Collections.<String>emptySet()); // no super types for struct type

        if (LOG.isDebugEnabled()) {
            LOG.debug("created vertex {} for struct {} value {}", string(structInstanceVertex), attributeInfo.name,
                    structInstance.toShortString());
        }

        // map all the attributes to this new vertex
        mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false,
                Operation.CREATE);
        // add an edge to the newly created vertex from the parent
        AtlasEdge newEdge = graphHelper.getOrCreateEdge(instanceVertex, structInstanceVertex, edgeLabel);

        return newEdge;
    }

    private void updateStructVertex(ITypedStruct newAttributeValue, AtlasEdge currentEdge,
            Operation operation) throws AtlasException {
        //Already existing vertex. Update
        AtlasVertex structInstanceVertex = currentEdge.getInVertex();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating struct vertex {} with struct {}", string(structInstanceVertex), newAttributeValue.toShortString());
        }

        // Update attributes
        final MessageDigest digester = MD5Utils.getDigester();
        String newSignature = newAttributeValue.getSignatureHash(digester);
        String curSignature = GraphHelper.getSingleValuedProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.class);

        if (!newSignature.equals(curSignature)) {
            //Update struct vertex instance only if there is a change
            if (LOG.isDebugEnabled()) {
                LOG.debug("Updating struct {} since signature has changed {} {} ", newAttributeValue, curSignature, newSignature);
            }

            mapInstanceToVertex(newAttributeValue, structInstanceVertex, newAttributeValue.fieldMapping().fields, false, operation);
            GraphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(newSignature));
        }
    }

    /******************************************** CLASS **************************************************/

    private AtlasEdge addOrUpdateClassVertex(AtlasVertex instanceVertex, AtlasEdge currentEdge,
            ITypedReferenceableInstance newAttributeValue, AttributeInfo attributeInfo,
            String edgeLabel) throws AtlasException {
        AtlasVertex newReferenceVertex = getClassVertex(newAttributeValue);
        if( ! GraphHelper.elementExists(newReferenceVertex) && newAttributeValue != null) {
            LOG.error("Could not find vertex for Class Reference {}", newAttributeValue);
            throw new EntityNotFoundException("Could not find vertex for Class Reference " + newAttributeValue);
        }

        AtlasEdge newEdge = null;
        if (GraphHelper.elementExists(currentEdge) && newAttributeValue != null) {
            newEdge = updateClassEdge(instanceVertex, currentEdge, newAttributeValue, newReferenceVertex,
                    attributeInfo, edgeLabel);
        } else if (! GraphHelper.elementExists(currentEdge) && newAttributeValue != null){
            newEdge = addClassEdge(instanceVertex, newReferenceVertex, edgeLabel);

        }
        return newEdge;
    }


    private AtlasEdge addClassEdge(AtlasVertex instanceVertex, AtlasVertex toVertex, String edgeLabel) throws AtlasException {
        // add an edge to the class vertex from the instance
        return graphHelper.getOrCreateEdge(instanceVertex, toVertex, edgeLabel);
    }

    private <V,E> AtlasVertex<V,E> getClassVertex(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        AtlasVertex<V,E> referenceVertex = null;
        Id id = null;
        if (typedReference != null) {
            id = getExistingId(typedReference);
            referenceVertex = idToVertexMap.get(id);
            if(referenceVertex == null && id.isAssigned()) {
                referenceVertex = graphHelper.getVertexForGUID(id.id);
            }
        }

        return referenceVertex;
    }

    Id getExistingId(IReferenceableInstance instance) {
        return instance instanceof Id ? (Id) instance : instance.getId();
    }

    private Id getId(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        if (typedReference == null) {
            throw new IllegalArgumentException("typedReference must be non-null");
        }
        Id id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();

        if (id.isUnassigned()) {
            AtlasVertex classVertex = idToVertexMap.get(id);
            String guid = GraphHelper.getGuid(classVertex);
            id = new Id(guid, 0, typedReference.getTypeName());
        }
        return id;
    }


    private AtlasEdge updateClassEdge(AtlasVertex instanceVertex, AtlasEdge currentEdge,
            ITypedReferenceableInstance newAttributeValue,
            AtlasVertex newVertex, AttributeInfo attributeInfo,
            String edgeLabel) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating {} for reference attribute {}", string(currentEdge), attributeInfo.name);
        }

        // Update edge if it exists
        AtlasVertex currentVertex = currentEdge.getInVertex();
        String currentEntityId = GraphHelper.getGuid(currentVertex);
        String newEntityId = getId(newAttributeValue).id;
        AtlasEdge newEdge = currentEdge;
        if (!currentEntityId.equals(newEntityId)) {
            // add an edge to the class vertex from the instance
            if (newVertex != null) {
                newEdge = graphHelper.getOrCreateEdge(instanceVertex, newVertex, edgeLabel);

            }
        }

        return newEdge;
    }

    /******************************************** TRAITS ****************************************************/

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, IDataType entityType, AtlasVertex parentInstanceVertex)
            throws AtlasException {
        // add a new AtlasVertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        AtlasVertex traitInstanceVertex = graphHelper.createVertexWithoutIdentity(traitInstance.getTypeName(), null,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        if (LOG.isDebugEnabled()) {
            LOG.debug("created vertex {} for trait {}", string(traitInstanceVertex), traitName);
        }

        // map all the attributes to this newly created AtlasVertex
        mapInstanceToVertex(traitInstance, traitInstanceVertex, traitInstance.fieldMapping().fields, false, Operation.CREATE);

        // add an edge to the newly created AtlasVertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        graphHelper.getOrCreateEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
    }

    /******************************************** PRIMITIVES **************************************************/

    private void mapPrimitiveOrEnumToVertex(ITypedInstance typedInstance, AtlasVertex instanceVertex,
                                            AttributeInfo attributeInfo) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);

        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        Object propertyValue = null;

        if (attrValue == null) {
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
            if (dateVal != null) {
                propertyValue = dateVal.getTime();
            }
        } else if (attributeInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            if (attrValue != null) {
                propertyValue = ((EnumValue) attrValue).value;
            }
        }

        GraphHelper.setProperty(instanceVertex, vertexPropertyName, propertyValue);
    }

    public AtlasVertex lookupVertex(Id refId) {
        return idToVertexMap.get(refId);
    }

    private void addToEntityCache(RequestContext context, ITypedReferenceableInstance instance)
            throws EntityNotFoundException {

        Id instanceId = instance.getId();
        if(instanceId.isUnassigned()) {
            if(instance instanceof ReferenceableInstance) {
                //When the id is unassigned, we can only cache the instance of it is
                //an instance of ReferenceableInstance, since replaceWithNewId is not
                //currently in the ITypedReferenceableInstance interface.
                Id id = getId(instance);
                ((ReferenceableInstance)instance).replaceWithNewId(id);
                context.cache(instance);
            }
        }
        else {
            context.cache(instance);
        }
    }

    public GuidMapping createGuidMapping() {
        Map<String,String> mapping = new HashMap<>(idToVertexMap.size());
        for(Map.Entry<Id, AtlasVertex> entry : idToVertexMap.entrySet()) {
            Id id = entry.getKey();
            if (id.isUnassigned()) {
                AtlasVertex classVertex = entry.getValue();
                mapping.put(id._getId(), GraphHelper.getGuid(classVertex));
            }
        }
        return new GuidMapping(mapping);
    }


    private <V,E> void addReverseReference(AtlasVertex<V,E> vertex, String reverseAttributeName, AtlasEdge<V,E> edge)
        throws AtlasException {

        String typeName = GraphHelper.getTypeName(vertex);
        Id id = GraphHelper.getIdFromVertex(typeName, vertex);

        AtlasVertex<V, E> reverseVertex = edge.getInVertex();
        String reverseTypeName = GraphHelper.getTypeName(reverseVertex);
        Id reverseId = GraphHelper.getIdFromVertex(reverseTypeName, reverseVertex);
        IDataType reverseType = typeSystem.getDataType(IDataType.class, reverseTypeName);
        AttributeInfo reverseAttrInfo = TypesUtil.getFieldMapping(reverseType).fields.get(reverseAttributeName);
        if (reverseAttrInfo.dataType().getTypeCategory() == TypeCategory.MAP) {
            // If the reverse reference is a map, what would be used as the key?
            // Not supporting automatic update of reverse map references.
            LOG.debug("Automatic update of reverse map reference is not supported - reference = {}",
                GraphHelper.getQualifiedFieldName(reverseType, reverseAttributeName));
            return;
        }

        String propertyName = GraphHelper.getQualifiedFieldName(reverseType, reverseAttributeName);
        String reverseEdgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        AtlasEdge<V, E> reverseEdge = graphHelper.getEdgeForLabel(reverseVertex, reverseEdgeLabel);

        AtlasEdge<V, E> newEdge = null;
        if (reverseEdge != null) {
            newEdge = updateClassEdge(reverseVertex, reverseEdge, id, vertex, reverseAttrInfo, reverseEdgeLabel);
        }
        else {
            newEdge = addClassEdge(reverseVertex, vertex, reverseEdgeLabel);
        }

        switch (reverseAttrInfo.dataType().getTypeCategory()) {
        case CLASS:
            if (reverseEdge != null && !reverseEdge.getId().toString().equals(newEdge.getId().toString())) {
                // Disconnect old reference
                deleteHandler.deleteEdgeReference(reverseEdge, reverseAttrInfo.dataType().getTypeCategory(),
                    reverseAttrInfo.isComposite, true);
            }
            break;
        case ARRAY:
            // Add edge ID to property value
            List<String> elements = reverseVertex.getProperty(propertyName, List.class);
            if (elements == null) {
                elements = new ArrayList<>();
                elements.add(newEdge.getId().toString());
                reverseVertex.setProperty(propertyName, elements);
            }
            else {
               if (!elements.contains(newEdge.getId().toString())) {
                    elements.add(newEdge.getId().toString());
                    reverseVertex.setProperty(propertyName, elements);
                }
            }
            break;
        }

        RequestContext requestContext = RequestContext.get();
        GraphHelper.setProperty(reverseVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                requestContext.getRequestTime());
        requestContext.recordEntityUpdate(reverseId._getId());
    }
}