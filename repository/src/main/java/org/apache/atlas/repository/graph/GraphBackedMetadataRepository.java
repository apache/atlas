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

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.MapIds;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
@Singleton
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedMetadataRepository.class);
    private static final String FULL_TEXT_DELIMITER = " ";

    private static final String EDGE_LABEL_PREFIX = "__";

    private final AtomicInteger ID_SEQ = new AtomicInteger(0);

    private final TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper();
    private final GraphToTypedInstanceMapper graphToInstanceMapper = new GraphToTypedInstanceMapper();

    private final TypeSystem typeSystem;
    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedMetadataRepository(GraphProvider<TitanGraph> graphProvider) throws AtlasException {
        this.typeSystem = TypeSystem.getInstance();

        this.titanGraph = graphProvider.get();
    }

    public GraphToTypedInstanceMapper getGraphToInstanceMapper() {
        return graphToInstanceMapper;
    }

    @Override
    public String getTypeAttributeName() {
        return Constants.ENTITY_TYPE_PROPERTY_KEY;
    }

    /**
     * Returns the property key used to store super type names.
     *
     * @return property key used to store super type names.
     */
    @Override
    public String getSuperTypeAttributeName() {
        return Constants.SUPER_TYPES_PROPERTY_KEY;
    }

    public String getIdAttributeName() {
        return Constants.GUID_PROPERTY_KEY;
    }

    @Override
    public String getTraitLabel(IDataType<?> dataType, String traitName) {
        return dataType.getName() + "." + traitName;
    }

    @Override
    public String getFieldNameInVertex(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException {
        return getQualifiedName(dataType, aInfo.name);
    }

    @Override
    public String getEdgeLabel(IDataType<?> dataType, AttributeInfo aInfo) {
        return getEdgeLabel(dataType.getName(), aInfo.name);
    }

    public String getEdgeLabel(String typeName, String attrName) {
        return EDGE_LABEL_PREFIX + typeName + "." + attrName;
    }

    public String getEdgeLabel(ITypedInstance typedInstance, AttributeInfo aInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getEdgeLabel(dataType, aInfo);
    }

    @Override
    @GraphTransaction
    public String createEntity(IReferenceableInstance typedInstance) throws RepositoryException {
        LOG.info("adding entity={}", typedInstance);
        try {
            return instanceToGraphMapper.mapTypedInstanceToGraph(typedInstance);
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException {
        LOG.info("Retrieving entity with guid={}", guid);

        Vertex instanceVertex = getVertexForGUID(guid);

        try {
            LOG.debug("Found a vertex {} for guid {}", instanceVertex, guid);
            return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);

        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    private Vertex getVertexForGUID(String guid) throws EntityNotFoundException {
        Vertex instanceVertex = GraphHelper.findVertexByGUID(titanGraph, guid);
        if (instanceVertex == null) {
            LOG.debug("Could not find a vertex for guid={}", guid);
            throw new EntityNotFoundException("Could not find an entity in the repository for guid: " + guid);
        }

        return instanceVertex;
    }

    @Override
    @GraphTransaction
    public List<String> getEntityList(String entityType) throws RepositoryException {
        LOG.info("Retrieving entity list for type={}", entityType);
        GraphQuery query = titanGraph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityType);
        Iterator<Vertex> results = query.vertices().iterator();
        if (!results.hasNext()) {
            return Collections.emptyList();
        }

        ArrayList<String> entityList = new ArrayList<>();
        while (results.hasNext()) {
            Vertex vertex = results.next();
            entityList.add(vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY));
        }

        return entityList;
    }

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     * @throws RepositoryException
     */
    @Override
    @GraphTransaction
    public List<String> getTraitNames(String guid) throws AtlasException {
        LOG.info("Retrieving trait names for entity={}", guid);
        Vertex instanceVertex = getVertexForGUID(guid);
        return getTraitNames(instanceVertex);
    }

    public List<String> getTraitNames(Vertex entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        for (TitanProperty property : ((TitanVertex) entityVertex).getProperties(Constants.TRAIT_NAMES_PROPERTY_KEY)) {
            traits.add((String) property.getValue());
        }

        return traits;
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid          globally unique identifier for the entity
     * @param traitInstance trait instance that needs to be added to entity
     * @throws RepositoryException
     */
    @Override
    @GraphTransaction
    public void addTrait(String guid, ITypedStruct traitInstance) throws RepositoryException {
        Preconditions.checkNotNull(traitInstance, "Trait instance cannot be null");
        final String traitName = traitInstance.getTypeName();
        LOG.info("Adding a new trait={} for entity={}", traitName, guid);

        try {
            Vertex instanceVertex = getVertexForGUID(guid);

            // add the trait instance as a new vertex
            final String typeName = getTypeName(instanceVertex);
            instanceToGraphMapper
                    .mapTraitInstanceToVertex(traitInstance, getIdFromVertex(typeName, instanceVertex), typeName,
                            instanceVertex, Collections.<Id, Vertex>emptyMap());

            // update the traits in entity once adding trait instance is successful
            ((TitanVertex) instanceVertex).addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);

        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid      globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws RepositoryException
     */
    @Override
    @GraphTransaction
    public void deleteTrait(String guid, String traitNameToBeDeleted) throws RepositoryException {
        LOG.info("Deleting trait={} from entity={}", traitNameToBeDeleted, guid);
        try {
            Vertex instanceVertex = getVertexForGUID(guid);

            List<String> traitNames = getTraitNames(instanceVertex);
            if (!traitNames.contains(traitNameToBeDeleted)) {
                throw new EntityNotFoundException(
                        "Could not find trait=" + traitNameToBeDeleted + " in the repository for entity: " + guid);
            }

            final String entityTypeName = getTypeName(instanceVertex);
            String relationshipLabel = getEdgeLabel(entityTypeName, traitNameToBeDeleted);
            Iterator<Edge> results = instanceVertex.getEdges(Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) { // there should only be one edge for this label
                final Edge traitEdge = results.next();
                final Vertex traitVertex = traitEdge.getVertex(Direction.IN);

                // remove the edge to the trait instance from the repository
                titanGraph.removeEdge(traitEdge);

                if (traitVertex != null) { // remove the trait instance from the repository
                    titanGraph.removeVertex(traitVertex);

                    // update the traits in entity once trait removal is successful
                    traitNames.remove(traitNameToBeDeleted);
                    updateTraits(instanceVertex, traitNames);
                }
            }
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    private void updateTraits(Vertex instanceVertex, List<String> traitNames) {
        // remove the key
        instanceVertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);

        // add it back again
        for (String traitName : traitNames) {
            ((TitanVertex) instanceVertex).addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
        }
    }

    @Override
    @GraphTransaction
    public void updateEntity(String guid, String property, String value) throws RepositoryException {
        LOG.info("Adding property {} for entity guid {}", property, guid);

        try {
            Vertex instanceVertex = getVertexForGUID(guid);

            LOG.debug("Found a vertex {} for guid {}", instanceVertex, guid);
            String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
            ClassType type = typeSystem.getDataType(ClassType.class, typeName);
            AttributeInfo attributeInfo = type.fieldMapping.fields.get(property);
            if (attributeInfo == null) {
                throw new AtlasException("Invalid property " + property + " for entity " + typeName);
            }

            DataTypes.TypeCategory attrTypeCategory = attributeInfo.dataType().getTypeCategory();
            ITypedReferenceableInstance instance = type.createInstance();
            if (attrTypeCategory == DataTypes.TypeCategory.PRIMITIVE) {
                instance.set(property, value);
            } else if (attrTypeCategory == DataTypes.TypeCategory.CLASS) {
                Id id = new Id(value, 0, attributeInfo.dataType().getName());
                instance.set(property, id);
            } else {
                throw new RepositoryException("Update of " + attrTypeCategory + " is not supported");
            }

            instanceToGraphMapper
                    .mapAttributesToVertex(getIdFromVertex(typeName, instanceVertex), instance, instanceVertex,
                            new HashMap<Id, Vertex>(), attributeInfo, attributeInfo.dataType());
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    public Id getIdFromVertex(String dataTypeName, Vertex vertex) {
        return new Id(vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY),
                vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), dataTypeName);
    }

    String getTypeName(Vertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
    }


    String getQualifiedName(ITypedInstance typedInstance, AttributeInfo attributeInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getQualifiedName(dataType, attributeInfo.name);
    }

    String getQualifiedName(IDataType dataType, String attributeName) throws AtlasException {
        return dataType.getTypeCategory() == DataTypes.TypeCategory.STRUCT ? dataType.getName() + "." + attributeName
                // else class or trait
                : ((HierarchicalType) dataType).getQualifiedName(attributeName);
    }

    private final class EntityProcessor implements ObjectGraphWalker.NodeProcessor {

        public final Map<Id, Id> idToNewIdMap;
        public final Map<Id, IReferenceableInstance> idToInstanceMap;
        public final Map<Id, Vertex> idToVertexMap;

        public EntityProcessor() {
            idToNewIdMap = new HashMap<>();
            idToInstanceMap = new HashMap<>();
            idToVertexMap = new HashMap<>();
        }

        @Override
        public void processNode(ObjectGraphWalker.Node nd) throws AtlasException {
            IReferenceableInstance ref = null;
            Id id = null;

            if (nd.attributeName == null) {
                ref = (IReferenceableInstance) nd.instance;
                id = ref.getId();
            } else if (nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                if (nd.value != null && (nd.value instanceof Id)) {
                    id = (Id) nd.value;
                }
            }

            if (id != null) {
                if (id.isUnassigned()) {
                    if (!idToNewIdMap.containsKey(id)) {
                        idToNewIdMap.put(id, new Id(ID_SEQ.getAndIncrement(), 0, id.className));
                    }

                    if (ref != null) {
                        if (idToInstanceMap.containsKey(id)) { // Oops
                            throw new RepositoryException(
                                    String.format("Unexpected internal error: Id %s processed again", id));
                        }

                        idToInstanceMap.put(id, ref);
                    }
                }
            }
        }

        private void createVerticesForClassTypes(List<ITypedReferenceableInstance> newInstances) throws AtlasException {
            for (ITypedReferenceableInstance typedInstance : newInstances) {
                final Id id = typedInstance.getId();
                if (!idToVertexMap.containsKey(id)) {
                    Vertex instanceVertex;
                    if (id.isAssigned()) {  // has a GUID
                        instanceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                    } else {
                        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
                        instanceVertex = GraphHelper
                                .createVertexWithIdentity(titanGraph, typedInstance, classType.getAllSuperTypeNames());
                    }

                    idToVertexMap.put(id, instanceVertex);
                }
            }
        }
    }

    private final class TypedInstanceToGraphMapper {

        private String mapTypedInstanceToGraph(IReferenceableInstance typedInstance) throws AtlasException {

            EntityProcessor entityProcessor = new EntityProcessor();
            try {
                LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
                new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
            } catch (AtlasException me) {
                throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
            }

            List<ITypedReferenceableInstance> newTypedInstances = discoverInstances(entityProcessor);
            entityProcessor.createVerticesForClassTypes(newTypedInstances);
            String guid = addDiscoveredInstances(typedInstance, entityProcessor, newTypedInstances);
            addFullTextProperty(entityProcessor, newTypedInstances);
            return guid;
        }

        private void addFullTextProperty(EntityProcessor entityProcessor,
                List<ITypedReferenceableInstance> newTypedInstances) throws AtlasException {

            for (ITypedReferenceableInstance typedInstance : newTypedInstances) { // Traverse
                Id id = typedInstance.getId();
                Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);
                String fullText = getFullTextForVertex(instanceVertex, true);
                addProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
            }
        }

        private String getFullTextForVertex(Vertex instanceVertex, boolean followReferences) throws AtlasException {
            String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
            ITypedReferenceableInstance typedReference =
                    graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
            String fullText = getFullTextForInstance(typedReference, followReferences);
            StringBuilder fullTextBuilder =
                    new StringBuilder(typedReference.getTypeName()).append(FULL_TEXT_DELIMITER).append(fullText);

            List<String> traits = typedReference.getTraits();
            for (String traitName : traits) {
                String traitText = getFullTextForInstance((ITypedInstance) typedReference.getTrait(traitName), false);
                fullTextBuilder.append(FULL_TEXT_DELIMITER).append(traitName).append(FULL_TEXT_DELIMITER)
                        .append(traitText);
            }
            return fullTextBuilder.toString();
        }

        private String getFullTextForAttribute(IDataType type, Object value, boolean followReferences)
        throws AtlasException {
            switch (type.getTypeCategory()) {
            case PRIMITIVE:
                return String.valueOf(value);

            case ENUM:
                return ((EnumValue) value).value;

            case ARRAY:
                StringBuilder fullText = new StringBuilder();
                IDataType elemType = ((DataTypes.ArrayType) type).getElemType();
                List list = (List) value;

                for (Object element : list) {
                    String elemFullText = getFullTextForAttribute(elemType, element, false);
                    if (StringUtils.isNotEmpty(elemFullText)) {
                        fullText = fullText.append(FULL_TEXT_DELIMITER).append(elemFullText);
                    }
                }
                return fullText.toString();

            case MAP:
                fullText = new StringBuilder();
                IDataType keyType = ((DataTypes.MapType) type).getKeyType();
                IDataType valueType = ((DataTypes.MapType) type).getValueType();
                Map map = (Map) value;

                for (Object entryObj : map.entrySet()) {
                    Map.Entry entry = (Map.Entry) entryObj;
                    String keyFullText = getFullTextForAttribute(keyType, entry.getKey(), false);
                    if (StringUtils.isNotEmpty(keyFullText)) {
                        fullText = fullText.append(FULL_TEXT_DELIMITER).append(keyFullText);
                    }
                    String valueFullText = getFullTextForAttribute(valueType, entry.getValue(), false);
                    if (StringUtils.isNotEmpty(valueFullText)) {
                        fullText = fullText.append(FULL_TEXT_DELIMITER).append(valueFullText);
                    }
                }
                return fullText.toString();

            case CLASS:
                if (followReferences) {
                    String refGuid = ((ITypedReferenceableInstance) value).getId()._getId();
                    Vertex refVertex = getVertexForGUID(refGuid);
                    return getFullTextForVertex(refVertex, false);
                }
                break;

            case STRUCT:
                if (followReferences) {
                    return getFullTextForInstance((ITypedInstance) value, false);
                }
                break;

            default:
                throw new IllegalStateException("Unhandled type category " + type.getTypeCategory());

            }
            return null;
        }

        private String getFullTextForInstance(ITypedInstance typedInstance, boolean followReferences)
        throws AtlasException {
            StringBuilder fullText = new StringBuilder();
            for (AttributeInfo attributeInfo : typedInstance.fieldMapping().fields.values()) {
                Object attrValue = typedInstance.get(attributeInfo.name);
                if (attrValue == null) {
                    continue;
                }

                String attrFullText = getFullTextForAttribute(attributeInfo.dataType(), attrValue, followReferences);
                if (StringUtils.isNotEmpty(attrFullText)) {
                    fullText =
                            fullText.append(FULL_TEXT_DELIMITER).append(attributeInfo.name).append(FULL_TEXT_DELIMITER)
                                    .append(attrFullText);
                }
            }
            return fullText.toString();
        }

        /**
         * Step 2: Traverse oldIdToInstance map create newInstances :
         * List[ITypedReferenceableInstance]
         *  - create a ITypedReferenceableInstance.
         *   replace any old References ( ids or object references) with new Ids.
         */
        private List<ITypedReferenceableInstance> discoverInstances(EntityProcessor entityProcessor)
                throws RepositoryException {
            List<ITypedReferenceableInstance> newTypedInstances = new ArrayList<>();
            for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap.values()) {
                LOG.debug("Discovered instance {}", transientInstance.getTypeName());
                try {
                    ClassType cT = typeSystem.getDataType(ClassType.class, transientInstance.getTypeName());
                    ITypedReferenceableInstance newInstance = cT.convert(transientInstance, Multiplicity.REQUIRED);
                    newTypedInstances.add(newInstance);

                    // Now replace old references with new Ids
                    MapIds mapIds = new MapIds(entityProcessor.idToNewIdMap);
                    new ObjectGraphWalker(typeSystem, mapIds, newTypedInstances).walk();

                } catch (AtlasException me) {
                    throw new RepositoryException(
                            String.format("Failed to create Instance(id = %s", transientInstance.getId()), me);
                }
            }

            return newTypedInstances;
        }

        private String addDiscoveredInstances(IReferenceableInstance entity, EntityProcessor entityProcessor,
                List<ITypedReferenceableInstance> newTypedInstances) throws AtlasException {

            String typedInstanceGUID = null;
            for (ITypedReferenceableInstance typedInstance : newTypedInstances) { // Traverse over newInstances
                LOG.debug("Adding typed instance {}", typedInstance.getTypeName());

                Id id = typedInstance.getId();
                if (id == null) { // oops
                    throw new RepositoryException("id cannot be null");
                }

                Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);

                // add the attributes for the instance
                ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
                final Map<String, AttributeInfo> fields = classType.fieldMapping().fields;

                mapInstanceToVertex(id, typedInstance, instanceVertex, fields, entityProcessor.idToVertexMap);

                for (String traitName : typedInstance.getTraits()) {
                    LOG.debug("mapping trait {}", traitName);
                    ((TitanVertex) instanceVertex).addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
                    ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

                    // add the attributes for the trait instance
                    mapTraitInstanceToVertex(traitInstance, typedInstance, instanceVertex,
                            entityProcessor.idToVertexMap);
                }

                if (typedInstance.getId() == entity.getId()) { // save the guid for return
                    typedInstanceGUID = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                }
            }

            return typedInstanceGUID;
        }

        private void mapInstanceToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                Map<String, AttributeInfo> fields, Map<Id, Vertex> idToVertexMap) throws AtlasException {
            LOG.debug("Mapping instance {} of {} to vertex {}", typedInstance, typedInstance.getTypeName(),
                    instanceVertex);
            for (AttributeInfo attributeInfo : fields.values()) {
                final IDataType dataType = attributeInfo.dataType();
                mapAttributesToVertex(id, typedInstance, instanceVertex, idToVertexMap, attributeInfo, dataType);
            }
        }

        private void mapAttributesToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                Map<Id, Vertex> idToVertexMap, AttributeInfo attributeInfo, IDataType dataType) throws AtlasException {
            Object attrValue = typedInstance.get(attributeInfo.name);
            LOG.debug("mapping attribute {} = {}", attributeInfo.name, attrValue);
            final String propertyName = getQualifiedName(typedInstance, attributeInfo);
            String edgeLabel = getEdgeLabel(typedInstance, attributeInfo);
            if (attrValue == null) {
                return;
            }

            switch (dataType.getTypeCategory()) {
            case PRIMITIVE:
                mapPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
                break;

            case ENUM:
                //handles both int and string for enum
                EnumValue enumValue =
                        (EnumValue) dataType.convert(typedInstance.get(attributeInfo.name), Multiplicity.REQUIRED);
                addProperty(instanceVertex, propertyName, enumValue.value);
                break;

            case ARRAY:
                mapArrayCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                break;

            case MAP:
                mapMapCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                break;

            case STRUCT:
                Vertex structInstanceVertex =
                        mapStructInstanceToVertex(id, (ITypedStruct) typedInstance.get(attributeInfo.name),
                                attributeInfo, idToVertexMap);
                // add an edge to the newly created vertex from the parent
                GraphHelper.addEdge(titanGraph, instanceVertex, structInstanceVertex, edgeLabel);
                break;

            case TRAIT:
                // do NOTHING - this is taken care of earlier
                break;

            case CLASS:
                Id referenceId = (Id) typedInstance.get(attributeInfo.name);
                mapClassReferenceAsEdge(instanceVertex, idToVertexMap, edgeLabel, referenceId);
                break;

            default:
                throw new IllegalArgumentException("Unknown type category: " + dataType.getTypeCategory());
            }
        }

        private void mapArrayCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                AttributeInfo attributeInfo, Map<Id, Vertex> idToVertexMap) throws AtlasException {
            LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                    attributeInfo.name);
            List list = (List) typedInstance.get(attributeInfo.name);
            if (list == null || list.isEmpty()) {
                return;
            }

            String propertyName = getQualifiedName(typedInstance, attributeInfo);
            IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();

            List<String> values = new ArrayList<>(list.size());
            for (int index = 0; index < list.size(); index++) {
                String entryId =
                        mapCollectionEntryToVertex(id, instanceVertex, attributeInfo, idToVertexMap, elementType,
                                list.get(index), propertyName);
                values.add(entryId);
            }

            // for dereference on way out
            addProperty(instanceVertex, propertyName, values);
        }

        private void mapMapCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                AttributeInfo attributeInfo, Map<Id, Vertex> idToVertexMap) throws AtlasException {
            LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                    attributeInfo.name);
            @SuppressWarnings("unchecked") Map<Object, Object> collection =
                    (Map<Object, Object>) typedInstance.get(attributeInfo.name);
            if (collection == null || collection.isEmpty()) {
                return;
            }

            String propertyName = getQualifiedName(typedInstance, attributeInfo);
            IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();
            for (Map.Entry entry : collection.entrySet()) {
                String myPropertyName = propertyName + "." + entry.getKey().toString();
                String value = mapCollectionEntryToVertex(id, instanceVertex, attributeInfo, idToVertexMap, elementType,
                        entry.getValue(), myPropertyName);
                addProperty(instanceVertex, myPropertyName, value);
            }

            // for dereference on way out
            addProperty(instanceVertex, propertyName, new ArrayList(collection.keySet()));
        }

        private String mapCollectionEntryToVertex(Id id, Vertex instanceVertex, AttributeInfo attributeInfo,
                Map<Id, Vertex> idToVertexMap, IDataType elementType, Object value, String propertyName)
        throws AtlasException {
            final String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
            switch (elementType.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
                return value.toString();

            case ARRAY:
            case MAP:
            case TRAIT:
                // do nothing
                return null;

            case STRUCT:
                Vertex structInstanceVertex =
                        mapStructInstanceToVertex(id, (ITypedStruct) value, attributeInfo, idToVertexMap);
                // add an edge to the newly created vertex from the parent
                Edge structElementEdge =
                        GraphHelper.addEdge(titanGraph, instanceVertex, structInstanceVertex, edgeLabel);
                return structElementEdge.getId().toString();

            case CLASS:
                Id referenceId = (Id) value;
                return mapClassReferenceAsEdge(instanceVertex, idToVertexMap, edgeLabel, referenceId);

            default:
                throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
            }
        }

        private String mapClassReferenceAsEdge(Vertex instanceVertex, Map<Id, Vertex> idToVertexMap, String propertyKey,
                Id id) throws AtlasException {
            if (id != null) {
                Vertex referenceVertex;
                if (id.isAssigned()) {
                    referenceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                } else {
                    referenceVertex = idToVertexMap.get(id);
                }

                if (referenceVertex != null) {
                    // add an edge to the class vertex from the instance
                    Edge edge = GraphHelper.addEdge(titanGraph, instanceVertex, referenceVertex, propertyKey);
                    return String.valueOf(edge.getId());
                }
            }

            return null;
        }

        private Vertex mapStructInstanceToVertex(Id id, ITypedStruct structInstance, AttributeInfo attributeInfo,
                Map<Id, Vertex> idToVertexMap) throws AtlasException {
            // add a new vertex for the struct or trait instance
            Vertex structInstanceVertex = GraphHelper
                    .createVertexWithoutIdentity(titanGraph, structInstance.getTypeName(), id,
                            Collections.<String>emptySet()); // no super types for struct type
            LOG.debug("created vertex {} for struct {} value {}", structInstanceVertex, attributeInfo.name,
                    structInstance);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(id, structInstance, structInstanceVertex, structInstance.fieldMapping().fields,
                    idToVertexMap);

            return structInstanceVertex;
        }

        private void mapTraitInstanceToVertex(ITypedStruct traitInstance, ITypedReferenceableInstance typedInstance,
                Vertex parentInstanceVertex, Map<Id, Vertex> idToVertexMap) throws AtlasException {
            // add a new vertex for the struct or trait instance
            mapTraitInstanceToVertex(traitInstance, typedInstance.getId(), typedInstance.getTypeName(),
                    parentInstanceVertex, idToVertexMap);
        }

        private void mapTraitInstanceToVertex(ITypedStruct traitInstance, Id typedInstanceId,
                String typedInstanceTypeName, Vertex parentInstanceVertex, Map<Id, Vertex> idToVertexMap)
        throws AtlasException {
            // add a new vertex for the struct or trait instance
            final String traitName = traitInstance.getTypeName();
            Vertex traitInstanceVertex = GraphHelper
                    .createVertexWithoutIdentity(titanGraph, traitInstance.getTypeName(), typedInstanceId,
                            typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
            LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(typedInstanceId, traitInstance, traitInstanceVertex,
                    traitInstance.fieldMapping().fields, idToVertexMap);

            // add an edge to the newly created vertex from the parent
            String relationshipLabel = getEdgeLabel(typedInstanceTypeName, traitName);
            GraphHelper.addEdge(titanGraph, parentInstanceVertex, traitInstanceVertex, relationshipLabel);
        }

        private void mapPrimitiveToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                AttributeInfo attributeInfo) throws AtlasException {
            Object attrValue = typedInstance.get(attributeInfo.name);
            if (attrValue == null) {
                return; // add only if instance has this attribute
            }

            final String vertexPropertyName = getQualifiedName(typedInstance, attributeInfo);
            Object propertyValue = null;
            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
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
            }
            addProperty(instanceVertex, vertexPropertyName, propertyValue);
        }
    }

    private void addProperty(Vertex vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.setProperty(propertyName, value);
    }

    public final class GraphToTypedInstanceMapper {

        public ITypedReferenceableInstance mapGraphToTypedInstance(String guid, Vertex instanceVertex)
        throws AtlasException {

            LOG.debug("Mapping graph root vertex {} to typed instance for guid {}", instanceVertex, guid);
            String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
            List<String> traits = getTraitNames(instanceVertex);

            Id id = new Id(guid, instanceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), typeName);
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
            final String vertexPropertyName = getQualifiedName(typedInstance, attributeInfo);

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
                mapVertexToStructInstance(instanceVertex, typedInstance, attributeInfo);
                break;

            case TRAIT:
                // do NOTHING - handled in class
                break;

            case CLASS:
                String relationshipLabel = getEdgeLabel(typedInstance, attributeInfo);
                Object idOrInstance = mapClassReferenceToVertex(instanceVertex, attributeInfo, relationshipLabel,
                        attributeInfo.dataType());
                typedInstance.set(attributeInfo.name, idOrInstance);
                break;

            default:
                break;
            }
        }

        private Object mapClassReferenceToVertex(Vertex instanceVertex, AttributeInfo attributeInfo,
                String relationshipLabel, IDataType dataType) throws AtlasException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            Iterator<Edge> results = instanceVertex.getEdges(Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) {
                final Vertex referenceVertex = results.next().getVertex(Direction.IN);
                if (referenceVertex != null) {
                    final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel, guid);
                    if (attributeInfo.isComposite) {
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

            ArrayList values = new ArrayList();
            for (Object listElement : list) {
                values.add(mapVertexToCollectionEntry(instanceVertex, attributeInfo, elementType, listElement,
                        propertyName));
            }

            typedInstance.set(attributeInfo.name, values);
        }

        private Object mapVertexToCollectionEntry(Vertex instanceVertex, AttributeInfo attributeInfo,
                IDataType elementType, Object value, String propertyName) throws AtlasException {
            String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
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
                return getStructInstanceFromVertex(instanceVertex, elementType, attributeInfo.name, edgeLabel,
                        (String) value);

            case CLASS:
                return mapClassReferenceToVertex(instanceVertex, attributeInfo, edgeLabel, elementType, (String) value);

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
                String keyPropertyName = propertyName + "." + key;
                Object keyValue = instanceVertex.getProperty(keyPropertyName);
                values.put(key,
                        mapVertexToCollectionEntry(instanceVertex, attributeInfo, valueType, keyValue, propertyName));
            }

            typedInstance.set(attributeInfo.name, values);
        }

        private ITypedStruct getStructInstanceFromVertex(Vertex instanceVertex, IDataType elemType,
                String attributeName, String relationshipLabel, String edgeId) throws AtlasException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                if (edgeId.equals(String.valueOf(edge.getId()))) {
                    Vertex structInstanceVertex = edge.getVertex(Direction.IN);
                    LOG.debug("mapping vertex {} to struct {}", structInstanceVertex, attributeName);

                    if (structInstanceVertex != null) {
                        LOG.debug("Found struct instance vertex {}, mapping to instance {} ", structInstanceVertex,
                                elemType.getName());
                        StructType structType = typeSystem.getDataType(StructType.class, elemType.getName());
                        ITypedStruct structInstance = structType.createInstance();

                        mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);
                        return structInstance;
                    }

                    break;
                }
            }

            return null;
        }

        private Object mapClassReferenceToVertex(Vertex instanceVertex, AttributeInfo attributeInfo,
                String relationshipLabel, IDataType dataType, String edgeId) throws AtlasException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                if (edgeId.equals(String.valueOf(edge.getId()))) {
                    final Vertex referenceVertex = edge.getVertex(Direction.IN);
                    if (referenceVertex != null) {
                        final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                        LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel,
                                guid);
                        if (attributeInfo.isComposite) {
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

                    break;
                }
            }

            return null;
        }

        private void mapVertexToStructInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                AttributeInfo attributeInfo) throws AtlasException {
            LOG.debug("mapping vertex {} to struct {}", instanceVertex, attributeInfo.name);
            StructType structType = typeSystem.getDataType(StructType.class, attributeInfo.dataType().getName());
            ITypedStruct structInstance = structType.createInstance();
            typedInstance.set(attributeInfo.name, structInstance);

            String relationshipLabel = getEdgeLabel(typedInstance, attributeInfo);
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                final Vertex structInstanceVertex = edge.getVertex(Direction.IN);
                if (structInstanceVertex != null) {
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ", structInstanceVertex,
                            structInstance.getTypeName());
                    mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);
                    break;
                }
            }
        }

        private void mapVertexToTraitInstance(Vertex instanceVertex, ITypedReferenceableInstance typedInstance,
                String traitName, TraitType traitType) throws AtlasException {
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(), traitName, traitType, traitInstance);
        }

        private void mapVertexToTraitInstance(Vertex instanceVertex, String typedInstanceTypeName, String traitName,
                TraitType traitType, ITypedStruct traitInstance) throws AtlasException {
            String relationshipLabel = getEdgeLabel(typedInstanceTypeName, traitName);
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
            final String vertexPropertyName = getQualifiedName(typedInstance, attributeInfo);
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
    }
}
