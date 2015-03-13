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

package org.apache.hadoop.metadata.repository.graph;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.apache.hadoop.metadata.typesystem.IReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.ITypedInstance;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.ITypedStruct;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.apache.hadoop.metadata.typesystem.persistence.MapIds;
import org.apache.hadoop.metadata.typesystem.types.AttributeInfo;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.ObjectGraphWalker;
import org.apache.hadoop.metadata.typesystem.types.StructType;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private final AtomicInteger ID_SEQ = new AtomicInteger(0);

    private final TypedInstanceToGraphMapper instanceToGraphMapper
            = new TypedInstanceToGraphMapper();
    private final GraphToTypedInstanceMapper graphToInstanceMapper
            = new GraphToTypedInstanceMapper();

    private final GraphService graphService;
    private final TypeSystem typeSystem;

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedMetadataRepository(GraphService graphService) throws MetadataException {
        this.graphService = graphService;
        this.typeSystem = TypeSystem.getInstance();

        this.titanGraph = ((TitanGraphService) graphService).getTitanGraph();
    }

    public GraphToTypedInstanceMapper getGraphToInstanceMapper() {
        return graphToInstanceMapper;
    }

    @Override
    public String getTypeAttributeName() {
        return Constants.ENTITY_TYPE_PROPERTY_KEY;
    }

    @Override
    public String getTraitLabel(IDataType<?> dataType, String traitName) {
        return dataType.getName() + "." + traitName;
    }

    @Override
    public String getFieldNameInVertex(IDataType<?> dataType, AttributeInfo aInfo) {
        return dataType.getName() + "." + aInfo.name;
    }

    @Override
    public String getEdgeLabel(IDataType<?> dataType, AttributeInfo aInfo) {
        return dataType.getName() + "." + aInfo.name;
    }

    @Override
    public String createEntity(IReferenceableInstance typedInstance,
                               String typeName) throws RepositoryException {
        LOG.info("adding entity={} type={}", typedInstance, typeName);

        try {
            titanGraph.rollback();
            final String guid = instanceToGraphMapper.mapTypedInstanceToGraph(typedInstance);
            titanGraph.commit();  // commit if there are no errors
            return guid;

        } catch (MetadataException e) {
            titanGraph.rollback();
            throw new RepositoryException(e);
        }
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException {
        LOG.info("Retrieving entity with guid={}", guid);

        try {
            titanGraph.rollback();  // clean up before starting a query
            Vertex instanceVertex = getVertexForGUID(guid);

            LOG.debug("Found a vertex {} for guid {}", instanceVertex, guid);
            return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);

        } catch (MetadataException e) {
            throw new RepositoryException(e);
        }
    }

    private Vertex getVertexForGUID(String guid) throws RepositoryException {
        Vertex instanceVertex = GraphHelper.findVertexByGUID(titanGraph, guid);
        if (instanceVertex == null) {
            LOG.debug("Could not find a vertex for guid={}", guid);
            throw new RepositoryException(
                    "Could not find an entity in the repository for guid: " + guid);
        }

        return instanceVertex;
    }

    @Override
    public List<String> getEntityList(String entityType) throws RepositoryException {
        LOG.info("Retrieving entity list for type={}", entityType);
        GraphQuery query = graphService.getBlueprintsGraph().query()
                .has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityType);
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
    public List<String> getTraitNames(String guid) throws RepositoryException {
        LOG.info("Retrieving trait names for entity={}", guid);
        titanGraph.rollback();  // clean up before starting a query
        Vertex instanceVertex = getVertexForGUID(guid);
        return getTraitNames(instanceVertex);
    }

    public List<String> getTraitNames(Vertex entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        for (TitanProperty property : ((TitanVertex) entityVertex)
                .getProperties(Constants.TRAIT_NAMES_PROPERTY_KEY)) {
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
    public void addTrait(String guid,
                         ITypedStruct traitInstance) throws RepositoryException {
        Preconditions.checkNotNull(traitInstance, "Trait instance cannot be null");
        final String traitName = traitInstance.getTypeName();
        LOG.info("Adding a new trait={} for entity={}", traitName, guid);

        try {
            titanGraph.rollback();  // clean up before starting a query
            Vertex instanceVertex = getVertexForGUID(guid);

            // add the trait instance as a new vertex
            final String typeName = getTypeName(instanceVertex);
            instanceToGraphMapper.mapTraitInstanceToVertex(
                    traitInstance, getIdFromVertex(typeName, instanceVertex),
                    typeName, instanceVertex, Collections.<Id, Vertex>emptyMap());

            // update the traits in entity once adding trait instance is successful
            ((TitanVertex) instanceVertex)
                    .addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);

            titanGraph.commit();  // commit if there are no errors
        } catch (MetadataException e) {
            titanGraph.rollback();
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
    public void deleteTrait(String guid, String traitNameToBeDeleted)
            throws RepositoryException {
        LOG.info("Deleting trait={} from entity={}", traitNameToBeDeleted, guid);
        try {
            titanGraph.rollback();  // clean up before starting a query
            Vertex instanceVertex = getVertexForGUID(guid);

            List<String> traitNames = getTraitNames(instanceVertex);
            if (!traitNames.contains(traitNameToBeDeleted)) {
                throw new RepositoryException("Could not find trait=" + traitNameToBeDeleted
                        + " in the repository for entity: " + guid);
            }

            final String entityTypeName = getTypeName(instanceVertex);
            String relationshipLabel = entityTypeName + "." + traitNameToBeDeleted;
            Iterator<Edge> results = instanceVertex.getEdges(
                    Direction.OUT, relationshipLabel).iterator();
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

                titanGraph.commit();  // commit if there are no errors
            }
        } catch (Exception e) {
            titanGraph.rollback();
            throw new RepositoryException(e);
        }
    }

    private void updateTraits(Vertex instanceVertex, List<String> traitNames) {
        // remove the key
        instanceVertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);

        // add it back again
        for (String traitName : traitNames) {
            ((TitanVertex) instanceVertex).addProperty(
                    Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
        }
    }

    public Id getIdFromVertex(String dataTypeName, Vertex vertex) {
        return new Id(
                vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY),
                vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                dataTypeName);
    }

    String getTypeName(Vertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
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
        public void processNode(ObjectGraphWalker.Node nd) throws MetadataException {
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
                            throw new RepositoryException(String.format(
                                    "Unexpected internal error: Id %s processed again", id));
                        }

                        idToInstanceMap.put(id, ref);
                    }
                }
            }
        }

        public void createVerticesForClassTypes(List<ITypedReferenceableInstance> newInstances) {
            for (ITypedReferenceableInstance typedInstance : newInstances) {
                final Id id = typedInstance.getId();
                if (!idToVertexMap.containsKey(id)) {
                    Vertex instanceVertex;
                    if (id.isAssigned()) {  // has a GUID
                        instanceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                    } else {
                        instanceVertex =
                                GraphHelper.createVertexWithIdentity(titanGraph, typedInstance);
                    }

                    idToVertexMap.put(id, instanceVertex);
                }
            }
        }
    }

    private final class TypedInstanceToGraphMapper {

        private String mapTypedInstanceToGraph(IReferenceableInstance typedInstance)
            throws MetadataException {

            EntityProcessor entityProcessor = new EntityProcessor();
            try {
                LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
                new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
            } catch (MetadataException me) {
                throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
            }

            List<ITypedReferenceableInstance> newTypedInstances = discoverInstances(
                    entityProcessor);
            entityProcessor.createVerticesForClassTypes(newTypedInstances);
            return addDiscoveredInstances(typedInstance, entityProcessor, newTypedInstances);
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
            for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap
                    .values()) {
                LOG.debug("Discovered instance {}", transientInstance.getTypeName());
                try {
                    ClassType cT = typeSystem.getDataType(
                            ClassType.class, transientInstance.getTypeName());
                    ITypedReferenceableInstance newInstance = cT.convert(
                            transientInstance, Multiplicity.REQUIRED);
                    newTypedInstances.add(newInstance);

                    // Now replace old references with new Ids
                    MapIds mapIds = new MapIds(entityProcessor.idToNewIdMap);
                    new ObjectGraphWalker(typeSystem, mapIds, newTypedInstances).walk();

                } catch (MetadataException me) {
                    throw new RepositoryException(
                            String.format("Failed to create Instance(id = %s",
                                    transientInstance.getId()), me);
                }
            }

            return newTypedInstances;
        }

        private String addDiscoveredInstances(IReferenceableInstance entity,
                                              EntityProcessor entityProcessor,
                                              List<ITypedReferenceableInstance> newTypedInstances)
        throws MetadataException {
            String typedInstanceGUID = null;
            for (ITypedReferenceableInstance typedInstance : newTypedInstances) { // Traverse
            // over newInstances
                LOG.debug("Adding typed instance {}", typedInstance.getTypeName());

                Id id = typedInstance.getId();
                if (id == null) { // oops
                    throw new RepositoryException("id cannot be null");
                }

                Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);

                // add the attributes for the instance
                final Map<String, AttributeInfo> fields = typedInstance.fieldMapping().fields;

                mapInstanceToVertex(
                        id, typedInstance, instanceVertex, fields, entityProcessor.idToVertexMap);

                for (String traitName : typedInstance.getTraits()) {
                    LOG.debug("mapping trait {}", traitName);
                    ((TitanVertex) instanceVertex)
                            .addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
                    ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

                    // add the attributes for the trait instance
                    mapTraitInstanceToVertex(traitInstance, typedInstance,
                            instanceVertex, entityProcessor.idToVertexMap);
                }

                if (typedInstance.getId() == entity.getId()) { // save the guid for return
                    typedInstanceGUID = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                }
            }

            return typedInstanceGUID;
        }

        private void mapInstanceToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
                                         Map<String, AttributeInfo> fields,
                                         Map<Id, Vertex> idToVertexMap) throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for fields {}",
                    typedInstance.getTypeName(), instanceVertex, fields);
            for (AttributeInfo attributeInfo : fields.values()) {
                final IDataType dataType = attributeInfo.dataType();
                mapAttributesToVertex(id, typedInstance, instanceVertex,
                        idToVertexMap, attributeInfo, dataType);
            }
        }

        private void mapAttributesToVertex(Id id, ITypedInstance typedInstance,
                                           Vertex instanceVertex,
                                           Map<Id, Vertex> idToVertexMap,
                                           AttributeInfo attributeInfo,
                                           IDataType dataType) throws MetadataException {
            LOG.debug("mapping attributeInfo {}", attributeInfo);
            final String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;

            switch (dataType.getTypeCategory()) {
                case PRIMITIVE:
                    mapPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
                    break;

                case ENUM:
                    instanceVertex.setProperty(propertyName,
                            typedInstance.getInt(attributeInfo.name));
                    break;

                case ARRAY:
                    mapArrayCollectionToVertex(
                            id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                    break;

                case MAP:
                    mapMapCollectionToVertex(
                            id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
                    break;

                case STRUCT:
                    Vertex structInstanceVertex = mapStructInstanceToVertex(id,
                            (ITypedStruct) typedInstance.get(attributeInfo.name),
                            attributeInfo, idToVertexMap);
                    // add an edge to the newly created vertex from the parent
                    GraphHelper.addEdge(
                            titanGraph, instanceVertex, structInstanceVertex, propertyName);
                    break;

                case TRAIT:
                    // do NOTHING - this is taken care of earlier
                    break;

                case CLASS:
                    Id referenceId = (Id) typedInstance.get(attributeInfo.name);
                    mapClassReferenceAsEdge(
                            instanceVertex, idToVertexMap, propertyName, referenceId
                    );
                    break;

                default:
                    break;
            }
        }

        private void mapArrayCollectionToVertex(Id id, ITypedInstance typedInstance,
                                                Vertex instanceVertex,
                                                AttributeInfo attributeInfo,
                                                Map<Id, Vertex> idToVertexMap)
        throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for name {}",
                    typedInstance.getTypeName(), instanceVertex, attributeInfo.name);
            List list = (List) typedInstance.get(attributeInfo.name);
            if (list == null || list.isEmpty()) {
                return;
            }

            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();

            StringBuilder buffer = new StringBuilder();
            Object[] array = list.toArray();
            for (int index = 0; index < array.length; index++) {
                String propertyNameWithSuffix = propertyName + "." + index;
                buffer.append(propertyNameWithSuffix).append(",");
                mapCollectionEntryToVertex(id, instanceVertex, attributeInfo,
                        idToVertexMap, elementType, array[index], propertyNameWithSuffix);
            }

            buffer.setLength(buffer.length() - 1);
            // for dereference on way out
            instanceVertex.setProperty(propertyName, buffer.toString());
        }

        private void mapMapCollectionToVertex(Id id, ITypedInstance typedInstance,
                                              Vertex instanceVertex,
                                              AttributeInfo attributeInfo,
                                              Map<Id, Vertex> idToVertexMap)
        throws MetadataException {
            LOG.debug("Mapping instance {} to vertex {} for name {}",
                    typedInstance.getTypeName(), instanceVertex, attributeInfo.name);
            @SuppressWarnings("unchecked")
            Map<Object, Object> collection = (Map<Object, Object>) typedInstance
                    .get(attributeInfo.name);
            if (collection == null || collection.isEmpty()) {
                return;
            }

            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            StringBuilder buffer = new StringBuilder();
            IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();
            for (Map.Entry entry : collection.entrySet()) {
                String propertyNameWithSuffix = propertyName + "." + entry.getKey();
                buffer.append(propertyNameWithSuffix).append(",");
                mapCollectionEntryToVertex(id, instanceVertex, attributeInfo,
                        idToVertexMap, elementType, entry.getValue(), propertyNameWithSuffix);
            }

            buffer.setLength(buffer.length() - 1);
            // for dereference on way out
            instanceVertex.setProperty(propertyName, buffer.toString());
        }

        private void mapCollectionEntryToVertex(Id id, Vertex instanceVertex,
                                                AttributeInfo attributeInfo,
                                                Map<Id, Vertex> idToVertexMap,
                                                IDataType elementType, Object value,
                                                String propertyName) throws MetadataException {
            switch (elementType.getTypeCategory()) {
                case PRIMITIVE:
                    instanceVertex.setProperty(propertyName, value);
                    break;

                case ENUM:
                    instanceVertex.setProperty(propertyName, value);
                    break;

                case ARRAY:
                case MAP:
                case TRAIT:
                    // do nothing
                    break;

                case STRUCT:
                    Vertex structInstanceVertex = mapStructInstanceToVertex(id,
                            (ITypedStruct) value, attributeInfo, idToVertexMap);
                    // add an edge to the newly created vertex from the parent
                    GraphHelper.addEdge(
                            titanGraph, instanceVertex, structInstanceVertex, propertyName);
                    break;

                case CLASS:
                    Id referenceId = (Id) value;
                    mapClassReferenceAsEdge(
                            instanceVertex, idToVertexMap, propertyName, referenceId);
                    break;

                default:
                    break;
            }
        }

        private void mapClassReferenceAsEdge(Vertex instanceVertex,
                                             Map<Id, Vertex> idToVertexMap,
                                             String propertyKey, Id id) throws MetadataException {

            if (id != null) {
                Vertex referenceVertex;
                if (id.isAssigned()) {
                    referenceVertex = GraphHelper.findVertexByGUID(titanGraph, id.id);
                } else {
                    referenceVertex = idToVertexMap.get(id);
                }

                if (referenceVertex != null) {
                    // add an edge to the class vertex from the instance
                    GraphHelper.addEdge(titanGraph, instanceVertex, referenceVertex, propertyKey);
                }
            }
        }

        private Vertex mapStructInstanceToVertex(Id id, ITypedStruct structInstance,
                                                 AttributeInfo attributeInfo,
                                                 Map<Id, Vertex> idToVertexMap)
        throws MetadataException {
            // add a new vertex for the struct or trait instance
            Vertex structInstanceVertex = GraphHelper.createVertexWithoutIdentity(
                    graphService.getBlueprintsGraph(), structInstance.getTypeName(), id);
            LOG.debug("created vertex {} for struct {}", structInstanceVertex, attributeInfo.name);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(id, structInstance, structInstanceVertex,
                    structInstance.fieldMapping().fields, idToVertexMap);

            return structInstanceVertex;
        }

        private void mapTraitInstanceToVertex(ITypedStruct traitInstance,
                                              ITypedReferenceableInstance typedInstance,
                                              Vertex parentInstanceVertex,
                                              Map<Id, Vertex> idToVertexMap)
            throws MetadataException {
            // add a new vertex for the struct or trait instance
            mapTraitInstanceToVertex(traitInstance, typedInstance.getId(),
                    typedInstance.getTypeName(), parentInstanceVertex, idToVertexMap);
        }

        private void mapTraitInstanceToVertex(ITypedStruct traitInstance,
                                              Id typedInstanceId, String typedInstanceTypeName,
                                              Vertex parentInstanceVertex,
                                              Map<Id, Vertex> idToVertexMap)
            throws MetadataException {
            // add a new vertex for the struct or trait instance
            final String traitName = traitInstance.getTypeName();
            Vertex traitInstanceVertex = GraphHelper.createVertexWithoutIdentity(
                    graphService.getBlueprintsGraph(), traitInstance.getTypeName(), typedInstanceId);
            LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

            // map all the attributes to this newly created vertex
            mapInstanceToVertex(typedInstanceId, traitInstance, traitInstanceVertex,
                    traitInstance.fieldMapping().fields, idToVertexMap);

            // add an edge to the newly created vertex from the parent
            String relationshipLabel = typedInstanceTypeName + "." + traitName;
            GraphHelper.addEdge(
                    titanGraph, parentInstanceVertex, traitInstanceVertex, relationshipLabel);
        }

        private void mapPrimitiveToVertex(ITypedInstance typedInstance,
                                          Vertex instanceVertex,
                                          AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("Adding primitive {} to v {}", attributeInfo, instanceVertex);
            if (typedInstance.get(attributeInfo.name) ==
                    null) { // add only if instance has this attribute
                return;
            }

            final String vertexPropertyName = typedInstance.getTypeName() + "." +
                    attributeInfo.name;

            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getString(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getShort(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getInt(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBigInt(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBoolean(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getByte(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getLong(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getFloat(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getDouble(attributeInfo.name));
            } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                instanceVertex.setProperty(vertexPropertyName,
                        typedInstance.getBigDecimal(attributeInfo.name));
            }
        }
    }

    public final class GraphToTypedInstanceMapper {

        public ITypedReferenceableInstance mapGraphToTypedInstance(String guid,
                                                                   Vertex instanceVertex)
            throws MetadataException {

            LOG.debug("Mapping graph root vertex {} to typed instance for guid {}",
                    instanceVertex, guid);
            String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
            List<String> traits = getTraitNames(instanceVertex);

            Id id = new Id(guid,
                    instanceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), typeName);
            LOG.debug("Created id {} for instance type {}", id, typeName);

            ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
            ITypedReferenceableInstance typedInstance = classType.createInstance(
                    id, traits.toArray(new String[traits.size()]));

            mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
            mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);

            return typedInstance;
        }

        public void mapVertexToInstanceTraits(Vertex instanceVertex,
                                              ITypedReferenceableInstance typedInstance,
                                              List<String> traits) throws MetadataException {
            for (String traitName : traits) {
                LOG.debug("mapping trait {} to instance", traitName);
                TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
                mapVertexToTraitInstance(
                        instanceVertex, typedInstance, traitName, traitType);
            }
        }

        public void mapVertexToInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                        Map<String, AttributeInfo> fields)
        throws MetadataException {

            LOG.debug("Mapping vertex {} to instance {} for fields",
                    instanceVertex, typedInstance.getTypeName(), fields);
            for (AttributeInfo attributeInfo : fields.values()) {
                mapVertexToAttribute(instanceVertex, typedInstance, attributeInfo);
            }
        }

        public void mapVertexToAttribute(Vertex instanceVertex, ITypedInstance typedInstance,
                                         AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping attributeInfo = " + attributeInfo);
            final IDataType dataType = attributeInfo.dataType();
            final String vertexPropertyName =
                    typedInstance.getTypeName() + "." + attributeInfo.name;

            switch (dataType.getTypeCategory()) {
                case PRIMITIVE:
                    mapVertexToPrimitive(instanceVertex, typedInstance, attributeInfo);
                    break;  // add only if vertex has this attribute

                case ENUM:
                    typedInstance.setInt(attributeInfo.name,
                            instanceVertex.<Integer>getProperty(vertexPropertyName));
                    break;

                case ARRAY:
                    mapVertexToArrayInstance(instanceVertex, typedInstance, attributeInfo);
                    break;

                case MAP:
                    mapVertexToMapInstance(instanceVertex, typedInstance, attributeInfo);
                    break;

                case STRUCT:
                    mapVertexToStructInstance(instanceVertex, typedInstance, attributeInfo);
                    break;

                case TRAIT:
                    // do NOTHING - handled in class
                    break;

                case CLASS:
                    String relationshipLabel = typedInstance.getTypeName() + "." +
                            attributeInfo.name;
                    Object idOrInstance = mapClassReferenceToVertex(instanceVertex,
                            attributeInfo, relationshipLabel, attributeInfo.dataType());
                    typedInstance.set(attributeInfo.name, idOrInstance);
                    break;

                default:
                    break;
            }
        }

        public Object mapClassReferenceToVertex(Vertex instanceVertex,
                                                AttributeInfo attributeInfo,
                                                String relationshipLabel,
                                                IDataType dataType) throws MetadataException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            Iterator<Edge> results = instanceVertex.getEdges(
                    Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) {
                final Vertex referenceVertex = results.next().getVertex(Direction.IN);
                if (referenceVertex != null) {
                    final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    LOG.debug("Found vertex {} for label {} with guid {}",
                            referenceVertex, relationshipLabel, guid);
                    if (attributeInfo.isComposite) {
                        LOG.debug("Found composite, mapping vertex to instance");
                        return mapGraphToTypedInstance(guid, referenceVertex);
                    } else {
                        Id referenceId = new Id(guid,
                                referenceVertex
                                        .<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                                dataType.getName());
                        LOG.debug("Found non-composite, adding id {} ", referenceId);
                        return referenceId;
                    }
                }
            }

            return null;
        }

        @SuppressWarnings("unchecked")
        public void mapVertexToArrayInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                             AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            String keys = instanceVertex.getProperty(propertyName);
            if (keys == null || keys.length() == 0) {
                return;
            }
            DataTypes.ArrayType arrayType = (DataTypes.ArrayType) attributeInfo.dataType();
            final IDataType elementType = arrayType.getElemType();

            ArrayList values = new ArrayList();
            for (String propertyNameWithSuffix : keys.split(",")) {
                values.add(mapVertexToCollectionEntry(
                        instanceVertex, attributeInfo, elementType, propertyNameWithSuffix));
            }

            typedInstance.set(attributeInfo.name, values);
        }

        public Object mapVertexToCollectionEntry(Vertex instanceVertex,
                                                 AttributeInfo attributeInfo,
                                                 IDataType elementType,
                                                 String propertyNameWithSuffix)
        throws MetadataException {

            switch (elementType.getTypeCategory()) {
                case PRIMITIVE:
                    return instanceVertex.getProperty(propertyNameWithSuffix);

                case ENUM:
                    return instanceVertex.<Integer>getProperty(propertyNameWithSuffix);

                case ARRAY:
                case MAP:
                case TRAIT:
                    // do nothing
                    break;

                case STRUCT:
                    return getStructInstanceFromVertex(instanceVertex,
                            elementType, attributeInfo.name, propertyNameWithSuffix);

                case CLASS:
                    return mapClassReferenceToVertex(
                            instanceVertex, attributeInfo, propertyNameWithSuffix, elementType);

                default:
                    break;
            }

            throw new IllegalArgumentException();
        }

        @SuppressWarnings("unchecked")
        private void mapVertexToMapInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                            AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
            String propertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
            String keys = instanceVertex.getProperty(propertyName);
            if (keys == null || keys.length() == 0) {
                return;
            }
            DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
            final IDataType elementType = mapType.getValueType();

            HashMap values = new HashMap();
            for (String propertyNameWithSuffix : keys.split(",")) {
                final String key = propertyNameWithSuffix.substring(
                        propertyNameWithSuffix.lastIndexOf("."), propertyNameWithSuffix.length());
                values.put(key, mapVertexToCollectionEntry(
                        instanceVertex, attributeInfo, elementType, propertyNameWithSuffix));
            }

            typedInstance.set(attributeInfo.name, values);
        }

        private ITypedStruct getStructInstanceFromVertex(Vertex instanceVertex,
                                                         IDataType elemType,
                                                         String attributeName,
                                                         String relationshipLabel)
        throws MetadataException {
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            Iterator<Edge> results = instanceVertex.getEdges(
                    Direction.OUT, relationshipLabel).iterator();
            Edge edge = results.hasNext() ? results.next() : null;
            if (edge == null) {
                return null;
            }

            Vertex structInstanceVertex = edge.getVertex(Direction.IN);
            LOG.debug("mapping vertex {} to struct {}", structInstanceVertex, attributeName);

            if (structInstanceVertex != null) {
                LOG.debug("Found struct instance vertex {}, mapping to instance {} ",
                        structInstanceVertex, elemType.getName());
                StructType structType = typeSystem
                        .getDataType(StructType.class, elemType.getName());
                ITypedStruct structInstance = structType.createInstance();

                mapVertexToInstance(structInstanceVertex, structInstance,
                        structType.fieldMapping().fields);
                return structInstance;
            }

            return null;
        }

        private void mapVertexToStructInstance(Vertex instanceVertex,
                                               ITypedInstance typedInstance,
                                               AttributeInfo attributeInfo)
        throws MetadataException {
            LOG.debug("mapping vertex {} to struct {}", instanceVertex, attributeInfo.name);
            StructType structType = typeSystem.getDataType(
                    StructType.class, attributeInfo.dataType().getName());
            ITypedStruct structInstance = structType.createInstance();
            typedInstance.set(attributeInfo.name, structInstance);

            String relationshipLabel = typedInstance.getTypeName() + "." + attributeInfo.name;
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                final Vertex structInstanceVertex = edge.getVertex(Direction.IN);
                if (structInstanceVertex != null) {
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ",
                            structInstanceVertex, structInstance.getTypeName());
                    mapVertexToInstance(structInstanceVertex, structInstance,
                            structType.fieldMapping().fields);
                    break;
                }
            }
        }

        private void mapVertexToTraitInstance(Vertex instanceVertex,
                                              ITypedReferenceableInstance typedInstance,
                                              String traitName,
                                              TraitType traitType) throws MetadataException {
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(),
                    traitName, traitType, traitInstance);
        }

        public void mapVertexToTraitInstance(Vertex instanceVertex, String typeName,
                                             String traitName, TraitType traitType,
                                             ITypedStruct traitInstance) throws MetadataException {
            String relationshipLabel = typeName + "." + traitName;
            LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
            for (Edge edge : instanceVertex.getEdges(Direction.OUT, relationshipLabel)) {
                final Vertex traitInstanceVertex = edge.getVertex(Direction.IN);
                if (traitInstanceVertex != null) {
                    LOG.debug("Found trait instance vertex {}, mapping to instance {} ",
                            traitInstanceVertex, traitInstance.getTypeName());
                    mapVertexToInstance(traitInstanceVertex, traitInstance,
                            traitType.fieldMapping().fields);
                    break;
                }
            }
        }

        private void mapVertexToPrimitive(Vertex instanceVertex,
                                          ITypedInstance typedInstance,
                                          AttributeInfo attributeInfo) throws MetadataException {
            LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
            final String vertexPropertyName = typedInstance.getTypeName() + "." +
                    attributeInfo.name;
            if (instanceVertex.getProperty(vertexPropertyName) == null) {
                return;
            }

            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
                typedInstance.setString(attributeInfo.name,
                        instanceVertex.<String>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
                typedInstance.setShort(attributeInfo.name,
                        instanceVertex.<Short>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
                typedInstance.setInt(attributeInfo.name,
                        instanceVertex.<Integer>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
                typedInstance.setBigInt(attributeInfo.name,
                        instanceVertex.<BigInteger>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
                typedInstance.setBoolean(attributeInfo.name,
                        instanceVertex.<Boolean>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
                typedInstance.setByte(attributeInfo.name,
                        instanceVertex.<Byte>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
                typedInstance.setLong(attributeInfo.name,
                        instanceVertex.<Long>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
                typedInstance.setFloat(attributeInfo.name,
                        instanceVertex.<Float>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
                typedInstance.setDouble(attributeInfo.name,
                        instanceVertex.<Double>getProperty(vertexPropertyName));
            } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                typedInstance.setBigDecimal(attributeInfo.name,
                        instanceVertex.<BigDecimal>getProperty(vertexPropertyName));
            }
        }
    }
}
