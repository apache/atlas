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

import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.ITypedStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.MapIds;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.AttributeInfo;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.ObjectGraphWalker;
import org.apache.hadoop.metadata.types.StructType;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

    @Inject
    GraphBackedMetadataRepository(GraphService graphService) throws MetadataException {
        this.graphService = graphService;
        this.typeSystem = TypeSystem.getInstance();
    }

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
    }

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    @Override
    public void stop() {
    }

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     *
     * @throws java.io.IOException never
     * @throws RuntimeException    on any failure during the stop operation
     */
    @Override
    public void close() throws IOException {
        stop();
    }

    @Override
    public String createEntity(IReferenceableInstance typedInstance,
                               String typeName) throws RepositoryException {
        LOG.info("adding entity={} type={}", typedInstance, typeName);

        final TransactionalGraph transactionalGraph = graphService.getTransactionalGraph();
        try {
            // todo check if this is a duplicate

            transactionalGraph.rollback();

            return instanceToGraphMapper.mapTypedInstanceToGraph(typedInstance, transactionalGraph);

        } catch (MetadataException e) {
            transactionalGraph.rollback();
            throw new RepositoryException(e);
        } finally {
            transactionalGraph.commit();
        }
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException {
        LOG.info("Retrieving entity with guid={}", guid);

        final Graph graph = graphService.getBlueprintsGraph();
        try {
            Vertex instanceVertex = GraphUtils.findVertex(graph, Constants.GUID_PROPERTY_KEY, guid);
            if (instanceVertex == null) {
                return null;
            }

            return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);

        } catch (Exception e) {
            throw new RepositoryException(e);
        } finally {
            GraphUtils.dumpToLog(graph);
        }
    }

    @Override
    public List<String> getEntityList(String entityType) throws RepositoryException {
        LOG.info("Retrieving entity list for type={}", entityType);
        return Collections.emptyList();
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

                    if (ref != null && idToInstanceMap.containsKey(id)) { // Oops
                        throw new RepositoryException(String.format(
                                "Unexpected internal error: Id %s processed again", id));
                    }

                    if (ref != null) {
                        idToInstanceMap.put(id, ref);
                    }
                }
            }
        }

        public void createVerticesForClassTypes(TransactionalGraph transactionalGraph,
                                                List<ITypedReferenceableInstance> newInstances) {
            for (ITypedReferenceableInstance typedInstance : newInstances) {
                final Vertex instanceVertex = transactionalGraph.addVertex(null);
                instanceVertex.setProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, typedInstance.getTypeName());
                // entityVertex.setProperty("entityName", instance.getString("name"));

                final String guid = UUID.randomUUID().toString();
                instanceVertex.setProperty(Constants.GUID_PROPERTY_KEY, guid);

                final Id typedInstanceId = typedInstance.getId();
                instanceVertex.setProperty(Constants.VERSION_PROPERTY_KEY, typedInstanceId.version);

                idToVertexMap.put(typedInstanceId, instanceVertex);
            }
        }
    }

    private final class TypedInstanceToGraphMapper {

        private String mapTypedInstanceToGraph(IReferenceableInstance entity,
                                               TransactionalGraph transactionalGraph)
        throws MetadataException {

            EntityProcessor entityProcessor = new EntityProcessor();
            try {
                new ObjectGraphWalker(typeSystem, entityProcessor, entity).walk();
            } catch (MetadataException me) {
                throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
            }

            List<ITypedReferenceableInstance> newTypedInstances = discoverInstances(entityProcessor);
            entityProcessor.createVerticesForClassTypes(transactionalGraph, newTypedInstances);
            return addDiscoveredInstances(entity, entityProcessor, newTypedInstances);
        }

        /*
         * Step 2: Traverse oldIdToInstance map create newInstances :
         * List[ITypedReferenceableInstance]
         *  - create a ITypedReferenceableInstance.
         *   replace any old References ( ids or object references) with new Ids.
         */
        private List<ITypedReferenceableInstance> discoverInstances(EntityProcessor entityProcessor)
                throws RepositoryException {
            List<ITypedReferenceableInstance> newTypedInstances = new ArrayList<>();
            for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap.values()) {
                LOG.debug("instance {}", transientInstance);
                try {
                    ClassType cT = typeSystem.getDataType(
                            ClassType.class, transientInstance.getTypeName());
                    ITypedReferenceableInstance newInstance = cT.convert(
                            transientInstance, Multiplicity.REQUIRED);
                    newTypedInstances.add(newInstance);

                /*
                 * Now replace old references with new Ids
                 */
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

            String guid = null;
            for (ITypedReferenceableInstance typedInstance : newTypedInstances) { // Traverse over newInstances

                Id id = typedInstance.getId();
                if (id == null) {
                    // oops
                    throw new RepositoryException("id cannot be null");
                }

                Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);

                // add the attributes for the instance
                final Map<String, AttributeInfo> fields = typedInstance.fieldMapping().fields;

                addInstanceToVertex(typedInstance, instanceVertex, fields,
                        entityProcessor.idToVertexMap);

                for (String traitName : typedInstance.getTraits()) {
                    ((TitanVertex) instanceVertex).addProperty("traits", traitName);

                    ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);
                    // add the attributes for the trait instance
                    final String vertexPropertyName = typedInstance.getTypeName() + "." + traitName;
                    instanceVertex.setProperty(vertexPropertyName, traitName);

                    addInstanceToVertex(traitInstance, instanceVertex,
                            traitInstance.fieldMapping().fields,
                            entityProcessor.idToVertexMap);
                }

                if (typedInstance.getId() == entity.getId()) {
                    guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                }
            }

            return guid;
        }

        private void addInstanceToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                         Map<String, AttributeInfo> fields,
                                         Map<Id, Vertex> idToVertexMap) throws MetadataException {
            for (AttributeInfo attributeInfo : fields.values()) {
                System.out.println("*** attributeInfo = " + attributeInfo);
                final IDataType dataType = attributeInfo.dataType();
                final Object attributeValue = typedInstance.get(attributeInfo.name);
                final String vertexPropertyName =
                        typedInstance.getTypeName() + "." + attributeInfo.name;

                switch (dataType.getTypeCategory()) {
                    case PRIMITIVE:
                        addPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
                        break;

                    case ENUM:
                        instanceVertex.setProperty(vertexPropertyName,
                                typedInstance.getInt(attributeInfo.name));
                        break;

                    case ARRAY:
                        // todo - Add to/from json for collections
                        break;

                    case MAP:
                        // todo - Add to/from json for collections
                        break;

                    case STRUCT:
                        ITypedStruct structInstance = (ITypedStruct) attributeValue;
                        addInstanceToVertex(structInstance, instanceVertex,
                                structInstance.fieldMapping().fields, idToVertexMap);
                        break;

                    case TRAIT:
                        ITypedStruct traitInstance = (ITypedStruct) attributeValue;
                        addInstanceToVertex(traitInstance, instanceVertex,
                                traitInstance.fieldMapping().fields, idToVertexMap);
                        break;

                    case CLASS:
                        Id id = (Id) typedInstance.get(attributeInfo.name);
                        if (id != null) {
                            Vertex referenceVertex = idToVertexMap.get(id);
                            GraphUtils.addEdge(instanceVertex, referenceVertex,
                                    Constants.GUID_PROPERTY_KEY, id.className);
                        }
                        break;

                    default:
                        break;
                }
            }
        }

        private void addPrimitiveToVertex(ITypedInstance typedInstance,
                                          Vertex instanceVertex,
                                          AttributeInfo attributeInfo) throws MetadataException {
            if (typedInstance.get(attributeInfo.name) == null) { // add only if instance has this attribute
                return;
            }

            final String vertexPropertyName = typedInstance.getTypeName() + "." + attributeInfo.name;

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

    private final class GraphToTypedInstanceMapper {

        private ITypedReferenceableInstance mapGraphToTypedInstance(String guid,
                                                                    Vertex instanceVertex)
            throws MetadataException {

            String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
            List<String> traits = new ArrayList<>();
            for (TitanProperty property : ((TitanVertex) instanceVertex).getProperties( "traits")) {
                traits.add((String) property.getValue());
            }

            Id id = new Id(guid, instanceVertex.<Integer>getProperty("version"), typeName);

            ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
            ITypedReferenceableInstance typedInstance = classType.createInstance(
                    id, traits.toArray(new String[traits.size()]));

            graphToInstanceMapper.mapVertexToInstance(
                    instanceVertex, typedInstance, classType.fieldMapping().fields);
            return typedInstance;
        }

        private void mapVertexToInstance(Vertex instanceVertex, ITypedInstance typedInstance,
                                         Map<String, AttributeInfo> fields) throws MetadataException {
            for (AttributeInfo attributeInfo : fields.values()) {
                System.out.println("*** attributeInfo = " + attributeInfo);
                final IDataType dataType = attributeInfo.dataType();
                final String vertexPropertyName =
                        typedInstance.getTypeName() + "." + attributeInfo.name;

                switch (dataType.getTypeCategory()) {
                    case PRIMITIVE:
                        mapVertexToInstance(instanceVertex, typedInstance, attributeInfo);
                        break;  // add only if vertex has this attribute

                    case ENUM:
                        // EnumType enumType = typeSystem.getDataType(
                        //        EnumType.class, attributeInfo.name);
                        // todo  - is this enough
                        typedInstance.setInt(attributeInfo.name,
                                instanceVertex.<Integer>getProperty(vertexPropertyName));
                        break;

                    case ARRAY:
                        // todo - Add to/from json for collections
                        break;

                    case MAP:
                        // todo - Add to/from json for collections
                        break;

                    case STRUCT:
                        StructType structType = typeSystem.getDataType(
                                StructType.class, attributeInfo.name);
                        ITypedStruct structInstance = structType.createInstance();
                        typedInstance.set(attributeInfo.name, structInstance);

                        mapVertexToInstance(instanceVertex, structInstance,
                                structInstance.fieldMapping().fields);
                        break;

                    case TRAIT:
                        TraitType traitType = typeSystem.getDataType(
                                TraitType.class, attributeInfo.name);
                        ITypedStruct traitInstance = (ITypedStruct)
                                ((ITypedReferenceableInstance) typedInstance).getTrait(attributeInfo.name);
                        typedInstance.set(attributeInfo.name, traitInstance);

                        mapVertexToInstance(instanceVertex, traitInstance,
                                traitType.fieldMapping().fields);
                        break;

                    case CLASS:
                        Id referenceId = null;
                        for (Edge edge : instanceVertex.getEdges(Direction.IN)) {
                            final Vertex vertex = edge.getVertex(Direction.OUT);
                            if (vertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY).equals(attributeInfo.name)) {
                                referenceId = new Id(
                                        vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY),
                                        vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                                        attributeInfo.name);
                                break;
                            }
                        }

                        if (referenceId != null) {
                            typedInstance.set(attributeInfo.name, referenceId);
                        }
                        break;

                    default:
                        break;
                }
            }
        }

        private void mapVertexToInstance(Vertex instanceVertex,
                                         ITypedInstance typedInstance,
                                         AttributeInfo attributeInfo) throws MetadataException {
            final String vertexPropertyName = typedInstance.getTypeName() + "." + attributeInfo.name;
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
