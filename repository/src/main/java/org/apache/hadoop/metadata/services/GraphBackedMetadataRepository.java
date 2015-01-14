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

package org.apache.hadoop.metadata.services;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.ITypedStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.MapIds;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.AttributeInfo;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.ObjectGraphWalker;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private static final String GUID_PROPERTY_KEY = "guid";
    private static final String TIMESTAMP_PROPERTY_KEY = "timestamp";
    private static final String ENTITY_TYPE_PROPERTY_KEY = "entityType";

    private static final String TRAIT_PROPERTY_SUFFIX = "trait.";

    private final AtomicInteger ID_SEQ = new AtomicInteger(0);

    // todo: remove this
    private final ConcurrentHashMap<String, ITypedReferenceableInstance> instances;

    private final GraphService graphService;
    private final TypeSystem typeSystem;
    
    @Inject
    GraphBackedMetadataRepository(GraphService graphService) throws MetadataException {
    	this.instances = new ConcurrentHashMap<>();
    	this.graphService = graphService;
    	this.typeSystem = new TypeSystem();
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
    public String createEntity(IReferenceableInstance entity,
                               String entityType) throws RepositoryException {
        LOG.info("adding entity={} type={}", entity, entityType);

        final TransactionalGraph transactionalGraph = graphService.getTransactionalGraph();
        try {
            // todo check if this is a duplicate

            transactionalGraph.rollback();

            EntityProcessor entityProcessor = new EntityProcessor();
            try {
                new ObjectGraphWalker(typeSystem, entityProcessor, entity).walk();
            } catch (MetadataException me) {
                throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
            }

            List<ITypedReferenceableInstance> newInstances = discoverInstances(entityProcessor);
            entityProcessor.createVerticesForClasses(transactionalGraph, newInstances);
            return addDiscoveredInstances(entity, entityProcessor, newInstances);

        } catch (MetadataException e) {
            transactionalGraph.rollback();
            throw new RepositoryException(e);
        } finally {
            transactionalGraph.commit();
        }
    }

    private String addDiscoveredInstances(IReferenceableInstance entity,
                                          EntityProcessor entityProcessor,
                                          List<ITypedReferenceableInstance> newInstances)
        throws MetadataException {

        String guid = null;
        for (ITypedReferenceableInstance instance : newInstances) { // Traverse over newInstances

            Id id = instance.getId();
            if (id == null) {
                // oops
                throw new RepositoryException("id cannot be null");
            }

            Vertex entityVertex = entityProcessor.idToVertexMap.get(id);
            instances.put((String) entityVertex.getProperty(GUID_PROPERTY_KEY), instance);

            // add the attributes for the instance
            final Map<String, AttributeInfo> fields = instance.fieldMapping().fields;

            addInstanceToVertex(instance, entityVertex, fields,
                    entityProcessor.idToVertexMap);

            for (String traitName : instance.getTraits()) {
                ITypedStruct traitInstance = (ITypedStruct) instance.getTrait(traitName);
                // add the attributes for the trait instance
                entityVertex.setProperty(TRAIT_PROPERTY_SUFFIX + traitName, traitName);
                addInstanceToVertex(traitInstance, entityVertex,
                        traitInstance.fieldMapping().fields,
                        entityProcessor.idToVertexMap);
            }

            if (instance.getId() == entity.getId()) {
                guid = entityVertex.getProperty(GUID_PROPERTY_KEY);
            }
        }

        return guid;
    }

    private void addInstanceToVertex(ITypedInstance instance, Vertex entityVertex,
                                     Map<String, AttributeInfo> fields,
                                     Map<Id, Vertex> idToVertexMap) throws MetadataException {
        for (AttributeInfo attributeInfo : fields.values()) {
            System.out.println("*** attributeInfo = " + attributeInfo);
            final IDataType dataType = attributeInfo.dataType();
            String attributeName = attributeInfo.name;
            Object attributeValue = instance.get(attributeInfo.name);

            switch (dataType.getTypeCategory()) {
                case PRIMITIVE:
                    addPrimitiveToVertex(instance, entityVertex, attributeInfo);
                    break;

                case ENUM:
                    addToVertex(entityVertex, attributeInfo.name,
                            instance.getInt(attributeInfo.name));
                    break;

                case ARRAY:
                    // todo - Add to/from json for collections
                    break;

                case MAP:
                    // todo - Add to/from json for collections
                    break;

                case STRUCT:
                    ITypedStruct structInstance = (ITypedStruct) attributeValue;
                    addInstanceToVertex(structInstance, entityVertex,
                            structInstance.fieldMapping().fields, idToVertexMap);
                    break;

                case TRAIT:
                    ITypedStruct traitInstance = (ITypedStruct) attributeValue;
                    addInstanceToVertex(traitInstance, entityVertex,
                            traitInstance.fieldMapping().fields, idToVertexMap);
                    break;

                case CLASS:
                    Id id = (Id) instance.get(attributeName);
                    if (id != null) {
                        Vertex referenceVertex = idToVertexMap.get(id);
                        addEdge(entityVertex, referenceVertex, "references");
                    }
                    break;

                default:
                    break;
            }
        }
    }

    protected Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return addEdge(fromVertex, toVertex, edgeLabel, null);
    }

    protected Edge addEdge(Vertex fromVertex, Vertex toVertex,
                           String edgeLabel, String timestamp) {
        Edge edge = findEdge(fromVertex, toVertex, edgeLabel);

        Edge edgeToVertex = edge != null ? edge : fromVertex.addEdge(edgeLabel, toVertex);
        if (timestamp != null) {
            edgeToVertex.setProperty(TIMESTAMP_PROPERTY_KEY, timestamp);
        }

        return edgeToVertex;
    }

    protected Edge findEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return findEdge(fromVertex, toVertex.getProperty(GUID_PROPERTY_KEY), edgeLabel);
    }

    protected Edge findEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        Edge edgeToFind = null;
        for (Edge edge : fromVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (edge.getVertex(Direction.IN).getProperty(GUID_PROPERTY_KEY).equals(toVertexName)) {
                edgeToFind = edge;
                break;
            }
        }

        return edgeToFind;
    }

    /*
     * Step 2: Traverse oldIdToInstance map create newInstances :
     * List[ITypedReferenceableInstance]
     *  - create a ITypedReferenceableInstance.
     *   replace any old References ( ids or object references) with new Ids.
     */
    private List<ITypedReferenceableInstance> discoverInstances(EntityProcessor entityProcessor)
        throws RepositoryException {
        List<ITypedReferenceableInstance> newInstances = new ArrayList<>();
        for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap.values()) {
            LOG.debug("instance {}", transientInstance);
            try {
                ClassType cT = typeSystem.getDataType(
                        ClassType.class, transientInstance.getTypeName());
                ITypedReferenceableInstance newInstance = cT.convert(
                        transientInstance, Multiplicity.REQUIRED);
                newInstances.add(newInstance);

                /*
                 * Now replace old references with new Ids
                 */
                MapIds mapIds = new MapIds(entityProcessor.idToNewIdMap);
                new ObjectGraphWalker(typeSystem, mapIds, newInstances).walk();

            } catch (MetadataException me) {
                throw new RepositoryException(
                        String.format("Failed to create Instance(id = %s",
                                transientInstance.getId()), me);
            }
        }

        return newInstances;
    }

    private void addPrimitiveToVertex(ITypedInstance instance,
                                      Vertex entityVertex,
                                      AttributeInfo attributeInfo) throws MetadataException {
        if (instance.get(attributeInfo.name) == null) { // add only if instance has this attribute
            return;
        }

        if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getString(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getShort(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getInt(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getBigInt(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getBoolean(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getByte(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getLong(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getFloat(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getDouble(attributeInfo.name));
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            entityVertex.setProperty(attributeInfo.name, instance.getBigDecimal(attributeInfo.name));
        }
    }

    public static void addToVertex(Vertex entityVertex, String name, int value) {
        entityVertex.setProperty(name, value);
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException {
        LOG.info("Retrieving entity with guid={}", guid);
        return instances.get(guid);
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

        public void createVerticesForClasses(TransactionalGraph transactionalGraph,
                                             List<ITypedReferenceableInstance> newInstances) {
            for (ITypedReferenceableInstance instance : newInstances) {
                final Vertex entityVertex = transactionalGraph.addVertex(null);
                entityVertex.setProperty(ENTITY_TYPE_PROPERTY_KEY, instance.getTypeName());
                // entityVertex.setProperty("entityName", instance.getString("name"));

                final String guid = UUID.randomUUID().toString();
                entityVertex.setProperty(GUID_PROPERTY_KEY, guid);

                idToVertexMap.put(instance.getId(), entityVertex);
            }
        }
    }
}
