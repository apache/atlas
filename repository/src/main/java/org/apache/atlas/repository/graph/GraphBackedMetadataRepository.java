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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
@Singleton
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private final GraphToTypedInstanceMapper graphToInstanceMapper;

    private static TypeSystem typeSystem = TypeSystem.getInstance();

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedMetadataRepository(GraphProvider<TitanGraph> graphProvider) {
        this.titanGraph = graphProvider.get();
        this.graphToInstanceMapper = new GraphToTypedInstanceMapper(titanGraph);
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
        return GraphHelper.getTraitLabel(dataType.getName(), traitName);
    }

    @Override
    public String getFieldNameInVertex(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException {
        return GraphHelper.getQualifiedFieldName(dataType, aInfo.name);
    }

    public String getFieldNameInVertex(IDataType<?> dataType, String attrName) throws AtlasException {
        return GraphHelper.getQualifiedFieldName(dataType, attrName);
    }

    @Override
    public String getEdgeLabel(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException {
        return GraphHelper.getEdgeLabel(dataType, aInfo);
    }

    @Override
    @GraphTransaction
    public List<String> createEntities(ITypedReferenceableInstance... entities) throws RepositoryException,
        EntityExistsException {
        LOG.info("adding entities={}", entities);
        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
            TypeUtils.Pair<List<String>, List<String>> idPair =
                    instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.CREATE, entities);
            return idPair.left;
        } catch (EntityExistsException e) {
            throw e;
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException, EntityNotFoundException {
        LOG.info("Retrieving entity with guid={}", guid);

        Vertex instanceVertex = graphHelper.getVertexForGUID(guid);

        try {
            return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public ITypedReferenceableInstance getEntityDefinition(String entityType, String attribute, Object value)
            throws AtlasException {
        LOG.info("Retrieving entity with type={} and {}={}", entityType, attribute, value);
        IDataType type = typeSystem.getDataType(IDataType.class, entityType);
        String propertyKey = getFieldNameInVertex(type, attribute);
        Vertex instanceVertex = graphHelper.getVertexForProperty(propertyKey, value);

        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
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
        Vertex instanceVertex = graphHelper.getVertexForGUID(guid);
        return GraphHelper.getTraitNames(instanceVertex);
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
            Vertex instanceVertex = graphHelper.getVertexForGUID(guid);

            // add the trait instance as a new vertex
            final String typeName = GraphHelper.getTypeName(instanceVertex);

            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
            instanceToGraphMapper.mapTraitInstanceToVertex(traitInstance,
                    typeSystem.getDataType(ClassType.class, typeName), instanceVertex);


            // update the traits in entity once adding trait instance is successful
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.valueOf(System.currentTimeMillis()));
            
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
    public void deleteTrait(String guid, String traitNameToBeDeleted) throws TraitNotFoundException, EntityNotFoundException, RepositoryException {
        LOG.info("Deleting trait={} from entity={}", traitNameToBeDeleted, guid);
        
        Vertex instanceVertex = graphHelper.getVertexForGUID(guid);

        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        if (!traitNames.contains(traitNameToBeDeleted)) {
                throw new TraitNotFoundException(
                        "Could not find trait=" + traitNameToBeDeleted + " in the repository for entity: " + guid);
        }

        try {
            

            final String entityTypeName = GraphHelper.getTypeName(instanceVertex);
            String relationshipLabel = GraphHelper.getTraitLabel(entityTypeName, traitNameToBeDeleted);
            Iterator<Edge> results = instanceVertex.getEdges(Direction.OUT, relationshipLabel).iterator();
            if (results.hasNext()) { // there should only be one edge for this label
                final Edge traitEdge = results.next();
                final Vertex traitVertex = traitEdge.getVertex(Direction.IN);

                // remove the edge to the trait instance from the repository
                titanGraph.removeEdge(traitEdge);

                if (traitVertex != null) { // remove the trait instance from the repository
                    TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
                    instanceToGraphMapper.deleteTraitVertex(traitNameToBeDeleted, traitVertex);

                    // update the traits in entity once trait removal is successful
                    traitNames.remove(traitNameToBeDeleted);
                    updateTraits(instanceVertex, traitNames);
                }
            }
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    
    private void updateTraits(Vertex instanceVertex, List<String> traitNames) {
        // remove the key
        instanceVertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);

        // add it back again
        for (String traitName : traitNames) {
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
        }
        GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.valueOf(System.currentTimeMillis()));
    }

    @Override
    @GraphTransaction
    public TypeUtils.Pair<List<String>, List<String>> updateEntities(ITypedReferenceableInstance... entitiesUpdated) throws RepositoryException {
        LOG.info("updating entity {}", entitiesUpdated);
        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
            return instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.UPDATE_FULL,
                    entitiesUpdated);
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public TypeUtils.Pair<List<String>, List<String>> updatePartial(ITypedReferenceableInstance entity) throws RepositoryException {
        LOG.info("updating entity {}", entity);
        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
            return instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.UPDATE_PARTIAL, entity);
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public TypeUtils.Pair<List<String>, List<ITypedReferenceableInstance>>  deleteEntities(List<String> guids) throws RepositoryException {

        if (guids == null || guids.size() == 0) {
            throw new IllegalArgumentException("guids must be non-null and non-empty");
        }
        
        TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper);
        for (String guid : guids) {
            if (guid == null) {
                LOG.warn("deleteEntities: Ignoring null guid");
                continue;
            }
            try {
                Vertex instanceVertex = graphHelper.getVertexForGUID(guid);
                String typeName = GraphHelper.getTypeName(instanceVertex);
                instanceToGraphMapper.deleteEntity(typeName, instanceVertex);
            } catch (EntityNotFoundException e) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.info("Deletion request ignored for non-existent entity with guid " + guid);
                continue;
            } catch (AtlasException e) {
                throw new RepositoryException(e);
            }
        }
        return new TypeUtils.Pair<>(
                instanceToGraphMapper.getDeletedEntityGuids(), instanceToGraphMapper.getDeletedEntities());
    }
}
