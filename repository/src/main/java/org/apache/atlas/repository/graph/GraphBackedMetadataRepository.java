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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.GuidMapping;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * An implementation backed by a Graph database provided
 * as a Graph Service.
 */
@Singleton
public class GraphBackedMetadataRepository implements MetadataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedMetadataRepository.class);

    private static TypeSystem typeSystem = TypeSystem.getInstance();

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private DeleteHandler deleteHandler;

    private final IAtlasGraphProvider graphProvider;
    private final GraphToTypedInstanceMapper graphToInstanceMapper;

    @Inject
    public GraphBackedMetadataRepository(DeleteHandler deleteHandler) {
        this.graphProvider = new AtlasGraphProvider();
        this.graphToInstanceMapper = new GraphToTypedInstanceMapper(graphProvider);
        this.deleteHandler = deleteHandler;
    }

    //for testing only
    public GraphBackedMetadataRepository(IAtlasGraphProvider graphProvider, DeleteHandler deleteHandler) {
        this.graphProvider = graphProvider;
        this.graphToInstanceMapper = new GraphToTypedInstanceMapper(graphProvider);
        this.deleteHandler = deleteHandler;
    }


    public GraphToTypedInstanceMapper getGraphToInstanceMapper() {
        return graphToInstanceMapper;
    }

    @Override
    public String getTypeAttributeName() {
        return Constants.ENTITY_TYPE_PROPERTY_KEY;
    }

    @Override
    public String getStateAttributeName() {
        return Constants.STATE_PROPERTY_KEY;
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
    public String getVersionAttributeName() {
        return Constants.VERSION_PROPERTY_KEY;
    }

    @Override
    public String getTraitLabel(IDataType<?> dataType, String traitName) {
        return GraphHelper.getTraitLabel(dataType.getName(), traitName);
    }

    @Override
    public String getFieldNameInVertex(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException {
        if (aInfo.name.startsWith(Constants.INTERNAL_PROPERTY_KEY_PREFIX)) {
            return aInfo.name;
        }
        return GraphHelper.encodePropertyKey(GraphHelper.getQualifiedFieldName(dataType, aInfo.name));
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
    public CreateUpdateEntitiesResult createEntities(ITypedReferenceableInstance... entities) throws RepositoryException,
        EntityExistsException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("adding entities={}", entities);
        }

        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper, deleteHandler);
            instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.CREATE, entities);
            List<String> createdGuids = RequestContext.get().getCreatedEntityIds();
            CreateUpdateEntitiesResult result = new CreateUpdateEntitiesResult();
            AtlasClient.EntityResult entityResult = new AtlasClient.EntityResult(createdGuids, null,  null);
            GuidMapping mapping = instanceToGraphMapper.createGuidMapping();
            result.setEntityResult(entityResult);
            result.setGuidMapping(mapping);
            return result;
        } catch (EntityExistsException e) {
            throw e;
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException, EntityNotFoundException {
        return getEntityDefinitions(guid).get(0);
    }

    @Override
    @GraphTransaction
    public List<ITypedReferenceableInstance> getEntityDefinitions(String... guids) throws RepositoryException, EntityNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving entities with guids={}", Arrays.toString(guids));
        }

        RequestContext context = RequestContext.get();
        ITypedReferenceableInstance[] result = new ITypedReferenceableInstance[guids.length];

        // Map of the guids of instances not in the cache to their index(es) in the result.
        // This is used to put the loaded instances into the location(s) corresponding
        // to their guid in the result.  Note that a set is needed since guids can
        // appear more than once in the list.
        Map<String, Set<Integer>> uncachedGuids = new HashMap<>();

        for (int i = 0; i < guids.length; i++) {
            String guid = guids[i];

            // First, check the cache.
            ITypedReferenceableInstance cached = context.getInstance(guid);
            if (cached != null) {
                result[i] = cached;
            } else {
                Set<Integer> indices = uncachedGuids.get(guid);
                if (indices == null) {
                    indices = new HashSet<>(1);
                    uncachedGuids.put(guid, indices);
                }
                indices.add(i);
            }
        }

        List<String> guidsToFetch = new ArrayList<>(uncachedGuids.keySet());
        Map<String, AtlasVertex> instanceVertices = graphHelper.getVerticesForGUIDs(guidsToFetch);

        // search for missing entities
        if (instanceVertices.size() != guidsToFetch.size()) {
            Set<String> missingGuids = new HashSet<String>(guidsToFetch);
            missingGuids.removeAll(instanceVertices.keySet());
            if (!missingGuids.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to find guids={}", missingGuids);
                }
                throw new EntityNotFoundException(
                    "Could not find entities in the repository with guids: " + missingGuids.toString());
            }
        }

        for (String guid : guidsToFetch) {
            try {
                ITypedReferenceableInstance entity = graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertices.get(guid));
                for(int index : uncachedGuids.get(guid)) {
                    result[index] = entity;
                }
            } catch (AtlasException e) {
                throw new RepositoryException(e);
            }
        }
        return Arrays.asList(result);
    }

    @Override
    @GraphTransaction
    public ITypedReferenceableInstance getEntityDefinition(String entityType, String attribute, Object value)
            throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving entity with type={} and {}={}", entityType, attribute, value);
        }

        IDataType type = typeSystem.getDataType(IDataType.class, entityType);
        String propertyKey = getFieldNameInVertex(type, attribute);
        AtlasVertex instanceVertex = graphHelper.findVertex(propertyKey, value,
                Constants.ENTITY_TYPE_PROPERTY_KEY, entityType,
                Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

        String guid = GraphHelper.getGuid(instanceVertex);
        ITypedReferenceableInstance cached = RequestContext.get().getInstance(guid);
        if(cached != null) {
            return cached;
        }
        return graphToInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
    }

    @Override
    @GraphTransaction
    public List<String> getEntityList(String entityType) throws RepositoryException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving entity list for type={}", entityType);
        }

        AtlasGraphQuery query = getGraph().query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityType);
        Iterator<AtlasVertex> results = query.vertices().iterator();
        if (!results.hasNext()) {
            return Collections.emptyList();
        }

        ArrayList<String> entityList = new ArrayList<>();
        while (results.hasNext()) {
            AtlasVertex vertex = results.next();
            entityList.add(GraphHelper.getGuid(vertex));
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving trait names for entity={}", guid);
        }

        AtlasVertex instanceVertex = graphHelper.getVertexForGUID(guid);
        return GraphHelper.getTraitNames(instanceVertex);
    }

    /**
     * Adds a new trait to the list of entities represented by their respective guids
     * @param entityGuids   list of globally unique identifier for the entities
     * @param traitInstance trait instance that needs to be added to entities
     * @throws RepositoryException
     */
    @Override
    @GraphTransaction
    public void addTrait(List<String> entityGuids, ITypedStruct traitInstance) throws RepositoryException {
        Preconditions.checkNotNull(entityGuids, "entityGuids list cannot be null");
        Preconditions.checkNotNull(traitInstance, "Trait instance cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding a new trait={} for entities={}", traitInstance.getTypeName(), entityGuids);
        }

        for (String entityGuid : entityGuids) {
            addTraitImpl(entityGuid, traitInstance);
        }
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
        Preconditions.checkNotNull(guid, "guid cannot be null");
        Preconditions.checkNotNull(traitInstance, "Trait instance cannot be null");

        addTraitImpl(guid, traitInstance);
    }

    private void addTraitImpl(String guid, ITypedStruct traitInstance) throws RepositoryException {
        final String traitName = traitInstance.getTypeName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding a new trait={} for entity={}", traitName, guid);
        }

        try {
            AtlasVertex instanceVertex = graphHelper.getVertexForGUID(guid);

            // add the trait instance as a new vertex
            final String typeName = GraphHelper.getTypeName(instanceVertex);

            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper, deleteHandler);
            instanceToGraphMapper.mapTraitInstanceToVertex(traitInstance,
                    typeSystem.getDataType(ClassType.class, typeName), instanceVertex);


            // update the traits in entity once adding trait instance is successful
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                    RequestContext.get().getRequestTime());
            GraphHelper.setProperty(instanceVertex, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting trait={} from entity={}", traitNameToBeDeleted, guid);
        }

        AtlasVertex instanceVertex = graphHelper.getVertexForGUID(guid);

        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        if (!traitNames.contains(traitNameToBeDeleted)) {
                throw new TraitNotFoundException(
                        "Could not find trait=" + traitNameToBeDeleted + " in the repository for entity: " + guid);
        }

        try {
            final String entityTypeName = GraphHelper.getTypeName(instanceVertex);
            String relationshipLabel = GraphHelper.getTraitLabel(entityTypeName, traitNameToBeDeleted);
            AtlasEdge edge = graphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
            if(edge != null) {
                deleteHandler.deleteEdgeReference(edge, DataTypes.TypeCategory.TRAIT, false, true);

                // update the traits in entity once trait removal is successful
                traitNames.remove(traitNameToBeDeleted);
                updateTraits(instanceVertex, traitNames);
            }
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }


    private void updateTraits(AtlasVertex instanceVertex, List<String> traitNames) {
        // remove the key
        instanceVertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);

        // add it back again
        for (String traitName : traitNames) {
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
        }
        GraphHelper.setProperty(instanceVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());
        GraphHelper.setProperty(instanceVertex, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());
    }

    @Override
    @GraphTransaction
    public CreateUpdateEntitiesResult updateEntities(ITypedReferenceableInstance... entitiesUpdated) throws RepositoryException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updating entity {}", entitiesUpdated);
        }

        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper, deleteHandler);
            instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.UPDATE_FULL,
                    entitiesUpdated);
            CreateUpdateEntitiesResult result = new CreateUpdateEntitiesResult();
            RequestContext requestContext = RequestContext.get();
            result.setEntityResult(createEntityResultFromContext(requestContext));
            GuidMapping mapping = instanceToGraphMapper.createGuidMapping();
            result.setGuidMapping(mapping);
            return result;
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @GraphTransaction
    public CreateUpdateEntitiesResult updatePartial(ITypedReferenceableInstance entity) throws RepositoryException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updating entity {}", entity);
        }

        try {
            TypedInstanceToGraphMapper instanceToGraphMapper = new TypedInstanceToGraphMapper(graphToInstanceMapper, deleteHandler);
            instanceToGraphMapper.mapTypedInstanceToGraph(TypedInstanceToGraphMapper.Operation.UPDATE_PARTIAL, entity);
            RequestContext requestContext = RequestContext.get();
            CreateUpdateEntitiesResult result = new CreateUpdateEntitiesResult();
            GuidMapping mapping = instanceToGraphMapper.createGuidMapping();
            result.setEntityResult(createEntityResultFromContext(requestContext));
            result.setGuidMapping(mapping);
            return result;
        } catch (AtlasException e) {
            throw new RepositoryException(e);
        }
    }



    @Override
    @GraphTransaction
    public AtlasClient.EntityResult deleteEntities(List<String> guids) throws RepositoryException {

        if (guids == null || guids.size() == 0) {
            throw new IllegalArgumentException("guids must be non-null and non-empty");
        }

        // Retrieve vertices for requested guids.
        Map<String, AtlasVertex> vertices = graphHelper.getVerticesForGUIDs(guids);
        Collection<AtlasVertex> deletionCandidates = vertices.values();

        if(LOG.isDebugEnabled()) {
            for(String guid : guids) {
                if(! vertices.containsKey(guid)) {
                    // Entity does not exist - treat as non-error, since the caller
                    // wanted to delete the entity and it's already gone.
                    LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
                }
            }
        }

        if (deletionCandidates.isEmpty()) {
            LOG.info("No deletion candidate entities were found for guids %s", guids);
            return new AtlasClient.EntityResult(Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
        }

        try {
            deleteHandler.deleteEntities(deletionCandidates);
        }
        catch (AtlasException e) {
            throw new RepositoryException(e);
        }

        RequestContext requestContext = RequestContext.get();
        return createEntityResultFromContext(requestContext);
    }

    private AtlasClient.EntityResult createEntityResultFromContext(RequestContext requestContext) {
        return new AtlasClient.EntityResult(
                requestContext.getCreatedEntityIds(),
                requestContext.getUpdatedEntityIds(),
                requestContext.getDeletedEntityIds());
    }

    public AtlasGraph getGraph() throws RepositoryException {
        return graphProvider.get();
    }
}
