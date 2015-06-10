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

package org.apache.atlas.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.MetadataException;
import org.apache.atlas.MetadataServiceClient;
import org.apache.atlas.ParamChecker;
import org.apache.atlas.TypeNotFoundException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.IndexCreationException;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple wrapper over TypeSystem and MetadataRepository services with hooks
 * for listening to changes to the repository.
 */
@Singleton
public class DefaultMetadataService implements MetadataService {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMetadataService.class);

    private final Set<EntityChangeListener> entityChangeListeners
            = new LinkedHashSet<>();

    private final TypeSystem typeSystem;
    private final MetadataRepository repository;
    private final ITypeStore typeStore;
    private final Set<Provider<SearchIndexer>> typeChangeListeners;

    @Inject
    DefaultMetadataService(final MetadataRepository repository,
                           final Provider<SearchIndexer> searchIndexProvider, final ITypeStore typeStore) throws MetadataException {
        this.typeStore = typeStore;
        this.typeSystem = TypeSystem.getInstance();
        this.repository = repository;

        this.typeChangeListeners = new LinkedHashSet<Provider<SearchIndexer>>() {{ add(searchIndexProvider); }};
        restoreTypeSystem();
    }

    private void restoreTypeSystem() {
        LOG.info("Restoring type system from the store");
        try {
            TypesDef typesDef = typeStore.restore();
            typeSystem.defineTypes(typesDef);

            // restore types before creating super types
            createSuperTypes();

        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Restored type system from the store");
    }

    private static final AttributeDefinition NAME_ATTRIBUTE =
            TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE);
    private static final AttributeDefinition DESCRIPTION_ATTRIBUTE =
            TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE);

    @InterfaceAudience.Private
    private void createSuperTypes() throws MetadataException {
        if (typeSystem.isRegistered(MetadataServiceClient.DATA_SET_SUPER_TYPE)) {
            return; // this is already registered
        }

        HierarchicalTypeDefinition<ClassType> infraType =
                TypesUtil.createClassTypeDef(MetadataServiceClient.INFRASTRUCTURE_SUPER_TYPE,
                        ImmutableList.<String>of(), NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE);

        HierarchicalTypeDefinition<ClassType> datasetType = TypesUtil
                .createClassTypeDef(MetadataServiceClient.DATA_SET_SUPER_TYPE,
                        ImmutableList.<String>of(),
                        NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE);

        HierarchicalTypeDefinition<ClassType> processType = TypesUtil
                .createClassTypeDef(MetadataServiceClient.PROCESS_SUPER_TYPE, ImmutableList.<String>of(),
                        NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE, new AttributeDefinition("inputs",
                                DataTypes.arrayTypeName(MetadataServiceClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("outputs",
                                DataTypes.arrayTypeName(MetadataServiceClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null));

        TypesDef typesDef = TypeUtils
                .getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                        ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                        ImmutableList.of(infraType, datasetType, processType));
        createType(TypesSerialization.toJson(typesDef));
    }

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    @Override
    public JSONObject createType(String typeDefinition) throws MetadataException {
        ParamChecker.notEmpty(typeDefinition, "type definition cannot be empty");

        TypesDef typesDef;
        try {
            typesDef = TypesSerialization.fromJson(typeDefinition);
            if (typesDef.isEmpty()) {
                throw new MetadataException("Invalid type definition");
            }
        } catch (Exception e) {
            LOG.error("Unable to deserialize json={}", typeDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json ", e);
        }

        try {
            final Map<String, IDataType> typesAdded = typeSystem.defineTypes(typesDef);

            try {
                /* Create indexes first so that if index creation fails then we rollback
                   the typesystem and also do not persist the graph
                 */
                onTypesAddedToRepo(typesAdded);
                typeStore.store(typeSystem, ImmutableList.copyOf(typesAdded.keySet()));
            } catch (Throwable t) {
                typeSystem.removeTypes(typesAdded.keySet());
                throw new MetadataException("Unable to persist types ", t);
            }

            return new JSONObject() {{
                put(MetadataServiceClient.TYPES, typesAdded.keySet());
            }};
        } catch (JSONException e) {
            LOG.error("Unable to create response for types={}", typeDefinition, e);
            throw new MetadataException("Unable to create response ", e);
        }
    }

    /**
     * Return the definition for the given type.
     *
     * @param typeName name for this type, must be unique
     * @return type definition as JSON
     */
    @Override
    public String getTypeDefinition(String typeName) throws MetadataException {
        final IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
        return TypesSerialization.toJson(typeSystem, dataType.getName());
    }

    /**
     * Return the list of types in the repository.
     *
     * @return list of type names in the repository
     */
    @Override
    public List<String> getTypeNamesList() throws MetadataException {
        return typeSystem.getTypeNames();
    }

    /**
     * Return the list of trait type names in the type system.
     *
     * @return list of trait type names in the type system
     */
    @Override
    public List<String> getTypeNamesByCategory(DataTypes.TypeCategory typeCategory) throws MetadataException {
        return typeSystem.getTypeNamesByCategory(typeCategory);
    }

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityInstanceDefinition definition
     * @return guid
     */
    @Override
    public String createEntity(String entityInstanceDefinition) throws MetadataException {
        ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition cannot be empty");

        ITypedReferenceableInstance entityTypedInstance =
                deserializeClassInstance(entityInstanceDefinition);

        final String guid = repository.createEntity(entityTypedInstance);

        onEntityAddedToRepo(entityTypedInstance);
        return guid;
    }

    private ITypedReferenceableInstance deserializeClassInstance(
            String entityInstanceDefinition) throws MetadataException {

        final Referenceable entityInstance;
        try {
            entityInstance = InstanceSerialization.fromJsonReferenceable(
                    entityInstanceDefinition, true);
        } catch (Exception e) {  // exception from deserializer
            LOG.error("Unable to deserialize json={}", entityInstanceDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json");
        }
        final String entityTypeName = entityInstance.getTypeName();
        ParamChecker.notEmpty(entityTypeName, "Entity type cannot be null");

        ClassType entityType = typeSystem.getDataType(ClassType.class, entityTypeName);
        return entityType.convert(entityInstance, Multiplicity.REQUIRED);
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String guid) throws MetadataException {
        ParamChecker.notEmpty(guid, "guid cannot be null");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return InstanceSerialization.toJson(instance, true);
    }

    /**
     * Return the list of entity names for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity names for the given type in the repository
     */
    @Override
    public List<String> getEntityList(String entityType) throws MetadataException {
        validateTypeExists(entityType);

        return repository.getEntityList(entityType);
    }

    @Override
    public void updateEntity(String guid, String property, String value) throws MetadataException {
        ParamChecker.notEmpty(guid, "guid cannot be null");
        ParamChecker.notEmpty(property, "property cannot be null");
        ParamChecker.notEmpty(value, "property value cannot be null");

        repository.updateEntity(guid, property, value);
    }

    private void validateTypeExists(String entityType) throws MetadataException {
        ParamChecker.notEmpty(entityType, "entity type cannot be null");

        // verify if the type exists
        if (!typeSystem.isRegistered(entityType)) {
            throw new TypeNotFoundException("type is not defined for : " + entityType);
        }
    }

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     * @throws MetadataException
     */
    @Override
    public List<String> getTraitNames(String guid) throws MetadataException {
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
        return repository.getTraitNames(guid);
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid                    globally unique identifier for the entity
     * @param traitInstanceDefinition trait instance json that needs to be added to entity
     * @throws MetadataException
     */
    @Override
    public void addTrait(String guid,
                         String traitInstanceDefinition) throws MetadataException {
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
        ParamChecker.notEmpty(traitInstanceDefinition, "Trait instance cannot be null");

        ITypedStruct traitInstance = deserializeTraitInstance(traitInstanceDefinition);
        final String traitName = traitInstance.getTypeName();

        // ensure trait type is already registered with the TS
        if ( !typeSystem.isRegistered(traitName) ) {
            String msg = String.format("trait=%s should be defined in type system before it can be added", traitName);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        // ensure trait is not already defined
        Preconditions.checkArgument(!getTraitNames(guid).contains(traitName),
                "trait=%s is already defined for entity=%s", traitName, guid);

        repository.addTrait(guid, traitInstance);

        onTraitAddedToEntity(guid, traitName);
    }

    private ITypedStruct deserializeTraitInstance(String traitInstanceDefinition)
            throws MetadataException {

        try {
            Struct traitInstance = InstanceSerialization.fromJsonStruct(
                    traitInstanceDefinition, true);
            final String entityTypeName = traitInstance.getTypeName();
            ParamChecker.notEmpty(entityTypeName, "entity type cannot be null");

            TraitType traitType = typeSystem.getDataType(TraitType.class, entityTypeName);
            return traitType.convert(
                    traitInstance, Multiplicity.REQUIRED);
        } catch ( TypeNotFoundException e ) {
            throw e;
        } catch (Exception e) {
            throw new MetadataException("Error deserializing trait instance", e);
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid                 globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws MetadataException
     */
    @Override
    public void deleteTrait(String guid,
                            String traitNameToBeDeleted) throws MetadataException {
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
        ParamChecker.notEmpty(traitNameToBeDeleted, "Trait name cannot be null");

        // ensure trait type is already registered with the TS
        if ( !typeSystem.isRegistered(traitNameToBeDeleted)) {
            final String msg = String.format("trait=%s should be defined in type system before it can be deleted",
                    traitNameToBeDeleted);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        repository.deleteTrait(guid, traitNameToBeDeleted);

        onTraitDeletedFromEntity(guid, traitNameToBeDeleted);
    }

    private void onTypesAddedToRepo(Map<String, IDataType> typesAdded) throws MetadataException {
        Map<SearchIndexer, Throwable> caughtExceptions = new HashMap<>();
        for(Provider<SearchIndexer> indexerProvider : typeChangeListeners) {
            final SearchIndexer indexer = indexerProvider.get();
            try {
                for (Map.Entry<String, IDataType> entry : typesAdded.entrySet()) {
                    indexer.onAdd(entry.getKey(), entry.getValue());
                }
                indexer.commit();
            } catch (IndexCreationException ice) {
                LOG.error("Index creation for listener {} failed ", indexerProvider, ice);
                indexer.rollback();
                caughtExceptions.put(indexer, ice);
            }
        }

        if (caughtExceptions.size() > 0) {
           throw new IndexCreationException("Index creation failed for types " + typesAdded.keySet() + ". Aborting");
        }
    }

    private void onEntityAddedToRepo(ITypedReferenceableInstance typedInstance)
            throws MetadataException {

        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onEntityAdded(typedInstance);
        }
    }

    private void onTraitAddedToEntity(String typeName,
                                      String traitName) throws MetadataException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onTraitAdded(typeName, traitName);
        }
    }

    private void onTraitDeletedFromEntity(String typeName,
                                          String traitName) throws MetadataException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onTraitDeleted(typeName, traitName);
        }
    }

    public void registerListener(EntityChangeListener listener) {
        entityChangeListeners.add(listener);
    }

    public void unregisterListener(EntityChangeListener listener) {
        entityChangeListeners.remove(listener);
    }

}