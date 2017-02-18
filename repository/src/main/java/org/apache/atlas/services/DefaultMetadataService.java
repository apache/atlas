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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;



/**
 * Simple wrapper over TypeSystem and MetadataRepository services with hooks
 * for listening to changes to the repository.
 */
@Singleton
public class DefaultMetadataService implements MetadataService, ActiveStateChangeHandler, TypeDefChangeListener {
    private enum OperationType {
        CREATE, UPDATE, DELETE
    };

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    private final short maxAuditResults;
    private static final String CONFIG_MAX_AUDIT_RESULTS = "atlas.audit.maxResults";
    private static final short DEFAULT_MAX_AUDIT_RESULTS = 1000;

    private final TypeSystem typeSystem;
    private final MetadataRepository repository;
    private final ITypeStore typeStore;

    private final Collection<TypesChangeListener> typeChangeListeners = new LinkedHashSet<>();
    private final Collection<EntityChangeListener> entityChangeListeners = new LinkedHashSet<>();

    @Inject
    private EntityAuditRepository auditRepository;

    @Inject
    DefaultMetadataService(final MetadataRepository repository, final ITypeStore typeStore,
                           final Collection<Provider<TypesChangeListener>> typeListenerProviders,
                           final Collection<Provider<EntityChangeListener>> entityListenerProviders, TypeCache typeCache)
            throws AtlasException {
        this(repository, typeStore, typeListenerProviders, entityListenerProviders,
                TypeSystem.getInstance(), ApplicationProperties.get(), typeCache);
    }
    
    //for testing only
    public DefaultMetadataService(final MetadataRepository repository, final ITypeStore typeStore,
                           final Collection<Provider<TypesChangeListener>> typeListenerProviders,
                           final Collection<Provider<EntityChangeListener>> entityListenerProviders,
                           final TypeSystem typeSystem,
                           final Configuration configuration, TypeCache typeCache) throws AtlasException {
        this.typeStore = typeStore;
        this.typeSystem = typeSystem;
        /**
         * Ideally a TypeCache implementation should have been injected in the TypeSystemProvider,
         * but a singleton of TypeSystem is constructed privately within the class so that
         * clients of TypeSystem would never instantiate a TypeSystem object directly in
         * their code. As soon as a client makes a call to TypeSystem.getInstance(), they
         * should have the singleton ready for consumption. Manually inject TypeSystem with
         * the Guice-instantiated type cache here, before types are restored.
         * This allows cache implementations to participate in Guice dependency injection.
         */
        this.typeSystem.setTypeCache(typeCache);

        this.repository = repository;

        for (Provider<TypesChangeListener> provider : typeListenerProviders) {
            typeChangeListeners.add(provider.get());
        }

        for (Provider<EntityChangeListener> provider : entityListenerProviders) {
            entityChangeListeners.add(provider.get());
        }

        if (!HAConfiguration.isHAEnabled(configuration)) {
            restoreTypeSystem();
        }

        maxAuditResults = configuration.getShort(CONFIG_MAX_AUDIT_RESULTS, DEFAULT_MAX_AUDIT_RESULTS);
    }

    private void restoreTypeSystem() throws AtlasException {
        LOG.info("Restoring type system from the store");

        TypesDef typesDef = typeStore.restore();

        refreshCache(typesDef);

        LOG.info("Restored type system from the store");
    }

    private void refreshCache(TypesDef typesDef) throws AtlasException {
        if (typesDef != null && !typesDef.isEmpty()) {
            TypeSystem.TransientTypeSystem transientTypeSystem = typeSystem.createTransientTypeSystem(typesDef, true);
            Map<String, IDataType> typesAdded = transientTypeSystem.getTypesAdded();
            LOG.info("Number of types got from transient type system: {}", typesAdded.size());
            typeSystem.commitTypes(typesAdded);
        }
    }

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    @Override
    public JSONObject createType(String typeDefinition) throws AtlasException {
        return createOrUpdateTypes(OperationType.CREATE, typeDefinition, false);
    }

    private JSONObject createOrUpdateTypes(OperationType opType, String typeDefinition, boolean isUpdate) throws AtlasException {
        typeDefinition = ParamChecker.notEmpty(typeDefinition, "type definition");
        TypesDef typesDef = validateTypeDefinition(opType, typeDefinition);


        try {
            final TypeSystem.TransientTypeSystem transientTypeSystem = typeSystem.createTransientTypeSystem(typesDef, isUpdate);
            final Map<String, IDataType> typesAdded = transientTypeSystem.getTypesAdded();
            try {
                /* Create indexes first so that if index creation fails then we rollback
                   the typesystem and also do not persist the graph
                 */
                if (isUpdate) {
                    onTypesUpdated(typesAdded);
                } else {
                    onTypesAdded(typesAdded);
                }
                typeStore.store(transientTypeSystem, ImmutableList.copyOf(typesAdded.keySet()));
                typeSystem.commitTypes(typesAdded);
            } catch (Throwable t) {
                throw new AtlasException("Unable to persist types ", t);
            }

            return new JSONObject() {{
                put(AtlasClient.TYPES, typesAdded.keySet());
            }};
        } catch (JSONException e) {
            LOG.error("Unable to create response for types={}", typeDefinition, e);
            throw new AtlasException("Unable to create response ", e);
        }
    }

    @Override
    public JSONObject updateType(String typeDefinition) throws AtlasException {
        return createOrUpdateTypes(OperationType.UPDATE, typeDefinition, true);
    }

    private TypesDef validateTypeDefinition(OperationType opType, String typeDefinition) throws AtlasException {
        final String exceptionErrorMessageFormat = "%s for '%s' failed: %s";

        try {
            TypesDef typesDef = TypesSerialization.fromJson(typeDefinition);
            if (typesDef.isEmpty()) {
                throw new IllegalArgumentException("Invalid type definition");
            }

            for (HierarchicalTypeDefinition<ClassType> t : typesDef.classTypesAsJavaList()) {
                if (!AtlasTypeUtil.isValidTypeName(t.typeName))
                    throw new AtlasException(
                            String.format(exceptionErrorMessageFormat, opType.toString(), t.typeName, AtlasTypeUtil.getInvalidTypeNameErrorMessage()));
            }

            for (StructTypeDefinition t : typesDef.structTypesAsJavaList()) {
                if (!AtlasTypeUtil.isValidTypeName(t.typeName))
                    throw new AtlasException(
                            String.format(exceptionErrorMessageFormat, opType.toString(), t.typeName, AtlasTypeUtil.getInvalidTypeNameErrorMessage()));
            }

            for (EnumTypeDefinition t : typesDef.enumTypesAsJavaList()) {
                if (!AtlasTypeUtil.isValidTypeName(t.name))
                    throw new AtlasException(
                            String.format(exceptionErrorMessageFormat, opType.toString(), t.name, AtlasTypeUtil.getInvalidTypeNameErrorMessage()));
            }

            for (HierarchicalTypeDefinition<TraitType> t : typesDef.traitTypesAsJavaList()) {
                if (!AtlasTypeUtil.isValidTraitTypeName(t.typeName))
                    throw new AtlasException(
                            String.format(exceptionErrorMessageFormat, opType.toString(), t.typeName, AtlasTypeUtil.getInvalidTraitTypeNameErrorMessage()));
            }

            return typesDef;
        }
        catch (Exception e) {
            LOG.error("Unable to deserialize json={}", typeDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json " + typeDefinition, e);
        }
    }

    /**
     * Return the definition for the given type.
     *
     * @param typeName name for this type, must be unique
     * @return type definition as JSON
     */
    @Override
    public String getTypeDefinition(String typeName) throws AtlasException {
        final IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
        return TypesSerialization.toJson(typeSystem, dataType.getName());
    }

    /**
     * Return the list of type names in the type system which match the specified filter.
     *
     * @return list of type names
     * @param filterMap - Map of filter for type names. Valid keys are CATEGORY, SUPERTYPE, NOT_SUPERTYPE
     * For example, CATEGORY = TRAIT && SUPERTYPE contains 'X' && SUPERTYPE !contains 'Y'
     * If there is no filter, all the types are returned
     */
    @Override
    public List<String> getTypeNames(Map<TypeCache.TYPE_FILTER, String> filterMap) throws AtlasException {
        return typeSystem.getTypeNames(filterMap);
    }

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityInstanceDefinition json array of entity definitions
     * @return guids - list of guids
     */
    @Override
    public CreateUpdateEntitiesResult createEntities(String entityInstanceDefinition) throws AtlasException {
        entityInstanceDefinition = ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition");

        ITypedReferenceableInstance[] typedInstances = deserializeClassInstances(entityInstanceDefinition);

        return createEntities(typedInstances);
    }

    public CreateUpdateEntitiesResult createEntities(ITypedReferenceableInstance[] typedInstances) throws AtlasException {
        final CreateUpdateEntitiesResult result = repository.createEntities(typedInstances);
        onEntitiesAdded(result.getCreatedEntities());
        return result;
    }

    @Override
    public ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition) throws AtlasException {
        return GraphHelper.deserializeClassInstances(typeSystem, entityInstanceDefinition);
    }
    
    @Override
    public ITypedReferenceableInstance getTypedReferenceableInstance(Referenceable entityInstance) throws AtlasException {
        return GraphHelper.getTypedReferenceableInstance(typeSystem, entityInstance);
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinitionJson(String guid) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "entity id");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return InstanceSerialization.toJson(instance, true);
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public ITypedReferenceableInstance getEntityDefinition(String guid) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "entity id");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return instance;
    }

    @Override
    public ITypedReferenceableInstance getEntityDefinitionReference(String entityType, String attribute, String value)
            throws AtlasException {
        validateTypeExists(entityType);
        validateUniqueAttribute(entityType, attribute);

        return repository.getEntityDefinition(entityType, attribute, value);
    }

        @Override
    public String getEntityDefinition(String entityType, String attribute, String value) throws AtlasException {
        final ITypedReferenceableInstance instance = getEntityDefinitionReference(entityType, attribute, value);
        return InstanceSerialization.toJson(instance, true);
    }

    /**
     * Validate that attribute is unique attribute
     * @param entityType     the entity type
     * @param attributeName  the name of the attribute
     */
    private void validateUniqueAttribute(String entityType, String attributeName) throws AtlasException {
        ClassType type = typeSystem.getDataType(ClassType.class, entityType);
        AttributeInfo attribute = type.fieldMapping().fields.get(attributeName);
        if(attribute == null) {
            throw new IllegalArgumentException(
                String.format("%s is not an attribute in %s", attributeName, entityType));
        }
        if (!attribute.isUnique) {
            throw new IllegalArgumentException(
                    String.format("%s.%s is not a unique attribute", entityType, attributeName));
        }
    }

    /**
     * Return the list of entity guids for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity guids for the given type in the repository
     */
    @Override
    public List<String> getEntityList(String entityType) throws AtlasException {
        validateTypeExists(entityType);

        return repository.getEntityList(entityType);
    }

    /**
     * Updates an entity, instance of the type based on the guid set.
     *
     * @param entityInstanceDefinition json array of entity definitions
     * @return guids - json array of guids
     */
    @Override
    public CreateUpdateEntitiesResult updateEntities(String entityInstanceDefinition) throws AtlasException {
        entityInstanceDefinition = ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition");
        ITypedReferenceableInstance[] typedInstances = deserializeClassInstances(entityInstanceDefinition);

        CreateUpdateEntitiesResult result = repository.updateEntities(typedInstances);
        onEntitiesAddedUpdated(result.getEntityResult());
        return result;
    }

    /**
     * Updates an entity, instance of the type based on the guid set.
     *
     * @param entityInstanceDefinitions
     * @return guids - json array of guids
     */
    @Override
    public CreateUpdateEntitiesResult updateEntities(ITypedReferenceableInstance[] entityInstanceDefinitions) throws AtlasException {
        CreateUpdateEntitiesResult result = repository.updateEntities(entityInstanceDefinitions);
        onEntitiesAddedUpdated(result.getEntityResult());
        return result;
    }

    private void onEntitiesAddedUpdated(AtlasClient.EntityResult entityResult) throws AtlasException {
        onEntitiesAdded(entityResult.getCreatedEntities());
        onEntitiesUpdated(entityResult.getUpdateEntities());
        //Note: doesn't access deletedEntities from entityResult
        onEntitiesDeleted(RequestContext.get().getDeletedEntities());
    }

    @Override
    public CreateUpdateEntitiesResult updateEntityAttributeByGuid(String guid, String attributeName,
                                                                String value) throws AtlasException {
        guid          = ParamChecker.notEmpty(guid, "entity id");
        attributeName = ParamChecker.notEmpty(attributeName, "attribute name");
        value         = ParamChecker.notEmpty(value, "attribute value");

        ITypedReferenceableInstance existInstance = validateEntityExists(guid);
        ClassType type = typeSystem.getDataType(ClassType.class, existInstance.getTypeName());
        ITypedReferenceableInstance newInstance = type.createInstance();

        AttributeInfo attributeInfo = type.fieldMapping.fields.get(attributeName);
        if (attributeInfo == null) {
            throw new AtlasException("Invalid property " + attributeName + " for entity " + existInstance.getTypeName());
        }

        DataTypes.TypeCategory attrTypeCategory = attributeInfo.dataType().getTypeCategory();

        switch(attrTypeCategory) {
            case PRIMITIVE:
                newInstance.set(attributeName, value);
                break;
            case CLASS:
                Id id = new Id(value, 0, attributeInfo.dataType().getName());
                newInstance.set(attributeName, id);
                break;
            default:
                throw new AtlasException("Update of " + attrTypeCategory + " is not supported");
        }

        ((ReferenceableInstance)newInstance).replaceWithNewId(new Id(guid, 0, newInstance.getTypeName()));
        CreateUpdateEntitiesResult result = repository.updatePartial(newInstance);
        onEntitiesAddedUpdated(result.getEntityResult());
        return result;
    }

    private ITypedReferenceableInstance validateEntityExists(String guid)
            throws EntityNotFoundException, RepositoryException {
        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        if (instance == null) {
            throw new EntityNotFoundException(String.format("Entity with guid %s not found ", guid));
        }
        return instance;
    }

    @Override
    public CreateUpdateEntitiesResult updateEntityPartialByGuid(String guid, Referenceable newEntity)
            throws AtlasException {
        guid      = ParamChecker.notEmpty(guid, "guid cannot be null");
        newEntity = ParamChecker.notNull(newEntity, "updatedEntity cannot be null");
        ITypedReferenceableInstance existInstance = validateEntityExists(guid);

        ITypedReferenceableInstance newInstance = convertToTypedInstance(newEntity, existInstance.getTypeName());
        ((ReferenceableInstance)newInstance).replaceWithNewId(new Id(guid, 0, newInstance.getTypeName()));

        CreateUpdateEntitiesResult result = repository.updatePartial(newInstance);
        onEntitiesAddedUpdated(result.getEntityResult());
        return result;
    }

    private ITypedReferenceableInstance convertToTypedInstance(Referenceable updatedEntity, String typeName)
            throws AtlasException {
        ClassType type = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance newInstance = type.createInstance();

        for (String attributeName : updatedEntity.getValuesMap().keySet()) {
            AttributeInfo attributeInfo = type.fieldMapping.fields.get(attributeName);
            if (attributeInfo == null) {
                throw new AtlasException("Invalid property " + attributeName + " for entity " + updatedEntity);
            }

            DataTypes.TypeCategory attrTypeCategory = attributeInfo.dataType().getTypeCategory();
            Object value = updatedEntity.get(attributeName);
            switch (attrTypeCategory) {
                case CLASS:
                    if (value != null) {
                        if (value instanceof Referenceable) {
                            newInstance.set(attributeName, value);
                        } else {
                            Id id = new Id((String) value, 0, attributeInfo.dataType().getName());
                            newInstance.set(attributeName, id);
                        }
                    }
                    break;

                case ENUM:
                case PRIMITIVE:
                case ARRAY:
                case STRUCT:
                case MAP:
                    newInstance.set(attributeName, value);
                    break;

                case TRAIT:
                    //TODO - handle trait updates as well?
                default:
                    throw new AtlasException("Update of " + attrTypeCategory + " is not supported");
            }
        }

        return newInstance;
    }

    @Override
    public CreateUpdateEntitiesResult updateEntityByUniqueAttribute(String typeName, String uniqueAttributeName,
                                                                  String attrValue,
                                                                  Referenceable updatedEntity) throws AtlasException {
        typeName            = ParamChecker.notEmpty(typeName, "typeName");
        uniqueAttributeName = ParamChecker.notEmpty(uniqueAttributeName, "uniqueAttributeName");
        attrValue           = ParamChecker.notNull(attrValue, "unique attribute value");
        updatedEntity       = ParamChecker.notNull(updatedEntity, "updatedEntity");

        ITypedReferenceableInstance oldInstance = getEntityDefinitionReference(typeName, uniqueAttributeName, attrValue);

        final ITypedReferenceableInstance newInstance = convertToTypedInstance(updatedEntity, typeName);
        ((ReferenceableInstance)newInstance).replaceWithNewId(oldInstance.getId());

        CreateUpdateEntitiesResult result = repository.updatePartial(newInstance);
        onEntitiesAddedUpdated(result.getEntityResult());
        return result;
    }

    private void validateTypeExists(String entityType) throws AtlasException {
        entityType = ParamChecker.notEmpty(entityType, "entity type");

        IDataType type = typeSystem.getDataType(IDataType.class, entityType);
        if (type.getTypeCategory() != DataTypes.TypeCategory.CLASS) {
            throw new IllegalArgumentException("type " + entityType + " not a CLASS type");
        }
    }

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     * @throws AtlasException
     */
    @Override
    public List<String> getTraitNames(String guid) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "entity id");
        return repository.getTraitNames(guid);
    }

    /**
     * Adds a new trait to the list of existing entities represented by their respective guids
     * @param entityGuids   list of guids of entities
     * @param traitInstance trait instance json that needs to be added to entities
     * @throws AtlasException
     */
    @Override
    public void addTrait(List<String> entityGuids, ITypedStruct traitInstance) throws AtlasException {
        Preconditions.checkNotNull(entityGuids, "entityGuids list cannot be null");
        Preconditions.checkNotNull(traitInstance, "Trait instance cannot be null");

        final String traitName = traitInstance.getTypeName();

        // ensure trait type is already registered with the TypeSystem
        if (!typeSystem.isRegistered(traitName)) {
            String msg = String.format("trait=%s should be defined in type system before it can be added", traitName);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        //ensure trait is not already registered with any of the given entities
        for (String entityGuid : entityGuids) {
            Preconditions.checkArgument(!getTraitNames(entityGuid).contains(traitName),
                    "trait=%s is already defined for entity=%s", traitName, entityGuid);
        }

        repository.addTrait(entityGuids, traitInstance);

        for (String entityGuid : entityGuids) {
            onTraitAddedToEntity(repository.getEntityDefinition(entityGuid), traitInstance);
        }
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid                    globally unique identifier for the entity
     * @param traitInstanceDefinition trait instance json that needs to be added to entity
     * @throws AtlasException
     */
    @Override
    public void addTrait(String guid, String traitInstanceDefinition) throws AtlasException {
        guid                    = ParamChecker.notEmpty(guid, "entity id");
        traitInstanceDefinition = ParamChecker.notEmpty(traitInstanceDefinition, "trait instance definition");

        ITypedStruct traitInstance = deserializeTraitInstance(traitInstanceDefinition);
        addTrait(guid, traitInstance);
    }

    public void addTrait(String guid, ITypedStruct traitInstance) throws AtlasException {
        final String traitName = traitInstance.getTypeName();

        // ensure trait type is already registered with the TS
        if (!typeSystem.isRegistered(traitName)) {
            String msg = String.format("trait=%s should be defined in type system before it can be added", traitName);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        // ensure trait is not already defined
        Preconditions
            .checkArgument(!getTraitNames(guid).contains(traitName), "trait=%s is already defined for entity=%s",
                    traitName, guid);

        repository.addTrait(guid, traitInstance);

        onTraitAddedToEntity(repository.getEntityDefinition(guid), traitInstance);
    }

    private ITypedStruct deserializeTraitInstance(String traitInstanceDefinition)
    throws AtlasException {
        return createTraitInstance(InstanceSerialization.fromJsonStruct(traitInstanceDefinition, true));
    }

    @Override
    public ITypedStruct createTraitInstance(Struct traitInstance) throws AtlasException {
        try {
            final String entityTypeName = ParamChecker.notEmpty(traitInstance.getTypeName(), "entity type");

            TraitType traitType = typeSystem.getDataType(TraitType.class, entityTypeName);
            return traitType.convert(traitInstance, Multiplicity.REQUIRED);
        } catch (TypeNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new AtlasException("Error deserializing trait instance", e);
        }
    }

    @Override
    public IStruct getTraitDefinition(String guid, final String traitName) throws AtlasException {
        guid = ParamChecker.notEmpty(guid, "entity id");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return instance.getTrait(traitName);
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid                 globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws AtlasException
     */
    @Override
    public void deleteTrait(String guid, String traitNameToBeDeleted) throws AtlasException {
        guid                 = ParamChecker.notEmpty(guid, "entity id");
        traitNameToBeDeleted = ParamChecker.notEmpty(traitNameToBeDeleted, "trait name");

        // ensure trait type is already registered with the TS
        if (!typeSystem.isRegistered(traitNameToBeDeleted)) {
            final String msg = String.format("trait=%s should be defined in type system before it can be deleted",
                    traitNameToBeDeleted);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        repository.deleteTrait(guid, traitNameToBeDeleted);

        onTraitDeletedFromEntity(repository.getEntityDefinition(guid), traitNameToBeDeleted);
    }

    private void onTypesAdded(Map<String, IDataType> typesAdded) throws AtlasException {
        for (TypesChangeListener listener : typeChangeListeners) {
            listener.onAdd(typesAdded.values());
        }
    }

    private void onEntitiesAdded(List<String> guids) throws AtlasException {
        List<ITypedReferenceableInstance> entities = loadEntities(guids);
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onEntitiesAdded(entities);
        }
    }

    private List<ITypedReferenceableInstance> loadEntities(List<String> guids) throws RepositoryException, EntityNotFoundException {
        return repository.getEntityDefinitions(guids.toArray(new String[guids.size()]));
    }

    private void onTypesUpdated(Map<String, IDataType> typesUpdated) throws AtlasException {
        for (TypesChangeListener listener : typeChangeListeners) {
            listener.onChange(typesUpdated.values());
        }
    }

    private void onEntitiesUpdated(List<String> guids) throws AtlasException {
        List<ITypedReferenceableInstance> entities = loadEntities(guids);
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onEntitiesUpdated(entities);
        }
    }

    private void onTraitAddedToEntity(ITypedReferenceableInstance entity, IStruct trait) throws AtlasException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onTraitAdded(entity, trait);
        }
    }

    private void onTraitDeletedFromEntity(ITypedReferenceableInstance entity, String traitName) throws AtlasException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onTraitDeleted(entity, traitName);
        }
    }

    public void registerListener(EntityChangeListener listener) {
        entityChangeListeners.add(listener);
    }

    public void unregisterListener(EntityChangeListener listener) {
        entityChangeListeners.remove(listener);
    }

    @Override
    public List<EntityAuditEvent> getAuditEvents(String guid, String startKey, short count) throws AtlasException {
        guid     = ParamChecker.notEmpty(guid, "entity id");
        startKey = ParamChecker.notEmptyIfNotNull(startKey, "start key");
        ParamChecker.lessThan(count, maxAuditResults, "count");

        return auditRepository.listEvents(guid, startKey, count);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.services.MetadataService#deleteEntities(java.lang.String)
     */
    @Override
    public AtlasClient.EntityResult deleteEntities(List<String> deleteCandidateGuids) throws AtlasException {
        ParamChecker.notEmpty(deleteCandidateGuids, "delete candidate guids");
        return deleteGuids(deleteCandidateGuids);
    }

    @Override
    public AtlasClient.EntityResult deleteEntityByUniqueAttribute(String typeName, String uniqueAttributeName,
                                                                  String attrValue) throws AtlasException {
        typeName            = ParamChecker.notEmpty(typeName, "delete candidate typeName");
        uniqueAttributeName = ParamChecker.notEmpty(uniqueAttributeName, "delete candidate unique attribute name");
        attrValue           = ParamChecker.notEmpty(attrValue, "delete candidate unique attribute value");

        //Throws EntityNotFoundException if the entity could not be found by its unique attribute
        ITypedReferenceableInstance instance = getEntityDefinitionReference(typeName, uniqueAttributeName, attrValue);
        final Id instanceId = instance.getId();
        List<String> deleteCandidateGuids  = new ArrayList<String>() {{ add(instanceId._getId());}};

        return deleteGuids(deleteCandidateGuids);
    }

    private AtlasClient.EntityResult deleteGuids(List<String> deleteCandidateGuids) throws AtlasException {
        AtlasClient.EntityResult entityResult = repository.deleteEntities(deleteCandidateGuids);
        onEntitiesAddedUpdated(entityResult);
        return entityResult;
    }

    private void onEntitiesDeleted(List<ITypedReferenceableInstance> entities) throws AtlasException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onEntitiesDeleted(entities);
        }
    }

    /**
     * Create or restore the {@link TypeSystem} cache on server activation.
     *
     * When an instance is passive, types could be created outside of its cache by the active instance.
     * Hence, when this instance becomes active, it needs to restore the cache from the backend store.
     * The first time initialization happens, the indices for these types also needs to be created.
     * This must happen only from the active instance, as it updates shared backend state.
     */
    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("Reacting to active state: restoring type system");
        restoreTypeSystem();
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive state: no action right now");
    }

    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        // All we need here is a restore of the type-system
        LOG.info("TypeSystem reset invoked by TypeRegistry changes");
        try {
            TypesDef typesDef = typeStore.restore();
            typeSystem.reset();
            TypeSystem.TransientTypeSystem transientTypeSystem
                    = typeSystem.createTransientTypeSystem(typesDef, false);
            Map<String, IDataType> typesAdded = transientTypeSystem.getTypesAdded();
            LOG.info("Number of types got from transient type system: {}", typesAdded.size());
            typeSystem.commitTypes(typesAdded);
        } catch (AtlasException e) {
            LOG.error("Failed to restore type-system after TypeRegistry changes", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }
}
