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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Provider;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.typestore.ITypeStore;
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
import org.apache.atlas.typesystem.types.AttributeDefinition;
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
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.TypeUtils.Pair;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Simple wrapper over TypeSystem and MetadataRepository services with hooks
 * for listening to changes to the repository.
 */
@Singleton
public class DefaultMetadataService implements MetadataService, ActiveStateChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);
    private final short maxAuditResults;
    private static final String CONFIG_MAX_AUDIT_RESULTS = "atlas.audit.maxResults";
    private static final short DEFAULT_MAX_AUDIT_RESULTS = 1000;

    private final TypeSystem typeSystem;
    private final MetadataRepository repository;
    private final ITypeStore typeStore;
    private IBootstrapTypesRegistrar typesRegistrar;

    private final Collection<TypesChangeListener> typeChangeListeners = new LinkedHashSet<>();
    private final Collection<EntityChangeListener> entityChangeListeners = new LinkedHashSet<>();

    private boolean wasInitialized = false;

    @Inject
    private EntityAuditRepository auditRepository;

    @Inject
    DefaultMetadataService(final MetadataRepository repository, final ITypeStore typeStore,
                           final IBootstrapTypesRegistrar typesRegistrar,
                           final Collection<Provider<TypesChangeListener>> typeListenerProviders,
                           final Collection<Provider<EntityChangeListener>> entityListenerProviders)
            throws AtlasException {
        this(repository, typeStore, typesRegistrar, typeListenerProviders, entityListenerProviders,
                TypeSystem.getInstance(), ApplicationProperties.get());
    }

    DefaultMetadataService(final MetadataRepository repository, final ITypeStore typeStore,
                           final IBootstrapTypesRegistrar typesRegistrar,
                           final Collection<Provider<TypesChangeListener>> typeListenerProviders,
                           final Collection<Provider<EntityChangeListener>> entityListenerProviders,
                           final TypeSystem typeSystem,
                           final Configuration configuration) throws AtlasException {
        this.typeStore = typeStore;
        this.typesRegistrar = typesRegistrar;
        this.typeSystem = typeSystem;
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
        if (!wasInitialized) {
            LOG.info("Initializing type system for the first time.");
            typeSystem.defineTypes(typesDef);

            // restore types before creating super types
            createSuperTypes();
            typesRegistrar.registerTypes(ReservedTypesRegistrar.getTypesDir(), typeSystem, this);
            wasInitialized = true;
        } else {
            LOG.info("Type system was already initialized, refreshing cache.");
            refreshCache(typesDef);
        }
        LOG.info("Restored type system from the store");
    }

    private void refreshCache(TypesDef typesDef) throws AtlasException {
        TypeSystem.TransientTypeSystem transientTypeSystem
                = typeSystem.createTransientTypeSystem(typesDef, true);
        Map<String, IDataType> typesAdded = transientTypeSystem.getTypesAdded();
        LOG.info("Number of types got from transient type system: " + typesAdded.size());
        typeSystem.commitTypes(typesAdded);
    }

    private static final AttributeDefinition NAME_ATTRIBUTE =
            TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE);
    private static final AttributeDefinition DESCRIPTION_ATTRIBUTE =
            TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE);

    @InterfaceAudience.Private
    private void createSuperTypes() throws AtlasException {
        HierarchicalTypeDefinition<ClassType> infraType = TypesUtil
                .createClassTypeDef(AtlasClient.INFRASTRUCTURE_SUPER_TYPE, ImmutableSet.<String>of(), NAME_ATTRIBUTE,
                        DESCRIPTION_ATTRIBUTE);
        createType(infraType);

        HierarchicalTypeDefinition<ClassType> datasetType = TypesUtil
                .createClassTypeDef(AtlasClient.DATA_SET_SUPER_TYPE, ImmutableSet.<String>of(), NAME_ATTRIBUTE,
                        DESCRIPTION_ATTRIBUTE);
        createType(datasetType);

        HierarchicalTypeDefinition<ClassType> referenceableType = TypesUtil
                .createClassTypeDef(AtlasClient.REFERENCEABLE_SUPER_TYPE, ImmutableSet.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                                DataTypes.STRING_TYPE));
        createType(referenceableType);

        HierarchicalTypeDefinition<ClassType> processType = TypesUtil
            .createClassTypeDef(AtlasClient.PROCESS_SUPER_TYPE, ImmutableSet.<String>of(AtlasClient.REFERENCEABLE_SUPER_TYPE),
                TypesUtil.createRequiredAttrDef(AtlasClient.NAME, DataTypes.STRING_TYPE),
                DESCRIPTION_ATTRIBUTE,
                new AttributeDefinition("inputs", DataTypes.arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                    Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("outputs", DataTypes.arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                    Multiplicity.OPTIONAL, false, null));
        createType(processType);
    }

    private void createType(HierarchicalTypeDefinition<ClassType> type) throws AtlasException {
        if (!typeSystem.isRegistered(type.typeName)) {
            TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                            ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                            ImmutableList.of(type));
            createType(TypesSerialization.toJson(typesDef));
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
        return createOrUpdateTypes(typeDefinition, false);
    }

    private JSONObject createOrUpdateTypes(String typeDefinition, boolean isUpdate) throws AtlasException {
        ParamChecker.notEmpty(typeDefinition, "type definition");
        TypesDef typesDef = validateTypeDefinition(typeDefinition);

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
        return createOrUpdateTypes(typeDefinition, true);
    }

    private TypesDef validateTypeDefinition(String typeDefinition) {
        try {
            TypesDef typesDef = TypesSerialization.fromJson(typeDefinition);
            if (typesDef.isEmpty()) {
                throw new IllegalArgumentException("Invalid type definition");
            }
            return typesDef;
        } catch (Exception e) {
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
     * Return the list of types in the repository.
     *
     * @return list of type names in the repository
     */
    @Override
    public List<String> getTypeNamesList() throws AtlasException {
        return typeSystem.getTypeNames();
    }

    /**
     * Return the list of trait type names in the type system.
     *
     * @return list of trait type names in the type system
     */
    @Override
    public List<String> getTypeNamesByCategory(DataTypes.TypeCategory typeCategory) throws AtlasException {
        return typeSystem.getTypeNamesByCategory(typeCategory);
    }

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityInstanceDefinition json array of entity definitions
     * @return guids - json array of guids
     */
    @Override
    public String createEntities(String entityInstanceDefinition) throws AtlasException {
        ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition");

        ITypedReferenceableInstance[] typedInstances = deserializeClassInstances(entityInstanceDefinition);

        List<String> guids = createEntities(typedInstances);
        return new JSONArray(guids).toString();
    }

    public List<String> createEntities(ITypedReferenceableInstance[] typedInstances) throws AtlasException {
        final List<String> guids = repository.createEntities(typedInstances);
        onEntitiesAdded(guids);
        return guids;
    }

    private ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition)
    throws AtlasException {
        try {
            JSONArray referableInstances = new JSONArray(entityInstanceDefinition);
            ITypedReferenceableInstance[] instances = new ITypedReferenceableInstance[referableInstances.length()];
            for (int index = 0; index < referableInstances.length(); index++) {
                Referenceable entityInstance =
                        InstanceSerialization.fromJsonReferenceable(referableInstances.getString(index), true);
                ITypedReferenceableInstance typedInstrance = getTypedReferenceableInstance(entityInstance);
                instances[index] = typedInstrance;
            }
            return instances;
        } catch(ValueConversionException | TypeNotFoundException  e) {
            throw e;
        } catch (Exception e) {  // exception from deserializer
            LOG.error("Unable to deserialize json={}", entityInstanceDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json", e);
        }
    }

    @Override
    public ITypedReferenceableInstance getTypedReferenceableInstance(Referenceable entityInstance) throws AtlasException {
        final String entityTypeName = entityInstance.getTypeName();
        ParamChecker.notEmpty(entityTypeName, "Entity type cannot be null");

        ClassType entityType = typeSystem.getDataType(ClassType.class, entityTypeName);

        //Both assigned id and values are required for full update
        //classtype.convert() will remove values if id is assigned. So, set temp id, convert and
        // then replace with original id
        Id origId = entityInstance.getId();
        entityInstance.replaceWithNewId(new Id(entityInstance.getTypeName()));
        ITypedReferenceableInstance typedInstrance = entityType.convert(entityInstance, Multiplicity.REQUIRED);
        ((ReferenceableInstance)typedInstrance).replaceWithNewId(origId);
        return typedInstrance;
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String guid) throws AtlasException {
        ParamChecker.notEmpty(guid, "entity id");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return InstanceSerialization.toJson(instance, true);
    }

    private ITypedReferenceableInstance getEntityDefinitionReference(String entityType, String attribute, String value)
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
    public String updateEntities(String entityInstanceDefinition) throws AtlasException {

        ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition");
        ITypedReferenceableInstance[] typedInstances = deserializeClassInstances(entityInstanceDefinition);

        TypeUtils.Pair<List<String>, List<String>> guids = repository.updateEntities(typedInstances);
        return onEntitiesAddedUpdated(guids);
    }

    private String onEntitiesAddedUpdated(TypeUtils.Pair<List<String>, List<String>> guids) throws AtlasException {
        onEntitiesAdded(guids.left);
        onEntitiesUpdated(guids.right);

        guids.left.addAll(guids.right);
        return new JSONArray(guids.left).toString();
    }

    @Override
    public String updateEntityAttributeByGuid(final String guid, String attributeName, String value) throws AtlasException {
        ParamChecker.notEmpty(guid, "entity id");
        ParamChecker.notEmpty(attributeName, "attribute name");
        ParamChecker.notEmpty(value, "attribute value");

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
        TypeUtils.Pair<List<String>, List<String>> guids = repository.updatePartial(newInstance);
        return onEntitiesAddedUpdated(guids);
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
    public String updateEntityPartialByGuid(final String guid, Referenceable newEntity) throws AtlasException {
        ParamChecker.notEmpty(guid, "guid cannot be null");
        ParamChecker.notNull(newEntity, "updatedEntity cannot be null");
        ITypedReferenceableInstance existInstance = validateEntityExists(guid);

        ITypedReferenceableInstance newInstance = convertToTypedInstance(newEntity, existInstance.getTypeName());
        ((ReferenceableInstance)newInstance).replaceWithNewId(new Id(guid, 0, newInstance.getTypeName()));

        TypeUtils.Pair<List<String>, List<String>> guids = repository.updatePartial(newInstance);
        return onEntitiesAddedUpdated(guids);
    }

    private ITypedReferenceableInstance convertToTypedInstance(Referenceable updatedEntity, String typeName) throws AtlasException {
        ClassType type = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance newInstance = type.createInstance();

        for (String attributeName : updatedEntity.getValuesMap().keySet()) {
            AttributeInfo attributeInfo = type.fieldMapping.fields.get(attributeName);
            if (attributeInfo == null) {
                throw new AtlasException("Invalid property " + attributeName + " for entity " + updatedEntity);
            }

            DataTypes.TypeCategory attrTypeCategory = attributeInfo.dataType().getTypeCategory();
            Object value = updatedEntity.get(attributeName);
            if (value != null) {
                switch (attrTypeCategory) {
                    case CLASS:
                        if (value instanceof Referenceable) {
                            newInstance.set(attributeName, value);
                        } else {
                            Id id = new Id((String) value, 0, attributeInfo.dataType().getName());
                            newInstance.set(attributeName, id);
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
        }

        return newInstance;
    }

    @Override
    public String updateEntityByUniqueAttribute(String typeName, String uniqueAttributeName, String attrValue,
                                                Referenceable updatedEntity) throws AtlasException {
        ParamChecker.notEmpty(typeName, "typeName");
        ParamChecker.notEmpty(uniqueAttributeName, "uniqueAttributeName");
        ParamChecker.notNull(attrValue, "unique attribute value");
        ParamChecker.notNull(updatedEntity, "updatedEntity");

        ITypedReferenceableInstance oldInstance = getEntityDefinitionReference(typeName, uniqueAttributeName, attrValue);

        final ITypedReferenceableInstance newInstance = convertToTypedInstance(updatedEntity, typeName);
        ((ReferenceableInstance)newInstance).replaceWithNewId(oldInstance.getId());

        TypeUtils.Pair<List<String>, List<String>> guids = repository.updatePartial(newInstance);
        return onEntitiesAddedUpdated(guids);
    }

    private void validateTypeExists(String entityType) throws AtlasException {
        ParamChecker.notEmpty(entityType, "entity type");

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
        ParamChecker.notEmpty(guid, "entity id");
        return repository.getTraitNames(guid);
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
        ParamChecker.notEmpty(guid, "entity id");
        ParamChecker.notEmpty(traitInstanceDefinition, "trait instance definition");

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
            final String entityTypeName = traitInstance.getTypeName();
            ParamChecker.notEmpty(entityTypeName, "entity type");

            TraitType traitType = typeSystem.getDataType(TraitType.class, entityTypeName);
            return traitType.convert(traitInstance, Multiplicity.REQUIRED);
        } catch (TypeNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new AtlasException("Error deserializing trait instance", e);
        }
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
        ParamChecker.notEmpty(guid, "entity id");
        ParamChecker.notEmpty(traitNameToBeDeleted, "trait name");

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

    private List<ITypedReferenceableInstance> loadEntities(List<String> guids) throws EntityNotFoundException,
            RepositoryException {
        List<ITypedReferenceableInstance> entities = new ArrayList<>();
        for (String guid : guids) {
            entities.add(repository.getEntityDefinition(guid));
        }
        return entities;
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
        ParamChecker.notEmpty(guid, "entity id");
        ParamChecker.notEmptyIfNotNull(startKey, "start key");
        ParamChecker.lessThan(count, maxAuditResults, "count");

        return auditRepository.listEvents(guid, startKey, count);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.services.MetadataService#deleteEntities(java.lang.String)
     */
    @Override
    public List<String> deleteEntities(List<String> deleteCandidateGuids) throws AtlasException {
        ParamChecker.notEmpty(deleteCandidateGuids, "delete candidate guids");
        return deleteGuids(deleteCandidateGuids);
    }

    @Override
    public List<String> deleteEntityByUniqueAttribute(String typeName, String uniqueAttributeName, String attrValue) throws AtlasException {
        ParamChecker.notEmpty(typeName, "delete candidate typeName");
        ParamChecker.notEmpty(uniqueAttributeName, "delete candidate unique attribute name");
        ParamChecker.notEmpty(attrValue, "delete candidate unique attribute value");

        //Throws EntityNotFoundException if the entity could not be found by its unique attribute
        ITypedReferenceableInstance instance = getEntityDefinitionReference(typeName, uniqueAttributeName, attrValue);
        final Id instanceId = instance.getId();
        List<String> deleteCandidateGuids  = new ArrayList<String>() {{ add(instanceId._getId());}};

        return deleteGuids(deleteCandidateGuids);
    }

    private List<String> deleteGuids(List<String> deleteCandidateGuids) throws AtlasException {
        Pair<List<String>, List<ITypedReferenceableInstance>> deleteEntitiesResult = repository.deleteEntities(deleteCandidateGuids);
        if (deleteEntitiesResult.right.size() > 0) {
            onEntitiesDeleted(deleteEntitiesResult.right);
        }
        return deleteEntitiesResult.left;
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
}
