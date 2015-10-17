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
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ParamChecker;
import org.apache.atlas.TypeNotFoundException;
import org.apache.atlas.classification.InterfaceAudience;
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
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Simple wrapper over TypeSystem and MetadataRepository services with hooks
 * for listening to changes to the repository.
 */
@Singleton
public class DefaultMetadataService implements MetadataService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataService.class);

    private final Collection<EntityChangeListener> entityChangeListeners = new LinkedHashSet<>();

    private final TypeSystem typeSystem;
    private final MetadataRepository repository;
    private final ITypeStore typeStore;
    private final Collection<Provider<TypesChangeListener>> typeChangeListeners;

    @Inject
    DefaultMetadataService(final MetadataRepository repository, final ITypeStore typeStore,
        final Collection<Provider<TypesChangeListener>> typeChangeListeners) throws AtlasException {

        this.typeStore = typeStore;
        this.typeSystem = TypeSystem.getInstance();
        this.repository = repository;

        this.typeChangeListeners = typeChangeListeners;
        restoreTypeSystem();
    }

    private void restoreTypeSystem() {
        LOG.info("Restoring type system from the store");
        try {
            TypesDef typesDef = typeStore.restore();
            typeSystem.defineTypes(typesDef);

            // restore types before creating super types
            createSuperTypes();

        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Restored type system from the store");
    }

    private static final AttributeDefinition NAME_ATTRIBUTE =
            TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE);
    private static final AttributeDefinition DESCRIPTION_ATTRIBUTE =
            TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE);

    @InterfaceAudience.Private
    private void createSuperTypes() throws AtlasException {
        HierarchicalTypeDefinition<ClassType> infraType = TypesUtil
                .createClassTypeDef(AtlasClient.INFRASTRUCTURE_SUPER_TYPE, ImmutableList.<String>of(), NAME_ATTRIBUTE,
                        DESCRIPTION_ATTRIBUTE);
        createType(infraType);

        HierarchicalTypeDefinition<ClassType> datasetType = TypesUtil
                .createClassTypeDef(AtlasClient.DATA_SET_SUPER_TYPE, ImmutableList.<String>of(), NAME_ATTRIBUTE,
                        DESCRIPTION_ATTRIBUTE);
        createType(datasetType);

        HierarchicalTypeDefinition<ClassType> processType = TypesUtil
                .createClassTypeDef(AtlasClient.PROCESS_SUPER_TYPE, ImmutableList.<String>of(), NAME_ATTRIBUTE,
                        DESCRIPTION_ATTRIBUTE,
                        new AttributeDefinition("inputs", DataTypes.arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("outputs", DataTypes.arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null));
        createType(processType);

        HierarchicalTypeDefinition<ClassType> referenceableType = TypesUtil
                .createClassTypeDef(AtlasClient.REFERENCEABLE_SUPER_TYPE, ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                                DataTypes.STRING_TYPE));
        createType(referenceableType);
    }

    private void createType(HierarchicalTypeDefinition<ClassType> type) throws AtlasException {
        if (!typeSystem.isRegistered(type.typeName)) {
            TypesDef typesDef = TypeUtils
                    .getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
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
        ParamChecker.notEmpty(typeDefinition, "type definition cannot be empty");
        TypesDef typesDef = validateTypeDefinition(typeDefinition);

        try {
            final Map<String, IDataType> typesAdded = typeSystem.defineTypes(typesDef);

            try {
                /* Create indexes first so that if index creation fails then we rollback
                   the typesystem and also do not persist the graph
                 */
                onTypesAdded(typesAdded);
                typeStore.store(typeSystem, ImmutableList.copyOf(typesAdded.keySet()));
            } catch (Throwable t) {
                typeSystem.removeTypes(typesAdded.keySet());
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
        ParamChecker.notEmpty(entityInstanceDefinition, "Entity instance definition cannot be empty");

        ITypedReferenceableInstance[] typedInstances = deserializeClassInstances(entityInstanceDefinition);

        final String[] guids = repository.createEntities(typedInstances);

        onEntityAddedToRepo(Arrays.asList(typedInstances));
        return new JSONArray(Arrays.asList(guids)).toString();
    }

    private ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition)
    throws AtlasException {
        try {
            JSONArray referableInstances = new JSONArray(entityInstanceDefinition);
            ITypedReferenceableInstance[] instances = new ITypedReferenceableInstance[referableInstances.length()];
            for (int index = 0; index < referableInstances.length(); index++) {
                Referenceable entityInstance =
                        InstanceSerialization.fromJsonReferenceable(referableInstances.getString(index), true);
                final String entityTypeName = entityInstance.getTypeName();
                ParamChecker.notEmpty(entityTypeName, "Entity type cannot be null");

                ClassType entityType = typeSystem.getDataType(ClassType.class, entityTypeName);
                ITypedReferenceableInstance typedInstrance = entityType.convert(entityInstance, Multiplicity.REQUIRED);
                instances[index] = typedInstrance;
            }
            return instances;
        } catch(ValueConversionException e) {
            throw e;
        } catch (Exception e) {  // exception from deserializer
            LOG.error("Unable to deserialize json={}", entityInstanceDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json");
        }
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String guid) throws AtlasException {
        ParamChecker.notEmpty(guid, "guid cannot be null");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return InstanceSerialization.toJson(instance, true);
    }

    @Override
    public String getEntityDefinition(String entityType, String attribute, String value) throws AtlasException {
        validateTypeExists(entityType);
        validateUniqueAttribute(entityType, attribute);

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(entityType, attribute, value);
        return InstanceSerialization.toJson(instance, true);
    }

    /**
     * Validate that attribute is unique attribute
     * @param entityType
     * @param attributeName
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
     * Return the list of entity names for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity names for the given type in the repository
     */
    @Override
    public List<String> getEntityList(String entityType) throws AtlasException {
        validateTypeExists(entityType);

        return repository.getEntityList(entityType);
    }

    @Override
    public void updateEntity(String guid, String property, String value) throws AtlasException {
        ParamChecker.notEmpty(guid, "guid cannot be null");
        ParamChecker.notEmpty(property, "property cannot be null");
        ParamChecker.notEmpty(value, "property value cannot be null");

        repository.updateEntity(guid, property, value);
    }

    private void validateTypeExists(String entityType) throws AtlasException {
        ParamChecker.notEmpty(entityType, "entity type cannot be null");

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
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
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
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
        ParamChecker.notEmpty(traitInstanceDefinition, "Trait instance cannot be null");

        ITypedStruct traitInstance = deserializeTraitInstance(traitInstanceDefinition);
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

        onTraitAddedToEntity(guid, traitName);
    }

    private ITypedStruct deserializeTraitInstance(String traitInstanceDefinition)
    throws AtlasException {

        try {
            Struct traitInstance = InstanceSerialization.fromJsonStruct(traitInstanceDefinition, true);
            final String entityTypeName = traitInstance.getTypeName();
            ParamChecker.notEmpty(entityTypeName, "entity type cannot be null");

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
        ParamChecker.notEmpty(guid, "entity GUID cannot be null");
        ParamChecker.notEmpty(traitNameToBeDeleted, "Trait name cannot be null");

        // ensure trait type is already registered with the TS
        if (!typeSystem.isRegistered(traitNameToBeDeleted)) {
            final String msg = String.format("trait=%s should be defined in type system before it can be deleted",
                    traitNameToBeDeleted);
            LOG.error(msg);
            throw new TypeNotFoundException(msg);
        }

        repository.deleteTrait(guid, traitNameToBeDeleted);

        onTraitDeletedFromEntity(guid, traitNameToBeDeleted);
    }

    private void onTypesAdded(Map<String, IDataType> typesAdded) throws AtlasException {
        Map<TypesChangeListener, Throwable> caughtExceptions = new HashMap<>();
        for (Provider<TypesChangeListener> indexerProvider : typeChangeListeners) {
            final TypesChangeListener listener = indexerProvider.get();
            try {
                listener.onAdd(typesAdded.values());
            } catch (IndexCreationException ice) {
                LOG.error("Index creation for listener {} failed ", indexerProvider, ice);
                caughtExceptions.put(listener, ice);
            }
        }

        if (caughtExceptions.size() > 0) {
            throw new IndexCreationException("Index creation failed for types " + typesAdded.keySet() + ". Aborting");
        }
    }

    private void onEntityAddedToRepo(Collection<ITypedReferenceableInstance> typedInstances)
    throws AtlasException {

        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onEntityAdded(typedInstances);
        }
    }

    private void onTraitAddedToEntity(String typeName, String traitName) throws AtlasException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onTraitAdded(typeName, traitName);
        }
    }

    private void onTraitDeletedFromEntity(String typeName, String traitName) throws AtlasException {
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
