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

import com.google.common.base.Preconditions;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.discovery.SearchIndexer;
import org.apache.hadoop.metadata.typesystem.json.Serialization$;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
import org.apache.hadoop.metadata.listener.EntityChangeListener;
import org.apache.hadoop.metadata.listener.TypesChangeListener;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultMetadataService implements MetadataService {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMetadataService.class);

    private final Set<TypesChangeListener> typesChangeListeners = new LinkedHashSet<>();
    private final Set<EntityChangeListener> entityChangeListeners
            = new LinkedHashSet<>();

    private final TypeSystem typeSystem;
    private final MetadataRepository repository;

    @Inject
    DefaultMetadataService(MetadataRepository repository,
                           SearchIndexer searchIndexer) throws MetadataException {
        this.typeSystem = TypeSystem.getInstance();
        this.repository = repository;

        registerListener(searchIndexer);
    }

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeName       name for this type, must be unique
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    @Override
    public JSONObject createType(String typeName,
                                 String typeDefinition) throws MetadataException {
        try {
            validateTypeDoesNotExist(typeName, typeDefinition);

            TypesDef typesDef = TypesSerialization.fromJson(typeDefinition);
            Map<String, IDataType> typesAdded = typeSystem.defineTypes(typesDef);

            onAdd(typesAdded);

            JSONObject response = new JSONObject();
            for (Map.Entry<String, IDataType> entry : typesAdded.entrySet()) {
                response.put(entry.getKey(), entry.getValue().getName());
            }

            return response;
        } catch (JSONException e) {
            LOG.error("Unable to persist type {}", typeName, e);
            throw new MetadataException("Unable to create response for: " + typeName);
        }
    }

    private void validateTypeDoesNotExist(String typeName,
                                          String typeDefinition) throws MetadataException {
        Preconditions.checkNotNull(typeName, "type name cannot be null");
        Preconditions.checkNotNull(typeDefinition, "type definition cannot be null");

        // verify if the type already exists
        IDataType existingTypeDefinition = null;
        try {
            existingTypeDefinition = typeSystem.getDataType(IDataType.class, typeName);
        } catch (MetadataException ignore) {
            // do nothing
        }

        if (existingTypeDefinition != null) {
            throw new RepositoryException("type is already defined for : " + typeName);
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
     * Creates an entity, instance of the type.
     *
     * @param entityType       type
     * @param entityDefinition definition
     * @return guid
     */
    @Override
    public String createEntity(String entityType,
                               String entityDefinition) throws MetadataException {
        try {
            validateEntity(entityDefinition, entityType);

            ITypedReferenceableInstance entityInstance =
                    Serialization$.MODULE$.fromJson(entityDefinition);
            final String guid = repository.createEntity(entityInstance, entityType);

            onAdd(entityType, entityInstance);

            return guid;
        } catch (ParseException e) {
            LOG.error("Unable to parse JSON {} for type {}", entityDefinition, entityType, e);
            throw new MetadataException("validation failed for: " + entityType);
        }
    }

    private void validateEntity(String entity, String entityType) throws ParseException {
        Preconditions.checkNotNull(entity, "entity cannot be null");
        Preconditions.checkNotNull(entityType, "entity type cannot be null");

        // todo: this is failing for instances but not types
        // JSONValue.parseWithException(entity);
    }

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    @Override
    public String getEntityDefinition(String guid) throws MetadataException {
        Preconditions.checkNotNull(guid, "guid cannot be null");

        final ITypedReferenceableInstance instance = repository.getEntityDefinition(guid);
        return instance == null
                ? null
                : Serialization$.MODULE$.toJson(instance);
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

    private void validateTypeExists(String entityType) throws MetadataException {
        Preconditions.checkNotNull(entityType, "entity type cannot be null");

        // verify if the type exists
        String existingTypeDefinition = null;
        try {
            existingTypeDefinition = getTypeDefinition(entityType);
        } catch (MetadataException ignore) {
            // do nothing
        }

        if (existingTypeDefinition == null) {
            throw new RepositoryException("type is not defined for : " + entityType);
        }
    }

    private void onAdd(Map<String, IDataType> typesAdded) throws MetadataException {
        for (TypesChangeListener listener : typesChangeListeners) {
            for (Map.Entry<String, IDataType> entry : typesAdded.entrySet()) {
                listener.onAdd(entry.getKey(), entry.getValue());
            }
        }
    }

    public void registerListener(TypesChangeListener listener) {
        typesChangeListeners.add(listener);
    }

    public void unregisterListener(TypesChangeListener listener) {
        typesChangeListeners.remove(listener);
    }

    private void onAdd(String typeName,
                       ITypedReferenceableInstance typedInstance) throws MetadataException {
        for (EntityChangeListener listener : entityChangeListeners) {
            listener.onAdd(typeName, typedInstance);
        }
    }

    public void registerListener(EntityChangeListener listener) {
        entityChangeListeners.add(listener);
    }

    public void unregisterListener(EntityChangeListener listener) {
        entityChangeListeners.remove(listener);
    }
}
