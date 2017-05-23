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

import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.model.legacy.EntityResult;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * Metadata service.
 */
@Deprecated
public interface MetadataService {

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    JSONObject createType(String typeDefinition) throws AtlasException;

    /**z
     * Updates the given types in the type definition
     * @param typeDefinition
     * @return
     * @throws AtlasException
     */
    JSONObject updateType(String typeDefinition) throws AtlasException;

    /**
     * Return the definition for the given type.
     *
     * @param typeName name for this type, must be unique
     * @return type definition as JSON
     */
    String getTypeDefinition(String typeName) throws AtlasException;

    /**
     * Return the list of type names in the type system which match the specified filter.
     *
     * @return list of type names
     * @param filterMap - Map of filter for type names. Valid keys are CATEGORY, SUPERTYPE, NOT_SUPERTYPE
     * For example, CATEGORY = TRAIT && SUPERTYPE contains 'X' && SUPERTYPE !contains 'Y'
     * If there is no filter, all the types are returned
     */
    List<String> getTypeNames(Map<TypeCache.TYPE_FILTER, String> filterMap) throws AtlasException;

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityDefinition definition
     * @return CreateUpdateEntitiesResult with the guids of the entities created
     */
   CreateUpdateEntitiesResult createEntities(String entityDefinition) throws AtlasException;

    /**
     * Get a typed entity instance.
     *
     * @param entity entity
     * @return typed entity instance
     *
     * @throws AtlasException if any failure occurs
     */
    ITypedReferenceableInstance getTypedReferenceableInstance(Referenceable entity) throws AtlasException;

    /**
     * Create entity instances.
     *
     * @param typedInstances  instance to create
     * @return CreateUpdateEntitiesResult with the guids of the entities created
     *
     * @throws AtlasException if unable to create the entities
     */
    CreateUpdateEntitiesResult createEntities(ITypedReferenceableInstance[] typedInstances) throws AtlasException;


    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    String getEntityDefinitionJson(String guid) throws AtlasException;

    ITypedReferenceableInstance getEntityDefinition(String guid) throws AtlasException;


    /**
     * Return the definition given type and attribute. The attribute has to be unique attribute for the type
     * @param entityType - type name
     * @param attribute - attribute name
     * @param value - attribute value
     * @return
     * @throws AtlasException
     */
    ITypedReferenceableInstance getEntityDefinitionReference(String entityType, String attribute, String value) throws AtlasException;

    /**
     * Return the definition given type and attribute. The attribute has to be unique attribute for the type
     * @param entityType - type name
     * @param attribute - attribute name
     * @param value - attribute value
     * @return
     * @throws AtlasException
     */
    String getEntityDefinition(String entityType, String attribute, String value) throws AtlasException;

    /**
     * Return the list of entity names for the given type in the repository.
     *
     * @param entityType type
     * @return list of entity names for the given type in the repository
     */
    List<String> getEntityList(String entityType) throws AtlasException;

    /**
     * Adds the property to the given entity id(guid).
     * Currently supports updates only on PRIMITIVE, CLASS attribute types
     *  @param guid entity id
     * @param attribute property name
     * @param value    property value
     * @return {@link CreateUpdateEntitiesResult} with the guids of the entities that were created/updated
     */
    CreateUpdateEntitiesResult updateEntityAttributeByGuid(String guid, String attribute, String value) throws AtlasException;

    /**
     * Supports Partial updates of an entity. Users can update a subset of attributes for an entity identified by its guid
     * Note however that it cannot be used to set attribute values to null or delete attrbute values
     * @param guid entity id
     * @param entity
     * @return {@link CreateUpdateEntitiesResult} with the guids of the entities that were created/updated
     * @throws AtlasException
     */
    CreateUpdateEntitiesResult updateEntityPartialByGuid(String guid, Referenceable entity) throws AtlasException;

    /**
     * Batch API - Adds/Updates the given entity id(guid).
     *
     * @param entityJson entity json
     * @return {@link CreateUpdateEntitiesResult} with the guids of the entities that were created/updated
     */
    CreateUpdateEntitiesResult updateEntities(String entityJson) throws AtlasException;


    /**
     * Batch API - Adds/Updates the given entity id(guid).
     *
     * @param entityJson entity json
     * @return {@link CreateUpdateEntitiesResult} with the guids of the entities that were created/updated
     */
    CreateUpdateEntitiesResult updateEntities(ITypedReferenceableInstance[] iTypedReferenceableInstances) throws AtlasException;

    // Trait management functions

    /**
     * Updates entity identified by a qualified name
     *
     * @param typeName
     * @param uniqueAttributeName
     * @param attrValue
     * @param updatedEntity
     * @return Guid of updated entity
     * @throws AtlasException
     */
    CreateUpdateEntitiesResult updateEntityByUniqueAttribute(String typeName, String uniqueAttributeName,
                                                           String attrValue,
                                                           Referenceable updatedEntity) throws AtlasException;

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     * @throws AtlasException
     */
    List<String> getTraitNames(String guid) throws AtlasException;

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid          globally unique identifier for the entity
     * @param traitInstanceDefinition trait instance that needs to be added to entity
     * @throws AtlasException
     */
    void addTrait(String guid, String traitInstanceDefinition) throws AtlasException;

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid           globally unique identifier for the entity
     * @param traitInstance  trait instance to add     *
     * @throws AtlasException if unable to add the trait instance
     */
    void addTrait(String guid, ITypedStruct traitInstance) throws AtlasException;


    /**
     * Adds a new trait to a list of existing entities represented by their respective guids
     * @param entityGuids   list of guids of entities
     * @param traitInstance trait instance json that needs to be added to entities
     * @throws AtlasException
     */
    void addTrait(List<String> entityGuids, ITypedStruct traitInstance) throws AtlasException;

    /**
     * Create a typed trait instance.
     *
     * @param traitInstance  trait instance
     * @return a typed trait instance
     * @throws AtlasException if unable to create the typed trait instance
     */
    ITypedStruct createTraitInstance(Struct traitInstance) throws AtlasException;

    /**
     * Return trait definition of a single trait for a given entity
     * @param guid - Guid of the entity to which the trait is tagged
     * @param traitName - Name of the trait
     * @return
     * @throws AtlasException
     */
    IStruct getTraitDefinition(String guid, String traitName) throws AtlasException;

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid                 globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws AtlasException
     */
    void deleteTrait(String guid, String traitNameToBeDeleted) throws AtlasException;

    /**
     * Delete the specified entities from the repository
     * 
     * @param guids entity guids to be deleted
     * @return List of guids for deleted entities
     * @throws AtlasException
     */
    EntityResult deleteEntities(List<String> guids) throws AtlasException;
    
    /**
     * Register a listener for entity change.
     *
     * @param listener  the listener to register
     */
    void registerListener(EntityChangeListener listener);

    /**
     * Unregister an entity change listener.
     *
     * @param listener  the listener to unregister
     */
    void unregisterListener(EntityChangeListener listener);

    /**
     * Delete the specified entity from the repository identified by its unique attribute (including its composite references)
     *
     * @param typeName The entity's type
     * @param uniqueAttributeName attribute name by which the entity could be identified uniquely
     * @param attrValue attribute value by which the entity could be identified uniquely
     * @return List of guids for deleted entities (including their composite references)
     * @throws AtlasException
     */
    EntityResult deleteEntityByUniqueAttribute(String typeName, String uniqueAttributeName,
                                               String attrValue) throws AtlasException;

    /**
     * Returns entity audit events for entity id in the decreasing order of timestamp
     * @param guid entity id
     * @param startKey key for the first event, used for pagination
     * @param count number of events to be returned
     * @return
     */
    List<EntityAuditEvent> getAuditEvents(String guid, String startKey, short count) throws AtlasException;

    /**
     * Deserializes entity instances into ITypedReferenceableInstance array.
     * @param entityInstanceDefinition
     * @return ITypedReferenceableInstance[]
     * @throws AtlasException
     */
    ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition) throws AtlasException;

    ITypedReferenceableInstance validateAndConvertToTypedInstance(IReferenceableInstance updatedEntity, String typeName)
                                                                                                throws AtlasException;
}
