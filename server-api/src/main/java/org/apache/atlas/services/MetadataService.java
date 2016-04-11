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
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.DataTypes;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

/**
 * Metadata service.
 */
public interface MetadataService {

    /**
     * Creates a new type based on the type system to enable adding
     * entities (instances for types).
     *
     * @param typeDefinition definition as json
     * @return a unique id for this type
     */
    JSONObject createType(String typeDefinition) throws AtlasException;

    /**
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
     * Return the list of types in the type system.
     *
     * @return list of type names in the type system
     */
    List<String> getTypeNamesList() throws AtlasException;

    /**
     * Return the list of trait type names in the type system.
     *
     * @return list of trait type names in the type system
     */
    List<String> getTypeNamesByCategory(DataTypes.TypeCategory typeCategory) throws AtlasException;

    /**
     * Creates an entity, instance of the type.
     *
     * @param entityDefinition definition
     * @return json array of guids of entities created
     */
    String createEntities(String entityDefinition) throws AtlasException;

    /**
     * Return the definition for the given guid.
     *
     * @param guid guid
     * @return entity definition as JSON
     */
    String getEntityDefinition(String guid) throws AtlasException;

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
     * @return json array of guids of entities created/updated
     */
    String updateEntityAttributeByGuid(String guid, String attribute, String value) throws AtlasException;

    /**
     * Supports Partial updates of an entity. Users can update a subset of attributes for an entity identified by its guid
     * Note however that it cannot be used to set attribute values to null or delete attrbute values
     * @param guid entity id
     * @param entity
     * @return json array of guids of entities created/updated
     * @throws AtlasException
     */
    String updateEntityPartialByGuid(String guid, Referenceable entity) throws AtlasException;

    /**
     * Batch API - Adds/Updates the given entity id(guid).
     *
     * @param entityJson entity json
     * @return json array of guids of entities created/updated
     */
    String updateEntities(String entityJson) throws AtlasException;

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
    String updateEntityByUniqueAttribute(String typeName, String uniqueAttributeName, String attrValue,
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
    List<String> deleteEntities(List<String> guids) throws AtlasException;
    
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
    List<String> deleteEntityByUniqueAttribute(String typeName, String uniqueAttributeName, String attrValue) throws AtlasException;

    /**
     * Returns entity audit events for entity id in the decreasing order of timestamp
     * @param guid entity id
     * @param startKey key for the first event, used for pagination
     * @param count number of events to be returned
     * @return
     */
    List<EntityAuditEvent> getAuditEvents(String guid, String startKey, short count) throws AtlasException;
}
