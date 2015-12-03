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
import org.apache.atlas.listener.EntityChangeListener;
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
     * @return guid
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
     *
     * @param guid entity id
     * @param property property name
     * @param value    property value
     */
    void updateEntity(String guid, String property, String value) throws AtlasException;

    // Trait management functions

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
}
