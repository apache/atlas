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

package org.apache.atlas.repository;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.CreateUpdateEntitiesResult;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.IDataType;

import java.util.List;

/**
 * An interface for persisting metadata into a blueprints enabled graph db.
 */
public interface MetadataRepository {

    /**
     * Returns the property key used to store entity type name.
     *
     * @return property key used to store entity type name.
     */
    String getTypeAttributeName();

    /**
     * Returns the property key used to store super type names.
     *
     * @return property key used to store super type names.
     */
    String getSuperTypeAttributeName();

    /**
     * Returns the attribute name used for entity state
     * @return
     */
    String getStateAttributeName();
    /**
     * Returns the attribute name used for entity version
     * @return
     */
    String getVersionAttributeName();

    /**
     * Return the property key used to store a given traitName in the repository.
     *
     * @param dataType  data type
     * @param traitName trait name
     * @return property key used to store a given traitName
     */
    String getTraitLabel(IDataType<?> dataType, String traitName);

    /**
     * Return the property key used to store a given attribute in the repository.
     *
     * @param dataType data type
     * @param aInfo    attribute info
     * @return property key used to store a given attribute
     */
    String getFieldNameInVertex(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException;

    /**
     * Return the edge label for a given attribute in the repository.
     *
     * @param dataType  data type
     * @param aInfo    attribute info
     * @return edge label for a given attribute
     */
    String getEdgeLabel(IDataType<?> dataType, AttributeInfo aInfo) throws AtlasException;

    /**
     * Creates an entity definition (instance) corresponding to a given type.
     *
     * @param entities     entity (typed instance)
     * @return CreateOrUpdateEntitiesResult with the guids of the entities that were created
     * @throws RepositoryException
     * @throws EntityExistsException
     */
    CreateUpdateEntitiesResult createEntities(ITypedReferenceableInstance... entities) throws RepositoryException, EntityExistsException;

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid globally unique identifier for the entity
     * @return entity (typed instance) definition
     * @throws RepositoryException
     * @throws EntityNotFoundException
     */
    ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException, EntityNotFoundException;

    /**
     * Fetch the complete entity definitions for the entities with the given GUIDs
     *
     * @param guids globally unique identifiers for the entities
     * @return entity (typed instance) definitions list
     * @throws RepositoryException
     * @throws EntityNotFoundException
     */
    List<ITypedReferenceableInstance> getEntityDefinitions(String... guids) throws RepositoryException, EntityNotFoundException;

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     * @return a list of entity names for the given type
     * @throws RepositoryException
     */
    List<String> getEntityList(String entityType) throws RepositoryException;

    /**
     * Deletes entities for the specified guids.
     *
     * @param guids globally unique identifiers for the deletion candidate entities
     * @return guids of deleted entities
     * @throws RepositoryException
     */
    AtlasClient.EntityResult deleteEntities(List<String> guids) throws RepositoryException;
    
    
    // Trait management functions

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     * @throws RepositoryException
     */
    List<String> getTraitNames(String guid) throws AtlasException;

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid          globally unique identifier for the entity
     * @param traitInstance trait instance that needs to be added to entity
     * @throws RepositoryException
     */
    void addTrait(String guid, ITypedStruct traitInstance) throws RepositoryException;

    /**
     * Adds a new trait to a list of entities represented by their respective guids
     * @param entityGuids   list of globally unique identifier for the entities
     * @param traitInstance trait instance that needs to be added to entities
     * @throws RepositoryException
     */
    void addTrait(List<String> entityGuids, ITypedStruct traitInstance) throws RepositoryException;

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid                 globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws RepositoryException
     */
    void deleteTrait(String guid, String traitNameToBeDeleted) throws TraitNotFoundException, EntityNotFoundException, RepositoryException;

    /**
     * Adds/Updates the property to the entity that corresponds to the GUID
     * Supports only primitive attribute/Class Id updations.
     */
    CreateUpdateEntitiesResult updatePartial(ITypedReferenceableInstance entity) throws RepositoryException;

    /**
     * Adds the property to the entity that corresponds to the GUID
     * @param entitiesToBeUpdated The entities to be updated
     */
    CreateUpdateEntitiesResult updateEntities(ITypedReferenceableInstance... entitiesToBeUpdated) throws RepositoryException;

    /**
     * Returns the entity for the given type and qualified name
     * @param entityType
     * @param attribute
     * @param value
     * @return entity instance
     */
    ITypedReferenceableInstance getEntityDefinition(String entityType, String attribute, Object value) throws AtlasException;
}
