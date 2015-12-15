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

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeUtils;

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
     * @return a globally unique identifier
     * @throws RepositoryException
     * @throws EntityExistsException
     */
    List<String> createEntities(ITypedReferenceableInstance... entities) throws RepositoryException, EntityExistsException;

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid globally unique identifier for the entity
     * @return entity (typed instance) definition
     * @throws RepositoryException
     */
    ITypedReferenceableInstance getEntityDefinition(String guid) throws RepositoryException, EntityNotFoundException;

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     * @return a list of entity names for the given type
     * @throws RepositoryException
     */
    List<String> getEntityList(String entityType) throws RepositoryException;

    /**
     * Deletes an entity definition (instance) corresponding to a given type.
     *
     * @param guid globally unique identifier for the entity
     * @return true if deleted else false
     * @throws RepositoryException
     */
    // boolean deleteEntity(String guid) throws RepositoryException;
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
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid                 globally unique identifier for the entity
     * @param traitNameToBeDeleted name of the trait
     * @throws RepositoryException
     */
    void deleteTrait(String guid, String traitNameToBeDeleted) throws EntityNotFoundException, RepositoryException;

    /**
     * Adds/Updates the property to the entity that corresponds to the GUID
     * Supports only primitive attribute/Class Id updations.
     */
    TypeUtils.Pair<List<String>, List<String>> updatePartial(ITypedReferenceableInstance entity) throws RepositoryException;

    /**
     * Adds the property to the entity that corresponds to the GUID
     * @param entitiesToBeUpdated The entities to be updated
     */
    TypeUtils.Pair<List<String>, List<String>> updateEntities(ITypedReferenceableInstance... entitiesToBeUpdated) throws RepositoryException;

    /**
     * Returns the entity for the given type and qualified name
     * @param entityType
     * @param attribute
     * @param value
     * @return entity instance
     */
    ITypedReferenceableInstance getEntityDefinition(String entityType, String attribute, Object value) throws AtlasException;
}
