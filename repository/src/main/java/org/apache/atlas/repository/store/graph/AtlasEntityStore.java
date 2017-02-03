/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.List;
import java.util.Map;

/**
 * Persistence/Retrieval API for AtlasEntity
 */
public interface AtlasEntityStore {

    /**
     * Initialization
     */
    void init(AtlasTypeRegistry typeRegistry) throws AtlasBaseException;


    /**
     *
     * Get entity definition by its guid
     * @param guid
     * @return
     */
    AtlasEntity   getById(String guid);

    /**
     * Delete an entity by its guid
     * @param guid
     * @return
     */
    EntityMutationResponse deleteById(String guid);

    /**
     * Create or update  entities
     * @param entities Map of the entity Id(guid or transient Id) to AtlasEntity objects that need to be created
     * @return EntityMutationResponse Entity mutations operations with the correspomding set of entities on which these operations were performed
     * @throws AtlasBaseException
     */

    EntityMutationResponse createOrUpdate(Map<String, AtlasEntity> entities) throws AtlasBaseException;

    /**
     * Batch GET to retrieve entities by their ID
     * @param guid
     * @return
     * @throws AtlasBaseException
     */
    AtlasEntity.AtlasEntities getByIds(List<String> guid) throws AtlasBaseException;

    /**
     * Batch GET to retrieve entities and their associations by their ID
     * @param guid
     * @return
     * @throws AtlasBaseException
     */
    AtlasEntityWithAssociations getWithAssociationsByIds(List<String> guid) throws AtlasBaseException;

    /*
     * Return list of deleted entity guids
     */
    EntityMutationResponse deleteByIds(List<String> guid) throws AtlasBaseException;

    /**
     *
     * Get an eneity by its unique attribute
     * @param typeName
     * @param attrName
     * @param attrValue
     * @return
     */
    AtlasEntity  getByUniqueAttribute(String typeName, String attrName, String attrValue);

    /**
     * @deprecated
     * Create or update a single entity
     * @param typeName The entity's type
     * @param attributeName Attribute that uniquely identifies the entity
     * @param attributeValue The unqiue attribute's value
     * @return EntityMutationResponse Entity mutations operations with the correspomding set of entities on which these operations were performed
     * @throws AtlasBaseException
     *
     */

    EntityMutationResponse updateByUniqueAttribute(String typeName, String attributeName, String attributeValue, AtlasEntity entity) throws AtlasBaseException;

    /**
     * @deprecated
     * @param typeName
     * @param attributeName
     * @param attributeValue
     * @return
     * @throws AtlasBaseException
     */
    EntityMutationResponse deleteByUniqueAttribute(String typeName, String attributeName, String attributeValue) throws AtlasBaseException;

    /**
     * Add classification(s)
     */
    void addClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException;


    /**
     * Update classification(s)
     */
    void updateClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException;

    /**
     * Delete classification(s)
     */
    void deleteClassifications(String guid, List<String> classificationNames) throws AtlasBaseException;

}
