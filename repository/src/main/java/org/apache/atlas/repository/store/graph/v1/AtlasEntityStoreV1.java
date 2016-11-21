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
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;

import java.util.List;

public class AtlasEntityStoreV1 implements AtlasEntityStore {
    @Override
    public void init() throws AtlasBaseException {
    }

    @Override
    public EntityMutationResponse createOrUpdate(final AtlasEntity entity) {
        return null;
    }

    @Override
    public EntityMutationResponse updateById(final String guid, final AtlasEntity entity) {
        return null;
    }

    @Override
    public AtlasEntity getById(final String guid) {
        return null;
    }

    @Override
    public EntityMutationResponse deleteById(final String guid) {
        return null;
    }

    @Override
    public EntityMutationResponse createOrUpdate(final List<AtlasEntity> entities) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse updateByIds(final String guid, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity.AtlasEntities getByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntityWithAssociations getWithAssociationsByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity getByUniqueAttribute(final String typeName, final String attrName, final String attrValue) {
        return null;
    }

    @Override
    public EntityMutationResponse updateByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse batchMutate(final EntityMutations mutations) throws AtlasBaseException {
        return null;
    }


    @Override
    public void addClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void updateClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void deleteClassifications(final String guid, final List<String> classificationNames) throws AtlasBaseException {

    }

    @Override
    public AtlasEntity.AtlasEntities searchEntities(final SearchFilter searchFilter) throws AtlasBaseException {
        // TODO: Add checks here to ensure that typename and supertype are mandatory in the requests
        return null;
    }
}
