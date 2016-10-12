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
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumDefs;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.util.TypeDefSorter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;


/**
 * Abstract class for graph persistence store for TypeDef
 */
public abstract class AtlasTypeDefGraphStore implements AtlasTypeDefStore {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefGraphStore.class);

    protected AtlasEnumDefStore           enumDefStore;
    protected AtlasStructDefStore         structDefStore;
    protected AtlasClassificationDefStore classificationDefStore;
    protected AtlasEntityDefStore         entityDefStore;

    protected AtlasTypeDefGraphStore() {
    }

    @Override
    public void init() throws AtlasBaseException {

    }

    @Override
    @GraphTransaction
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException {
        return enumDefStore.create(enumDef);
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> createEnumDefs(List<AtlasEnumDef> atlasEnumDefs) throws AtlasBaseException {
        return enumDefStore.create(atlasEnumDefs);
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> getAllEnumDefs() throws AtlasBaseException {
        return enumDefStore.getAll();
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByName(String name) throws AtlasBaseException {
        return enumDefStore.getByName(name);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByGuid(String guid) throws AtlasBaseException {
        return enumDefStore.getByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException {
        return enumDefStore.updateByName(name, enumDef);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException {
        return enumDefStore.updateByGuid(guid, enumDef);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByName(String name) throws AtlasBaseException {
        enumDefStore.deleteByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByGuid(String guid) throws AtlasBaseException {
        enumDefStore.deleteByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException {
        return enumDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException {
        return structDefStore.create(structDef);
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> createStructDefs(List<AtlasStructDef> structDefs) throws AtlasBaseException {
        return structDefStore.create(structDefs);
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> getAllStructDefs() throws AtlasBaseException {
        return structDefStore.getAll();
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByName(String name) throws AtlasBaseException {
        return structDefStore.getByName(name);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByGuid(String guid) throws AtlasBaseException {
        return structDefStore.getByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByName(String name, AtlasStructDef structDef) throws AtlasBaseException {
        return structDefStore.updateByName(name, structDef);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException {
        return structDefStore.updateByGuid(guid, structDef);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByName(String name) throws AtlasBaseException {
        structDefStore.deleteByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByGuid(String guid) throws AtlasBaseException {
        structDefStore.deleteByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException {
        return structDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef) throws AtlasBaseException {
        return classificationDefStore.create(classificationDef);
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> createClassificationDefs(List<AtlasClassificationDef> classificationDefs) throws AtlasBaseException {
        return classificationDefStore.create(classificationDefs);
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> getAllClassificationDefs() throws AtlasBaseException {
        return classificationDefStore.getAll();
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByName(String name) throws AtlasBaseException {
        return classificationDefStore.getByName(name);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByGuid(String guid) throws AtlasBaseException {
        return classificationDefStore.getByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByName(String name, AtlasClassificationDef classificationDef) throws AtlasBaseException {
        return classificationDefStore.updateByName(name, classificationDef);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef) throws AtlasBaseException {
        return classificationDefStore.updateByGuid(guid, classificationDef);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByName(String name) throws AtlasBaseException {
        classificationDefStore.deleteByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByGuid(String guid) throws AtlasBaseException {
        classificationDefStore.deleteByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException {
        return classificationDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef createEntityDefs(AtlasEntityDef entityDef) throws AtlasBaseException {
        return entityDefStore.create(entityDef);
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> createEntityDefs(List<AtlasEntityDef> entityDefs) throws AtlasBaseException {
        return entityDefStore.create(entityDefs);
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> getAllEntityDefs() throws AtlasBaseException {
        return entityDefStore.getAll();
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByName(String name) throws AtlasBaseException {
        return entityDefStore.getByName(name);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByGuid(String guid) throws AtlasBaseException {
        return entityDefStore.getByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException {
        return entityDefStore.updateByName(name, entityDef);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException {
        return entityDefStore.updateByGuid(guid, entityDef);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByName(String name) throws AtlasBaseException {
        entityDefStore.deleteByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByGuid(String guid) throws AtlasBaseException {
        entityDefStore.deleteByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException {
        return entityDefStore.search(filter);
    }

    private List<? extends AtlasBaseTypeDef> createOrUpdateTypeDefs(List<? extends AtlasBaseTypeDef> typeDefs, boolean isUpdate) {
        List<AtlasBaseTypeDef> ret = Collections.emptyList();

        if (CollectionUtils.isNotEmpty(typeDefs)) {
            AtlasBaseTypeDef typeDef = typeDefs.get(0);
            if (LOG.isDebugEnabled()) {
                if (isUpdate) {
                    LOG.debug("Updating {} {}", typeDefs.size(), typeDef.getClass().getSimpleName());
                } else {
                    LOG.debug("Creating {} {}", typeDefs.size(), typeDef.getClass().getSimpleName());
                }
            }

            if (typeDef instanceof AtlasEntityDef) {
                List<AtlasEntityDef> entityDefs = TypeDefSorter.sortTypes((List<AtlasEntityDef>) typeDefs);
                try {
                    if (isUpdate) {
                        return entityDefStore.update((List<AtlasEntityDef>) typeDefs);
                    } else {
                        return entityDefStore.create((List<AtlasEntityDef>) typeDefs);
                    }
                } catch (AtlasBaseException ex) {
                    LOG.error("Failed to " + (isUpdate ? "update" : "create") + " EntityDefs", ex);
                }
            } else if (typeDef instanceof AtlasClassificationDef) {
                List<AtlasClassificationDef> classificationDefs =
                        TypeDefSorter.sortTypes((List<AtlasClassificationDef>) typeDefs);
                try {
                    if (isUpdate) {
                        return classificationDefStore.update((List<AtlasClassificationDef>) typeDefs);
                    } else {
                        return classificationDefStore.create((List<AtlasClassificationDef>) typeDefs);
                    }
                } catch (AtlasBaseException ex) {
                    LOG.error("Failed to " + (isUpdate ? "update" : "create") + " ClassificationDefs", ex);
                }

            } else if (typeDef instanceof AtlasStructDef) {
                try {
                    if (isUpdate) {
                        return structDefStore.update((List<AtlasStructDef>) typeDefs);
                    } else {
                        return structDefStore.create((List<AtlasStructDef>) typeDefs);
                    }
                } catch (AtlasBaseException ex) {
                    LOG.error("Failed to " + (isUpdate ? "update" : "create") + " StructDefs", ex);
                }
            } else if (typeDef instanceof AtlasEnumDef) {
                try {
                    if (isUpdate) {
                        return enumDefStore.update((List<AtlasEnumDef>) typeDefs);
                    } else {
                        return enumDefStore.create((List<AtlasEnumDef>) typeDefs);
                    }
                } catch (AtlasBaseException ex) {
                    LOG.error("Failed to " + (isUpdate ? "update" : "create") + " EnumDefs", ex);
                }
            }
        }
        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef createTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasTypesDef createdTypeDefs = new AtlasTypesDef();

        LOG.info("Creating EnumDefs");
        List<? extends AtlasBaseTypeDef> createdEnumDefs = createOrUpdateTypeDefs(typesDef.getEnumDefs(), false);
        LOG.info("Creating StructDefs");
        List<? extends AtlasBaseTypeDef> createdStructDefs = createOrUpdateTypeDefs(typesDef.getStructDefs(), false);
        LOG.info("Creating ClassificationDefs");
        List<? extends AtlasBaseTypeDef> createdClassificationDefs = createOrUpdateTypeDefs(typesDef.getClassificationDefs(), false);
        LOG.info("Creating EntityDefs");
        List<? extends AtlasBaseTypeDef> createdEntityDefs = createOrUpdateTypeDefs(typesDef.getEntityDefs(), false);

        typesDef.setEnumDefs((List<AtlasEnumDef>) createdEnumDefs);
        typesDef.setStructDefs((List<AtlasStructDef>) createdStructDefs);
        typesDef.setClassificationDefs((List<AtlasClassificationDef>) createdClassificationDefs);
        typesDef.setEntityDefs((List<AtlasEntityDef>) createdEntityDefs);

        return typesDef;
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef updateTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasTypesDef createdTypeDefs = new AtlasTypesDef();

        LOG.info("Updating EnumDefs");
        List<? extends AtlasBaseTypeDef> updatedEnumDefs = createOrUpdateTypeDefs(typesDef.getEnumDefs(), true);
        LOG.info("Updating StructDefs");
        List<? extends AtlasBaseTypeDef> updatedStructDefs = createOrUpdateTypeDefs(typesDef.getStructDefs(), true);
        LOG.info("Updating ClassificationDefs");
        List<? extends AtlasBaseTypeDef> updatedClassficationDefs = createOrUpdateTypeDefs(typesDef.getClassificationDefs(), true);
        LOG.info("Updating EntityDefs");
        List<? extends AtlasBaseTypeDef> updatedEntityDefs = createOrUpdateTypeDefs(typesDef.getEntityDefs(), true);

        typesDef.setEnumDefs((List<AtlasEnumDef>) updatedEnumDefs);
        typesDef.setStructDefs((List<AtlasStructDef>) updatedStructDefs);
        typesDef.setClassificationDefs((List<AtlasClassificationDef>) updatedClassficationDefs);
        typesDef.setEntityDefs((List<AtlasEntityDef>) updatedEntityDefs);

        return typesDef;

    }

    @Override
    @GraphTransaction
    public AtlasTypesDef searchTypesDef(SearchFilter searchFilter) throws AtlasBaseException {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        Predicate searchPredicates = FilterUtil.getPredicateFromSearchFilter(searchFilter);
        try {
            List<AtlasEnumDef> enumDefs = enumDefStore.getAll();
            CollectionUtils.filter(enumDefs, searchPredicates);
            typesDef.setEnumDefs(enumDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EnumDefs", ex);
        }

        try {
            List<AtlasStructDef> structDefs = structDefStore.getAll();
            CollectionUtils.filter(structDefs, searchPredicates);
            typesDef.setStructDefs(structDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the StructDefs", ex);
        }

        try {
            List<AtlasClassificationDef> classificationDefs = classificationDefStore.getAll();
            CollectionUtils.filter(classificationDefs, searchPredicates);
            typesDef.setClassificationDefs(classificationDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the ClassificationDefs", ex);
        }

        try {
            List<AtlasEntityDef> entityDefs = entityDefStore.getAll();
            CollectionUtils.filter(entityDefs, searchPredicates);
            typesDef.setEntityDefs(entityDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EntityDefs", ex);
        }

        return typesDef;
    }
}
