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
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.TypeDefSorter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Abstract class for graph persistence store for TypeDef
 */
public abstract class AtlasTypeDefGraphStore implements AtlasTypeDefStore {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefGraphStore.class);

    private AtlasTypeRegistry           typeRegistry;
    private AtlasEnumDefStore           enumDefStore;
    private AtlasStructDefStore         structDefStore;
    private AtlasClassificationDefStore classificationDefStore;
    private AtlasEntityDefStore         entityDefStore;

    protected AtlasTypeDefGraphStore() {
    }

    protected void init(AtlasEnumDefStore           enumDefStore,
                        AtlasStructDefStore         structDefStore,
                        AtlasClassificationDefStore classificationDefStore,
                        AtlasEntityDefStore         entityDefStore) throws AtlasBaseException {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        typeRegistry.addTypesWithNoRefResolve(enumDefStore.getAll());
        typeRegistry.addTypesWithNoRefResolve(structDefStore.getAll());
        typeRegistry.addTypesWithNoRefResolve(classificationDefStore.getAll());
        typeRegistry.addTypesWithNoRefResolve(entityDefStore.getAll());

        typeRegistry.resolveReferences();

        this.enumDefStore           = enumDefStore;
        this.structDefStore         = structDefStore;
        this.classificationDefStore = classificationDefStore;
        this.entityDefStore         = entityDefStore;
        this.typeRegistry           = typeRegistry;
    }

    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    @Override
    public void init() throws AtlasBaseException {

    }

    @Override
    @GraphTransaction
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasEnumDef ret = enumDefStore.create(enumDef);

        typeRegistry.addType(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> createEnumDefs(List<AtlasEnumDef> atlasEnumDefs) throws AtlasBaseException {
        List<AtlasEnumDef> ret = enumDefStore.create(atlasEnumDefs);

        typeRegistry.addTypes(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> getAllEnumDefs() throws AtlasBaseException {
        List<AtlasEnumDef> ret = null;

        Collection<AtlasEnumDef> enumDefs = typeRegistry.getAllEnumDefs();

        if (enumDefs != null) {
            ret = new ArrayList<>(enumDefs);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByName(String name) throws AtlasBaseException {
        AtlasEnumDef ret = typeRegistry.getEnumDefByName(name);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByGuid(String guid) throws AtlasBaseException {
        AtlasEnumDef ret = typeRegistry.getEnumDefByGuid(guid);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasEnumDef ret = enumDefStore.updateByName(name, enumDef);

        typeRegistry.updateTypeByName(name, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasEnumDef ret = enumDefStore.updateByGuid(guid, enumDef);

        typeRegistry.updateTypeByGuid(guid, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByName(String name) throws AtlasBaseException {
        enumDefStore.deleteByName(name);

        typeRegistry.removeTypeByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByGuid(String guid) throws AtlasBaseException {
        enumDefStore.deleteByGuid(guid);

        typeRegistry.removeTypeByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException {
        return enumDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException {
        AtlasStructDef ret = structDefStore.create(structDef);

        typeRegistry.addType(structDef);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> createStructDefs(List<AtlasStructDef> structDefs) throws AtlasBaseException {
        List<AtlasStructDef> ret = structDefStore.create(structDefs);

        typeRegistry.addTypes(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> getAllStructDefs() throws AtlasBaseException {
        List<AtlasStructDef> ret = null;

        Collection<AtlasStructDef> structDefs = typeRegistry.getAllStructDefs();

        if (structDefs != null) {
            ret = new ArrayList<>(structDefs);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByName(String name) throws AtlasBaseException {
        AtlasStructDef ret = typeRegistry.getStructDefByName(name);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByGuid(String guid) throws AtlasBaseException {
        AtlasStructDef ret = typeRegistry.getStructDefByGuid(guid);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByName(String name, AtlasStructDef structDef) throws AtlasBaseException {
        AtlasStructDef ret = structDefStore.updateByName(name, structDef);

        typeRegistry.updateTypeByName(name, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException {
        AtlasStructDef ret = structDefStore.updateByGuid(guid, structDef);

        typeRegistry.updateTypeByGuid(guid, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByName(String name) throws AtlasBaseException {
        structDefStore.deleteByName(name);

        typeRegistry.removeTypeByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByGuid(String guid) throws AtlasBaseException {
        structDefStore.deleteByGuid(guid);

        typeRegistry.removeTypeByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException {
        return structDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasClassificationDef ret = classificationDefStore.create(classificationDef);

        typeRegistry.addType(classificationDef);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> createClassificationDefs(List<AtlasClassificationDef> classificationDefs)
        throws AtlasBaseException {
        List<AtlasClassificationDef> ret = classificationDefStore.create(classificationDefs);

        typeRegistry.addTypes(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> getAllClassificationDefs() throws AtlasBaseException {
        List<AtlasClassificationDef> ret = null;

        Collection<AtlasClassificationDef> classificationDefs = typeRegistry.getAllClassificationDefs();

        if (classificationDefs != null) {
            ret = new ArrayList<>(classificationDefs);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByName(String name) throws AtlasBaseException {
        AtlasClassificationDef ret = typeRegistry.getClassificationDefByName(name);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByGuid(String guid) throws AtlasBaseException {
        AtlasClassificationDef ret = typeRegistry.getClassificationDefByGuid(guid);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByName(String name, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasClassificationDef ret = classificationDefStore.updateByName(name, classificationDef);

        typeRegistry.updateTypeByName(name, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasClassificationDef ret = classificationDefStore.updateByGuid(guid, classificationDef);

        typeRegistry.updateTypeByGuid(guid, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByName(String name) throws AtlasBaseException {
        classificationDefStore.deleteByName(name);

        typeRegistry.removeTypeByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByGuid(String guid) throws AtlasBaseException {
        classificationDefStore.deleteByGuid(guid);

        typeRegistry.removeTypeByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException {
        return classificationDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasEntityDef ret = entityDefStore.create(entityDef);

        typeRegistry.addType(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> createEntityDefs(List<AtlasEntityDef> entityDefs) throws AtlasBaseException {
        List<AtlasEntityDef> ret = entityDefStore.create(entityDefs);

        typeRegistry.addTypes(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> getAllEntityDefs() throws AtlasBaseException {
        List<AtlasEntityDef> ret = null;

        Collection<AtlasEntityDef> entityDefs = typeRegistry.getAllEntityDefs();

        if (entityDefs != null) {
            ret = new ArrayList<>(entityDefs);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByName(String name) throws AtlasBaseException {
        AtlasEntityDef ret = typeRegistry.getEntityDefByName(name);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByGuid(String guid) throws AtlasBaseException {
        AtlasEntityDef ret = typeRegistry.getEntityDefByGuid(guid);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasEntityDef ret = entityDefStore.updateByName(name, entityDef);

        typeRegistry.updateTypeByName(name, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasEntityDef ret = entityDefStore.updateByGuid(guid, entityDef);

        typeRegistry.updateTypeByGuid(guid, ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByName(String name) throws AtlasBaseException {
        entityDefStore.deleteByName(name);

        typeRegistry.removeTypeByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByGuid(String guid) throws AtlasBaseException {
        entityDefStore.deleteByGuid(guid);

        typeRegistry.removeTypeByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException {
        return entityDefStore.search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef createTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        LOG.info("Creating EnumDefs");
        List<AtlasEnumDef>           enumDefs     = enumDefStore.create(typesDef.getEnumDefs());
        List<AtlasStructDef>         structDefs   = structDefStore.create(typesDef.getStructDefs());
        List<AtlasClassificationDef> classifiDefs = classificationDefStore.create(typesDef.getClassificationDefs());
        List<AtlasEntityDef>         entityDefs   = entityDefStore.create(typesDef.getEntityDefs());

        // typeRegistry should be updated only after resovleReferences() returns success; until then use a temp registry
        typeRegistry.addTypes(enumDefs);
        typeRegistry.addTypes(structDefs);
        typeRegistry.addTypes(classifiDefs);
        typeRegistry.addTypes(entityDefs);

        typeRegistry.resolveReferences();

        AtlasTypesDef ret = new AtlasTypesDef(enumDefs, structDefs, classifiDefs, entityDefs);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef updateTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        LOG.info("Updating EnumDefs");
        List<AtlasEnumDef>           enumDefs     = enumDefStore.update(typesDef.getEnumDefs());
        List<AtlasStructDef>         structDefs   = structDefStore.update(typesDef.getStructDefs());
        List<AtlasClassificationDef> classifiDefs = classificationDefStore.update(typesDef.getClassificationDefs());
        List<AtlasEntityDef>         entityDefs   = entityDefStore.update(typesDef.getEntityDefs());

        // typeRegistry should be updated only after resovleReferences() returns success; until then use a temp registry
        typeRegistry.updateTypes(enumDefs);
        typeRegistry.updateTypes(structDefs);
        typeRegistry.updateTypes(classifiDefs);
        typeRegistry.updateTypes(entityDefs);

        typeRegistry.resolveReferences();

        AtlasTypesDef ret = new AtlasTypesDef(enumDefs, structDefs, classifiDefs, entityDefs);

        return ret;

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
