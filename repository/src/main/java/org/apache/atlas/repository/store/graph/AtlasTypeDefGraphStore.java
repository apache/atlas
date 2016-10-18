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
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Abstract class for graph persistence store for TypeDef
 */
public abstract class AtlasTypeDefGraphStore implements AtlasTypeDefStore {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefGraphStore.class);

    private final AtlasTypeRegistry typeRegistry;

    protected AtlasTypeDefGraphStore(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    protected abstract AtlasEnumDefStore getEnumDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasStructDefStore getStructDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasClassificationDefStore getClassificationDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasEntityDefStore getEntityDefStore(AtlasTypeRegistry typeRegistry);

    @Override
    public void init() throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        AtlasTypesDef typesDef = new AtlasTypesDef(getEnumDefStore(ttr).getAll(),
                                                   getStructDefStore(ttr).getAll(),
                                                   getClassificationDefStore(ttr).getAll(),
                                                   getEntityDefStore(ttr).getAll());

        ttr.addTypes(typesDef);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addType(enumDef);

        AtlasEnumDef ret = getEnumDefStore(ttr).create(enumDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> createEnumDefs(List<AtlasEnumDef> enumDefs) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addTypes(enumDefs);

        List<AtlasEnumDef> ret = getEnumDefStore(ttr).create(enumDefs);

        typeRegistry.commitTransientTypeRegistry(ttr);

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
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByName(name, enumDef);

        AtlasEnumDef ret = getEnumDefStore(ttr).updateByName(name, enumDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByGuid(guid, enumDef);

        AtlasEnumDef ret = getEnumDefStore(ttr).updateByGuid(guid, enumDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByName(name);

        getEnumDefStore(ttr).deleteByName(name);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByGuid(guid);

        getEnumDefStore(ttr).deleteByGuid(guid);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException {
        return getEnumDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addType(structDef);

        AtlasStructDef ret = getStructDefStore(ttr).create(structDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> createStructDefs(List<AtlasStructDef> structDefs) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addTypes(structDefs);

        List<AtlasStructDef> ret = getStructDefStore(ttr).create(structDefs);

        typeRegistry.commitTransientTypeRegistry(ttr);

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
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByName(name, structDef);

        AtlasStructDef ret = getStructDefStore(ttr).updateByName(name, structDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByGuid(guid, structDef);

        AtlasStructDef ret = getStructDefStore(ttr).updateByGuid(guid, structDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByName(name);

        getStructDefStore(ttr).deleteByName(name);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByGuid(guid);

        getStructDefStore(ttr).deleteByGuid(guid);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException {
        return getStructDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addType(classificationDef);

        AtlasClassificationDef ret = getClassificationDefStore(ttr).create(classificationDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> createClassificationDefs(List<AtlasClassificationDef> classificationDefs)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addTypes(classificationDefs);

        List<AtlasClassificationDef> ret = getClassificationDefStore(ttr).create(classificationDefs);

        typeRegistry.commitTransientTypeRegistry(ttr);

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
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByName(name, classificationDef);

        AtlasClassificationDef ret = getClassificationDefStore(ttr).updateByName(name, classificationDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByGuid(guid, classificationDef);

        AtlasClassificationDef ret = getClassificationDefStore(ttr).updateByGuid(guid, classificationDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByName(name);

        getClassificationDefStore(ttr).deleteByName(name);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByGuid(guid);

        getClassificationDefStore(ttr).deleteByGuid(guid);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException {
        return getClassificationDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addType(entityDef);

        AtlasEntityDef ret = getEntityDefStore(ttr).create(entityDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> createEntityDefs(List<AtlasEntityDef> entityDefs) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addTypes(entityDefs);

        List<AtlasEntityDef> ret = getEntityDefStore(ttr).create(entityDefs);

        typeRegistry.commitTransientTypeRegistry(ttr);

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
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByName(name, entityDef);

        AtlasEntityDef ret = getEntityDefStore(ttr).updateByName(name, entityDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypeByGuid(guid, entityDef);

        AtlasEntityDef ret = getEntityDefStore(ttr).updateByGuid(guid, entityDef);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByName(name);

        getEntityDefStore(ttr).deleteByName(name);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.removeTypeByGuid(guid);

        getEntityDefStore(ttr).deleteByGuid(guid);

        typeRegistry.commitTransientTypeRegistry(ttr);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException {
        return getEntityDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef createTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        LOG.info("Creating EnumDefs");
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.addTypes(typesDef);

        List<AtlasEnumDef>           enumDefs     = getEnumDefStore(ttr).create(typesDef.getEnumDefs());
        List<AtlasStructDef>         structDefs   = getStructDefStore(ttr).create(typesDef.getStructDefs());
        List<AtlasClassificationDef> classifiDefs = getClassificationDefStore(ttr).create(typesDef.getClassificationDefs());
        List<AtlasEntityDef>         entityDefs   = getEntityDefStore(ttr).create(typesDef.getEntityDefs());

        AtlasTypesDef ret = new AtlasTypesDef(enumDefs, structDefs, classifiDefs, entityDefs);

        typeRegistry.commitTransientTypeRegistry(ttr);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef updateTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        LOG.info("Updating EnumDefs");
        AtlasTransientTypeRegistry ttr = typeRegistry.createTransientTypeRegistry();

        ttr.updateTypes(typesDef);

        List<AtlasEnumDef>           enumDefs     = getEnumDefStore(ttr).update(typesDef.getEnumDefs());
        List<AtlasStructDef>         structDefs   = getStructDefStore(ttr).update(typesDef.getStructDefs());
        List<AtlasClassificationDef> classifiDefs = getClassificationDefStore(ttr).update(typesDef.getClassificationDefs());
        List<AtlasEntityDef>         entityDefs   = getEntityDefStore(ttr).update(typesDef.getEntityDefs());

        typeRegistry.commitTransientTypeRegistry(ttr);

        AtlasTypesDef ret = new AtlasTypesDef(enumDefs, structDefs, classifiDefs, entityDefs);

        return ret;

    }

    @Override
    @GraphTransaction
    public AtlasTypesDef searchTypesDef(SearchFilter searchFilter) throws AtlasBaseException {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        Predicate searchPredicates = FilterUtil.getPredicateFromSearchFilter(searchFilter);
        try {
            List<AtlasEnumDef> enumDefs = getEnumDefStore(typeRegistry).getAll();
            CollectionUtils.filter(enumDefs, searchPredicates);
            typesDef.setEnumDefs(enumDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EnumDefs", ex);
        }

        try {
            List<AtlasStructDef> structDefs = getStructDefStore(typeRegistry).getAll();
            CollectionUtils.filter(structDefs, searchPredicates);
            typesDef.setStructDefs(structDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the StructDefs", ex);
        }

        try {
            List<AtlasClassificationDef> classificationDefs = getClassificationDefStore(typeRegistry).getAll();
            CollectionUtils.filter(classificationDefs, searchPredicates);
            typesDef.setClassificationDefs(classificationDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the ClassificationDefs", ex);
        }

        try {
            List<AtlasEntityDef> entityDefs = getEntityDefStore(typeRegistry).getAll();
            CollectionUtils.filter(entityDefs, searchPredicates);
            typesDef.setEntityDefs(entityDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EntityDefs", ex);
        }

        return typesDef;
    }
}
