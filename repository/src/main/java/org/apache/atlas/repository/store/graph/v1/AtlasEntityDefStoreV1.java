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
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityDefStore;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * EntityDef store in v1 format.
 */
public class AtlasEntityDefStoreV1 implements AtlasEntityDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityDefStoreV1.class);

    private final AtlasTypeDefGraphStoreV1 typeDefStore;

    public AtlasEntityDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore) {
        super();

        this.typeDefStore = typeDefStore;
    }

    @Override
    public AtlasEntityDef create(AtlasEntityDef entityDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.create({})", entityDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByName(entityDef.getName());

        if (vertex != null) {
            throw new AtlasBaseException(entityDef.getName() + ": type already exists");
        }

        vertex = typeDefStore.createTypeVertex(entityDef);

        toVertex(entityDef, vertex);

        AtlasEntityDef ret = toEntityDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.create({}): {}", entityDef, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasEntityDef> create(List<AtlasEntityDef> entityDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.create({})", entityDefs);
        }
        List<AtlasEntityDef> entityDefList = new LinkedList<>();
        for (AtlasEntityDef structDef : entityDefs) {
            try {
                AtlasEntityDef atlasEntityDef = create(structDef);
                entityDefList.add(atlasEntityDef);
            } catch (AtlasBaseException baseException) {
                LOG.error("Failed to create {}", structDef);
                LOG.error("Exception: {}", baseException);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.create({}, {})", entityDefs, entityDefList);
        }
        return entityDefList;
    }

    @Override
    public List<AtlasEntityDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.getAll()");
        }

        List<AtlasEntityDef> entityDefs = new LinkedList<>();
        Iterator<AtlasVertex> verticesByCategory = typeDefStore.findTypeVerticesByCategory(TypeCategory.CLASS);
        while (verticesByCategory.hasNext()) {
            AtlasEntityDef atlasEntityDef = toEntityDef(verticesByCategory.next());
            entityDefs.add(atlasEntityDef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.getAll()");
        }
        return entityDefs;
    }

    @Override
    public AtlasEntityDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with name " + name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);

        AtlasEntityDef ret = toEntityDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasEntityDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with guid " + guid);
        }

        AtlasEntityDef ret = toEntityDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasEntityDef updateByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.updateByName({}, {})", name, entityDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with name " + name);
        }

        toVertex(entityDef, vertex);

        AtlasEntityDef ret = toEntityDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.updateByName({}, {}): {}", name, entityDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasEntityDef updateByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.updateByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with guid " + guid);
        }

        toVertex(entityDef, vertex);

        AtlasEntityDef ret = toEntityDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasEntityDef> update(List<AtlasEntityDef> entityDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.update({})", entityDefs);
        }

        List<AtlasEntityDef> updatedEntityDefs = new ArrayList<>();

        for (AtlasEntityDef entityDef : entityDefs) {
            try {
                AtlasEntityDef atlasEntityDef = updateByName(entityDef.getName(), entityDef);
                updatedEntityDefs.add(atlasEntityDef);
            } catch (AtlasBaseException ex) {
                LOG.error("Failed to update {}", entityDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.update({}): {}", entityDefs, updatedEntityDefs);
        }

        return updatedEntityDefs;
    }

    @Override
    public void deleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.deleteByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with name " + name);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.deleteByName({})", name);
        }
    }

    @Override
    public void deleteByNames(List<String> names) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.deleteByNames({})", names);
        }

        List<AtlasStructDef> updatedDefs = new ArrayList<>();

        for (String name : names) {
            try {
                deleteByName(name);
            } catch (AtlasBaseException ex) {}
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.deleteByNames({})", names);
        }
    }

    @Override
    public void deleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.deleteByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.CLASS);

        if (vertex == null) {
            throw new AtlasBaseException("no entityDef exists with guid " + guid);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.deleteByGuid({})", guid);
        }
    }

    @Override
    public void deleteByGuids(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.deleteByGuids({})", guids);
        }

        List<AtlasStructDef> updatedDefs = new ArrayList<>();

        for (String guid : guids) {
            try {
                deleteByGuid(guid);
            } catch (AtlasBaseException ex) {}
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.deleteByGuids({})", guids);
        }
    }

    @Override
    public AtlasEntityDefs search(SearchFilter filter) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.search({})", filter);
        }

        List<AtlasEntityDef> entityDefs = new ArrayList<AtlasEntityDef>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.CLASS);

        while(vertices.hasNext()) {
            AtlasVertex       vertex  = vertices.next();
            AtlasEntityDef entityDef = toEntityDef(vertex);

            if (entityDef != null) {
                entityDefs.add(entityDef);
            }
        }

        if (CollectionUtils.isNotEmpty(entityDefs)) {
            CollectionUtils.filter(entityDefs, FilterUtil.getPredicateFromSearchFilter(filter));
        }


        AtlasEntityDefs ret = new AtlasEntityDefs(entityDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private void toVertex(AtlasEntityDef entityDef, AtlasVertex vertex) {
        AtlasStructDefStoreV1.toVertex(entityDef, vertex, typeDefStore);

        typeDefStore.createSuperTypeEdges(vertex, entityDef.getSuperTypes());
    }

    private AtlasEntityDef toEntityDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntityDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.CLASS)) {
            ret = new AtlasEntityDef();

            AtlasStructDefStoreV1.toStructDef(vertex, ret, typeDefStore);

            ret.setSuperTypes(typeDefStore.getSuperTypeNames(vertex));
        }

        return ret;
    }
}
