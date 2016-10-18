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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityDefStore;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * EntityDef store in v1 format.
 */
public class AtlasEntityDefStoreV1 extends AtlasAbstractDefStoreV1 implements AtlasEntityDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityDefStoreV1.class);

    public AtlasEntityDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
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
        List<AtlasEntityDef> ret = new ArrayList<>();

        for (AtlasEntityDef entityDef : entityDefs) {
            ret.add(create(entityDef));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.create({}, {})", entityDefs, ret);
        }
        return ret;
    }

    @Override
    public List<AtlasEntityDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityDefStoreV1.getAll()");
        }

        List<AtlasEntityDef>  ret      = new ArrayList<>();
        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.CLASS);

        while (vertices.hasNext()) {
            ret.add(toEntityDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.getAll(): count={}", ret.size());
        }

        return ret;
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

        List<AtlasEntityDef> ret = new ArrayList<>();

        for (AtlasEntityDef entityDef : entityDefs) {
            ret.add(updateByName(entityDef.getName(), entityDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.update({}): {}", entityDefs, ret);
        }

        return ret;
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

        for (String name : names) {
            deleteByName(name);
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

        for (String guid : guids) {
            deleteByGuid(guid);
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

        List<AtlasEntityDef>  entityDefs = new ArrayList<AtlasEntityDef>();
        Iterator<AtlasVertex> vertices   = typeDefStore.findTypeVerticesByCategory(TypeCategory.CLASS);

        while(vertices.hasNext()) {
            AtlasVertex    vertex    = vertices.next();
            AtlasEntityDef entityDef = toEntityDef(vertex);

            if (entityDef != null) {
                entityDefs.add(entityDef);
            }
        }

        CollectionUtils.filter(entityDefs, FilterUtil.getPredicateFromSearchFilter(filter));

        AtlasEntityDefs ret = new AtlasEntityDefs(entityDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private void toVertex(AtlasEntityDef entityDef, AtlasVertex vertex) throws AtlasBaseException {
        AtlasType type = typeRegistry.getType(entityDef.getName());

        if (type.getTypeCategory() != AtlasType.TypeCategory.ENTITY) {
            throw new AtlasBaseException(entityDef.getName() + ": not a entity type");
        }

        AtlasStructDefStoreV1.toVertex(entityDef, (AtlasEntityType)type, vertex, typeDefStore, typeRegistry);

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
