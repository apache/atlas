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
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasClassificationDefStore;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.type.AtlasClassificationType;
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
 * ClassificationDef store in v1 format.
 */
public class AtlasClassificationDefStoreV1 extends AtlasAbstractDefStoreV1 implements AtlasClassificationDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClassificationDefStoreV1.class);

    public AtlasClassificationDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasClassificationDef create(AtlasClassificationDef classificationDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.create({})", classificationDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByName(classificationDef.getName());

        if (vertex != null) {
            throw new AtlasBaseException(classificationDef.getName() + ": type already exists");
        }

        vertex = typeDefStore.createTypeVertex(classificationDef);

        toVertex(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.create({}): {}", classificationDef, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasClassificationDef> create(List<AtlasClassificationDef> classificationDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.create({})", classificationDefs);
        }

        List<AtlasClassificationDef> ret = new ArrayList<>();

        for (AtlasClassificationDef classificationDef : classificationDefs) {
            ret.add(create(classificationDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.create({}, {})", classificationDefs, ret);
        }
        return ret;
    }

    @Override
    public List<AtlasClassificationDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getAll()");
        }

        List<AtlasClassificationDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.TRAIT);
        while (vertices.hasNext()) {
            ret.add(toClassificationDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getAll(): count={}", ret.size());
        }
        return ret;
    }

    @Override
    public AtlasClassificationDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with name " + name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with guid " + guid);
        }

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef updateByName(String name, AtlasClassificationDef classificationDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.updateByName({}, {})", name, classificationDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with name " + name);
        }

        toVertex(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.updateByName({}, {}): {}", name, classificationDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef updateByGuid(String guid, AtlasClassificationDef classificationDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.updateByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with guid " + guid);
        }

        toVertex(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasClassificationDef> update(List<AtlasClassificationDef> classificationDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.update({})", classificationDefs);
        }

        List<AtlasClassificationDef> ret = new ArrayList<>();

        for (AtlasClassificationDef classificationDef : classificationDefs) {
            ret.add(updateByName(classificationDef.getName(), classificationDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.update({}): {}", classificationDefs, ret);
        }

        return ret;
    }

    @Override
    public void deleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.deleteByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with name " + name);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.deleteByName({})", name);
        }
    }

    @Override
    public void deleteByNames(List<String> names) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.deleteByNames({})", names);
        }

        for (String name : names) {
            deleteByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.deleteByNames({})", names);
        }
    }

    @Override
    public void deleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.deleteByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException("no classificationDef exists with guid " + guid);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.deleteByGuid({})", guid);
        }
    }

    @Override
    public void deleteByGuids(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.deleteByGuids({})", guids);
        }

        for (String guid : guids) {
            deleteByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.deleteByGuids({})", guids);
        }

    }

    @Override
    public AtlasClassificationDefs search(SearchFilter filter) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.search({})", filter);
        }

        List<AtlasClassificationDef> classificationDefs = new ArrayList<AtlasClassificationDef>();
        Iterator<AtlasVertex>        vertices           = typeDefStore.findTypeVerticesByCategory(TypeCategory.TRAIT);

        while(vertices.hasNext()) {
            AtlasVertex            vertex            = vertices.next();
            AtlasClassificationDef classificationDef = toClassificationDef(vertex);

            if (classificationDef != null) {
                classificationDefs.add(classificationDef);
            }
        }

        CollectionUtils.filter(classificationDefs, FilterUtil.getPredicateFromSearchFilter(filter));

        AtlasClassificationDefs ret = new AtlasClassificationDefs(classificationDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private void toVertex(AtlasClassificationDef classificationDef, AtlasVertex vertex) throws AtlasBaseException {
        AtlasType type = typeRegistry.getType(classificationDef.getName());

        if (type.getTypeCategory() != AtlasType.TypeCategory.CLASSIFICATION) {
            throw new AtlasBaseException(classificationDef.getName() + ": not a classification type");
        }

        AtlasStructDefStoreV1.toVertex(classificationDef, (AtlasClassificationType)type,
                                       vertex, typeDefStore, typeRegistry);

        typeDefStore.createSuperTypeEdges(vertex, classificationDef.getSuperTypes());
    }

    private AtlasClassificationDef toClassificationDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasClassificationDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.TRAIT)) {
            ret = new AtlasClassificationDef();

            AtlasStructDefStoreV1.toStructDef(vertex, ret, typeDefStore);

            ret.setSuperTypes(typeDefStore.getSuperTypeNames(vertex));
        }

        return ret;
    }
}
