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
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * ClassificationDef store in v1 format.
 */
public class AtlasClassificationDefStoreV1 implements AtlasClassificationDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClassificationDefStoreV1.class);

    private final AtlasTypeDefGraphStoreV1 typeDefStore;

    public AtlasClassificationDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore) {
        super();

        this.typeDefStore = typeDefStore;
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
        List<AtlasClassificationDef> classificationDefList = new LinkedList<>();
        for (AtlasClassificationDef structDef : classificationDefs) {
            try {
                AtlasClassificationDef atlasClassificationDef = create(structDef);
                classificationDefList.add(atlasClassificationDef);
            } catch (AtlasBaseException baseException) {
                LOG.error("Failed to create {}", structDef);
                LOG.error("Exception: {}", baseException);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.create({}, {})", classificationDefs, classificationDefList);
        }
        return classificationDefList;
    }

    @Override
    public List<AtlasClassificationDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getAll()");
        }

        List<AtlasClassificationDef> classificationDefs = new LinkedList<>();
        Iterator<AtlasVertex> verticesByCategory = typeDefStore.findTypeVerticesByCategory(TypeCategory.TRAIT);
        while (verticesByCategory.hasNext()) {
            AtlasClassificationDef classificationDef = toClassificationDef(verticesByCategory.next());
            classificationDefs.add(classificationDef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getAll()");
        }
        return classificationDefs;
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

        List<AtlasClassificationDef> updatedClassificationDefs = new ArrayList<>();

        for (AtlasClassificationDef classificationDef : classificationDefs) {
            try {
                AtlasClassificationDef updatedDef = updateByName(classificationDef.getName(), classificationDef);
                updatedClassificationDefs.add(updatedDef);
            } catch (AtlasBaseException ex) {
                LOG.error("Failed to update {}", classificationDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.update({}): {}", classificationDefs, updatedClassificationDefs);
        }

        return updatedClassificationDefs;
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
            try {
                deleteByName(name);
            } catch (AtlasBaseException ex) {
                LOG.error("Failed to delete {}", name);
            }
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
            try {
                deleteByGuid(guid);
            } catch (AtlasBaseException ex) {
                LOG.error("Failed to delete {}", guid);
            }
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

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.TRAIT);

        while(vertices.hasNext()) {
            AtlasVertex       vertex  = vertices.next();
            AtlasClassificationDef classificationDef = toClassificationDef(vertex);

            if (classificationDef != null) {
                classificationDefs.add(classificationDef); // TODO: add only if this passes filter
            }
        }

        if (CollectionUtils.isNotEmpty(classificationDefs)) {
            CollectionUtils.filter(classificationDefs, FilterUtil.getPredicateFromSearchFilter(filter));
        }


        AtlasClassificationDefs ret = new AtlasClassificationDefs(classificationDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private void toVertex(AtlasClassificationDef classificationDef, AtlasVertex vertex) {
        AtlasStructDefStoreV1.toVertex(classificationDef, vertex, typeDefStore);

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
