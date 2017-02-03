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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class EntityGraphDiscoveryContext {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphDiscoveryContext.class);

    private final AtlasTypeRegistry         typeRegistry;
    private List<AtlasEntity>               rootEntities               = new ArrayList<>();
    private Map<AtlasObjectId, AtlasVertex> resolvedIds                = new LinkedHashMap<>();
    private Set<AtlasObjectId>              unresolvedIds              = new HashSet<>();
    private List<AtlasObjectId>             unresolvedIdsByUniqAttribs = new ArrayList<>();

    public EntityGraphDiscoveryContext(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }


    public Collection<AtlasEntity> getRootEntities() {
        return rootEntities;
    }

    public Map<AtlasObjectId, AtlasVertex> getResolvedIds() {
        return resolvedIds;
    }

    public Set<AtlasObjectId> getUnresolvedIds() {
        return unresolvedIds;
    }

    public List<AtlasObjectId> getUnresolvedIdsByUniqAttribs() {
        return unresolvedIdsByUniqAttribs;
    }


    public void addRootEntity(AtlasEntity rootEntity) {
        this.rootEntities.add(rootEntity);
    }


    public void addResolvedId(AtlasObjectId objId, AtlasVertex vertex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addResolvedId({})", objId);
        }

        resolvedIds.put(objId, vertex);
    }

    public boolean removeUnResolvedId(AtlasObjectId objId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeUnResolvedId({})", objId);
        }

        return unresolvedIds.remove(objId);
    }


    public void addUnResolvedId(AtlasObjectId objId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addUnResolvedId({})", objId);
        }

        this.unresolvedIds.add(objId);
    }

    public boolean removeUnResolvedIds(List<AtlasObjectId> objIds) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeUnResolvedIds({})", objIds);
        }

        return unresolvedIds.removeAll(objIds);
    }


    public void addUnresolvedIdByUniqAttribs(AtlasObjectId objId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addUnresolvedIdByUniqAttribs({})", objId);
        }

        this.unresolvedIdsByUniqAttribs.add(objId);
    }

    public boolean removeUnresolvedIdsByUniqAttribs(List<AtlasObjectId> objIds) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeUnresolvedIdsByUniqAttribs({})", objIds);
        }

        return unresolvedIdsByUniqAttribs.removeAll(objIds);
    }

    public boolean hasUnresolvedReferences() {
        return unresolvedIdsByUniqAttribs.size() > 0 || unresolvedIds.size() > 0;
    }

    public boolean isResolvedId(AtlasObjectId id) {
        return resolvedIds.containsKey(id);
    }

    public AtlasVertex getResolvedEntityVertex(AtlasObjectId ref) throws AtlasBaseException {
        AtlasVertex vertex = resolvedIds.get(ref);

        // check also for sub-types; ref={typeName=Asset; guid=abcd} should match {typeName=hive_table; guid=abcd}
        if (vertex == null) {
            final AtlasEntityType entityType  = typeRegistry.getEntityTypeByName(ref.getTypeName());
            final Set<String>     allSubTypes = entityType.getAllSubTypes();

            for (String subType : allSubTypes) {
                AtlasObjectId subTypeObjId = new AtlasObjectId(subType, ref.getGuid(), ref.getUniqueAttributes());

                vertex = resolvedIds.get(subTypeObjId);

                if (vertex != null) {
                    resolvedIds.put(ref, vertex);
                    break;
                }
            }
        }

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS,
                                         " : Could not find an entity with " + ref.toString());
        }

        return vertex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj.getClass() != getClass()) {
            return false;
        } else {
            EntityGraphDiscoveryContext ctx = (EntityGraphDiscoveryContext) obj;
            return Objects.equals(rootEntities, ctx.getRootEntities()) &&
                Objects.equals(resolvedIds, ctx.getResolvedIds()) &&
                Objects.equals(unresolvedIdsByUniqAttribs, ctx.getUnresolvedIdsByUniqAttribs()) &&
                Objects.equals(unresolvedIds, ctx.getUnresolvedIds());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rootEntities, resolvedIds, unresolvedIdsByUniqAttribs, unresolvedIds);
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("EntityGraphDiscoveryCtx{");
        sb.append("rootEntities='").append(rootEntities).append('\'');
        sb.append(", resolvedIds=").append(resolvedIds);
        sb.append(", unresolvedIdsByUniqAttribs='").append(unresolvedIdsByUniqAttribs).append('\'');
        sb.append(", unresolvedIds='").append(unresolvedIds).append('\'');
        sb.append('}');

        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public void cleanUp() {
        rootEntities.clear();
        unresolvedIdsByUniqAttribs.clear();
        resolvedIds.clear();
        unresolvedIds.clear();
    }
}
