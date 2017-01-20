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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class EntityGraphDiscoveryContext {

    /**
     *  Keeps track of all the entities that need to be created/updated including its child entities *
     */
    private Set<AtlasEntity> rootEntities = new LinkedHashSet<>();

    //Key is a transient id/guid
    /**
     * These references have been resolved using a unique identifier like guid or a qualified name etc in Atlas repository
     */
    private Map<String, AtlasVertex> repositoryResolvedReferences = new LinkedHashMap<>();

    /**
     * Unresolved entity references
     */
    private List<AtlasEntity> unresolvedEntityReferences = new ArrayList<>();

    /**
     * Unresolved entity id references
     */
    private Set<AtlasObjectId> unresolvedIdReferences = new HashSet<>();

    public void addRepositoryResolvedReference(AtlasObjectId id, AtlasVertex vertex) {
        repositoryResolvedReferences.put(id.getGuid(), vertex);
    }

    public void addUnResolvedEntityReference(AtlasEntity entity) {
        this.unresolvedEntityReferences.add(entity);
    }

    public void addUnResolvedIdReference(AtlasEntityType entityType, String id) {
        this.unresolvedIdReferences.add(new AtlasObjectId(entityType.getTypeName(), id));
    }

    public Set<AtlasObjectId> getUnresolvedIdReferences() {
        return unresolvedIdReferences;
    }

    public boolean isResolved(String guid) {
        return repositoryResolvedReferences.containsKey(guid);
    }

    public AtlasVertex getResolvedReference(AtlasObjectId ref) {
        return repositoryResolvedReferences.get(ref.getGuid());
    }

    public Map<String, AtlasVertex> getRepositoryResolvedReferences() {
        return repositoryResolvedReferences;
    }

    public AtlasVertex getResolvedReference(String id) {
        return repositoryResolvedReferences.get(id);
    }

    public List<AtlasEntity> getUnResolvedEntityReferences() {
        return unresolvedEntityReferences;
    }

    public void addRootEntity(AtlasEntity rootEntity) {
        this.rootEntities.add(rootEntity);
    }

    public Collection<AtlasEntity> getRootEntities() {
        return rootEntities;
    }

    public boolean removeUnResolvedEntityReference(final AtlasEntity entity) {
        return unresolvedEntityReferences.remove(entity);
    }

    public boolean removeUnResolvedEntityReferences(final List<AtlasEntity> entities) {
        return unresolvedEntityReferences.removeAll(entities);
    }

    public boolean removeUnResolvedIdReferences(final List<AtlasObjectId> entities) {
        return unresolvedIdReferences.removeAll(entities);
    }

    public boolean removeUnResolvedIdReference(final AtlasObjectId entity) {
        return unresolvedIdReferences.remove(entity);
    }

    public boolean hasUnresolvedReferences() {
        return unresolvedEntityReferences.size() > 0 || unresolvedIdReferences.size() > 0;
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
                Objects.equals(repositoryResolvedReferences, ctx.getRepositoryResolvedReferences()) &&
                Objects.equals(unresolvedEntityReferences, ctx.getUnResolvedEntityReferences()) &&
                Objects.equals(unresolvedIdReferences, ctx.getUnresolvedIdReferences());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rootEntities, repositoryResolvedReferences, unresolvedEntityReferences, unresolvedIdReferences);
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("EntityGraphDiscoveryCtx{");
        sb.append("rootEntities='").append(rootEntities).append('\'');
        sb.append(", repositoryResolvedReferences=").append(repositoryResolvedReferences);
        sb.append(", unresolvedEntityReferences='").append(unresolvedEntityReferences).append('\'');
        sb.append(", unresolvedIdReferences='").append(unresolvedIdReferences).append('\'');
        sb.append('}');

        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public void cleanUp() {
        rootEntities.clear();
        unresolvedEntityReferences.clear();
        repositoryResolvedReferences.clear();
        unresolvedIdReferences.clear();
    }
}
