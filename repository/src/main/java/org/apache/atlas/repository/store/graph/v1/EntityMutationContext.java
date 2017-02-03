/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.model.instance.AtlasEntity;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;

import java.util.*;

public class EntityMutationContext {
    private final EntityGraphDiscoveryContext         context;
    private final List<AtlasEntity>                   entitiesCreated = new ArrayList<>();
    private final List<AtlasEntity>                   entitiesUpdated = new ArrayList<>();
    private final Map<AtlasObjectId, AtlasEntityType> entityVsType    = new HashMap<>();
    private final Map<AtlasObjectId, AtlasVertex>     entityVsVertex  = new HashMap<>();

    public EntityMutationContext(final EntityGraphDiscoveryContext context) {
        this.context = context;
    }

    public void addCreated(AtlasEntity entity, AtlasEntityType type, AtlasVertex atlasVertex) {
        AtlasObjectId objId = entity.getAtlasObjectId();
        entitiesCreated.add(entity);
        entityVsType.put(objId, type);
        entityVsVertex.put(objId, atlasVertex);
    }

    public void addUpdated(AtlasEntity entity, AtlasEntityType type, AtlasVertex atlasVertex) {
        AtlasObjectId objId = entity.getAtlasObjectId();
        entitiesUpdated.add(entity);
        entityVsType.put(objId, type);
        entityVsVertex.put(objId, atlasVertex);
    }

    public EntityGraphDiscoveryContext getDiscoveryContext() {
        return this.context;
    }

    public Collection<AtlasEntity> getCreatedEntities() {
        return entitiesCreated;
    }

    public Collection<AtlasEntity> getUpdatedEntities() {
        return entitiesUpdated;
    }

    public AtlasEntityType getType(AtlasEntity entity) {
        return entityVsType.get(entity.getAtlasObjectId());
    }

    public AtlasType getType(AtlasObjectId entityId) {
        return entityVsType.get(entityId);
    }

    public AtlasVertex getVertex(AtlasEntity entity) {
        return entityVsVertex.get(entity.getAtlasObjectId());
    }

    public AtlasVertex getVertex(AtlasObjectId entityId) {
        return entityVsVertex.get(entityId);
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EntityMutationContext that = (EntityMutationContext) o;

        return Objects.equals(context, that.context) &&
               Objects.equals(entitiesCreated, that.entitiesCreated) &&
               Objects.equals(entitiesUpdated, that.entitiesUpdated) &&
               Objects.equals(entityVsType, that.entityVsType) &&
               Objects.equals(entityVsVertex, that.entityVsVertex);
    }

    @Override
    public int hashCode() {
        int result = (context != null ? context.hashCode() : 0);
        result = 31 * result + entitiesCreated.hashCode();
        result = 31 * result + entitiesUpdated.hashCode();
        result = 31 * result + entityVsType.hashCode();
        result = 31 * result + entityVsVertex.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EntityMutationContext{" +
            "context=" + context +
            ", entitiesCreated=" + entitiesCreated +
            ", entitiesUpdated=" + entitiesUpdated +
            ", entityVsType=" + entityVsType +
            ", entityVsVertex=" + entityVsVertex +
            '}';
    }
}
