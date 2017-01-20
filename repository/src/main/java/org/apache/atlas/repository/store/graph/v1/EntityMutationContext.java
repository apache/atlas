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

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.type.AtlasType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityMutationContext {

    private List<AtlasEntity> entitiesCreated  = new ArrayList<>();
    private List<AtlasEntity> entitiesUpdated  = new ArrayList<>();

    private EntityGraphDiscoveryContext context;
    private Map<String, AtlasType> entityVsType = new HashMap<>();
    private Map<String, AtlasVertex> entityVsVertex = new HashMap<>();

    public EntityMutationContext(final EntityGraphDiscoveryContext context) {
        this.context = context;
    }

    public void addCreated(AtlasEntity entity, AtlasType type, AtlasVertex atlasVertex) {
        entitiesCreated.add(entity);
        entityVsVertex.put(entity.getGuid(), atlasVertex);
        entityVsType.put(entity.getGuid(), type);
    }

    public void addUpdated(AtlasEntity entity, AtlasType type, AtlasVertex atlasVertex) {
        entitiesUpdated.add(entity);
        entityVsVertex.put(entity.getGuid(), atlasVertex);
        entityVsType.put(entity.getGuid(), type);
    }

    public Collection<AtlasEntity> getCreatedEntities() {
        return entitiesCreated;
    }

    public Collection<AtlasEntity> getUpdatedEntities() {
        return entitiesUpdated;
    }

    public AtlasType getType(AtlasEntity entity) {
        return entityVsType.get(entity.getGuid());
    }

    public AtlasType getType(String entityId) {
        return entityVsType.get(entityId);
    }

    public AtlasVertex getVertex(AtlasEntity entity) {
        return entityVsVertex.get(entity.getGuid());
    }

    public AtlasVertex getVertex(String entityId) {
        return entityVsVertex.get(entityId);
    }

    public EntityGraphDiscoveryContext getDiscoveryContext() {
        return this.context;
    }

    //TODO - equals/hashCode/toString

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EntityMutationContext that = (EntityMutationContext) o;

        if (entitiesCreated != null ? !entitiesCreated.equals(that.entitiesCreated) : that.entitiesCreated != null)
            return false;
        if (entitiesUpdated != null ? !entitiesUpdated.equals(that.entitiesUpdated) : that.entitiesUpdated != null)
            return false;
        if (context != null ? !context.equals(that.context) : that.context != null) return false;
        if (entityVsType != null ? !entityVsType.equals(that.entityVsType) : that.entityVsType != null) return false;
        return !(entityVsVertex != null ? !entityVsVertex.equals(that.entityVsVertex) : that.entityVsVertex != null);

    }

    @Override
    public int hashCode() {
        int result = entitiesCreated != null ? entitiesCreated.hashCode() : 0;
        result = 31 * result + (entitiesUpdated != null ? entitiesUpdated.hashCode() : 0);
        result = 31 * result + (context != null ? context.hashCode() : 0);
        result = 31 * result + (entityVsType != null ? entityVsType.hashCode() : 0);
        result = 31 * result + (entityVsVertex != null ? entityVsVertex.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EntityMutationContext{" +
            "entitiesCreated=" + entitiesCreated +
            ", entitiesUpdated=" + entitiesUpdated +
            ", context=" + context +
            ", entityVsType=" + entityVsType +
            ", entityVsVertex=" + entityVsVertex +
            '}';
    }
}
