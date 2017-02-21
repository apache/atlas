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

package org.apache.atlas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.metrics.Metrics;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestContext {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    private static final ThreadLocal<RequestContext> CURRENT_CONTEXT = new ThreadLocal<>();

    private Set<String> createdEntityIds = new LinkedHashSet<>();
    private Set<String> updatedEntityIds = new LinkedHashSet<>();
    private Set<String> deletedEntityIds = new LinkedHashSet<>();
    private List<ITypedReferenceableInstance> deletedEntities = new ArrayList<>();
    private Map<String,ITypedReferenceableInstance> entityCache = new HashMap<>();

    private String user;
    private long requestTime;

    TypeSystem typeSystem = TypeSystem.getInstance();
    private Metrics metrics = new Metrics();

    private RequestContext() {
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContext get() {
        if (CURRENT_CONTEXT.get() == null) {
            synchronized (RequestContext.class) {
                if (CURRENT_CONTEXT.get() == null) {
                    createContext();
                }
            }
        }

        // ensure that RequestContextV1 is also initialized for this request
        RequestContextV1.get();

        return CURRENT_CONTEXT.get();
    }

    public static RequestContext createContext() {
        RequestContext context = new RequestContext();
        context.requestTime = System.currentTimeMillis();
        CURRENT_CONTEXT.set(context);
        return context;
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(ITypedReferenceableInstance instance) {
        entityCache.put(instance.getId()._getId(), instance);
    }

    /**
     * Checks if an instance with the given guid is in the cache for this request.  Either returns the instance
     * or null if it is not in the cache.
     *
     * @param guid the guid to find
     * @return Either the instance or null if it is not in the cache.
     */
    public ITypedReferenceableInstance getInstance(String guid) {
        return entityCache.get(guid);
    }

    public static void clear() {
        RequestContext instance = CURRENT_CONTEXT.get();

        if (instance != null) {
            if (instance.entityCache != null) {
                instance.entityCache.clear();
            }
        }

        CURRENT_CONTEXT.remove();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;

        RequestContextV1.get().setUser(user);
    }

    public void recordEntityCreate(Collection<String> createdEntityIds) {
        this.createdEntityIds.addAll(createdEntityIds);
    }

    public void recordEntityUpdate(Collection<String> updatedEntityIds) {
        this.updatedEntityIds.addAll(updatedEntityIds);
    }

    public void recordEntityUpdate(String entityId) {
        this.updatedEntityIds.add(entityId);
    }

    public void recordEntityDelete(String entityId, String typeName) throws AtlasException {
        ClassType type = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance entity = type.createInstance(new Id(entityId, 0, typeName));
        if (deletedEntityIds.add(entityId)) {
            deletedEntities.add(entity);
        }
    }

    public List<String> getCreatedEntityIds() {
        return new ArrayList<>(createdEntityIds);
    }

    public List<String> getUpdatedEntityIds() {
        return new ArrayList<>(updatedEntityIds);
    }

    public List<String> getDeletedEntityIds() {
        return new ArrayList<>(deletedEntityIds);
    }

    public List<ITypedReferenceableInstance> getDeletedEntities() {
        return deletedEntities;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public boolean isDeletedEntity(String entityGuid) {
        return deletedEntityIds.contains(entityGuid);
    }

    public static Metrics getMetrics() {
        return get().metrics;
    }
}
