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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.metrics.Metrics;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class RequestContext {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    private static final ThreadLocal<RequestContext> CURRENT_CONTEXT = new ThreadLocal<>();
    private static final Set<RequestContext>         ACTIVE_REQUESTS = new HashSet<>();

    private final Set<String>                             createdEntityIds = new LinkedHashSet<>();
    private final Set<String>                             updatedEntityIds = new LinkedHashSet<>();
    private final Set<String>                             deletedEntityIds = new LinkedHashSet<>();
    private final List<ITypedReferenceableInstance>       deletedEntities  = new ArrayList<>();
    private final Map<String,ITypedReferenceableInstance> entityCacheV1    = new HashMap<>();
    private final Map<String,AtlasEntityWithExtInfo>      entityCacheV2    = new HashMap<>();
    private final Metrics                                 metrics          = new Metrics();
    private final long                                    requestTime      = System.currentTimeMillis();

    private String user;
    private int    maxAttempts  = 1;
    private int    attemptCount = 1;

    private RequestContext() {
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContext get() {
        if (CURRENT_CONTEXT.get() == null) {
            RequestContext context = new RequestContext();

            CURRENT_CONTEXT.set(context);

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.add(context);
            }
        }

        // ensure that RequestContextV1 is also initialized for this request
        RequestContextV1.get();

        return CURRENT_CONTEXT.get();
    }

    public static void clear() {
        RequestContext instance = CURRENT_CONTEXT.get();

        if (instance != null) {
            if (instance.entityCacheV1 != null) {
                instance.entityCacheV1.clear();
            }

            if (instance.entityCacheV2 != null) {
                instance.entityCacheV2.clear();
            }

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.remove(instance);
            }
        }

        CURRENT_CONTEXT.remove();
    }

    public static RequestContext createContext() {
        clear();

        return get();
    }

    public static int getActiveRequestsCount() {
        return ACTIVE_REQUESTS.size();
    }

    public static long earliestActiveRequestTime() {
        long ret = System.currentTimeMillis();

        synchronized (ACTIVE_REQUESTS) {
            for (RequestContext context : ACTIVE_REQUESTS) {
                if (ret > context.getRequestTime()) {
                    ret = context.getRequestTime();
                }
            }
        }

        return ret;
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(ITypedReferenceableInstance instance) {
        entityCacheV1.put(instance.getId()._getId(), instance);
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(AtlasEntityWithExtInfo entity) {
        if (entity != null && entity.getEntity() != null && entity.getEntity().getGuid() != null) {
            entityCacheV2.put(entity.getEntity().getGuid(), entity);
        }
    }

    /**
     * Checks if an instance with the given guid is in the cache for this request.  Either returns the instance
     * or null if it is not in the cache.
     *
     * @param guid the guid to find
     * @return Either the instance or null if it is not in the cache.
     */
    public ITypedReferenceableInstance getInstanceV1(String guid) {
        return entityCacheV1.get(guid);
    }

    /**
     * Checks if an instance with the given guid is in the cache for this request.  Either returns the instance
     * or null if it is not in the cache.
     *
     * @param guid the guid to find
     * @return Either the instance or null if it is not in the cache.
     */
    public AtlasEntityWithExtInfo getInstanceV2(String guid) {
        return entityCacheV2.get(guid);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;

        RequestContextV1.get().setUser(user);
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
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

    public void recordEntityDelete(ITypedReferenceableInstance entity) throws AtlasException {
        if (deletedEntityIds.add(entity.getId()._getId())) {
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
