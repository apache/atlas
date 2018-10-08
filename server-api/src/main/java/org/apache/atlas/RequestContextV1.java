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

import org.apache.atlas.metrics.Metrics;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RequestContextV1 {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContextV1.class);

    private static final ThreadLocal<RequestContextV1> CURRENT_CONTEXT = new ThreadLocal<>();
    private static final Set<RequestContextV1>         ACTIVE_REQUESTS = new HashSet<>();

    private final Map<String, AtlasObjectId> updatedEntities = new HashMap<>();
    private final Map<String, AtlasObjectId> deletedEntities = new HashMap<>();
    private final Map<String, AtlasEntity>   entityCacheV2   = new HashMap<>();
    private final Metrics                    metrics         = new Metrics();
    private final long                       requestTime     = System.currentTimeMillis();
    private       List<EntityGuidPair>       entityGuidInRequest = null;

    private String user;

    private RequestContextV1() {
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContextV1 get() {
        RequestContextV1 ret = CURRENT_CONTEXT.get();

        if (ret == null) {
            ret = new RequestContextV1();
            CURRENT_CONTEXT.set(ret);

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.add(ret);
            }
        }

        return ret;
    }

    public static void clear() {
        RequestContextV1 instance = CURRENT_CONTEXT.get();

        if (instance != null) {
            instance.updatedEntities.clear();
            instance.deletedEntities.clear();
            instance.entityCacheV2.clear();

            if (instance.entityGuidInRequest != null) {
                instance.entityGuidInRequest.clear();
            }

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.remove(instance);
            }
        }

        CURRENT_CONTEXT.remove();
    }

    public static int getActiveRequestsCount() {
        return ACTIVE_REQUESTS.size();
    }

    public static String getCurrentUser() {
        RequestContextV1 context = CURRENT_CONTEXT.get();
        return context != null ? context.getUser() : null;
    }

    public static long earliestActiveRequestTime() {
        long ret = System.currentTimeMillis();

        synchronized (ACTIVE_REQUESTS) {
            for (RequestContextV1 context : ACTIVE_REQUESTS) {
                if (ret > context.getRequestTime()) {
                    ret = context.getRequestTime();
                }
            }
        }

        return ret;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void recordEntityUpdate(AtlasObjectId entity) {
        if (entity != null && entity.getGuid() != null) {
            updatedEntities.put(entity.getGuid(), entity);
        }
    }

    public void recordEntityDelete(AtlasObjectId entity) {
        if (entity != null && entity.getGuid() != null) {
            deletedEntities.put(entity.getGuid(), entity);
        }
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(AtlasEntity entity) {
        if (entity != null && entity.getGuid() != null) {
            entityCacheV2.put(entity.getGuid(), entity);
        }
    }

    public Collection<AtlasObjectId> getUpdatedEntities() {
        return updatedEntities.values();
    }

    public Collection<AtlasObjectId> getDeletedEntities() {
        return deletedEntities.values();
    }

    /**
     * Checks if an instance with the given guid is in the cache for this request.  Either returns the instance
     * or null if it is not in the cache.
     *
     * @param guid the guid to find
     * @return Either the instance or null if it is not in the cache.
     */
    public AtlasEntity getInstanceV2(String guid) {
        return entityCacheV2.get(guid);
    }

    public long getRequestTime() {
        return requestTime;
    }

    public boolean isUpdatedEntity(String guid) {
        return updatedEntities.containsKey(guid);
    }

    public boolean isDeletedEntity(String guid) {
        return deletedEntities.containsKey(guid);
    }

    public static Metrics getMetrics() {
        return get().metrics;
    }


    public void recordEntityGuidUpdate(AtlasEntity entity, String guidInRequest) {
        if (entityGuidInRequest == null) {
            entityGuidInRequest = new ArrayList<>();
        }

        entityGuidInRequest.add(new EntityGuidPair(entity, guidInRequest));
    }

    public void resetEntityGuidUpdates() {
        if (entityGuidInRequest != null) {
            for (EntityGuidPair entityGuidPair : entityGuidInRequest) {
                entityGuidPair.resetEntityGuid();
            }
        }
    }

    public class EntityGuidPair {
        private final AtlasEntity entity;
        private final String      guid;

        public EntityGuidPair(AtlasEntity entity, String guid) {
            this.entity = entity;
            this.guid   = guid;
        }

        public void resetEntityGuid() {
            entity.setGuid(guid);
        }
    }
}
