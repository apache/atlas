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

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RequestContext {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    private static final ThreadLocal<RequestContext> CURRENT_CONTEXT = new ThreadLocal<>();
    private static final Set<RequestContext>         ACTIVE_REQUESTS = new HashSet<>();

    private final Map<String, AtlasObjectId>             updatedEntities     = new HashMap<>();
    private final Map<String, AtlasObjectId>             deletedEntities     = new HashMap<>();
    private final Map<String, AtlasEntity>               entityCache         = new HashMap<>();
    private final Map<String, AtlasEntityWithExtInfo>    entityExtInfoCache  = new HashMap<>();
    private final Map<String, List<AtlasClassification>> addedPropagations   = new HashMap<>();
    private final Map<String, List<AtlasClassification>> removedPropagations = new HashMap<>();
    private final long                                   requestTime         = System.currentTimeMillis();
    private       List<EntityGuidPair>                   entityGuidInRequest = null;

    private String      user;
    private Set<String> userGroups;
    private String clientIPAddress;
    private int    maxAttempts  = 1;
    private int    attemptCount = 1;


    private RequestContext() {
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContext get() {
        RequestContext ret = CURRENT_CONTEXT.get();

        if (ret == null) {
            ret = new RequestContext();
            CURRENT_CONTEXT.set(ret);

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.add(ret);
            }
        }

        return ret;
    }

    public static void clear() {
        RequestContext instance = CURRENT_CONTEXT.get();

        if (instance != null) {
            instance.updatedEntities.clear();
            instance.deletedEntities.clear();
            instance.entityCache.clear();
            instance.entityExtInfoCache.clear();
            instance.addedPropagations.clear();
            instance.removedPropagations.clear();

            if (instance.entityGuidInRequest != null) {
                instance.entityGuidInRequest.clear();
            }

            synchronized (ACTIVE_REQUESTS) {
                ACTIVE_REQUESTS.remove(instance);
            }
        }

        CURRENT_CONTEXT.remove();
    }

    public static String getCurrentUser() {
        RequestContext context = CURRENT_CONTEXT.get();
        return context != null ? context.getUser() : null;
    }

    public String getUser() {
        return user;
    }

    public Set<String> getUserGroups() {
        return userGroups;
    }

    public void setUser(String user, Set<String> userGroups) {
        this.user       = user;
        this.userGroups = userGroups;
    }

    public String getClientIPAddress() {
        return clientIPAddress;
    }

    public void setClientIPAddress(String clientIPAddress) {
        this.clientIPAddress = clientIPAddress;
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

    public void recordAddedPropagation(String guid, AtlasClassification classification) {
        if (StringUtils.isNotEmpty(guid) && classification != null) {
            List<AtlasClassification> classifications = addedPropagations.get(guid);

            if (classifications == null) {
                classifications = new ArrayList<>();
            }

            classifications.add(classification);

            addedPropagations.put(guid, classifications);
        }
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

    public void recordRemovedPropagation(String guid, AtlasClassification classification) {
        if (StringUtils.isNotEmpty(guid) && classification != null) {
            List<AtlasClassification> classifications = removedPropagations.get(guid);

            if (classifications == null) {
                classifications = new ArrayList<>();
            }

            classifications.add(classification);

            removedPropagations.put(guid, classifications);
        }
    }

    public Map<String, List<AtlasClassification>> getAddedPropagations() {
        return addedPropagations;
    }

    public Map<String, List<AtlasClassification>> getRemovedPropagations() {
        return removedPropagations;
    }

    /**
     * Adds the specified instance to the cache
     *
     */
    public void cache(AtlasEntityWithExtInfo entity) {
        if (entity != null && entity.getEntity() != null && entity.getEntity().getGuid() != null) {
            entityExtInfoCache.put(entity.getEntity().getGuid(), entity);
            entityCache.put(entity.getEntity().getGuid(), entity.getEntity());
        }
    }

    public void cache(AtlasEntity entity) {
        if (entity != null && entity.getGuid() != null) {
            entityCache.put(entity.getGuid(), entity);
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
    public AtlasEntityWithExtInfo getEntityWithExtInfo(String guid) {
        return entityExtInfoCache.get(guid);
    }

    public AtlasEntity getEntity(String guid) {
        return entityCache.get(guid);
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
