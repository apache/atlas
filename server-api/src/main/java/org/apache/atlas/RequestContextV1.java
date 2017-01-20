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
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RequestContextV1 {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContextV1.class);

    private static final ThreadLocal<RequestContextV1> CURRENT_CONTEXT = new ThreadLocal<>();

    private Set<String> createdEntityIds = new LinkedHashSet<>();
    private Set<String> updatedEntityIds = new LinkedHashSet<>();
    private Set<String> deletedEntityIds = new LinkedHashSet<>();

    private String user;
    private final long requestTime;

    TypeSystem typeSystem = TypeSystem.getInstance();
    private Metrics metrics = new Metrics();

    private RequestContextV1() {
        requestTime = System.currentTimeMillis();
    }

    //To handle gets from background threads where createContext() is not called
    //createContext called for every request in the filter
    public static RequestContextV1 get() {
        RequestContextV1 ret = CURRENT_CONTEXT.get();

        if (ret == null) {
            ret = new RequestContextV1();
            CURRENT_CONTEXT.set(ret);
        }

        return ret;
    }
    public static void clear() {
        CURRENT_CONTEXT.remove();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void recordEntityCreate(Collection<String> createdEntityIds) {
        this.createdEntityIds.addAll(createdEntityIds);
    }

    public void recordEntityCreate(String createdEntityId) {
        this.createdEntityIds.add(createdEntityId);
    }

    public void recordEntityUpdate(Collection<String> updatedEntityIds) {
        this.updatedEntityIds.addAll(updatedEntityIds);
    }

    public void recordEntityUpdate(String entityId) {
        this.updatedEntityIds.add(entityId);
    }
    public void recordEntityDelete(String entityId) {
        deletedEntityIds.add(entityId);
    }

    public Collection<String> getCreatedEntityIds() {
        return createdEntityIds;
    }

    public Collection<String> getUpdatedEntityIds() {
        return updatedEntityIds;
    }

    public Collection<String> getDeletedEntityIds() {
        return deletedEntityIds;
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
