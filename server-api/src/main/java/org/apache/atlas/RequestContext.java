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

public class RequestContext {
    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    private static final ThreadLocal<RequestContext> CURRENT_CONTEXT = new ThreadLocal<>();

    private Set<String> createdEntityIds = new LinkedHashSet<>();
    private Set<String> updatedEntityIds = new LinkedHashSet<>();
    private Set<String> deletedEntityIds = new LinkedHashSet<>();
    private List<ITypedReferenceableInstance> deletedEntities = new ArrayList<>();

    private String user;
    private long requestTime;

    TypeSystem typeSystem = TypeSystem.getInstance();

    private RequestContext() {
    }

    public static RequestContext get() {
        return CURRENT_CONTEXT.get();
    }

    public static RequestContext createContext() {
        RequestContext context = new RequestContext();
        context.requestTime = System.currentTimeMillis();
        CURRENT_CONTEXT.set(context);
        return context;
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

    public void recordCreatedEntities(Collection<String> createdEntityIds) {
        this.createdEntityIds.addAll(createdEntityIds);
    }

    public void recordUpdatedEntities(Collection<String> updatedEntityIds) {
        this.updatedEntityIds.addAll(updatedEntityIds);
    }

    public void recordDeletedEntity(String entityId, String typeName) throws AtlasException {
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
}
