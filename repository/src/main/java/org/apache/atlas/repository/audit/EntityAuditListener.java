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

package org.apache.atlas.repository.audit;

import com.google.inject.Inject;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.RequestContext;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.json.InstanceSerialization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Listener on entity create/update/delete, tag add/delete. Adds the corresponding audit event to the audit repository.
 */
public class EntityAuditListener implements EntityChangeListener {
    private EntityAuditRepository auditRepository;

    @Inject
    public EntityAuditListener(EntityAuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }

    @Override
    public void onEntitiesAdded(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        long currentTime = RequestContext.get().getRequestTime();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, currentTime, EntityAuditEvent.EntityAuditAction.ENTITY_CREATE,
                    "Created: " + InstanceSerialization.toJson(entity, true));
            events.add(event);
        }
        auditRepository.putEvents(events);
    }

    private EntityAuditEvent createEvent(ITypedReferenceableInstance entity, long ts,
                                         EntityAuditEvent.EntityAuditAction action, String details) {
        return new EntityAuditEvent(entity.getId()._getId(), ts, RequestContext.get().getUser(), action, details);
    }

    @Override
    public void onEntitiesUpdated(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        long currentTime = RequestContext.get().getRequestTime();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, currentTime, EntityAuditEvent.EntityAuditAction.ENTITY_UPDATE,
                    "Updated: " + InstanceSerialization.toJson(entity, true));
            events.add(event);
        }
        auditRepository.putEvents(events);
    }

    @Override
    public void onTraitAdded(ITypedReferenceableInstance entity, IStruct trait) throws AtlasException {
        EntityAuditEvent event = createEvent(entity, RequestContext.get().getRequestTime(),
                EntityAuditEvent.EntityAuditAction.TAG_ADD,
                "Added trait: " + InstanceSerialization.toJson(trait, true));
        auditRepository.putEvents(event);
    }

    @Override
    public void onTraitDeleted(ITypedReferenceableInstance entity, String traitName) throws AtlasException {
        EntityAuditEvent event = createEvent(entity, RequestContext.get().getRequestTime(),
                EntityAuditEvent.EntityAuditAction.TAG_DELETE, "Deleted trait: " + traitName);
        auditRepository.putEvents(event);
    }

    @Override
    public void onEntitiesDeleted(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        long currentTime = RequestContext.get().getRequestTime();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, currentTime,
                    EntityAuditEvent.EntityAuditAction.ENTITY_DELETE, "Deleted entity");
            events.add(event);
        }
        auditRepository.putEvents(events);
    }
}
