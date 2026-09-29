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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Validates entity type names referenced in hook notifications before store operations.
 */
public final class NotificationMessageValidator {
    private NotificationMessageValidator() {
    }

    public static void validate(HookNotification message, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        if (message == null || typeRegistry == null) {
            return;
        }

        switch (message.getType()) {
            case ENTITY_CREATE: {
                HookNotificationV1.EntityCreateRequest createRequest = (HookNotificationV1.EntityCreateRequest) message;
                validateReferenceables(typeRegistry, createRequest.getEntities());
            }
            break;

            case ENTITY_PARTIAL_UPDATE: {
                HookNotificationV1.EntityPartialUpdateRequest partialUpdateRequest =
                        (HookNotificationV1.EntityPartialUpdateRequest) message;
                validateEntityTypeName(typeRegistry, partialUpdateRequest.getTypeName());
            }
            break;

            case ENTITY_DELETE: {
                HookNotificationV1.EntityDeleteRequest deleteRequest = (HookNotificationV1.EntityDeleteRequest) message;
                validateEntityTypeName(typeRegistry, deleteRequest.getTypeName());
            }
            break;

            case ENTITY_FULL_UPDATE: {
                HookNotificationV1.EntityUpdateRequest updateRequest = (HookNotificationV1.EntityUpdateRequest) message;
                validateReferenceables(typeRegistry, updateRequest.getEntities());
            }
            break;

            case ENTITY_CREATE_V2: {
                EntityCreateRequestV2 createRequestV2 = (EntityCreateRequestV2) message;
                validateEntities(typeRegistry, createRequestV2.getEntities());
            }
            break;

            case ENTITY_PARTIAL_UPDATE_V2: {
                EntityPartialUpdateRequestV2 partialUpdateRequest = (EntityPartialUpdateRequestV2) message;
                AtlasObjectId                entityId             = partialUpdateRequest.getEntityId();
                AtlasEntityWithExtInfo         entity               = partialUpdateRequest.getEntity();

                if (entityId != null) {
                    validateEntityTypeName(typeRegistry, entityId.getTypeName());
                }

                if (entity != null && entity.getEntity() != null) {
                    validateEntityTypeName(typeRegistry, entity.getEntity().getTypeName());
                }
            }
            break;

            case ENTITY_FULL_UPDATE_V2: {
                EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) message;
                validateEntities(typeRegistry, updateRequest.getEntities());
            }
            break;

            case ENTITY_DELETE_V2: {
                EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) message;
                List<AtlasObjectId>   entities      = deleteRequest.getEntities();

                if (CollectionUtils.isNotEmpty(entities)) {
                    for (AtlasObjectId entity : entities) {
                        validateEntityTypeName(typeRegistry, entity.getTypeName());
                    }
                }
            }
            break;

            default:
                break;
        }
    }

    private static void validateReferenceables(AtlasTypeRegistry typeRegistry, List<Referenceable> entities) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }

        for (Referenceable entity : entities) {
            if (entity != null) {
                validateEntityTypeName(typeRegistry, entity.getTypeName());
            }
        }
    }

    private static void validateEntities(AtlasTypeRegistry typeRegistry, AtlasEntitiesWithExtInfo entities) throws AtlasBaseException {
        if (entities == null) {
            return;
        }

        if (CollectionUtils.isNotEmpty(entities.getEntities())) {
            for (AtlasEntity entity : entities.getEntities()) {
                if (entity != null) {
                    validateEntityTypeName(typeRegistry, entity.getTypeName());
                }
            }
        }

        Map<String, AtlasEntity> referredEntities = entities.getReferredEntities();

        if (MapUtils.isNotEmpty(referredEntities)) {
            for (AtlasEntity entity : referredEntities.values()) {
                if (entity != null) {
                    validateEntityTypeName(typeRegistry, entity.getTypeName());
                }
            }
        }
    }

    private static void validateEntityTypeName(AtlasTypeRegistry typeRegistry, String typeName) throws AtlasBaseException {
        if (StringUtils.isBlank(typeName) || typeRegistry.getEntityTypeByName(typeName) == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
        }
    }
}
