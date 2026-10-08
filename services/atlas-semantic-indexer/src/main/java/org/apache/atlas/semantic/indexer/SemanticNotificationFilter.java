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
package org.apache.atlas.semantic.indexer;

import org.apache.atlas.glossary.GlossaryUtils;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Filters {@link EntityNotificationV2} messages for the Semantic Indexer Kafka consumer.
 * Uses an explicit allowlist of operation types that can change embeddable entity text.
 */
public final class SemanticNotificationFilter {
    private static final Set<OperationType> INDEXED_OPERATION_TYPES = Collections.unmodifiableSet(EnumSet.of(
            OperationType.ENTITY_CREATE,
            OperationType.ENTITY_UPDATE,
            OperationType.CLASSIFICATION_ADD,
            OperationType.CLASSIFICATION_DELETE,
            OperationType.CLASSIFICATION_UPDATE,
            OperationType.RELATIONSHIP_CREATE,
            OperationType.RELATIONSHIP_DELETE));

    private SemanticNotificationFilter() {
    }

    public static boolean shouldProcess(EntityNotificationV2 notification) {
        return notification != null && INDEXED_OPERATION_TYPES.contains(notification.getOperationType());
    }

    /**
     * Guids to re-embed, with the type name the notification carries for each (may be null).
     */
    public static Map<String, String> extractGuidTypes(EntityNotificationV2 notification) {
        if (notification == null) {
            return Collections.emptyMap();
        }

        Map<String, String> guids = new LinkedHashMap<>();

        if (notification.getEntity() != null && StringUtils.isNotBlank(notification.getEntity().getGuid())) {
            guids.put(notification.getEntity().getGuid(), notification.getEntity().getTypeName());
        }

        // A term assignment (or removal) changes the assigned terms in the entity's text; other relationships are
        // edges the text builder doesn't follow. Only end2 (the entity): end1 is the term, whose text didn't change,
        // and expanding it would re-embed every entity of that term.
        AtlasRelationshipHeader relationship = notification.getRelationship();
        if (relationship != null && GlossaryUtils.TERM_ASSIGNMENT.equals(relationship.getTypeName())) {
            addObjectIdGuid(guids, relationship.getEnd2());
        }

        return guids;
    }

    private static void addObjectIdGuid(Map<String, String> guids, AtlasObjectId objectId) {
        if (objectId != null && StringUtils.isNotBlank(objectId.getGuid())) {
            guids.put(objectId.getGuid(), objectId.getTypeName());
        }
    }
}
