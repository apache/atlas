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
package org.apache.atlas.semantic;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Filters {@link EntityNotificationV2} messages for the Semantic Indexer Kafka consumer.
 * Uses an explicit allowlist of operation types that can change embeddable entity text.
 */
public final class SemanticNotificationFilter {
    private static final String TERM_ASSIGNMENT_RELATIONSHIP = "AtlasGlossarySemanticAssignment";

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
        if (notification == null || notification.getOperationType() == null) {
            return false;
        }

        if (!INDEXED_OPERATION_TYPES.contains(notification.getOperationType())) {
            return false;
        }

        return !extractGuids(notification).isEmpty();
    }

    public static Set<String> extractGuids(EntityNotificationV2 notification) {
        if (notification == null) {
            return Collections.emptySet();
        }

        Set<String> guids = new LinkedHashSet<>();

        if (notification.getEntity() != null && StringUtils.isNotBlank(notification.getEntity().getGuid())) {
            guids.add(notification.getEntity().getGuid());
        }

        // A term assignment (or removal) changes the assigned terms in the entity's text; other relationships are
        // edges the text builder doesn't follow. Only end2 (the entity): end1 is the term, whose text didn't change,
        // and expanding it would re-embed every entity of that term.
        AtlasRelationshipHeader relationship = notification.getRelationship();
        if (relationship != null && TERM_ASSIGNMENT_RELATIONSHIP.equals(relationship.getTypeName())) {
            addObjectIdGuid(guids, relationship.getEnd2());
        }

        return guids;
    }

    private static void addObjectIdGuid(Set<String> guids, AtlasObjectId objectId) {
        if (objectId != null && StringUtils.isNotBlank(objectId.getGuid())) {
            guids.add(objectId.getGuid());
        }
    }
}
