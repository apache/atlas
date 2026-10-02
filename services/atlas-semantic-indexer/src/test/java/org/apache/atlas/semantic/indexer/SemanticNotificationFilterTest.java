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

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SemanticNotificationFilterTest {
    @Test
    public void skipsEntityDelete() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                entity("guid-1"), OperationType.ENTITY_DELETE);
        assertFalse(SemanticNotificationFilter.shouldProcess(notification));
    }

    @Test
    public void acceptsEntityUpdate() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                entity("guid-2"), OperationType.ENTITY_UPDATE);
        assertTrue(SemanticNotificationFilter.shouldProcess(notification));
        assertEquals(SemanticNotificationFilter.extractGuids(notification), Set.of("guid-2"));
    }

    @Test
    public void acceptsClassificationAdd() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                entity("guid-3"), OperationType.CLASSIFICATION_ADD);
        assertTrue(SemanticNotificationFilter.shouldProcess(notification));
    }

    @Test
    public void skipsNotificationWithoutGuids() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                entity(""), OperationType.ENTITY_UPDATE);
        assertFalse(SemanticNotificationFilter.shouldProcess(notification));
    }

    @Test
    public void termAssignmentKeepsOnlyTheEntityEnd() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                relationship("AtlasGlossarySemanticAssignment"), OperationType.RELATIONSHIP_CREATE, System.currentTimeMillis());

        assertTrue(SemanticNotificationFilter.shouldProcess(notification));
        assertEquals(SemanticNotificationFilter.extractGuids(notification), Set.of("entity-guid"));
    }

    @Test
    public void skipsTermAssignmentUpdate() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                relationship("AtlasGlossarySemanticAssignment"), OperationType.RELATIONSHIP_UPDATE, System.currentTimeMillis());

        assertFalse(SemanticNotificationFilter.shouldProcess(notification));
    }

    @Test
    public void skipsOtherRelationships() {
        EntityNotificationV2 notification = new EntityNotificationV2(
                relationship("hive_table_columns"), OperationType.RELATIONSHIP_CREATE, System.currentTimeMillis());

        assertFalse(SemanticNotificationFilter.shouldProcess(notification));
    }

    private static AtlasRelationshipHeader relationship(String typeName) {
        AtlasRelationshipHeader relationship = new AtlasRelationshipHeader();
        relationship.setTypeName(typeName);
        relationship.setEnd1(objectId("term-guid"));
        relationship.setEnd2(objectId("entity-guid"));
        return relationship;
    }

    @Test
    public void dedupeGuidsFromMultipleNotifications() {
        EntityNotificationV2 create = new EntityNotificationV2(entity("g1"), OperationType.ENTITY_CREATE);
        EntityNotificationV2 update = new EntityNotificationV2(entity("g1"), OperationType.ENTITY_UPDATE);
        EntityNotificationV2 delete = new EntityNotificationV2(entity("g1"), OperationType.ENTITY_DELETE);

        assertTrue(SemanticNotificationFilter.shouldProcess(create));
        assertTrue(SemanticNotificationFilter.shouldProcess(update));
        assertFalse(SemanticNotificationFilter.shouldProcess(delete));
    }

    private static AtlasEntityHeader entity(String guid) {
        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid(guid);
        header.setTypeName("hive_table");
        return header;
    }

    private static AtlasObjectId objectId(String guid) {
        AtlasObjectId objectId = new AtlasObjectId();
        objectId.setGuid(guid);
        return objectId;
    }
}
