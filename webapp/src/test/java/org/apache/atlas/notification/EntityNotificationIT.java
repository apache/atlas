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

package org.apache.atlas.notification;

import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.v1.model.typedef.TraitTypeDefinition;
import org.apache.atlas.v1.model.notification.EntityNotification;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.apache.atlas.web.integration.BaseResourceIT;
import org.testng.annotations.BeforeClass;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Entity Notification Integration Tests.
 */
public class EntityNotificationIT extends BaseResourceIT {

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    private NotificationInterface notificationInterface = NotificationProvider.get();
    private Id tableId;
    private Id dbId;
    private String traitName;
    private NotificationConsumer notificationConsumer;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitionsV1();
        Referenceable HiveDBInstance = createHiveDBInstanceBuiltIn(DATABASE_NAME);
        dbId = createInstance(HiveDBInstance);

        notificationConsumer = notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1).get(0);
    }

    public void testCreateEntity() throws Exception {
        Referenceable tableInstance = createHiveTableInstanceBuiltIn(DATABASE_NAME, TABLE_NAME, dbId);
        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_CREATE, HIVE_TABLE_TYPE_BUILTIN, guid));
    }

    public void testUpdateEntity() throws Exception {
        final String property = "description";
        final String newValue = "New description!";

        final String guid = tableId._getId();

        atlasClientV1.updateEntityAttribute(guid, property, newValue);

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_UPDATE, HIVE_TABLE_TYPE_BUILTIN, guid));
    }

    public void testDeleteEntity() throws Exception {
        final String tableName = "table-" + randomString();
        final String dbName = "db-" + randomString();
        Referenceable HiveDBInstance = createHiveDBInstanceBuiltIn(dbName);
        Id dbId = createInstance(HiveDBInstance);

        Referenceable tableInstance = createHiveTableInstanceBuiltIn(dbName, tableName, dbId);
        final Id tableId = createInstance(tableInstance);
        final String guid = tableId._getId();

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
            newNotificationPredicate(EntityNotification.OperationType.ENTITY_CREATE, HIVE_TABLE_TYPE_BUILTIN, guid));

        final String name = (String) tableInstance.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME);

        atlasClientV1.deleteEntity(HIVE_TABLE_TYPE_BUILTIN, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
            newNotificationPredicate(EntityNotification.OperationType.ENTITY_DELETE, HIVE_TABLE_TYPE_BUILTIN, guid));
    }

    public void testAddTrait() throws Exception {
        String superSuperTraitName = "SuperTrait" + randomString();
        createTrait(superSuperTraitName);

        String superTraitName = "SuperTrait" + randomString();
        createTrait(superTraitName, superSuperTraitName);

        traitName = "Trait" + randomString();
        createTrait(traitName, superTraitName);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceJSON = AtlasType.toV1Json(traitInstance);
        LOG.debug("Trait instance = {}", traitInstanceJSON);

        final String guid = tableId._getId();

        atlasClientV1.addTrait(guid, traitInstance);

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE_BUILTIN, guid));

        Referenceable entity = entityNotification.getEntity();
        assertTrue(entity.getTraitNames().contains(traitName));

        List<Struct> allTraits = entityNotification.getAllTraits();
        List<String> allTraitNames = new LinkedList<>();

        for (Struct struct : allTraits) {
            allTraitNames.add(struct.getTypeName());
        }
        assertTrue(allTraitNames.contains(traitName));
        assertTrue(allTraitNames.contains(superTraitName));
        assertTrue(allTraitNames.contains(superSuperTraitName));

        String anotherTraitName = "Trait" + randomString();
        createTrait(anotherTraitName, superTraitName);

        traitInstance = new Struct(anotherTraitName);
        traitInstanceJSON = AtlasType.toV1Json(traitInstance);
        LOG.debug("Trait instance = {}", traitInstanceJSON);

        atlasClientV1.addTrait(guid, traitInstance);

        entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE_BUILTIN, guid));

        allTraits = entityNotification.getAllTraits();
        allTraitNames = new LinkedList<>();

        for (Struct struct : allTraits) {
            allTraitNames.add(struct.getTypeName());
        }
        assertTrue(allTraitNames.contains(traitName));
        assertTrue(allTraitNames.contains(anotherTraitName));
        // verify that the super type shows up twice in all traits
        assertEquals(2, Collections.frequency(allTraitNames, superTraitName));
    }

    public void testDeleteTrait() throws Exception {
        final String guid = tableId._getId();

        atlasClientV1.deleteTrait(guid, traitName);

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_DELETE, HIVE_TABLE_TYPE_BUILTIN, guid));

        assertFalse(entityNotification.getEntity().getTraitNames().contains(traitName));
    }


    // ----- helper methods ---------------------------------------------------

    private void createTrait(String traitName, String ... superTraitNames) throws Exception {
        TraitTypeDefinition trait =
            TypesUtil.createTraitTypeDef(traitName, null, ImmutableSet.copyOf(superTraitNames));

        String traitDefinitionJSON = AtlasType.toV1Json(trait);
        LOG.debug("Trait definition = {}", traitDefinitionJSON);
        createType(traitDefinitionJSON);
    }

}
