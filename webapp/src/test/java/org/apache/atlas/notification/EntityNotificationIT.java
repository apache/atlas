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
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.web.integration.BaseResourceIT;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
    private NotificationConsumer<EntityNotification> notificationConsumer;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitionsV1();
        Referenceable HiveDBInstance = createHiveDBInstanceBuiltIn(DATABASE_NAME);
        dbId = createInstance(HiveDBInstance);

        List<NotificationConsumer<EntityNotification>> consumers =
            notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

        notificationConsumer = consumers.iterator().next();
    }

    @Test
    public void testCreateEntity() throws Exception {
        Referenceable tableInstance = createHiveTableInstanceBuiltIn(DATABASE_NAME, TABLE_NAME, dbId);
        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_CREATE, HIVE_TABLE_TYPE_BUILTIN, guid));
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testUpdateEntity() throws Exception {
        final String property = "description";
        final String newValue = "New description!";

        final String guid = tableId._getId();

        atlasClientV1.updateEntityAttribute(guid, property, newValue);

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_UPDATE, HIVE_TABLE_TYPE_BUILTIN, guid));
    }

    @Test
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

    @Test(dependsOnMethods = "testCreateEntity")
    public void testAddTrait() throws Exception {
        String superSuperTraitName = "SuperTrait" + randomString();
        createTrait(superSuperTraitName);

        String superTraitName = "SuperTrait" + randomString();
        createTrait(superTraitName, superSuperTraitName);

        traitName = "Trait" + randomString();
        createTrait(traitName, superTraitName);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("Trait instance = {}", traitInstanceJSON);

        final String guid = tableId._getId();

        atlasClientV1.addTrait(guid, traitInstance);

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE_BUILTIN, guid));

        IReferenceableInstance entity = entityNotification.getEntity();
        assertTrue(entity.getTraits().contains(traitName));

        List<IStruct> allTraits = entityNotification.getAllTraits();
        List<String> allTraitNames = new LinkedList<>();

        for (IStruct struct : allTraits) {
            allTraitNames.add(struct.getTypeName());
        }
        assertTrue(allTraitNames.contains(traitName));
        assertTrue(allTraitNames.contains(superTraitName));
        assertTrue(allTraitNames.contains(superSuperTraitName));

        String anotherTraitName = "Trait" + randomString();
        createTrait(anotherTraitName, superTraitName);

        traitInstance = new Struct(anotherTraitName);
        traitInstanceJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("Trait instance = {}", traitInstanceJSON);

        atlasClientV1.addTrait(guid, traitInstance);

        entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE_BUILTIN, guid));

        allTraits = entityNotification.getAllTraits();
        allTraitNames = new LinkedList<>();

        for (IStruct struct : allTraits) {
            allTraitNames.add(struct.getTypeName());
        }
        assertTrue(allTraitNames.contains(traitName));
        assertTrue(allTraitNames.contains(anotherTraitName));
        // verify that the super type shows up twice in all traits
        assertEquals(2, Collections.frequency(allTraitNames, superTraitName));
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = tableId._getId();

        atlasClientV1.deleteTrait(guid, traitName);

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_DELETE, HIVE_TABLE_TYPE_BUILTIN, guid));

        assertFalse(entityNotification.getEntity().getTraits().contains(traitName));
    }


    // ----- helper methods ---------------------------------------------------

    private void createTrait(String traitName, String ... superTraitNames) throws Exception {
        HierarchicalTypeDefinition<TraitType> trait =
            TypesUtil.createTraitTypeDef(traitName, ImmutableSet.copyOf(superTraitNames));

        String traitDefinitionJSON = TypesSerialization$.MODULE$.toJson(trait, true);
        LOG.debug("Trait definition = {}", traitDefinitionJSON);
        createType(traitDefinitionJSON);
    }

}
