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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
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
import org.apache.atlas.web.resources.BaseResourceIT;
import org.apache.atlas.web.util.Servlets;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Entity Notification Integration Tests.
 */
@Guice(modules = NotificationModule.class)
public class EntityNotificationIT extends BaseResourceIT {

    private static final String ENTITIES = "api/atlas/entities";
    private static final String TRAITS = "traits";
    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    @Inject
    private NotificationInterface notificationInterface;
    private Id tableId;
    private String traitName;
    private NotificationConsumer<EntityNotification> notificationConsumer;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitions();

        List<NotificationConsumer<EntityNotification>> consumers =
            notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

        notificationConsumer = consumers.iterator().next();
    }

    @Test
    public void testCreateEntity() throws Exception {
        Referenceable tableInstance = createHiveTableInstance(DATABASE_NAME, TABLE_NAME);

        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_CREATE, HIVE_TABLE_TYPE, guid));
    }

    @Test(dependsOnMethods = "testCreateEntity")
    public void testUpdateEntity() throws Exception {
        final String property = "description";
        final String newValue = "New description!";

        final String guid = tableId._getId();

        serviceClient.updateEntityAttribute(guid, property, newValue);

        waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.ENTITY_UPDATE, HIVE_TABLE_TYPE, guid));
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
        LOG.debug("Trait instance = " + traitInstanceJSON);

        final String guid = tableId._getId();

        ClientResponse clientResponse = addTrait(guid, traitInstanceJSON);
        assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE, guid));

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
        LOG.debug("Trait instance = " + traitInstanceJSON);

        clientResponse = addTrait(guid, traitInstanceJSON);
        assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_ADD, HIVE_TABLE_TYPE, guid));

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

        ClientResponse clientResponse = deleteTrait(guid, traitName);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        EntityNotification entityNotification = waitForNotification(notificationConsumer, MAX_WAIT_TIME,
                newNotificationPredicate(EntityNotification.OperationType.TRAIT_DELETE, HIVE_TABLE_TYPE, guid));

        assertFalse(entityNotification.getEntity().getTraits().contains(traitName));
    }


    // ----- helper methods ---------------------------------------------------

    private void createTrait(String traitName, String ... superTraitNames) throws Exception {
        HierarchicalTypeDefinition<TraitType> trait =
            TypesUtil.createTraitTypeDef(traitName, ImmutableList.copyOf(superTraitNames));

        String traitDefinitionJSON = TypesSerialization$.MODULE$.toJson(trait, true);
        LOG.debug("Trait definition = " + traitDefinitionJSON);
        createType(traitDefinitionJSON);
    }

    private ClientResponse addTrait(String guid, String traitInstance) {
        WebResource resource = service.path(ENTITIES).path(guid).path(TRAITS);

        return resource.accept(Servlets.JSON_MEDIA_TYPE)
            .type(Servlets.JSON_MEDIA_TYPE)
            .method(HttpMethod.POST, ClientResponse.class, traitInstance);
    }

    private ClientResponse deleteTrait(String guid, String traitName) {
        WebResource resource = service.path(ENTITIES).path(guid).path(TRAITS).path(traitName);

        return resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
            .method(HttpMethod.DELETE, ClientResponse.class);
    }
}
