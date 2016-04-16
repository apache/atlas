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

import com.google.inject.Inject;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.resources.BaseResourceIT;
import org.codehaus.jettison.json.JSONArray;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Guice(modules = NotificationModule.class)
public class NotificationHookConsumerIT extends BaseResourceIT {

    private static final String TEST_USER = "testuser";

    @Inject
    private NotificationInterface kafka;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitions();
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.close();
    }

    private void sendHookMessage(HookNotification.HookNotificationMessage message) throws NotificationException {
        kafka.send(NotificationInterface.NotificationType.HOOK, message);
    }

    @Test
    public void testCreateEntity() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        entity.set("name", "db" + randomString());
        entity.set("description", randomString());

        sendHookMessage(new HookNotification.EntityCreateRequest(TEST_USER, entity));

        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE,
                        entity.get("name")));
                return results.length() == 1;
            }
        });
    }

    @Test
    public void testUpdateEntityPartial() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set("name", dbName);
        entity.set("description", randomString());
        serviceClient.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        newEntity.set("owner", randomString());
        sendHookMessage(
                new HookNotification.EntityPartialUpdateRequest(TEST_USER, DATABASE_TYPE, "name", dbName, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                Referenceable localEntity = serviceClient.getEntity(DATABASE_TYPE, "name", dbName);
                return (localEntity.get("owner") != null && localEntity.get("owner").equals(newEntity.get("owner")));
            }
        });

        //Its partial update and un-set fields are not updated
        Referenceable actualEntity = serviceClient.getEntity(DATABASE_TYPE, "name", dbName);
        assertEquals(actualEntity.get("description"), entity.get("description"));
    }

    @Test
    public void testUpdatePartialUpdatingQualifiedName() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set("name", dbName);
        entity.set("description", randomString());
        serviceClient.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        final String newName = "db" + randomString();
        newEntity.set("name", newName);

        sendHookMessage(
                new HookNotification.EntityPartialUpdateRequest(TEST_USER, DATABASE_TYPE, "name", dbName, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE,
                        newName));
                return results.length() == 1;
            }
        });

        //no entity with the old qualified name
        JSONArray results = serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, dbName));
        assertEquals(results.length(), 0);

    }

    @Test
    public void testDeleteByQualifiedName() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set("name", dbName);
        entity.set("description", randomString());
        final String dbId = serviceClient.createEntity(entity).getString(0);

        sendHookMessage(
            new HookNotification.EntityDeleteRequest(TEST_USER, DATABASE_TYPE, "name", dbName));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                Referenceable getEntity = serviceClient.getEntity(dbId);
                return getEntity.getId().getState() == Id.EntityState.DELETED;
            }
        });
    }

    @Test
    public void testUpdateEntityFullUpdate() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set("name", dbName);
        entity.set("description", randomString());
        serviceClient.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        newEntity.set("name", dbName);
        newEntity.set("description", randomString());
        newEntity.set("owner", randomString());

        //updating unique attribute
        sendHookMessage(new HookNotification.EntityUpdateRequest(TEST_USER, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE,
                        dbName));
                return results.length() == 1;
            }
        });

        Referenceable actualEntity = serviceClient.getEntity(DATABASE_TYPE, "name", dbName);
        assertEquals(actualEntity.get("description"), newEntity.get("description"));
        assertEquals(actualEntity.get("owner"), newEntity.get("owner"));
    }


}
