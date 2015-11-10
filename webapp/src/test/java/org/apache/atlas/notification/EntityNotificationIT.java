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
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.resources.BaseResourceIT;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Entity Notification Integration Tests.
 */
@Guice(modules = NotificationModule.class)
public class EntityNotificationIT extends BaseResourceIT {

  @Inject
  private NotificationInterface notificationInterface;

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    createTypeDefinitions();
  }

  @Test
  public void testEntityNotification() throws Exception {

    List<NotificationConsumer<EntityNotification>> consumers =
        notificationInterface.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

    NotificationConsumer<EntityNotification> consumer =  consumers.iterator().next();
    final EntityNotificationConsumer notificationConsumer = new EntityNotificationConsumer(consumer);
    Thread thread = new Thread(notificationConsumer);
    thread.start();

    createEntity("Sales", "Sales Database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");

    waitFor(10000, new Predicate() {
      @Override
      public boolean evaluate() throws Exception {
        return notificationConsumer.entityNotification != null;
      }
    });

    Assert.assertNotNull(notificationConsumer.entityNotification);
    Assert.assertEquals(EntityNotification.OperationType.ENTITY_CREATE, notificationConsumer.entityNotification.getOperationType());
    Assert.assertEquals(DATABASE_TYPE, notificationConsumer.entityNotification.getEntity().getTypeName());
    Assert.assertEquals("Sales", notificationConsumer.entityNotification.getEntity().get("name"));
  }

  private void createEntity(String name, String description, String owner, String locationUri, String... traitNames)
      throws Exception {

    Referenceable referenceable = new Referenceable(DATABASE_TYPE, traitNames);
    referenceable.set("name", name);
    referenceable.set("description", description);
    referenceable.set("owner", owner);
    referenceable.set("locationUri", locationUri);
    referenceable.set("createTime", System.currentTimeMillis());

    createInstance(referenceable);
  }

  private static class EntityNotificationConsumer implements Runnable {
    private final NotificationConsumer<EntityNotification> consumerIterator;
    private EntityNotification entityNotification = null;

    public EntityNotificationConsumer(NotificationConsumer<EntityNotification> consumerIterator) {
      this.consumerIterator = consumerIterator;
    }

    @Override
    public void run() {
      while(consumerIterator.hasNext()) {
        entityNotification = consumerIterator.next();
      }
    }
  }
}
