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
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.web.resources.BaseResourceIT;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(modules = NotificationModule.class)
public class NotificationHookConsumerIT extends BaseResourceIT {

    @Inject
    private NotificationInterface kafka;
    private String dbName;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitions();
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.close();
    }

    private void sendHookMessage(Referenceable entity) throws NotificationException {
        String entityJson = InstanceSerialization.toJson(entity, true);
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(entityJson);
        kafka.send(NotificationInterface.NotificationType.HOOK, jsonArray.toString());
    }

    @Test
    public void testConsumeHookMessage() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        dbName = "db" + randomString();
        entity.set("name", dbName);
        entity.set("description", randomString());

        sendHookMessage(entity);

        waitFor(1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results =
                        serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, dbName));
                return results.length() == 1;
            }
        });
    }

    @Test (dependsOnMethods = "testConsumeHookMessage")
    public void testEnityDeduping() throws Exception {
//        Referenceable db = serviceClient.getEntity(DATABASE_TYPE, "name", dbName);
        Referenceable db = new Referenceable(DATABASE_TYPE);
        db.set("name", dbName);
        db.set("description", randomString());

        Referenceable table = new Referenceable(HIVE_TABLE_TYPE);
        final String tableName = randomString();
        table.set("name", tableName);
        table.set("db", db);

        sendHookMessage(table);
        waitFor(1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results =
                        serviceClient.searchByDSL(String.format("%s where name='%s'", HIVE_TABLE_TYPE, tableName));
                return results.length() == 1;
            }
        });

        JSONArray results =
                serviceClient.searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, dbName));
        Assert.assertEquals(results.length(), 1);
    }

}
