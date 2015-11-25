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

package org.apache.atlas.service;

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TypeNotFoundException;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(modules = RepositoryMetadataModule.class)
public class DefaultMetadataServiceTest {
    @Inject
    private MetadataService metadataService;
    @Inject
    private GraphProvider<TitanGraph> graphProvider;


    @BeforeClass
    public void setUp() throws Exception {
        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }
    }

    @AfterClass
    public void shudown() {
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private String createInstance(Referenceable entity) throws Exception {
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.createEntities(entitiesJson.toString());
        return new JSONArray(response).getString(0);
    }

    private Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(TestUtils.DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set("name", dbName);
        entity.set("description", "us db");
        return entity;
    }

    @Test
    public void testCreateEntityWithUniqueAttribute() throws Exception {
        //name is the unique attribute
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);

        //using the same name should succeed, but not create another entity
        String newId = createInstance(entity);
        Assert.assertEquals(newId, id);

        //Same entity, but different qualified name should succeed
        entity.set("name", TestUtils.randomString());
        newId = createInstance(entity);
        Assert.assertNotEquals(newId, id);
    }

    @Test
    public void testCreateEntityWithUniqueAttributeWithReference() throws Exception {
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable table = new Referenceable(TestUtils.TABLE_TYPE);
        table.set("name", TestUtils.randomString());
        table.set("description", "random table");
        table.set("type", "type");
        table.set("tableType", "MANAGED");
        table.set("database", db);
        createInstance(table);

        //table create should re-use the db instance created earlier
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Referenceable actualDb = (Referenceable) tableDefinition.get("database");
        Assert.assertEquals(actualDb.getId().id, dbId);
    }

    @Test
    public void testCreateEntityWithEnum() throws Exception {
        Referenceable dbEntity = createDBEntity();
        String db = createInstance(dbEntity);

        Referenceable table = new Referenceable(TestUtils.TABLE_TYPE);
        table.set("name", TestUtils.randomString());
        table.set("description", "random table");
        table.set("type", "type");
        table.set("tableType", "MANAGED");
        table.set("database", dbEntity);
        createInstance(table);

        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        EnumValue tableType = (EnumValue) tableDefinition.get("tableType");

        Assert.assertEquals(tableType, new EnumValue("MANAGED", 1));
    }

    @Test
    public void testGetEntityByUniqueAttribute() throws Exception {
        Referenceable entity = createDBEntity();
        createInstance(entity);

        //get entity by valid qualified name
        String entityJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name",
                (String) entity.get("name"));
        Assert.assertNotNull(entityJson);
        Referenceable referenceable = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Assert.assertEquals(referenceable.get("name"), entity.get("name"));

        //get entity by invalid qualified name
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", "random");
            Assert.fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        //get entity by non-unique attribute
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "description",
                    (String) entity.get("description"));
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }
}
