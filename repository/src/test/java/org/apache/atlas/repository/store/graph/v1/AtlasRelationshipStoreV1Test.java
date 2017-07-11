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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.mockito.Mockito.mock;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasRelationshipStoreV1Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    @Inject
    DeleteHandlerV1   deleteHandler;

    @Inject
    EntityGraphMapper graphMapper;

    AtlasEntityStore          entityStore;
    AtlasRelationshipStore    relationshipStore;
    AtlasEntityWithExtInfo    dbEntity;
    AtlasEntityWithExtInfo    tblEntity;
    AtlasEntityChangeNotifier mockChangeNotifier = mock(AtlasEntityChangeNotifier.class);

    @BeforeClass
    public void setUp() throws Exception {
        new GraphBackedSearchIndexer(typeRegistry);

        AtlasTypesDef[] testTypesDefs = new AtlasTypesDef[] { TestUtilsV2.defineDeptEmployeeTypes(),
                                                              TestUtilsV2.defineHiveTypes() };

        for (AtlasTypesDef typesDef : testTypesDefs) {
            AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

            if (!typesToCreate.isEmpty()) {
                typeDefStore.createTypesDef(typesToCreate);
            }
        }

        dbEntity   = TestUtilsV2.createDBEntityV2();
        tblEntity  = TestUtilsV2.createTableEntityV2(dbEntity.getEntity());
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
    }

    @BeforeTest
    public void init() throws Exception {
        entityStore       = new AtlasEntityStoreV1(deleteHandler, typeRegistry, mockChangeNotifier, graphMapper);
        relationshipStore = new AtlasRelationshipStoreV1(typeRegistry);

        RequestContextV1.clear();
    }

    @Test
    public void testDbTableRelationship() throws Exception  {
        // Add tests - in progress
    }
}
