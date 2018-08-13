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

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.SoftDeleteModule.class)
public class SoftReferenceTest {
    private static final String TYPE_RDBMS_DB = "rdbms_db";
    private static final String RDBMS_DB_FILE = "rdbms-db";
    private static final String TYPE_RDBMS_STORAGE = "rdbms_storage";
    private static final String TYPESDEF_FILE_NAME = "typesdef-soft-ref";
    private static final String RDBMS_DB_STORAGE_PROPERTY = "sd";
    private static final String RDBMS_DB_TABLES_PROPERTY = "tables";
    private static final String RDBMS_DB_REGIONS_PROPERTY = "regions";
    private static final String RDBMS_SD_PROPERTY = "rdbms_db.sd";
    private static final String TYPE_RDBMS_TABLES = "rdbms_table";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasEntityStore entityStore;

    private AtlasType dbType;

    @Test
    public void typeCreationFromFile() throws IOException, AtlasBaseException {
        String typesDefJson = TestResourceFileUtils.getJson(TYPESDEF_FILE_NAME);

        AtlasTypesDef typesDef = AtlasType.fromJson(typesDefJson, AtlasTypesDef.class);
        assertNotNull(typesDef);

        typeDefStore.createTypesDef(typesDef);

        dbType = typeRegistry.getType(TYPE_RDBMS_DB);
        assertNotNull(dbType);
        AtlasEntityDef dbType = typeRegistry.getEntityDefByName(TYPE_RDBMS_DB);
        assertNotNull(dbType);
        assertTrue(dbType.getAttribute(RDBMS_DB_STORAGE_PROPERTY).isSoftReferenced());
        assertTrue(dbType.getAttribute(RDBMS_DB_TABLES_PROPERTY).isSoftReferenced());
        assertTrue(dbType.getAttribute(RDBMS_DB_REGIONS_PROPERTY).isSoftReferenced());
        assertNotNull(typeRegistry.getEntityDefByName(TYPE_RDBMS_STORAGE));
        assertNotNull(typeRegistry.getEntityDefByName(TYPE_RDBMS_TABLES));
    }

    @Test(dependsOnMethods = "typeCreationFromFile")
    public void entityCreationUsingSoftRef() throws IOException, AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo dbEntity = AtlasType.fromJson(
                TestResourceFileUtils.getJson(RDBMS_DB_FILE), AtlasEntity.AtlasEntityWithExtInfo.class);

        EntityMutationResponse  response = entityStore.createOrUpdate(new AtlasEntityStream(dbEntity), false);
        assertNotNull(response);
        assertTrue(response.getCreatedEntities().size() == 6);
        assertGraphStructure(response.getCreatedEntities().get(0).getGuid(),
                                    response.getCreatedEntities().get(1).getGuid(), RDBMS_SD_PROPERTY);
    }

    private void assertGraphStructure(String dbGuid, String storageGuid, String propertyName) throws AtlasBaseException {
        AtlasVertex vertex = AtlasGraphUtilsV1.findByGuid(dbGuid);
        Iterator<AtlasEdge> edgesOut = vertex.getEdges(AtlasEdgeDirection.OUT).iterator();
        Iterator<AtlasEdge> edgesIn = vertex.getEdges(AtlasEdgeDirection.IN).iterator();

        String sd = AtlasGraphUtilsV1.getProperty(vertex, propertyName, String.class);

        assertNotNull(sd);
        assertAttribute(dbGuid, storageGuid);
        assertFalse(edgesOut.hasNext());
        assertFalse(edgesIn.hasNext());
        assertNotNull(vertex);
    }

    private void assertAttribute(String dbGuid, String storageGuid) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityStore.getById(dbGuid);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        Object val = entity.getAttribute(RDBMS_DB_STORAGE_PROPERTY);
        assertTrue(val instanceof AtlasObjectId);
        assertEquals(((AtlasObjectId) val).getTypeName(), TYPE_RDBMS_STORAGE);
        assertEquals(((AtlasObjectId) val).getGuid(), storageGuid);
        assertNotNull(entity.getAttribute(RDBMS_DB_TABLES_PROPERTY));
        assertEquals(((List) entity.getAttribute(RDBMS_DB_TABLES_PROPERTY)).size(), 2);
        assertNotNull(entity.getAttribute(RDBMS_DB_REGIONS_PROPERTY));
        assertEquals(((Map) entity.getAttribute(RDBMS_DB_REGIONS_PROPERTY)).size(), 2);
    }
}
