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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.SkipException;
import scala.actors.threadpool.Arrays;

import java.io.IOException;

import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.createAtlasEntity;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadBaseModel;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadEntity;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadHiveModel;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ExportImportTestBase {
    protected static final String ENTITIES_SUB_DIR = "stocksDB-Entities";
    protected static final String DB_GUID = "1637a33e-6512-447b-ade7-249c8cb5344b";
    protected static final String TABLE_GUID = "df122fc3-5555-40f8-a30f-3090b8a622f8";
    protected static final String TABLE_TABLE_GUID = "6f3b305a-c459-4ae4-b651-aee0deb0685f";
    protected static final String TABLE_VIEW_GUID = "56415119-7cb0-40dd-ace8-1e50efd54991";
    protected static final String COLUMN_GUID_HIGH = "f87a5320-1529-4369-8d63-b637ebdf2c1c";

    protected DeleteHandlerV1 deleteHandler = mock(SoftDeleteHandlerV1.class);

    protected void basicSetup(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
        loadHiveModel(typeDefStore, typeRegistry);
    }

    protected int createEntities(AtlasEntityStoreV1 entityStore, String subDir, String entityFileNames[]) {
        for (String fileName : entityFileNames) {
            createAtlasEntity(entityStore, loadEntity(subDir, fileName));
        }

        return entityFileNames.length;
    }

    protected void verifyCreatedEntities(AtlasEntityStoreV1 entityStore, Object[] entityGuids, int expectedNumberOfEntitiesCreated) {
        try {
            AtlasEntity.AtlasEntitiesWithExtInfo entities = entityStore.getByIds(Arrays.asList(entityGuids));
            assertEquals(entities.getEntities().size(), expectedNumberOfEntitiesCreated);
        } catch (AtlasBaseException e) {
            throw new SkipException(String.format("getByIds: could not load '%s'", entityGuids.toString()));
        }
    }

    protected void assertAuditEntry(ExportImportAuditService auditService) {
        AtlasSearchResult result = null;
        try {
            result = auditService.get("", "", "", "", "", "", 10, 0);
        } catch (AtlasBaseException e) {
            fail("auditService.get: failed!");
        }

        assertNotNull(result);
        assertNotNull(result.getEntities());
        assertTrue(result.getEntities().size() > 0);
    }

    private String getCurrentCluster() throws AtlasException {
        return ApplicationProperties.get().getString(AtlasConstants.CLUSTER_NAME_KEY, "default");
    }
}
