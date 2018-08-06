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


import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityChangeNotifier;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;

import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadBaseModel;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadHiveModel;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runExportWithParameters;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ExportSkipLineageTest extends ExportImportTestBase {
    private final String ENTITIES_SUB_DIR = "stocksDB-Entities";
    private final String DB_GUID = "1637a33e-6512-447b-ade7-249c8cb5344b";
    private final String TABLE_GUID = "df122fc3-5555-40f8-a30f-3090b8a622f8";
    private final String TABLE_TABLE_GUID = "6f3b305a-c459-4ae4-b651-aee0deb0685f";
    private final String TABLE_VIEW_GUID = "56415119-7cb0-40dd-ace8-1e50efd54991";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private EntityGraphMapper graphMapper;

    @Inject
    ExportService exportService;

    private DeleteHandlerV1 deleteHandler = mock(SoftDeleteHandlerV1.class);
    private AtlasEntityChangeNotifier mockChangeNotifier = mock(AtlasEntityChangeNotifier.class);
    private AtlasEntityStoreV1 entityStore;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel(typeDefStore, typeRegistry);
        loadHiveModel(typeDefStore, typeRegistry);

        entityStore = new AtlasEntityStoreV1(deleteHandler, typeRegistry, mockChangeNotifier, graphMapper);
        createEntities(entityStore, ENTITIES_SUB_DIR, new String[]{"db", "table-columns", "table-view", "table-table-lineage"});
        final Object[] entityGuids = new Object[]{DB_GUID, TABLE_GUID, TABLE_TABLE_GUID, TABLE_VIEW_GUID};
        verifyCreatedEntities(entityStore, entityGuids, 4);
    }

    @BeforeMethod
    public void setupTest() {
        RequestContextV1.clear();
        RequestContextV1.get().setUser(TestUtilsV2.TEST_USER);
    }

    @Test
    public void exportWithoutLineage() {
        final int expectedEntityCount = 3;

        AtlasExportRequest request = getRequest();
        ZipSource source = runExportWithParameters(exportService, request);
        AtlasEntity.AtlasEntityWithExtInfo entities = ZipFileResourceTestUtils.getEntities(source, expectedEntityCount);

        int count = 0;
        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            assertNotNull(entry.getValue());
            if(entry.getValue().getTypeName().equals("hive_process")) {
                fail("Process entities should not be part of export!");
            }
            count++;
        }

        assertEquals(count, expectedEntityCount);
    }

    private AtlasExportRequest getRequest() {
        final String filename = "export-skip-lineage";
        try {
            AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, filename, AtlasExportRequest.class);

            return request;
        } catch (IOException e) {
            throw new SkipException(String.format("getRequest: '%s' could not be laoded.", filename));
        }
    }
}
