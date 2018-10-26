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


import com.google.common.collect.ImmutableList;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.util.UniqueList;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.createTypes;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.getEntities;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runExportWithParameters;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ExportIncrementalTest extends ExportImportTestBase {
    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    ExportService exportService;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    private final String EXPORT_REQUEST_INCREMENTAL = "export-incremental";
    private final String EXPORT_REQUEST_CONNECTED = "export-connected";
    private long nextTimestamp;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        basicSetup(typeDefStore, typeRegistry);
        createEntities(entityStore, ENTITIES_SUB_DIR, new String[] { "db", "table-columns"});
        final String[] entityGuids = {DB_GUID, TABLE_GUID};
        verifyCreatedEntities(entityStore, entityGuids, 2);
    }

    @BeforeMethod
    public void setupTest() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @Test
    public void atT0_ReturnsAllEntities() throws AtlasBaseException {
        final int expectedEntityCount = 2;

        AtlasExportRequest request = getIncrementalRequest(0);
        ZipSource source = runExportWithParameters(exportService, request);
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        int count = 0;
        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            assertNotNull(entry.getValue());
            count++;
        }

        nextTimestamp = updateTimesampForNextIncrementalExport(source);
        assertEquals(count, expectedEntityCount);
    }

    private long updateTimesampForNextIncrementalExport(ZipSource source) throws AtlasBaseException {
        return source.getExportResult().getChangeMarker();
    }

    @Test(dependsOnMethods = "atT0_ReturnsAllEntities")
    public void atT1_NewClassificationAttachedToTable_ReturnsChangedTable() throws AtlasBaseException {
        final int expectedEntityCount = 1;

        AtlasClassificationType ct = createNewClassification();
        entityStore.addClassifications(TABLE_GUID, ImmutableList.of(ct.createDefaultValue()));

        AtlasExportRequest request = getIncrementalRequest(nextTimestamp);
        ZipSource source = runExportWithParameters(exportService, request);
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        AtlasEntity entity = null;
        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            entity = entry.getValue();
            assertNotNull(entity);
            break;
        }

        nextTimestamp = updateTimesampForNextIncrementalExport(source);
        assertEquals(entity.getGuid(),TABLE_GUID);
    }

    private AtlasClassificationType createNewClassification() {
        createTypes(typeDefStore, ENTITIES_SUB_DIR,"typesDef-new-classification");
        return typeRegistry.getClassificationTypeByName("T1");
    }

    @Test(dependsOnMethods = "atT1_NewClassificationAttachedToTable_ReturnsChangedTable")
    public void atT2_NewClassificationAttachedToColumn_ReturnsChangedColumn() throws AtlasBaseException {
        final int expectedEntityCount = 1;

        AtlasEntity.AtlasEntityWithExtInfo tableEntity = entityStore.getById(TABLE_GUID);
        long preExportTableEntityTimestamp = tableEntity.getEntity().getUpdateTime().getTime();

        entityStore.addClassifications(COLUMN_GUID_HIGH, ImmutableList.of(typeRegistry.getClassificationTypeByName("T1").createDefaultValue()));

        ZipSource source = runExportWithParameters(exportService, getIncrementalRequest(nextTimestamp));
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            AtlasEntity entity = entry.getValue();
            assertNotNull(entity.getGuid());
            break;
        }

        long postUpdateTableEntityTimestamp = tableEntity.getEntity().getUpdateTime().getTime();
        assertEquals(preExportTableEntityTimestamp, postUpdateTableEntityTimestamp);
        nextTimestamp = updateTimesampForNextIncrementalExport(source);
    }

    @Test(dependsOnMethods = "atT2_NewClassificationAttachedToColumn_ReturnsChangedColumn")
    public void exportingWithSameParameters_Succeeds() {
        ZipSource source = runExportWithParameters(exportService, getIncrementalRequest(nextTimestamp));

        assertNotNull(source);
    }

    @Test
    public void connectedExport() {
        ZipSource source = runExportWithParameters(exportService, getConnected());

        UniqueList<String> creationOrder = new UniqueList<>();
        List<String> zipCreationOrder = source.getCreationOrder();
        creationOrder.addAll(zipCreationOrder);
        assertNotNull(source);
        assertEquals(creationOrder.size(), zipCreationOrder.size());
    }

    private AtlasExportRequest getIncrementalRequest(long timestamp) {
        try {
            AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, EXPORT_REQUEST_INCREMENTAL, AtlasExportRequest.class);
            request.getOptions().put(FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, timestamp);

            return request;
        } catch (IOException e) {
            throw new SkipException(String.format("getIncrementalRequest: '%s' could not be laoded.", EXPORT_REQUEST_INCREMENTAL));
        }
    }

    private AtlasExportRequest getConnected() {
        try {
            return TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, EXPORT_REQUEST_CONNECTED, AtlasExportRequest.class);
        } catch (IOException e) {
            throw new SkipException(String.format("getIncrementalRequest: '%s' could not be laoded.", EXPORT_REQUEST_CONNECTED));
        }
    }

}
