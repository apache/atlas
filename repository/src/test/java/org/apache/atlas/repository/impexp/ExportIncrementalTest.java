/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.util.UniqueList;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.ITestContext;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.getEntities;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.getZipSource;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runExportWithParameters;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithNoParameters;
import static org.apache.atlas.utils.TestLoadModelUtils.createTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ExportIncrementalTest extends AtlasTestBase {
    private static final String EXPORT_INCREMENTAL           = "incremental";
    private static final String QUALIFIED_NAME_DB            = "db_test_1@02052019";
    private static final String QUALIFIED_NAME_TABLE_LINEAGE = "db_test_1.test_tbl_ctas_2@02052019";
    private static final String QUALIFIED_NAME_TABLE_2       = "db_test_1.test_tbl_2@02052019";
    private static final String GUID_DB                      = "f0b72ab4-7452-4e42-ac74-2aee7728cce4";
    private static final String GUID_TABLE_2                 = "8d0b834c-61ce-42d8-8f66-6fa51c36bccb";
    private static final String GUID_TABLE_CTAS_2            = "eaec545b-3ac7-4e1b-a497-bd4a2b6434a2";
    private static final String GUID_HIVE_PROCESS            = "bd3138b2-f29e-4226-b859-de25eaa1c18b";
    private static final String GUID_TABLE_1                 = "4d5adf00-2c9b-4877-ad23-c41fd7319150";
    private static final String EXPORT_REQUEST_INCREMENTAL   = "export-incremental";
    private static final String EXPORT_REQUEST_CONNECTED     = "export-connected";

    private static final String FIRSTPARENT                  = "589a233a-f00e-4928-8efd-e7e72e30d370";
    private static final String HIVEDB                       = "12eb7a9b-3b4d-48c9-902c-1fa2401823f7";
    private static final String CTASLEVEL13                  = "6f1c413c-1b35-421a-aabd-d5f94873ddf0";
    private static final String CTASLEVEL12                  = "58a68a94-67bb-488d-b111-3dfdd3a220eb";
    private static final String CTASLEVEL11                  = "c6657df3-3bea-44cc-a356-a81c9e72f9c7";
    private static final String SECONDPARENT                 = "0ce3573b-c535-4bf9-970e-4d37f01806ef";
    private static final String CTLASLEVEL11_1               = "80a3ead2-6ad7-4881-bd85-5e8b4fdb01c5";
    private static final String HDFS_PATH                    = "d9c50322-b130-405e-b560-2b15bcdddb97";
    private static final String SECONDPARENT_PROCESS         = "f611662a-4ea6-4707-b7e9-02848fb28529";
    private static final String CTASLEVEL13_PROCESS          = "da34b191-5ab9-4934-94c6-5a97d3e59608";
    private static final String CTASLEVEL12_PROCESS          = "33fc0f3c-3522-4aaa-83c7-258752abe824";
    private static final String CTASLEVEL11_1_PROCESS        = "1339782e-fde7-402b-8271-2f91a65396e9";
    private static final String CTASLEVEL11_PROCESS          = "64cde929-195a-4c90-a921-b8c4d79ddfcf";

    // Resolved after import
    private static final String CTASLEVEL11_1_TABLE_QUALIFIED_NAME = "default.ctaslevel11_1@cm";
    private static final String CTASLEVEL13_TABLE_QUALIFIED_NAME   = "default.ctaslevel13@cm";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    ExportService exportService;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private ImportService importService;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    private AtlasClassificationType classificationTypeT1;
    private AtlasClassificationType classificationTypeT2;
    private AtlasClassificationType classificationTypeT3;

    private long                    nextTimestamp;

    @DataProvider(name = "hiveDb")
    public static Object[][] getData(ITestContext context) throws IOException, AtlasBaseException {
        return getZipSource("hive_db_lineage.zip");
    }

    @DataProvider(name = "classificationLineage")
    public static Object[][] getClassificationData(ITestContext context) throws IOException, AtlasBaseException {
        return getZipSource("classificationLineage.zip");
    }

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        basicSetup(typeDefStore, typeRegistry);

        RequestContext.get().setImportInProgress(true);

        classificationTypeT1 = createNewClassification();
        classificationTypeT2 = createNewClassificationT2();
        classificationTypeT3 = createNewClassificationT3();

        createEntities(entityStore, ENTITIES_SUB_DIR, new String[] {"db", "table-columns"});

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

        AtlasExportRequest request     = getIncrementalRequest(0);
        InputStream        inputStream = runExportWithParameters(exportService, request);

        ZipSource                          source   = getZipSourceFromInputStream(inputStream);
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        int count = 0;

        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            assertNotNull(entry.getValue());

            count++;
        }

        nextTimestamp = updateTimesampForNextIncrementalExport(source);

        assertEquals(count, expectedEntityCount);
    }

    @Test(dependsOnMethods = "atT0_ReturnsAllEntities")
    public void atT1_NewClassificationAttachedToTable_ReturnsChangedTable() throws AtlasBaseException {
        final int expectedEntityCount = 1;

        entityStore.addClassifications(TABLE_GUID, ImmutableList.of(classificationTypeT1.createDefaultValue()));

        AtlasExportRequest request     = getIncrementalRequest(nextTimestamp);
        InputStream        inputStream = runExportWithParameters(exportService, request);

        ZipSource                          source   = getZipSourceFromInputStream(inputStream);
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        AtlasEntity entity = null;

        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            entity = entry.getValue();

            assertNotNull(entity);
            break;
        }

        nextTimestamp = updateTimesampForNextIncrementalExport(source);

        assertEquals(entity.getGuid(), TABLE_GUID);
    }

    @Test(dependsOnMethods = "atT1_NewClassificationAttachedToTable_ReturnsChangedTable")
    public void atT2_NewClassificationAttachedToColumn_ReturnsChangedColumn() throws AtlasBaseException {
        final int expectedEntityCount = 1;

        AtlasEntity.AtlasEntityWithExtInfo tableEntity                   = entityStore.getById(TABLE_GUID);
        long                               preExportTableEntityTimestamp = tableEntity.getEntity().getUpdateTime().getTime();

        entityStore.addClassifications(COLUMN_GUID_HIGH, ImmutableList.of(typeRegistry.getClassificationTypeByName("T1").createDefaultValue()));

        InputStream inputStream = runExportWithParameters(exportService, getIncrementalRequest(nextTimestamp));

        ZipSource                          source   = getZipSourceFromInputStream(inputStream);
        AtlasEntity.AtlasEntityWithExtInfo entities = getEntities(source, expectedEntityCount);

        for (Map.Entry<String, AtlasEntity> entry : entities.getReferredEntities().entrySet()) {
            AtlasEntity entity = entry.getValue();

            assertNotNull(entity.getGuid());
            break;
        }

        long postUpdateTableEntityTimestamp = tableEntity.getEntity().getUpdateTime().getTime();

        assertEquals(preExportTableEntityTimestamp, postUpdateTableEntityTimestamp);
    }

    @Test(dependsOnMethods = "atT2_NewClassificationAttachedToColumn_ReturnsChangedColumn")
    public void exportingWithSameParameters_Succeeds() {
        InputStream inputStream = runExportWithParameters(exportService, getIncrementalRequest(nextTimestamp));

        assertNotNull(getZipSourceFromInputStream(inputStream));
    }

    @Test
    public void connectedExport() {
        InputStream        inputStream      = runExportWithParameters(exportService, getConnected());
        ZipSource          source           = getZipSourceFromInputStream(inputStream);
        UniqueList<String> creationOrder    = new UniqueList<>();
        List<String>       zipCreationOrder = source.getCreationOrder();

        creationOrder.addAll(zipCreationOrder);

        assertNotNull(source);
        assertEquals(creationOrder.size(), zipCreationOrder.size());
    }

    @Test(dataProvider = "hiveDb")
    public void importHiveDb(InputStream stream) throws AtlasBaseException, IOException {
        runImportWithNoParameters(importService, stream);
    }

    @Test(dataProvider = "classificationLineage")
    public void classificationineageDb(InputStream stream) throws AtlasBaseException, IOException {
        runImportWithNoParameters(importService, stream);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableInrementalConnected() throws AtlasBaseException, IOException {
        InputStream source     = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, 0, true));
        ZipSource   sourceCopy = getZipSourceCopy(source);

        verifyExpectedEntities(getFileNames(sourceCopy), GUID_DB, GUID_TABLE_CTAS_2);

        nextTimestamp = updateTimesampForNextIncrementalExport(sourceCopy);

        try {
            source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, nextTimestamp, true));
        } catch (SkipException e) {
            throw e;
        }

        entityStore.addClassifications(GUID_TABLE_CTAS_2, ImmutableList.of(classificationTypeT1.createDefaultValue()));

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, nextTimestamp, true));
        verifyExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_CTAS_2);
    }

    @Test(dependsOnMethods = "classificationineageDb")
    public void exportTableInrementalConnectedClassificationLineage() throws AtlasBaseException, IOException {
        InputStream source     = runExportWithParameters(exportService, getExportRequestForHiveTable(CTASLEVEL11_1_TABLE_QUALIFIED_NAME, EXPORT_INCREMENTAL, 0, false));
        ZipSource   sourceCopy = getZipSourceCopy(source);
        if  (entityStore.getClassification(FIRSTPARENT, "firstclassi") == null) {
            entityStore.addClassification(Arrays.asList(FIRSTPARENT), new AtlasClassification("firstclassi", null));
        }

        verifyExpectedEntities(getFileNames(sourceCopy), HDFS_PATH, HIVEDB, CTLASLEVEL11_1, CTASLEVEL11_1_PROCESS, CTASLEVEL11_PROCESS, CTASLEVEL11, SECONDPARENT_PROCESS,
                SECONDPARENT, FIRSTPARENT);

        nextTimestamp = updateTimesampForNextIncrementalExport(sourceCopy);

        entityStore.deleteClassification(FIRSTPARENT, "firstclassi", FIRSTPARENT);

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(CTASLEVEL11_1_TABLE_QUALIFIED_NAME, EXPORT_INCREMENTAL, nextTimestamp, false));
        verifyExpectedEntities(getFileNames(getZipSourceCopy(source)), CTLASLEVEL11_1, CTASLEVEL11_1_PROCESS, CTASLEVEL11_PROCESS, CTASLEVEL11, SECONDPARENT);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableIncrementalForParentEntity() throws AtlasBaseException, IOException {
        InputStream source     = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_2, EXPORT_INCREMENTAL, 0, false));
        ZipSource   sourceCopy = getZipSourceCopy(source);

        verifyExpectedEntities(getFileNames(sourceCopy), GUID_DB, GUID_HIVE_PROCESS, GUID_TABLE_2, GUID_TABLE_CTAS_2);

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_2, EXPORT_INCREMENTAL, 0, false));

        verifyUnExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_1);

        nextTimestamp = updateTimesampForNextIncrementalExport(sourceCopy);

        try {
            source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_2, EXPORT_INCREMENTAL, nextTimestamp, false));
        } catch (SkipException e) {
            throw e;
        }

        entityStore.addClassifications(GUID_TABLE_1, ImmutableList.of(classificationTypeT2.createDefaultValue()));
        entityStore.addClassifications(GUID_TABLE_2, ImmutableList.of(classificationTypeT2.createDefaultValue()));

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_2, EXPORT_INCREMENTAL, nextTimestamp, false));

        verifyExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_2);

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_2, EXPORT_INCREMENTAL, nextTimestamp, false));

        verifyUnExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_1);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableIncremental() throws AtlasBaseException, IOException {
        InputStream source     = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, 0, true));
        ZipSource   sourceCopy = getZipSourceCopy(source);

        verifyExpectedEntities(getFileNames(sourceCopy), GUID_DB, GUID_TABLE_CTAS_2);

        nextTimestamp = updateTimesampForNextIncrementalExport(sourceCopy);

        entityStore.addClassifications(GUID_TABLE_1, ImmutableList.of(classificationTypeT3.createDefaultValue()));
        entityStore.addClassifications(GUID_TABLE_CTAS_2, ImmutableList.of(classificationTypeT3.createDefaultValue()));

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, nextTimestamp, false));

        verifyExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_CTAS_2);

        source = runExportWithParameters(exportService, getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_INCREMENTAL, nextTimestamp, false));

        verifyUnExpectedEntities(getFileNames(getZipSourceCopy(source)), GUID_TABLE_1);
    }

    private long updateTimesampForNextIncrementalExport(ZipSource source) throws AtlasBaseException {
        return source.getExportResult().getChangeMarker();
    }

    private AtlasClassificationType createNewClassification() {
        createTypes(typeDefStore, ENTITIES_SUB_DIR, "typesdef-new-classification");

        return typeRegistry.getClassificationTypeByName("T1");
    }

    private AtlasClassificationType createNewClassificationT2() {
        createTypes(typeDefStore, ENTITIES_SUB_DIR, "typesdef-new-classification-T2");

        return typeRegistry.getClassificationTypeByName("T2");
    }

    private AtlasClassificationType createNewClassificationT3() {
        createTypes(typeDefStore, ENTITIES_SUB_DIR, "typedef-new-classification-T3");

        return typeRegistry.getClassificationTypeByName("T3");
    }

    private ZipSource getZipSourceFromInputStream(InputStream inputStream) {
        try {
            return new ZipSource(inputStream);
        } catch (IOException | AtlasBaseException e) {
            return null;
        }
    }

    private AtlasExportRequest getIncrementalRequest(long timestamp) {
        try {
            AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, EXPORT_REQUEST_INCREMENTAL, AtlasExportRequest.class);

            request.getOptions().put(FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, timestamp);

            return request;
        } catch (IOException e) {
            throw new SkipException(String.format("getIncrementalRequest: '%s' could not be loaded.", EXPORT_REQUEST_INCREMENTAL));
        }
    }

    private AtlasExportRequest getConnected() {
        try {
            return TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, EXPORT_REQUEST_CONNECTED, AtlasExportRequest.class);
        } catch (IOException e) {
            throw new SkipException(String.format("getIncrementalRequest: '%s' could not be loaded.", EXPORT_REQUEST_CONNECTED));
        }
    }

    private AtlasExportRequest getExportRequestForHiveTable(String name, String fetchType, long changeMarker, boolean skipLineage) {
        AtlasExportRequest  request       = new AtlasExportRequest();
        List<AtlasObjectId> itemsToExport = new ArrayList<>();

        itemsToExport.add(new AtlasObjectId("hive_table", "qualifiedName", name));
        request.setItemsToExport(itemsToExport);
        request.setOptions(getOptionsMap(fetchType, changeMarker, skipLineage));

        return request;
    }

    private Map<String, Object> getOptionsMap(String fetchType, long changeMarker, boolean skipLineage) {
        Map<String, Object> optionsMap = new HashMap<>();

        optionsMap.put("fetchType", fetchType.isEmpty() ? "full" : fetchType);
        optionsMap.put("changeMarker", changeMarker);
        optionsMap.put("skipLineage", skipLineage);

        return optionsMap;
    }

    private void verifyExpectedEntities(List<String> fileNames, String... guids) {
        assertEquals(fileNames.size(), guids.length);

        for (String guid : guids) {
            assertTrue(fileNames.contains(guid.toLowerCase()));
        }
    }

    private void verifyUnExpectedEntities(List<String> fileNames, String... guids) {
        for (String guid : guids) {
            assertFalse(fileNames.contains(guid.toLowerCase()));
        }
    }

    private List<String> getFileNames(ZipSource zipSource) {
        List<String> ret = new ArrayList<>();

        assertTrue(zipSource.hasNext());

        while (zipSource.hasNext()) {
            AtlasEntity atlasEntity = zipSource.next();

            assertNotNull(atlasEntity);

            ret.add(atlasEntity.getGuid());
        }

        return ret;
    }

    private ZipSource getZipSourceCopy(InputStream is) throws IOException, AtlasBaseException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        IOUtils.copy(is, baos);

        return new ZipSource(new ByteArrayInputStream(baos.toByteArray()));
    }
}
