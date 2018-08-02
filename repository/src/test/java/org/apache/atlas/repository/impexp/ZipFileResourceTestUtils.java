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

import com.google.common.collect.Sets;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStreamForImport;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ZipFileResourceTestUtils {
    public static final Logger LOG = LoggerFactory.getLogger(ZipFileResourceTestUtils.class);

    public static FileInputStream getFileInputStream(String fileName) {
        return TestResourceFileUtils.getFileInputStream(fileName);
    }

    public static String getModelJson(String fileName) throws IOException {
        final String userDir = System.getProperty("user.dir");
        String filePath = userDir + "/../addons/models/" + fileName;
        File f = new File(filePath);
        String s = FileUtils.readFileToString(f);
        assertFalse(StringUtils.isEmpty(s), "Model file read correctly!");
        if(StringUtils.isEmpty(s)) {
            throw new IOException("Unable to read file: " + fileName);
        }

        return s;
    }

    public static String getModelJsonFromResources(String fileName) throws IOException {
        String filePath = TestResourceFileUtils.getTestFilePath(fileName);
        File f = new File(filePath);
        String s = FileUtils.readFileToString(f);
        assertFalse(StringUtils.isEmpty(s), "Model file read correctly from resources!");

        return s;
    }

    public static Object[][] getZipSource(String fileName) throws IOException {
        return new Object[][]{{getZipSourceFrom(fileName)}};
    }

    public static ZipSource getZipSourceFrom(String fileName) throws IOException {
        FileInputStream fs = ZipFileResourceTestUtils.getFileInputStream(fileName);

        return new ZipSource(fs);
    }

    private static ZipSource getZipSourceFrom(ByteArrayOutputStream baos) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
        ZipSource zipSource = new ZipSource(bis);
        return zipSource;
    }

    public static void verifyImportedEntities(List<String> creationOrder, List<String> processedEntities) {
        Set<String> lhs = com.google.common.collect.Sets.newHashSet(creationOrder);
        Set<String> rhs = com.google.common.collect.Sets.newHashSet(processedEntities);
        Set<String> difference = Sets.difference(lhs, rhs);

        assertNotNull(difference);
        assertEquals(difference.size(), 0);
    }

    public static void verifyImportedMetrics(AtlasExportResult exportResult, AtlasImportResult importResult) {
        Map<String, Integer> metricsForCompare = getImportMetricsForCompare(importResult);
        for (Map.Entry<String, Integer> entry : exportResult.getMetrics().entrySet()) {
            if(entry.getKey().startsWith("entity") == false ||
                    entry.getKey().contains("withExtInfo") ||
                    entry.getKey().contains("Column") ||
                    entry.getKey().contains("StorageDesc")) continue;

            assertTrue(metricsForCompare.containsKey(entry.getKey()), entry.getKey());
            assertEquals(entry.getValue(), metricsForCompare.get(entry.getKey()), entry.getKey());
        }
    }

    public static AtlasTypesDef loadTypes(String entitiesSubDir, String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(entitiesSubDir, fileName, AtlasTypesDef.class);
        } catch (IOException e) {
            throw new SkipException(String.format("createTypes: '%s' could not be laoded.", fileName));
        }
    }


    public static AtlasEntity.AtlasEntityWithExtInfo loadEntity(String entitiesSubDir, String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(entitiesSubDir, fileName, AtlasEntity.AtlasEntityWithExtInfo.class);
        } catch (IOException e) {
            throw new SkipException(String.format("createTypes: '%s' could not be laoded.", fileName));
        }
    }

    public static void createTypes(AtlasTypeDefStore typeDefStore, String entitiesSubDir, String typesDef) {
        try {
            typeDefStore.createTypesDef(loadTypes(entitiesSubDir, typesDef));
        } catch (AtlasBaseException e) {
            throw new SkipException("setup: could not load typesDef.");
        }
    }

    public static void createAtlasEntity(AtlasEntityStoreV1 entityStoreV1, AtlasEntity.AtlasEntityWithExtInfo atlasEntity) {
        try {
            EntityMutationResponse response = entityStoreV1.createOrUpdateForImport(new AtlasEntityStreamForImport(atlasEntity, null));
            assertNotNull(response);
            assertTrue((response.getCreatedEntities() != null && response.getCreatedEntities().size() > 0) ||
                    (response.getMutatedEntities() != null && response.getMutatedEntities().size() > 0));
        } catch (AtlasBaseException e) {
            throw new SkipException(String.format("createAtlasEntity: could not load '%s'.", atlasEntity.getEntity().getTypeName()));
        }
    }

    public static ZipSource runExportWithParameters(ExportService exportService, AtlasExportRequest request) {
        final String requestingIP = "1.0.0.0";
        final String hostName = "localhost";
        final String userName = "admin";

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZipSink zipSink = new ZipSink(baos);

            AtlasExportResult result = exportService.run(zipSink, request, userName, hostName, requestingIP);
            assertEquals(result.getOperationStatus(), AtlasExportResult.OperationStatus.SUCCESS);

            zipSink.close();
            return getZipSourceFrom(baos);
        }
        catch(Exception ex) {
            throw new SkipException(String.format("runExportWithParameters: %s: failed!", request.toString()));
        }
    }

    private static Map<String,Integer> getImportMetricsForCompare(AtlasImportResult result) {
        Map<String, Integer> r = new HashMap<>();
        for (Map.Entry<String, Integer> entry : result.getMetrics().entrySet()) {
            r.put(entry.getKey().replace(":updated", "").replace(":created", ""), entry.getValue());
        }

        return r;
    }

    public static void loadModelFromJson(String fileName, AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        AtlasTypesDef typesFromJson = getAtlasTypesDefFromFile(fileName);
        createTypesAsNeeded(typesFromJson, typeDefStore, typeRegistry);
    }

    public static void loadModelFromResourcesJson(String fileName, AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        AtlasTypesDef typesFromJson = getAtlasTypesDefFromResourceFile(fileName);
        createTypesAsNeeded(typesFromJson, typeDefStore, typeRegistry);
    }

    private static void createTypesAsNeeded(AtlasTypesDef typesFromJson, AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        if(typesFromJson == null) {
            return;
        }

        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesFromJson, typeRegistry);
        if (typesToCreate != null && !typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }
    }

    private static AtlasTypesDef getAtlasTypesDefFromFile(String fileName) throws IOException {
        String sampleTypes = ZipFileResourceTestUtils.getModelJson(fileName);
        return AtlasType.fromJson(sampleTypes, AtlasTypesDef.class);
    }

    private static AtlasTypesDef getAtlasTypesDefFromResourceFile(String fileName) throws IOException {
        String sampleTypes = getModelJsonFromResources(fileName);
        return AtlasType.fromJson(sampleTypes, AtlasTypesDef.class);
    }

    public static AtlasImportRequest getDefaultImportRequest() {
        return new AtlasImportRequest();
    }


    public static AtlasImportResult runImportWithParameters(ImportService importService, AtlasImportRequest request, ZipSource source) throws AtlasBaseException, IOException {
        final String requestingIP = "1.0.0.0";
        final String hostName = "localhost";
        final String userName = "admin";

        AtlasImportResult result = importService.run(source, request, userName, hostName, requestingIP);
        assertEquals(result.getOperationStatus(), AtlasImportResult.OperationStatus.SUCCESS);
        return result;
    }

    public static AtlasImportResult runImportWithNoParameters(ImportService importService, ZipSource source) throws AtlasBaseException, IOException {
        final String requestingIP = "1.0.0.0";
        final String hostName = "localhost";
        final String userName = "admin";

        AtlasImportResult result = importService.run(source, userName, hostName, requestingIP);
        assertEquals(result.getOperationStatus(), AtlasImportResult.OperationStatus.SUCCESS);
        return result;
    }

    public static void runAndVerifyQuickStart_v1_Import(ImportService importService, ZipSource zipSource) throws AtlasBaseException, IOException {
        AtlasExportResult exportResult = zipSource.getExportResult();
        List<String> creationOrder = zipSource.getCreationOrder();

        RequestContextV1.clear();
        RequestContextV1.get().setUser(TestUtilsV2.TEST_USER);

        AtlasImportRequest request = getDefaultImportRequest();
        AtlasImportResult result = runImportWithParameters(importService, request, zipSource);

        assertNotNull(result);
        verifyImportedMetrics(exportResult, result);
        verifyImportedEntities(creationOrder, result.getProcessedEntities());
    }

    public static void loadBaseModel(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        loadModelFromJson("0010-base_model.json", typeDefStore, typeRegistry);
    }

    public static void loadFsModel(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        loadModelFromJson("0020-fs_model.json", typeDefStore, typeRegistry);
    }

    public static void loadHiveModel(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        loadModelFromJson("0030-hive_model.json", typeDefStore, typeRegistry);
    }
}
