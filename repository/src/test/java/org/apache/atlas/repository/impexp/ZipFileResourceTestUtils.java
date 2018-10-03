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
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public static List<String> getAllModels(String dirName) throws IOException {
        List<String> ret                  = null;
        File         topModelsDir         = new File(System.getProperty("user.dir") + "/../addons/models");
        File[]       topModelsDirContents = topModelsDir.exists() ? topModelsDir.listFiles() : null;

        assertTrue(topModelsDirContents != null, topModelsDir.getAbsolutePath() + ": unable to find/read directory");
        if(topModelsDirContents != null) {
            Arrays.sort(topModelsDirContents);
            for (File modelDir : topModelsDirContents) {
                if (modelDir.exists() && modelDir.isDirectory() && modelDir.getAbsolutePath().contains(dirName)) {
                    File[] models = modelDir.listFiles();
                    Arrays.sort(models);
                    ret = new ArrayList<>();
                    for (File model : Objects.requireNonNull(models)) {
                        ret.add(getFileContents(modelDir, model.getName()));
                    }

                }

                if (ret != null && ret.size() > 0) {
                    break;
                }
            }
        } else {
            throw new IOException("Unable to retrieve model contents.");
        }

        return ret;
    }

    public static String getModelJson(String fileName) throws IOException {
        String  ret                 = null;
        File   topModelsDir         = new File(System.getProperty("user.dir") + "/../addons/models");
        File[] topModelsDirContents = topModelsDir.exists() ? topModelsDir.listFiles() : null;

        assertTrue(topModelsDirContents != null, topModelsDir.getAbsolutePath() + ": unable to find/read directory");
        if(topModelsDirContents != null) {
            Arrays.sort(topModelsDirContents);
            for (File modelDir : topModelsDirContents) {
                if (modelDir.exists() && modelDir.isDirectory()) {
                    ret = getFileContents(modelDir, fileName);

                    if (ret != null) {
                        break;
                    }
                }
            }

            if (ret == null) {
                ret = getFileContents(topModelsDir, fileName);
            }

            assertTrue(ret != null, fileName + ": unable to find model file");
        } else {
            throw new IOException("Unable to retrieve model contents.");
        }

        return ret;
    }

    public static String getFileContents(File dir, String fileName) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            File file = new File(dir, fileName);

            if (file.exists() && file.isFile()) {
                return FileUtils.readFileToString(file);
            }
        }

        return null;
    }

    public static String getModelJsonFromResources(String fileName) throws IOException {
        String filePath = TestResourceFileUtils.getTestFilePath(fileName);
        File f = new File(filePath);
        String s = FileUtils.readFileToString(f);
        assertFalse(StringUtils.isEmpty(s), "Model file read correctly from resources!");

        return s;
    }

    public static Object[][] getZipSource(String fileName) throws IOException, AtlasBaseException {
        return new Object[][]{{getZipSourceFrom(fileName)}};
    }

    public static ZipSource getZipSourceFrom(String fileName) throws IOException, AtlasBaseException {
        FileInputStream fs = ZipFileResourceTestUtils.getFileInputStream(fileName);

        return new ZipSource(fs);
    }

    private static ZipSource getZipSourceFrom(ByteArrayOutputStream baos) throws IOException, AtlasBaseException {
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

    public static void createAtlasEntity(AtlasEntityStoreV2 entityStoreV1, AtlasEntity.AtlasEntityWithExtInfo atlasEntity) {
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

    public static AtlasEntity.AtlasEntityWithExtInfo getEntities(ZipSource source, int expectedCount) {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        int count = 0;
        for (String s : source.getCreationOrder()) {
            AtlasEntity entity = source.getByGuid(s);
            entityWithExtInfo.addReferredEntity(s, entity);
            count++;
        }

        assertEquals(count, expectedCount);
        return entityWithExtInfo;
    }

    public static void loadModelFromJson(String fileName, AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        AtlasTypesDef typesFromJson = getAtlasTypesDefFromFile(fileName);
        addReplicationAttributes(typesFromJson);
        createTypesAsNeeded(typesFromJson, typeDefStore, typeRegistry);
    }

    public static void loadAllModels(String dirName, AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        List<String>  allModels     = getAllModels(dirName);
        for (String model : allModels) {
            AtlasTypesDef typesFromJson = AtlasJson.fromJson(model, AtlasTypesDef.class);
            createTypesAsNeeded(typesFromJson, typeDefStore, typeRegistry);
        }
    }

    private static void addReplicationAttributes(AtlasTypesDef typesFromJson) throws IOException {
        if(typesFromJson.getEntityDefs() == null || typesFromJson.getEntityDefs().size() == 0) return;

        AtlasEntityDef ed = typesFromJson.getEntityDefs().get(0);
        if(!ed.getName().equals("Referenceable")) return;

        String replAttr1Json = TestResourceFileUtils.getJson("stocksDB-Entities","replicationAttrs");
        String replAttr2Json = StringUtils.replace(replAttr1Json, "From", "To");

        ed.addAttribute(AtlasType.fromJson(replAttr1Json, AtlasStructDef.AtlasAttributeDef.class));
        ed.addAttribute(AtlasType.fromJson(replAttr2Json, AtlasStructDef.AtlasAttributeDef.class));
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
        if(sampleTypes == null) return null;
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

        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);

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
        loadModelFromJson("1020-fs_model.json", typeDefStore, typeRegistry);
    }

    public static void loadHiveModel(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) throws IOException, AtlasBaseException {
        loadModelFromJson("1030-hive_model.json", typeDefStore, typeRegistry);
    }
}
