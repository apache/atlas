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
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

public class ZipFileResourceTestUtils {
    public static final Logger LOG = LoggerFactory.getLogger(ZipFileResourceTestUtils.class);

    public static FileInputStream getFileInputStream(String fileName) {
        String filePath = getFileFromResources(fileName);
        File f = new File(filePath);
        FileInputStream fs = null;
        try {
            fs = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            LOG.error("File could not be found at: %s", filePath, e);
        }
        return fs;
    }

    private static String getFileFromResources(String fileName) {
        final String userDir = System.getProperty("user.dir");
        return getFilePath(userDir, fileName);
    }

    private static String getFilePath(String startPath, String fileName) {
        return startPath + "/src/test/resources/" + fileName;
    }

    public static String getModelJson(String fileName) throws IOException {
        final String userDir = System.getProperty("user.dir");
        String filePath = userDir + "/../addons/models/" + fileName;
        File f = new File(filePath);
        String s = FileUtils.readFileToString(f);
        assertFalse(StringUtils.isEmpty(s), "Model file read correctly!");

        return s;
    }

    public static String getModelJsonFromResources(String fileName) throws IOException {
        String filePath = getFileFromResources(fileName);
        File f = new File(filePath);
        String s = FileUtils.readFileToString(f);
        assertFalse(StringUtils.isEmpty(s), "Model file read correctly from resources!");

        return s;
    }

    public static Object[][] getZipSource(String fileName) throws IOException {
        FileInputStream fs = ZipFileResourceTestUtils.getFileInputStream(fileName);

        return new Object[][]{{new ZipSource(fs)}};
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
        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesFromJson, typeRegistry);

        if (!typesToCreate.isEmpty()) {
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
}
