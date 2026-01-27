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
package org.apache.atlas.repository.migration;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.GraphDBMigrator;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;

import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DataMigrationServiceTest {
    @Mock
    private GraphDBMigrator mockMigrator;
    @Mock
    private AtlasTypeDefStore mockTypeDefStore;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private GraphBackedSearchIndexer mockIndexer;
    @Mock
    private AtlasTypeDefStoreInitializer mockStoreInitializer;
    @Mock
    private AtlasTypeRegistry mockTypeRegistry;
    @Mock
    private ImportService mockImportService;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDataMigrationServiceConstants() throws Exception {
        // Test FILE_EXTENSION_ZIP constant
        Field fileExtensionZipField = DataMigrationService.class.getDeclaredField("FILE_EXTENSION_ZIP");
        fileExtensionZipField.setAccessible(true);
        String fileExtensionZip = (String) fileExtensionZipField.get(null);
        assertEquals(fileExtensionZip, ".zip");

        // Test ATLAS_MIGRATION_DATA_NAME constant
        Field atlasMigrationDataNameField = DataMigrationService.class.getDeclaredField("ATLAS_MIGRATION_DATA_NAME");
        atlasMigrationDataNameField.setAccessible(true);
        String atlasMigrationDataName = (String) atlasMigrationDataNameField.get(null);
        assertEquals(atlasMigrationDataName, "atlas-migration-data.json");

        // Test ATLAS_MIGRATION_TYPESDEF_NAME constant
        Field atlasMigrationTypesdefNameField = DataMigrationService.class.getDeclaredField("ATLAS_MIGRATION_TYPESDEF_NAME");
        atlasMigrationTypesdefNameField.setAccessible(true);
        String atlasMigrationTypesdefName = (String) atlasMigrationTypesdefNameField.get(null);
        assertEquals(atlasMigrationTypesdefName, "atlas-migration-typesdef.json");
    }

    @Test
    public void testLoggerField() throws Exception {
        Field loggerField = DataMigrationService.class.getDeclaredField("LOG");
        loggerField.setAccessible(true);
        Logger logger = (Logger) loggerField.get(null);
        assertNotNull(logger);
        assertEquals(logger.getName(), DataMigrationService.class.getName());
    }

    @Test
    public void testFileImporterInnerClass() {
        Class<?>[] innerClasses = DataMigrationService.class.getDeclaredClasses();
        boolean foundFileImporter = false;
        for (Class<?> innerClass : innerClasses) {
            if (innerClass.getSimpleName().equals("FileImporter")) {
                foundFileImporter = true;
                assertTrue(Runnable.class.isAssignableFrom(innerClass));
                break;
            }
        }
        assertTrue(foundFileImporter, "FileImporter inner class should exist");
    }

    @Test
    public void testFileImporterConstructor() {
        Class<?> fileImporterClass = getFileImporterClass();
        assertNotNull(fileImporterClass);
        try {
            Constructor<?> constructor = fileImporterClass.getDeclaredConstructor(GraphDBMigrator.class, AtlasTypeDefStore.class, AtlasTypeRegistry.class, AtlasTypeDefStoreInitializer.class, String.class, GraphBackedSearchIndexer.class);
            assertNotNull(constructor);
        } catch (NoSuchMethodException e) {
            fail("FileImporter constructor should exist");
        }
    }

    @Test
    public void testFileImporterFieldsExist() throws Exception {
        Class<?> fileImporterClass = getFileImporterClass();
        // Test that required fields exist
        Field migratorField = fileImporterClass.getDeclaredField("migrator");
        assertEquals(migratorField.getType(), GraphDBMigrator.class);
        Field typeDefStoreField = fileImporterClass.getDeclaredField("typeDefStore");
        assertEquals(typeDefStoreField.getType(), AtlasTypeDefStore.class);
        Field importDirectoryField = fileImporterClass.getDeclaredField("importDirectory");
        assertEquals(importDirectoryField.getType(), String.class);
        Field indexerField = fileImporterClass.getDeclaredField("indexer");
        assertEquals(indexerField.getType(), GraphBackedSearchIndexer.class);
        Field typeRegistryField = fileImporterClass.getDeclaredField("typeRegistry");
        assertEquals(typeRegistryField.getType(), AtlasTypeRegistry.class);
        Field storeInitializerField = fileImporterClass.getDeclaredField("storeInitializer");
        assertEquals(storeInitializerField.getType(), AtlasTypeDefStoreInitializer.class);
    }

    @Test
    public void testFileImporterPrivateMethods() throws Exception {
        Class<?> fileImporterClass = getFileImporterClass();
        // Test performAccessChecks method exists
        Method performAccessChecksMethod = fileImporterClass.getDeclaredMethod("performAccessChecks", String.class);
        assertNotNull(performAccessChecksMethod);
        assertEquals(performAccessChecksMethod.getReturnType(), boolean.class);
        // Test getFileFromImportDirectory method exists
        Method getFileFromImportDirectoryMethod = fileImporterClass.getDeclaredMethod("getFileFromImportDirectory", String.class, String.class);
        assertNotNull(getFileFromImportDirectoryMethod);
        assertEquals(getFileFromImportDirectoryMethod.getReturnType(), File.class);
        // Test performInit method exists
        Method performInitMethod = fileImporterClass.getDeclaredMethod("performInit");
        assertNotNull(performInitMethod);
        assertEquals(performInitMethod.getReturnType(), void.class);
        // Test processIncomingTypesDef method exists
        Method processIncomingTypesDefMethod = fileImporterClass.getDeclaredMethod("processIncomingTypesDef", File.class);
        assertNotNull(processIncomingTypesDefMethod);
        assertEquals(processIncomingTypesDefMethod.getReturnType(), void.class);
        // Test performImport method exists
        Method performImportMethod = fileImporterClass.getDeclaredMethod("performImport");
        assertNotNull(performImportMethod);
        assertEquals(performImportMethod.getReturnType(), void.class);
    }

    @Test
    public void testPerformAccessChecksWithValidPath() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method performAccessChecksMethod = getFileImporterClass().getDeclaredMethod("performAccessChecks", String.class);
        performAccessChecksMethod.setAccessible(true);
        // Create a temporary directory for testing
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "atlas-test-" + System.currentTimeMillis());
        tempDir.mkdirs();
        tempDir.deleteOnExit();
        Boolean result = (Boolean) performAccessChecksMethod.invoke(fileImporter, tempDir.getAbsolutePath());
        assertTrue(result);
        tempDir.delete();
    }

    @Test
    public void testPerformAccessChecksWithInvalidPath() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method performAccessChecksMethod = getFileImporterClass().getDeclaredMethod("performAccessChecks", String.class);
        performAccessChecksMethod.setAccessible(true);
        Boolean result = (Boolean) performAccessChecksMethod.invoke(fileImporter, "/non/existent/path");
        assertFalse(result);
    }

    @Test
    public void testPerformAccessChecksWithNullPath() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method performAccessChecksMethod = getFileImporterClass().getDeclaredMethod("performAccessChecks", String.class);
        performAccessChecksMethod.setAccessible(true);
        Boolean result = (Boolean) performAccessChecksMethod.invoke(fileImporter, (String) null);
        assertFalse(result);
    }

    @Test
    public void testPerformAccessChecksWithEmptyPath() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method performAccessChecksMethod = getFileImporterClass().getDeclaredMethod("performAccessChecks", String.class);
        performAccessChecksMethod.setAccessible(true);
        Boolean result = (Boolean) performAccessChecksMethod.invoke(fileImporter, "");
        assertFalse(result);
    }

    @Test
    public void testGetFileFromImportDirectory() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method getFileFromImportDirectoryMethod = getFileImporterClass().getDeclaredMethod("getFileFromImportDirectory", String.class, String.class);
        getFileFromImportDirectoryMethod.setAccessible(true);
        File result = (File) getFileFromImportDirectoryMethod.invoke(fileImporter, "/test/path", "test-file.json");
        assertNotNull(result);
        assertEquals(result.getPath(), Paths.get("/test/path", "test-file.json").toString());
    }

    @Test
    public void testGetFileFromImportDirectoryWithNullDirectory() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method getFileFromImportDirectoryMethod = getFileImporterClass().getDeclaredMethod("getFileFromImportDirectory", String.class, String.class);
        getFileFromImportDirectoryMethod.setAccessible(true);
        File result = (File) getFileFromImportDirectoryMethod.invoke(fileImporter, null, "test-file.json");
        assertNotNull(result);
    }

    @Test
    public void testFileImporterRunMethod() throws Exception {
        Object fileImporter = createFileImporterInstance();
        Method runMethod = getFileImporterClass().getDeclaredMethod("run");
        assertNotNull(runMethod);
        assertTrue(java.lang.reflect.Modifier.isPublic(runMethod.getModifiers()));
    }

    @Test
    public void testStringUtilsUsagePattern() {
        // Test the StringUtils usage pattern from the constructor
        String zipFile = "test.zip";
        String txtFile = "test.txt";
        assertTrue(StringUtils.endsWithIgnoreCase(zipFile, ".zip"));
        assertFalse(StringUtils.endsWithIgnoreCase(txtFile, ".zip"));
        assertTrue(StringUtils.endsWithIgnoreCase("TEST.ZIP", ".zip"));
    }

    @Test
    public void testGetFileNameMethod() throws Exception {
        // Test the getFileName method signature
        Method getFileNameMethod = DataMigrationService.class.getDeclaredMethod("getFileName");
        assertNotNull(getFileNameMethod);
        assertEquals(getFileNameMethod.getReturnType(), String.class);
        assertTrue(java.lang.reflect.Modifier.isPublic(getFileNameMethod.getModifiers()));
    }

    @Test
    public void testAtlasMigrationModeFilenameConstant() {
        assertEquals(ATLAS_MIGRATION_MODE_FILENAME, "atlas.migration.data.filename");
    }

    @Test
    public void testServiceInterfaceImplementation() {
        assertTrue(org.apache.atlas.service.Service.class.isAssignableFrom(DataMigrationService.class));
    }

    @Test
    public void testStartAndStopMethods() throws Exception {
        Method startMethod = DataMigrationService.class.getDeclaredMethod("start");
        assertNotNull(startMethod);
        assertTrue(java.lang.reflect.Modifier.isPublic(startMethod.getModifiers()));
        Method stopMethod = DataMigrationService.class.getDeclaredMethod("stop");
        assertNotNull(stopMethod);
        assertTrue(java.lang.reflect.Modifier.isPublic(stopMethod.getModifiers()));
    }

    @Test
    public void testFieldsAccessibility() throws Exception {
        Field configurationField = DataMigrationService.class.getDeclaredField("configuration");
        assertEquals(configurationField.getType(), Configuration.class);
        assertTrue(java.lang.reflect.Modifier.isFinal(configurationField.getModifiers()));
        Field threadField = DataMigrationService.class.getDeclaredField("thread");
        assertEquals(threadField.getType(), Thread.class);
        assertTrue(java.lang.reflect.Modifier.isFinal(threadField.getModifiers()));
    }

    @Test
    public void testConfigurationStringRetrieval() {
        when(mockConfiguration.getString(anyString(), anyString())).thenReturn("test-value");
        String result = mockConfiguration.getString(ATLAS_MIGRATION_MODE_FILENAME, "");
        assertEquals(result, "test-value");
        verify(mockConfiguration).getString(ATLAS_MIGRATION_MODE_FILENAME, "");
    }

    @Test
    public void testFileInputStreamUsage() throws Exception {
        // Test File and FileInputStream usage patterns
        File tempFile = File.createTempFile("atlas-test", ".json");
        tempFile.deleteOnExit();
        try (FileInputStream fis = new FileInputStream(tempFile)) {
            assertNotNull(fis);
        }
    }

    @Test
    public void testAtlasImportResultCreation() {
        AtlasImportResult result = new AtlasImportResult();
        assertNotNull(result);
        assertNotNull(result.getMetrics());
    }

    @Test
    public void testAtlasTypesDefCreation() {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        assertNotNull(typesDef);
        assertTrue(typesDef.isEmpty());
    }

    @Test
    public void testExceptionTypes() {
        AtlasException atlasException = new AtlasException("Test message");
        assertEquals(atlasException.getMessage(), "Test message");
        AtlasBaseException baseException = new AtlasBaseException("Test base message");
        assertEquals(baseException.getMessage(), "Test base message");
    }

    @Test
    public void testThreadCreationPattern() {
        Runnable testRunnable = new Runnable() {
            @Override
            public void run() {
                // Empty implementation for testing
            }
        };
        Thread thread1 = new Thread(testRunnable, "zipFileBasedMigrationImporter");
        assertEquals(thread1.getName(), "zipFileBasedMigrationImporter");
        Thread thread2 = new Thread(testRunnable);
        assertNotNull(thread2);
    }

    @Test
    public void testMockingPatterns() throws Exception {
        when(mockMigrator.getScrubbedTypesDef(anyString())).thenReturn(new AtlasTypesDef());
        doNothing().when(mockIndexer).instanceIsActive();
        doNothing().when(mockStoreInitializer).instanceIsActive();
        AtlasTypesDef result = mockMigrator.getScrubbedTypesDef("test json");
        assertNotNull(result);
        verify(mockMigrator).getScrubbedTypesDef("test json");
    }

    @Test
    public void testArgumentMatchersUsage() {
        when(mockConfiguration.getString(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn("matched-value");
        String result = mockConfiguration.getString("any.key", "default");
        assertEquals(result, "matched-value");
        verify(mockConfiguration).getString(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    private Class<?> getFileImporterClass() {
        for (Class<?> innerClass : DataMigrationService.class.getDeclaredClasses()) {
            if (innerClass.getSimpleName().equals("FileImporter")) {
                return innerClass;
            }
        }
        return null;
    }

    private Object createFileImporterInstance() throws Exception {
        Class<?> fileImporterClass = getFileImporterClass();
        Constructor<?> constructor = fileImporterClass.getDeclaredConstructor(GraphDBMigrator.class, AtlasTypeDefStore.class, AtlasTypeRegistry.class, AtlasTypeDefStoreInitializer.class, String.class, GraphBackedSearchIndexer.class);
        constructor.setAccessible(true);
        return constructor.newInstance(mockMigrator, mockTypeDefStore, mockTypeRegistry,
            mockStoreInitializer, "/test/path", mockIndexer);
    }
}
