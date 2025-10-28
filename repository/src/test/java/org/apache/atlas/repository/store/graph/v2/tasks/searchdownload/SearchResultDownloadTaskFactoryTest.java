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
package org.apache.atlas.repository.store.graph.v2.tasks.searchdownload;

import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;

import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTaskFactory.MAX_PENDING_TASKS_ALLOWED;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTaskFactory.SEARCH_RESULT_DOWNLOAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchResultDownloadTaskFactoryTest {
    @Mock private AtlasDiscoveryService discoveryService;
    @Mock private AtlasTypeRegistry typeRegistry;
    @Mock private AtlasTask task;

    private SearchResultDownloadTaskFactory factory;
    private MockedStatic<Files> filesMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new SearchResultDownloadTaskFactory(discoveryService, typeRegistry);

        // Mock Files for file operations
        filesMock = mockStatic(Files.class);
    }

    @AfterMethod
    public void tearDown() {
        if (filesMock != null) {
            filesMock.close();
        }
    }

    @Test
    public void testCreateSearchResultDownloadTask() {
        when(task.getType()).thenReturn(SEARCH_RESULT_DOWNLOAD);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof SearchResultDownloadTask);
    }

    @Test
    public void testCreateUnknownTaskType() {
        when(task.getType()).thenReturn("UNKNOWN_TYPE");
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNull(result);
    }

    @Test
    public void testGetSupportedTypes() {
        List<String> supportedTypes = factory.getSupportedTypes();

        assertNotNull(supportedTypes);
        assertEquals(supportedTypes.size(), 1);
        assertTrue(supportedTypes.contains(SEARCH_RESULT_DOWNLOAD));
    }

    @Test
    public void testConstructor() {
        SearchResultDownloadTaskFactory testFactory = new SearchResultDownloadTaskFactory(discoveryService, typeRegistry);
        assertNotNull(testFactory);
    }

    @Test
    public void testStaticConstants() {
        assertEquals(SEARCH_RESULT_DOWNLOAD, "SEARCH_RESULT_DOWNLOAD");
        assertTrue(MAX_PENDING_TASKS_ALLOWED > 0);
    }

    @Test
    public void testDeleteFilesWithNullSubDirs() throws Exception {
        File mockDownloadDir = mock(File.class);
        when(mockDownloadDir.listFiles()).thenReturn(null);

        invokeDeleteFilesMethod(mockDownloadDir);
    }

    @Test
    public void testDeleteFilesWithEmptySubDirs() throws Exception {
        File mockDownloadDir = mock(File.class);
        when(mockDownloadDir.listFiles()).thenReturn(new File[0]);

        invokeDeleteFilesMethod(mockDownloadDir);
    }

    @Test
    public void testDeleteFilesWithSubDirsContainingFiles() throws Exception {
        File mockDownloadDir = mock(File.class);
        File mockSubDir = mock(File.class);
        File mockCsvFile = mock(File.class);

        when(mockDownloadDir.listFiles()).thenReturn(new File[] {mockSubDir});
        when(mockSubDir.listFiles()).thenReturn(new File[] {mockCsvFile});

        // Mock file attributes for expired file
        BasicFileAttributes mockAttrs = mock(BasicFileAttributes.class);
        when(mockAttrs.creationTime()).thenReturn(FileTime.from(Instant.now().minusSeconds(86400 * 2))); // 2 days old

        filesMock.when(() -> Files.readAttributes(any(), any(Class.class))).thenReturn(mockAttrs);

        when(mockCsvFile.toPath()).thenReturn(java.nio.file.Paths.get("test.csv"));
        when(mockCsvFile.delete()).thenReturn(true);

        invokeDeleteFilesMethod(mockDownloadDir);

        verify(mockCsvFile).delete();
    }

    @Test
    public void testDeleteFilesWithSubDirsContainingRecentFiles() throws Exception {
        File mockDownloadDir = mock(File.class);
        File mockSubDir = mock(File.class);
        File mockCsvFile = mock(File.class);

        when(mockDownloadDir.listFiles()).thenReturn(new File[] {mockSubDir});
        when(mockSubDir.listFiles()).thenReturn(new File[] {mockCsvFile});

        // Mock file attributes for recent file
        BasicFileAttributes mockAttrs = mock(BasicFileAttributes.class);
        when(mockAttrs.creationTime()).thenReturn(FileTime.from(Instant.now().minusSeconds(3600))); // 1 hour old

        filesMock.when(() -> Files.readAttributes(any(), any(Class.class))).thenReturn(mockAttrs);

        when(mockCsvFile.toPath()).thenReturn(java.nio.file.Paths.get("test.csv"));

        invokeDeleteFilesMethod(mockDownloadDir);

        verify(mockCsvFile, times(0)).delete(); // Should not delete recent files
    }

    @Test
    public void testDeleteFilesWithIOException() throws Exception {
        File mockDownloadDir = mock(File.class);
        File mockSubDir = mock(File.class);
        File mockCsvFile = mock(File.class);

        when(mockDownloadDir.listFiles()).thenReturn(new File[] {mockSubDir});
        when(mockSubDir.listFiles()).thenReturn(new File[] {mockCsvFile});

        filesMock.when(() -> Files.readAttributes(any(), any(Class.class))).thenThrow(new IOException("Test exception"));

        when(mockCsvFile.toPath()).thenReturn(java.nio.file.Paths.get("test.csv"));

        expectThrows(RuntimeException.class, () -> {
            try {
                invokeDeleteFilesMethod(mockDownloadDir);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testGetCronExpressionForCleanup() throws Exception {
        String cronExpression = invokeGetCronExpressionMethod();

        assertNotNull(cronExpression);
        assertEquals(cronExpression, "0 0/1 * * * *");
    }

    @Test
    public void testCreateWithNullTaskType() {
        when(task.getType()).thenReturn(null);
        when(task.getGuid()).thenReturn("test-guid");

        expectThrows(NullPointerException.class, () -> factory.create(task));
    }

    @Test
    public void testCreateWithEmptyTaskType() {
        when(task.getType()).thenReturn("");
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNull(result);
    }

    @Test
    public void testCreateWithValidTaskButNullGuid() {
        when(task.getType()).thenReturn(SEARCH_RESULT_DOWNLOAD);
        when(task.getGuid()).thenReturn(null);

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof SearchResultDownloadTask);
    }

    @Test
    public void testCreateWithValidTaskButEmptyGuid() {
        when(task.getType()).thenReturn(SEARCH_RESULT_DOWNLOAD);
        when(task.getGuid()).thenReturn("");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof SearchResultDownloadTask);
    }

    @Test
    public void testSupportedTypesImmutability() {
        List<String> supportedTypes1 = factory.getSupportedTypes();
        List<String> supportedTypes2 = factory.getSupportedTypes();

        // Test that the same content is returned
        assertEquals(supportedTypes1, supportedTypes2);
        assertEquals(supportedTypes1.size(), 1);
    }

    @Test
    public void testFactoryDependencyInjection() {
        // Test that all dependencies are properly injected
        assertNotNull(factory);

        // Create task to ensure dependencies are used
        when(task.getType()).thenReturn(SEARCH_RESULT_DOWNLOAD);
        AbstractTask downloadTask = factory.create(task);
        assertNotNull(downloadTask);
    }

    @Test
    public void testStaticInitializationConstants() throws Exception {
        // Test static initialization by accessing static fields
        Field maxPendingTasksField = SearchResultDownloadTaskFactory.class.getDeclaredField("MAX_PENDING_TASKS_ALLOWED");
        maxPendingTasksField.setAccessible(true);
        int maxPendingTasks = (int) maxPendingTasksField.get(null);

        assertTrue(maxPendingTasks > 0);

        Field fileExpDurationField = SearchResultDownloadTaskFactory.class.getDeclaredField("FILE_EXP_DURATION_IN_MILLIS");
        fileExpDurationField.setAccessible(true);
        long fileExpDuration = (long) fileExpDurationField.get(null);

        assertTrue(fileExpDuration > 0);
    }

    @Test
    public void testDeleteFilesWithSubDirContainingNullFiles() throws Exception {
        File mockDownloadDir = mock(File.class);
        File mockSubDir = mock(File.class);

        when(mockDownloadDir.listFiles()).thenReturn(new File[] {mockSubDir});
        when(mockSubDir.listFiles()).thenReturn(null);

        invokeDeleteFilesMethod(mockDownloadDir);
    }

    @Test
    public void testDeleteFilesWithMultipleSubDirsAndFiles() throws Exception {
        File mockDownloadDir = mock(File.class);
        File mockSubDir1 = mock(File.class);
        File mockSubDir2 = mock(File.class);
        File mockCsvFile1 = mock(File.class);
        File mockCsvFile2 = mock(File.class);

        when(mockDownloadDir.listFiles()).thenReturn(new File[] {mockSubDir1, mockSubDir2});
        when(mockSubDir1.listFiles()).thenReturn(new File[] {mockCsvFile1});
        when(mockSubDir2.listFiles()).thenReturn(new File[] {mockCsvFile2});

        // Mock file attributes for both files as expired
        BasicFileAttributes mockAttrs = mock(BasicFileAttributes.class);
        when(mockAttrs.creationTime()).thenReturn(FileTime.from(Instant.now().minusSeconds(86400 * 2))); // 2 days old

        filesMock.when(() -> Files.readAttributes(any(), any(Class.class))).thenReturn(mockAttrs);

        when(mockCsvFile1.toPath()).thenReturn(java.nio.file.Paths.get("test1.csv"));
        when(mockCsvFile2.toPath()).thenReturn(java.nio.file.Paths.get("test2.csv"));
        when(mockCsvFile1.delete()).thenReturn(true);
        when(mockCsvFile2.delete()).thenReturn(true);

        invokeDeleteFilesMethod(mockDownloadDir);

        verify(mockCsvFile1).delete();
        verify(mockCsvFile2).delete();
    }

    private void invokeDeleteFilesMethod(File downloadDir) throws Exception {
        Method deleteFilesMethod = SearchResultDownloadTaskFactory.class.getDeclaredMethod("deleteFiles", File.class);
        deleteFilesMethod.setAccessible(true);
        deleteFilesMethod.invoke(factory, downloadDir);
    }

    private String invokeGetCronExpressionMethod() throws Exception {
        Method getCronMethod = SearchResultDownloadTaskFactory.class.getDeclaredMethod("getCronExpressionForCleanup");
        getCronMethod.setAccessible(true);
        return (String) getCronMethod.invoke(factory);
    }

    private <T extends Throwable> T expectThrows(Class<T> expectedType, Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " to be thrown");
        } catch (Throwable throwable) {
            if (expectedType.isInstance(throwable)) {
                return expectedType.cast(throwable);
            }
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " but got " +
                throwable.getClass().getSimpleName(), throwable);
        }
    }
}
