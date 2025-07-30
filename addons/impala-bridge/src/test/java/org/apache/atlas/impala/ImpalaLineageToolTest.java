package org.apache.atlas.impala;

/** Licensed to the Apache Software Foundation (ASF) under one
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
**/

import org.apache.atlas.impala.hook.ImpalaLineageHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
        import static org.testng.Assert.*;

public class ImpalaLineageToolTest {

    private static final Logger LOG = LoggerFactory.getLogger(ImpalaLineageToolTest.class);

    @Mock
    private ImpalaLineageHook mockImpalaLineageHook;

    private Path tempDir;
    private final PrintStream originalOut = System.out;
    private ByteArrayOutputStream testOut;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);

        tempDir = Files.createTempDirectory("impala-lineage-test");

        testOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOut));
    }

    @AfterMethod
    public void tearDown() throws IOException {

        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        System.setOut(originalOut);
    }

    @Test
    public void testConstructorWithValidArguments() {
        String[] args = {"-d", "/test/directory", "-p", "test_prefix"};

        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        assertNotNull(tool);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testConstructorWithInvalidArguments() {
        String[] args = {"-invalid", "argument"};

        new ImpalaLineageTool(args);
    }

    @Test
    public void testGetCurrentFilesWithNoFiles() throws IOException {
        String[] args = {"-d", tempDir.toString(), "-p", "nonexistent"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        File[] files = tool.getCurrentFiles();

        assertNotNull(files);
        assertEquals(files.length, 0);
    }

    @Test
    public void testGetCurrentFilesWithSingleFile() throws IOException {

        File testFile = new File(tempDir.toFile(), "test_lineage.log");
        testFile.createNewFile();

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        File[] files = tool.getCurrentFiles();

        assertNotNull(files);
        assertEquals(files.length, 1);
        assertEquals(files[0].getName(), "test_lineage.log");
    }

    @Test
    public void testGetCurrentFilesWithMultipleFilesSorted() throws IOException {

        File file1 = new File(tempDir.toFile(), "test_file1.log");
        File file2 = new File(tempDir.toFile(), "test_file2.log");
        File file3 = new File(tempDir.toFile(), "test_file3.log");

        file1.createNewFile();
        file2.createNewFile();
        file3.createNewFile();


        long baseTime = System.currentTimeMillis();
        file1.setLastModified(baseTime - 3000); // 3 seconds ago
        file2.setLastModified(baseTime - 2000); // 2 seconds ago
        file3.setLastModified(baseTime - 1000); // 1 second ago

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        File[] files = tool.getCurrentFiles();

        assertNotNull(files);
        assertEquals(files.length, 3);

        assertTrue(files[0].lastModified() <= files[1].lastModified());
        assertTrue(files[1].lastModified() <= files[2].lastModified());
    }

    @Test
    public void testGetCurrentFilesWithNonExistentDirectory() {
        String[] args = {"-d", "/nonexistent/directory", "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        File[] files = tool.getCurrentFiles();

        assertNotNull(files);
        assertEquals(files.length, 0);
    }

    @Test
    public void testDeleteLineageAndWalSuccess() throws IOException {
        File lineageFile = new File(tempDir.toFile(), "test_lineage.log");
        File walFile = new File(tempDir.toFile(), "test_wal.wal");

        lineageFile.createNewFile();
        walFile.createNewFile();

        assertTrue(lineageFile.exists());
        assertTrue(walFile.exists());

        ImpalaLineageTool.deleteLineageAndWal(lineageFile, walFile.getAbsolutePath());

        assertFalse(lineageFile.exists());
        assertFalse(walFile.exists());
    }

    @Test
    public void testDeleteLineageAndWalNonExistentFiles() {
        File nonExistentFile = new File(tempDir.toFile(), "nonexistent.log");
        String nonExistentWalPath = tempDir.resolve("nonexistent.wal").toString();

        ImpalaLineageTool.deleteLineageAndWal(nonExistentFile, nonExistentWalPath);
    }

    @Test
    public void testImportHImpalaEntitiesWithNewWalFile() throws Exception {

        File lineageFile = new File(tempDir.toFile(), "test_lineage.log");
        String testContent = "test lineage content";

        try (FileWriter writer = new FileWriter(lineageFile)) {
            writer.write(testContent);
        }

        String walPath = tempDir.resolve("test.wal").toString();

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);


        doNothing().when(mockImpalaLineageHook).process(anyString());

        tool.importHImpalaEntities(mockImpalaLineageHook, lineageFile.getAbsolutePath(), walPath);

        verify(mockImpalaLineageHook, times(1)).process(testContent);

        File walFile = new File(walPath);
        assertTrue(walFile.exists());
    }

    @Test
    public void testImportHImpalaEntitiesWithExistingWalFile() throws Exception {

        File lineageFile = new File(tempDir.toFile(), "test_lineage.log");
        String testContent = "test lineage content";

        try (FileWriter writer = new FileWriter(lineageFile)) {
            writer.write(testContent);
        }


        String walPath = tempDir.resolve("test.wal").toString();
        File walFile = new File(walPath);
        try (FileWriter writer = new FileWriter(walFile)) {
            writer.write("0, "+  lineageFile.getAbsolutePath());
        }

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        doNothing().when(mockImpalaLineageHook).process(anyString());

        tool.importHImpalaEntities(mockImpalaLineageHook, lineageFile.getAbsolutePath(), walPath);

        verify(mockImpalaLineageHook, times(1)).process(testContent);
    }

    @Test
    public void testImportHImpalaEntitiesWithProcessingFailure() throws Exception {

        File lineageFile = new File(tempDir.toFile(), "test_lineage.log");
        String testContent = "test lineage content";

        try (FileWriter writer = new FileWriter(lineageFile)) {
            writer.write(testContent);
        }

        String walPath = tempDir.resolve("test.wal").toString();

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        doThrow(new RuntimeException("Processing failed")).when(mockImpalaLineageHook).process(anyString());

        tool.importHImpalaEntities(mockImpalaLineageHook, lineageFile.getAbsolutePath(), walPath);

        verify(mockImpalaLineageHook, times(1)).process(testContent);

        File walFile = new File(walPath);
        assertTrue(walFile.exists());
        String walContent = new String(Files.readAllBytes(walFile.toPath()));
        assertEquals(walContent.trim(), "0, "+  lineageFile.getAbsolutePath());
    }


    @Test
    public void testMainWithIncorrectNumberOfArguments() {
        String[] args = {"-d", tempDir.toString()};

        ImpalaLineageTool.main(args);

        String output = testOut.toString();
        assertTrue(output.contains("wrong number of arguments"));
        assertTrue(output.contains("Usage: import-impala.sh"));
    }


    @Test
    public void testRunWithMultipleFiles() throws IOException {
        // Create multiple test files
        File file1 = new File(tempDir.toFile(), "test_file1.log");
        File file2 = new File(tempDir.toFile(), "test_file2.log");
        File file3 = new File(tempDir.toFile(), "test_file3.log");

        file1.createNewFile();
        try (FileWriter writer = new FileWriter(file1)) {
            writer.write("content1");
        }

        file2.createNewFile();
        try (FileWriter writer = new FileWriter(file2)) {
            writer.write("content2");
        }

        file3.createNewFile();
        try (FileWriter writer = new FileWriter(file3)) {
            writer.write("content3");
        }

        long baseTime = System.currentTimeMillis();
        file1.setLastModified(baseTime - 3000);
        file2.setLastModified(baseTime - 2000);
        file3.setLastModified(baseTime - 1000);

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        try {
            tool.run();
        } catch (Exception e) {
            LOG.info("Error running test case ImpalaLineageToolTest.testRunWithMultipleFiles()");
        }

        assertFalse(file1.exists());
        assertFalse(file2.exists());
        assertTrue(file3.exists());
    }

    @Test
    public void testRunWithNoFiles() {
        String[] args = {"-d", tempDir.toString(), "-p", "nonexistent"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        tool.run();
    }

    @Test
    public void testRunWithSingleFile() throws IOException {
        File testFile = new File(tempDir.toFile(), "test_single.log");
        testFile.createNewFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("single file content");
        }

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        try {
            tool.run();
        } catch (Exception e) {
            LOG.info("Error running test case ImpalaLineageToolTest.testRunWithSingleFile()");
        }

        assertTrue(testFile.exists());
    }

    @Test
    public void testProcessImpalaLineageHookSuccess() throws Exception {
        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        java.lang.reflect.Method method = ImpalaLineageTool.class.getDeclaredMethod(
                "processImpalaLineageHook",
                ImpalaLineageHook.class,
                java.util.List.class
        );
        method.setAccessible(true);

        doNothing().when(mockImpalaLineageHook).process(anyString());

        java.util.List<String> lineageList = java.util.Arrays.asList("query1", "query2", "query3");
        boolean result = (Boolean) method.invoke(tool, mockImpalaLineageHook, lineageList);

        assertTrue(result);
        verify(mockImpalaLineageHook, times(3)).process(anyString());
    }

    @Test
    public void testProcessImpalaLineageHookWithFailures() throws Exception {
        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        // Use reflection to access the private method
        java.lang.reflect.Method method = ImpalaLineageTool.class.getDeclaredMethod(
                "processImpalaLineageHook",
                ImpalaLineageHook.class,
                java.util.List.class
        );
        method.setAccessible(true);

        doNothing().when(mockImpalaLineageHook).process("query1");
        doThrow(new RuntimeException("Processing failed")).when(mockImpalaLineageHook).process("query2");
        doNothing().when(mockImpalaLineageHook).process("query3");

        java.util.List<String> lineageList = java.util.Arrays.asList("query1", "query2", "query3");
        boolean result = (Boolean) method.invoke(tool, mockImpalaLineageHook, lineageList);

        assertFalse(result);
        verify(mockImpalaLineageHook, times(3)).process(anyString());
    }

    @Test
    public void testProcessImpalaLineageHookWithEmptyList() throws Exception {
        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        java.lang.reflect.Method method = ImpalaLineageTool.class.getDeclaredMethod(
                "processImpalaLineageHook",
                ImpalaLineageHook.class,
                java.util.List.class
        );
        method.setAccessible(true);

        java.util.List<String> emptyList = java.util.Collections.emptyList();
        boolean result = (Boolean) method.invoke(tool, mockImpalaLineageHook, emptyList);

        assertTrue(result);
        verify(mockImpalaLineageHook, never()).process(anyString());
    }

    @Test
    public void testGetCurrentFilesWithMultipleFilesSortedByModificationTime() throws IOException, InterruptedException {
        // Create multiple test files and ensure different modification times
        File file1 = new File(tempDir.toFile(), "test_old.log");
        file1.createNewFile();
        file1.setLastModified(System.currentTimeMillis() - 3000); // 3 seconds ago

        File file2 = new File(tempDir.toFile(), "test_medium.log");
        file2.createNewFile();
        file2.setLastModified(System.currentTimeMillis() - 2000); // 2 seconds ago

        File file3 = new File(tempDir.toFile(), "test_new.log");
        file3.createNewFile();
        file3.setLastModified(System.currentTimeMillis() - 1000); // 1 second ago

        String[] args = {"-d", tempDir.toString(), "-p", "test"};
        ImpalaLineageTool tool = new ImpalaLineageTool(args);

        File[] files = tool.getCurrentFiles();

        assertNotNull(files);
        assertEquals(files.length, 3);
        assertEquals(files[0].getName(), "test_old.log");
        assertEquals(files[1].getName(), "test_medium.log");
        assertEquals(files[2].getName(), "test_new.log");
    }

}
