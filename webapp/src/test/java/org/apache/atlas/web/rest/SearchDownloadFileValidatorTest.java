/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.common.TestUtility;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class SearchDownloadFileValidatorTest {
    private static final String VALID_FILE_NAME = "admin_BASIC_2026-07-23_09-01-50.437.csv";

    private File userDownloadDir;

    @BeforeMethod
    public void setUp() throws IOException {
        userDownloadDir = Files.createTempDirectory("search-download-validator-test").toFile();
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(userDownloadDir);
    }

    @Test
    public void testResolveDownloadFile_ValidFileName() throws Exception {
        File expectedFile = new File(userDownloadDir, VALID_FILE_NAME);

        assertTrue(expectedFile.createNewFile());

        File resolvedFile = SearchDownloadFileValidator.resolveDownloadFile(VALID_FILE_NAME, userDownloadDir);

        assertEquals(resolvedFile, expectedFile);
    }

    @Test
    public void testResolveDownloadFile_ValidDslFileName() throws Exception {
        String fileName     = "admin_DSL_2026-07-23_09-01-50.437.csv";
        File   expectedFile = new File(userDownloadDir, fileName);

        assertTrue(expectedFile.createNewFile());

        File resolvedFile = SearchDownloadFileValidator.resolveDownloadFile(fileName, userDownloadDir);

        assertEquals(resolvedFile, expectedFile);
    }

    @Test
    public void testResolveDownloadFile_PathTraversal_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("../../../../conf/users-credentials.properties", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_ParentDirectorySegment_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("..", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_AbsolutePath_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("/etc/passwd", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_InvalidExtension_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("admin_BASIC_2026-07-23_09-01-50.437.txt", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_InvalidPrefix_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("results.csv", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_BlankFileName_ThrowsException() {
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> SearchDownloadFileValidator.resolveDownloadFile("  ", userDownloadDir));

        TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
    }

    @Test
    public void testResolveDownloadFile_SymbolicLinkOutsideUserDir_ThrowsException() throws Exception {
        File outsideDir  = Files.createTempDirectory("search-download-outside").toFile();
        File outsideFile = new File(outsideDir, VALID_FILE_NAME);

        try {
            assertTrue(outsideFile.createNewFile());

            File linkInUserDir = new File(userDownloadDir, VALID_FILE_NAME);

            Files.createSymbolicLink(linkInUserDir.toPath(), outsideFile.toPath());

            AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                    () -> SearchDownloadFileValidator.resolveDownloadFile(VALID_FILE_NAME, userDownloadDir));

            TestUtility.assertBadRequests(exception, SearchDownloadFileValidator.INVALID_DOWNLOAD_FILE_NAME_MSG);
        } finally {
            FileUtils.deleteDirectory(outsideDir);
        }
    }
}
