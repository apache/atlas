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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ImportFilePathValidatorTest {
    private File importDirectory;
    private String previousAllowedDirectory;

    @BeforeMethod
    public void setUp() throws Exception {
        importDirectory = Files.createTempDirectory("atlas-import-allowed-").toFile();
        previousAllowedDirectory = ApplicationProperties.get().getString(AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getPropertyName());

        ApplicationProperties.get().setProperty(AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getPropertyName(),
                importDirectory.getAbsolutePath());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (previousAllowedDirectory == null) {
            ApplicationProperties.get().clearProperty(AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getPropertyName());
        } else {
            ApplicationProperties.get().setProperty(AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getPropertyName(),
                    previousAllowedDirectory);
        }

        deleteRecursively(importDirectory);
    }

    @Test
    public void validateAcceptsReadableZipInAllowedDirectory() throws Exception {
        File zipFile = new File(importDirectory, "export.zip");
        assertTrue(zipFile.createNewFile());

        File validatedFile = ImportFilePathValidator.validate("export.zip");

        assertNotNull(validatedFile);
        assertEquals(validatedFile.getCanonicalPath(), zipFile.getCanonicalPath());
    }

    @Test
    public void validateAcceptsNestedZipInAllowedDirectory() throws Exception {
        File nestedDir = new File(importDirectory, "nested");
        assertTrue(nestedDir.mkdir());

        File zipFile = new File(nestedDir, "export.zip");
        assertTrue(zipFile.createNewFile());

        File validatedFile = ImportFilePathValidator.validate("nested/export.zip");

        assertEquals(validatedFile.getCanonicalPath(), zipFile.getCanonicalPath());
    }

    @Test
    public void validateRejectsAbsolutePath() {
        assertImportFileNotAccessible("/etc/passwd");
    }

    @Test
    public void validateRejectsPathTraversal() {
        assertImportFileNotAccessible("../outside.zip");
    }

    @Test
    public void validateRejectsNonZipExtension() throws Exception {
        File textFile = new File(importDirectory, "notes.txt");
        assertTrue(textFile.createNewFile());

        assertImportFileNotAccessible("notes.txt");
    }

    @Test
    public void validateRejectsMissingFile() {
        assertImportFileNotAccessible("missing.zip");
    }

    @Test
    public void validateRejectsExistingNonZipFile() throws Exception {
        File passwdLikeFile = new File(importDirectory, "passwd");
        assertTrue(passwdLikeFile.createNewFile());

        assertImportFileNotAccessible("passwd");
    }

    @Test
    public void validateRejectsWhenAllowedDirectoryNotConfigured() throws Exception {
        ApplicationProperties.get().clearProperty(AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getPropertyName());

        assertImportFileNotAccessible("export.zip");
    }

    @Test
    public void validateReturnsSameErrorForMissingAndInvalidFiles() throws Exception {
        File existingFile = new File(importDirectory, "existing.txt");
        assertTrue(existingFile.createNewFile());

        AtlasErrorCode missingFileError = getImportFileError("missing.zip");
        AtlasErrorCode existingNonZipError = getImportFileError("existing.txt");

        assertEquals(missingFileError, AtlasErrorCode.IMPORT_FILE_NOT_ACCESSIBLE);
        assertEquals(existingNonZipError, AtlasErrorCode.IMPORT_FILE_NOT_ACCESSIBLE);
        assertEquals(missingFileError, existingNonZipError);
    }

    private AtlasErrorCode getImportFileError(String fileName) {
        try {
            ImportFilePathValidator.validate(fileName);
            fail("Expected AtlasBaseException for fileName=" + fileName);
        } catch (AtlasBaseException excp) {
            return excp.getAtlasErrorCode();
        }

        return null;
    }

    private void assertImportFileNotAccessible(String fileName) {
        try {
            ImportFilePathValidator.validate(fileName);
            fail("Expected AtlasBaseException for fileName=" + fileName);
        } catch (AtlasBaseException excp) {
            assertEquals(excp.getAtlasErrorCode(), AtlasErrorCode.IMPORT_FILE_NOT_ACCESSIBLE);
        }
    }

    private static void deleteRecursively(File file) throws IOException {
        if (file == null || !file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] children = file.listFiles();

            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }

        if (!file.delete()) {
            file.deleteOnExit();
        }
    }
}
