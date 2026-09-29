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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Validates server-side import file paths for {@code /api/atlas/admin/importfile}.
 * Resolves paths relative to a configured base directory and rejects path traversal.
 */
public final class ImportFilePathValidator {
    private static final Logger LOG = LoggerFactory.getLogger(ImportFilePathValidator.class);
    private static final String ZIP_EXTENSION = ".zip";

    private ImportFilePathValidator() {
    }

    public static File validate(String fileName) throws AtlasBaseException {
        try {
            return doValidate(fileName);
        } catch (AtlasBaseException excp) {
            if (excp.getAtlasErrorCode() == AtlasErrorCode.IMPORT_FILE_NOT_ACCESSIBLE) {
                throw excp;
            }

            throw importFileNotAccessible(fileName, excp.getMessage());
        } catch (Exception excp) {
            throw importFileNotAccessible(fileName, excp.getMessage());
        }
    }

    private static File doValidate(String fileName) throws AtlasBaseException, IOException {
        String allowedDirectory = AtlasConfiguration.IMPORT_ALLOWED_DIRECTORY.getString();

        if (StringUtils.isBlank(allowedDirectory)) {
            throw importFileNotAccessible(fileName, "atlas.import.allowed.directory is not configured");
        }

        if (new File(fileName).isAbsolute()) {
            throw importFileNotAccessible(fileName, "absolute paths are not allowed");
        }

        File allowedBase = getCanonicalDirectory(new File(allowedDirectory));
        File importFile = getCanonicalFile(new File(allowedBase, fileName));

        if (!isWithinDirectory(importFile, allowedBase)) {
            throw importFileNotAccessible(fileName, "path is outside the configured import directory");
        }

        if (!StringUtils.endsWithIgnoreCase(importFile.getName(), ZIP_EXTENSION)) {
            throw importFileNotAccessible(fileName, "file is not a zip archive");
        }

        if (!importFile.isFile() || !importFile.canRead()) {
            throw importFileNotAccessible(fileName, "file does not exist or is not readable");
        }

        return importFile;
    }

    private static File getCanonicalDirectory(File directory) throws AtlasBaseException, IOException {
        if (!directory.exists()) {
            throw importFileNotAccessible(directory.getPath(), "configured import directory does not exist");
        }

        if (!directory.isDirectory()) {
            throw importFileNotAccessible(directory.getPath(), "configured import path is not a directory");
        }

        return directory.getCanonicalFile();
    }

    private static File getCanonicalFile(File file) throws IOException {
        return file.getCanonicalFile();
    }

    private static boolean isWithinDirectory(File file, File directory) throws IOException {
        String filePath      = file.getCanonicalPath();
        String directoryPath = directory.getCanonicalPath();

        return filePath.equals(directoryPath) || filePath.startsWith(directoryPath + File.separator);
    }

    private static AtlasBaseException importFileNotAccessible(String fileName, String reason) {
        LOG.warn("Rejected import file request for '{}': {}", fileName, reason);

        return new AtlasBaseException(AtlasErrorCode.IMPORT_FILE_NOT_ACCESSIBLE);
    }
}
