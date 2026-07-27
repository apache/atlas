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
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.CSV_FILE_EXTENSION;

final class SearchDownloadFileValidator {
    static final String INVALID_DOWNLOAD_FILE_NAME_MSG = "Invalid download file name";

    private SearchDownloadFileValidator() {
    }

    static File resolveDownloadFile(String fileName, File userDownloadDir) throws AtlasBaseException {
        validateFileName(fileName);

        File csvFile = new File(userDownloadDir, fileName);

        try {
            String userDirPath = userDownloadDir.getCanonicalPath();
            String filePath    = csvFile.getCanonicalPath();

            if (!filePath.startsWith(userDirPath + File.separator)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
            }
        } catch (IOException e) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
        }

        return csvFile;
    }

    private static void validateFileName(String fileName) throws AtlasBaseException {
        if (StringUtils.isBlank(fileName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
        }

        if (fileName.indexOf('/') >= 0 || fileName.indexOf('\\') >= 0 || fileName.contains("..")) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
        }

        if (!fileName.endsWith(CSV_FILE_EXTENSION)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
        }

        if (fileName.indexOf("_BASIC_") <= 0 && fileName.indexOf("_DSL_") <= 0) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, INVALID_DOWNLOAD_FILE_NAME_MSG);
        }
    }
}
