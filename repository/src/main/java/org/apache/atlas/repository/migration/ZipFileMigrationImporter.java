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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.repository.impexp.ImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

public class ZipFileMigrationImporter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ZipFileMigrationImporter.class);

    private static String ENV_USER_NAME = "user.name";

    private final ImportService importService;
    private final String fileToImport;

    public ZipFileMigrationImporter(ImportService importService, String fileName) {
        this.importService = importService;
        this.fileToImport = fileName;
    }

    @Override
    public void run() {
        try {
            FileWatcher fileWatcher = new FileWatcher(fileToImport);
            fileWatcher.start();

            performImport(new FileInputStream(new File(fileToImport)));
        } catch (IOException e) {
            LOG.error("Migration Import: IO Error!", e);
        } catch (AtlasBaseException e) {
            LOG.error("Migration Import: Error!", e);
        }
    }

    private void performImport(InputStream fs) throws AtlasBaseException {
        try {
            LOG.info("Migration Import: {}: Starting...", fileToImport);

            RequestContext.get().setUser(getUserNameFromEnvironment(), null);

            importService.run(fs, getImportRequest(),
                    getUserNameFromEnvironment(),
                    InetAddress.getLocalHost().getHostName(),
                    InetAddress.getLocalHost().getHostAddress());

        } catch (Exception ex) {
            LOG.error("Error loading zip for migration", ex);
            throw new AtlasBaseException(ex);
        } finally {
            LOG.info("Migration Import: {}: Done!", fileToImport);
        }
    }

    private String getUserNameFromEnvironment() {
        return System.getProperty(ENV_USER_NAME);
    }

    private AtlasImportRequest getImportRequest() throws AtlasException {
        AtlasImportRequest request = new AtlasImportRequest();
        request.setOption(AtlasImportRequest.OPTION_KEY_FORMAT, AtlasImportRequest.OPTION_KEY_FORMAT_ZIP_DIRECT);
        return request;
    }
}
