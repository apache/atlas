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
import org.apache.atlas.model.migration.MigrationImportStatus;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Map;
import java.util.zip.ZipFile;

public class ZipFileMigrationImporter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ZipFileMigrationImporter.class);

    private static final String APPLICATION_PROPERTY_MIGRATION_NUMER_OF_WORKERS = "atlas.migration.mode.workers";
    private static final String APPLICATION_PROPERTY_MIGRATION_BATCH_SIZE = "atlas.migration.mode.batch.size";
    private static final String DEFAULT_NUMBER_OF_WORKERS = "4";
    private static final String DEFAULT_BATCH_SIZE = "100";
    private static final String ZIP_FILE_COMMENT_ENTITIES_COUNT = "entitiesCount";
    private static final String ZIP_FILE_COMMENT_TOTAL_COUNT = "total";

    private final static String ENV_USER_NAME = "user.name";

    private final ImportService importService;
    private final String fileToImport;
    private DataMigrationStatusService dataMigrationStatusService;
    private MigrationImportStatus migrationImportStatus;

    public ZipFileMigrationImporter(ImportService importService, String fileName) {
        this.importService = importService;
        this.fileToImport = fileName;
        this.dataMigrationStatusService = new DataMigrationStatusService(AtlasGraphProvider.getGraphInstance());
    }

    @Override
    public void run() {
        try {
            detectFileToImport();

            int streamSize = getStreamSizeFromComment(fileToImport);
            migrationImportStatus = getCreateMigrationStatus(fileToImport, streamSize);
            performImport(fileToImport, streamSize, Long.toString(migrationImportStatus.getCurrentIndex()));
            dataMigrationStatusService.setStatus("DONE");
        } catch (IOException e) {
            LOG.error("Migration Import: IO Error!", e);
            dataMigrationStatusService.setStatus("FAIL");
        } catch (AtlasBaseException e) {
            LOG.error("Migration Import: Error!", e);
            dataMigrationStatusService.setStatus("FAIL");
        }
    }

    private MigrationImportStatus getCreateMigrationStatus(String fileName, int streamSize) {
        MigrationImportStatus status = new MigrationImportStatus(fileName);
        status.setTotalCount(streamSize);

        MigrationImportStatus statusRetrieved = dataMigrationStatusService.getCreate(status);

        LOG.info("DataMigrationStatusService: Position: {}", statusRetrieved.getCurrentIndex());
        dataMigrationStatusService.setStatus("STARTED");
        return statusRetrieved;
    }

    private void detectFileToImport() throws IOException {
        FileWatcher fileWatcher = new FileWatcher(fileToImport);
        fileWatcher.start();
    }

    private int getStreamSizeFromComment(String fileToImport) {
        int ret = 1;
        try {
            ZipFile zipFile = new ZipFile(fileToImport);
            String comment = zipFile.getComment();
            ret = processZipFileStreamSizeComment(comment);
            zipFile.close();
        } catch (IOException e) {
            LOG.error("Error opening ZIP file: {}", fileToImport, e);
        }

        return ret;
    }

    private int processZipFileStreamSizeComment(String comment) {
        if (StringUtils.isEmpty(comment)) {
            return 1;
        }

        Map map = AtlasType.fromJson(comment, Map.class);
        int entitiesCount = (int) map.get(ZIP_FILE_COMMENT_ENTITIES_COUNT);
        int totalCount = (int) map.get(ZIP_FILE_COMMENT_TOTAL_COUNT);
        LOG.info("ZipFileMigrationImporter: Zip file: Comment: streamSize: {}: total: {}", entitiesCount, totalCount);

        return entitiesCount;
    }

    private void performImport(String fileToImport, int streamSize, String startPosition) throws AtlasBaseException {
        try {
            LOG.info("Migration Import: {}: Starting at: {}...", fileToImport, startPosition);
            InputStream fs = new FileInputStream(new File(fileToImport));
            RequestContext.get().setUser(getUserNameFromEnvironment(), null);

            importService.run(fs, getImportRequest(fileToImport, streamSize, startPosition),
                    getUserNameFromEnvironment(),
                    InetAddress.getLocalHost().getHostName(),
                    InetAddress.getLocalHost().getHostAddress());

        } catch (Exception ex) {
            LOG.error("Migration Import: Error loading zip for migration!", ex);
            throw new AtlasBaseException(ex);
        } finally {
            LOG.info("Migration Import: {}: Done!", fileToImport);
        }
    }

    private String getUserNameFromEnvironment() {
        return System.getProperty(ENV_USER_NAME);
    }

    private AtlasImportRequest getImportRequest(String fileToImport, int streamSize, String position) throws AtlasException {
        AtlasImportRequest request = new AtlasImportRequest();

        request.setOption(AtlasImportRequest.OPTION_KEY_MIGRATION_FILE_NAME, fileToImport);
        request.setSizeOption(streamSize);
        request.setOption(AtlasImportRequest.OPTION_KEY_MIGRATION, "true");
        request.setOption(AtlasImportRequest.OPTION_KEY_NUM_WORKERS, getPropertyValue(APPLICATION_PROPERTY_MIGRATION_NUMER_OF_WORKERS, DEFAULT_NUMBER_OF_WORKERS));
        request.setOption(AtlasImportRequest.OPTION_KEY_BATCH_SIZE, getPropertyValue(APPLICATION_PROPERTY_MIGRATION_BATCH_SIZE, DEFAULT_BATCH_SIZE));
        request.setOption(AtlasImportRequest.START_POSITION_KEY, (StringUtils.isEmpty(position) ? "0" : position));

        return request;
    }

    private String getPropertyValue(String property, String defaultValue) throws AtlasException {
        return ApplicationProperties.get().getString(property, defaultValue);
    }
}
