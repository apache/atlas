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

import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.migration.MigrationImportStatus;
import org.apache.atlas.repository.ogm.DataAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

@AtlasService
public class DataMigrationStatusService {
    private static final Logger LOG = LoggerFactory.getLogger(DataMigrationStatusService.class);

    private final DataAccess dataAccess;
    private MigrationImportStatus status;

    @Inject
    public DataMigrationStatusService(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public MigrationImportStatus getCreate(MigrationImportStatus status) {
        try {
            this.status = this.dataAccess.load(status);
            this.status.setSize(status.getSize());
            this.status.setStartTime(status.getStartTime());

            this.status = dataAccess.save(this.status);
        } catch (Exception ex) {
            LOG.info("DataMigrationStatusService: Setting status: {}...", status.getName());
            try {
                this.status = dataAccess.save(status);
            } catch (AtlasBaseException e) {
                LOG.info("DataMigrationStatusService: Error saving status: {}...", status.getName());
            }
        }

        return this.status;
    }

    public MigrationImportStatus get() {
        return this.status;
    }

    public MigrationImportStatus getByName(String name) throws AtlasBaseException {
        MigrationImportStatus status = new MigrationImportStatus(name);

        return dataAccess.load(status);
    }

    public void deleteStatus() throws AtlasBaseException {
        if (this.status == null) {
            return;
        }

        MigrationImportStatus status = getByName(this.status.getName());
        dataAccess.delete(status.getGuid());
    }

    public void savePosition(String position) {
        this.status.setPosition(position);
        try {
            this.dataAccess.saveNoLoad(this.status);
        } catch (AtlasBaseException e) {
            LOG.error("Error saving status: {}", position, e);
        }
    }

    public void setEndTime() {
        this.status.setEndTime(System.currentTimeMillis());
        try {
            this.dataAccess.saveNoLoad(this.status);
        } catch (AtlasBaseException e) {
            LOG.error("Error saving status: endTime", e);
        }
    }

    public MigrationImportStatus createGet(String fileToImport, int streamSize) {
        MigrationImportStatus status = new MigrationImportStatus(fileToImport);
        status.setSize(streamSize);

        return getCreate(status);
    }
}
