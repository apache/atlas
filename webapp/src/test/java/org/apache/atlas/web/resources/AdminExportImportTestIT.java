/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.resources;


import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.repository.impexp.ZipSource;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.atlas.web.integration.BaseResourceIT;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AdminExportImportTestIT extends BaseResourceIT {
    private final String FILE_TO_IMPORT = "stocks-base.zip";

    @Test
    public void isActive() throws AtlasServiceException {
        assertEquals(atlasClientV2.getAdminStatus(), "ACTIVE");
    }

    @Test(dependsOnMethods = "isActive")
    public void importData() throws AtlasServiceException, IOException {
        performImport(FILE_TO_IMPORT);
    }

    @Test(dependsOnMethods = "importData")
    public void exportData() throws AtlasServiceException, IOException, AtlasBaseException {
        final int EXPECTED_CREATION_ORDER_SIZE = 13;

        AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(".", "export-incremental", AtlasExportRequest.class);
        byte[] exportedBytes = atlasClientV2.exportData(request);
        assertNotNull(exportedBytes);

        ZipSource zs = new ZipSource(new ByteArrayInputStream(exportedBytes));
        assertNotNull(zs.getExportResult());
        assertEquals(zs.getCreationOrder().size(), EXPECTED_CREATION_ORDER_SIZE);
    }

    private void performImport(String fileToImport) throws AtlasServiceException {
        AtlasImportRequest request = new AtlasImportRequest();
        byte[] fileBytes = new byte[0];
        try {
            fileBytes = Files.readAllBytes(Paths.get(TestResourceFileUtils.getTestFilePath(fileToImport)));
        } catch (IOException e) {
            assertFalse(true, "Exception: " + e.getMessage());
        }
        AtlasImportResult result = atlasClientV2.importData(request, fileBytes);

        assertNotNull(result);
        assertEquals(result.getOperationStatus(), AtlasImportResult.OperationStatus.SUCCESS);
        assertNotNull(result.getMetrics());
        assertEquals(result.getProcessedEntities().size(), 37);
    }
}
