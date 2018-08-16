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
import org.apache.atlas.model.impexp.AtlasCluster;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.repository.impexp.ZipSource;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.atlas.web.integration.BaseResourceIT;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
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
    private final String EXPORT_REQUEST_FILE = "export-incremental";
    private final String SOURCE_CLUSTER_NAME = "cl1";

    static final String IMPORT_TRANSFORM_CLEAR_ATTRS =
            "{ \"Asset\": { \"*\":[ \"clearAttrValue:replicatedToCluster,replicatedFromCluster\" ] } }";
    static final String IMPORT_TRANSFORM_SET_DELETED =
            "{ \"Asset\": { \"*\":[ \"setDeleted\" ] } }";

    @Test
    public void isActive() throws AtlasServiceException {
        assertEquals(atlasClientV2.getAdminStatus(), "ACTIVE");
    }

    @Test(dependsOnMethods = "isActive")
    public void importData() throws AtlasServiceException {
        performImport(FILE_TO_IMPORT);
        assertReplicationData("cl1");
    }

    @Test(dependsOnMethods = "importData")
    public void exportData() throws AtlasServiceException, IOException, AtlasBaseException {
        final int EXPECTED_CREATION_ORDER_SIZE = 10;

        AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(".", EXPORT_REQUEST_FILE, AtlasExportRequest.class);
        byte[] exportedBytes = atlasClientV2.exportData(request);
        assertNotNull(exportedBytes);

        ZipSource zs = new ZipSource(new ByteArrayInputStream(exportedBytes));
        assertNotNull(zs.getExportResult());
        assertTrue(zs.getCreationOrder().size() > EXPECTED_CREATION_ORDER_SIZE);
    }

    private void performImport(String fileToImport) throws AtlasServiceException {
        AtlasImportRequest request = new AtlasImportRequest();
        request.getOptions().put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, SOURCE_CLUSTER_NAME);
        request.getOptions().put(AtlasImportRequest.TRANSFORMS_KEY, IMPORT_TRANSFORM_CLEAR_ATTRS);

        performImport(fileToImport, request);
    }

    private void performImport(String fileToImport, AtlasImportRequest request) throws AtlasServiceException {

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

    private void assertReplicationData(String clusterName) throws AtlasServiceException {
        AtlasCluster cluster = atlasClientV2.getCluster(clusterName);
        assertNotNull(cluster);
        assertNotNull(cluster.getAdditionalInfo());
        assertTrue(cluster.getAdditionalInfo().size() > 0);
    }

    @AfterClass
    protected void teardown() {
        AtlasImportRequest request = new AtlasImportRequest();
        request.getOptions().put(AtlasImportRequest.TRANSFORMS_KEY, IMPORT_TRANSFORM_SET_DELETED);

        try {
            performImport(FILE_TO_IMPORT, request);
        } catch (AtlasServiceException e) {
            throw new SkipException("performTeardown: failed! Subsequent tests results may be affected.", e);
        }
    }
}
