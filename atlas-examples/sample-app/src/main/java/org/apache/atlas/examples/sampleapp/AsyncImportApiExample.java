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
package org.apache.atlas.examples.sampleapp;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.impexp.AsyncImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;

public class AsyncImportApiExample {
    private final AtlasClientV2 client;

    public AsyncImportApiExample(AtlasClientV2 client) {
        this.client = client;
    }

    public AtlasAsyncImportRequest testImportAsyncWithZip() throws Exception {
        URL url = AsyncImportApiExample.class.getClassLoader().getResource("importFile.zip");

        if (url == null) {
            System.err.println("importFile.zip not found in classpath.");

            return null;
        }

        File               zipFile = new File(url.toURI());
        AtlasImportRequest request = new AtlasImportRequest();

        try (InputStream zipStream = Files.newInputStream(zipFile.toPath())) {
            System.out.println("Testing Async Import with ZIP file...");

            try {
                AtlasAsyncImportRequest asyncRequest = client.importAsync(request, zipStream);

                System.out.println("Async Import Request Created: " + asyncRequest);

                return asyncRequest;
            } catch (AtlasServiceException e) {
                System.err.println("Async Import with ZIP file failed: " + e.getMessage());

                throw e;
            }
        } catch (IOException e) {
            System.err.println("Failed to open ZIP file: " + e.getMessage());

            throw e;
        }
    }

    public void testGetAsyncImportStatus() throws Exception {
        System.out.println("Testing getAllAsyncImportStatus...");

        try {
            PList<AsyncImportStatus> statuses = client.getAsyncImportStatus(null, null);

            System.out.println("All Async Import Statuses:");
            for (AsyncImportStatus status : statuses.getList()) {
                System.out.println(status);
            }
        } catch (AtlasServiceException e) {
            System.err.println("Failed to fetch all async import statuses: " + e.getMessage());

            throw e;
        }
    }

    public void testGetAsyncImportStatusById(String importId) throws Exception {
        System.out.println("Testing getImportStatus for id=" + importId);

        try {
            AtlasAsyncImportRequest importStatus = client.getAsyncImportStatusById(importId);

            System.out.println("Import Status for ID (" + importId + "): " + importStatus);
        } catch (AtlasServiceException e) {
            System.err.println("Failed to fetch import status for id=" + importId + ": " + e.getMessage());

            throw e;
        }
    }

    public void testAbortAsyncImportById(String importId) throws Exception {
        System.out.println("Testing abortAsyncImport for id=" + importId);

        try {
            client.abortAsyncImport(importId);

            System.out.println("Successfully aborted async import with ID: " + importId);
        } catch (AtlasServiceException e) {
            System.err.println("Failed to abort async import for ID (" + importId + "): " + e.getMessage());

            throw e;
        }
    }
}
