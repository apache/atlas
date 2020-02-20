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


import com.google.inject.Inject;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.bulkimport.MigrationImport;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class MigrationImportTest  extends ExportImportTestBase {

    private final ImportService importService;

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private EntityDiscoveryService discoveryService;

    @Inject
    AtlasEntityStore entityStore;

    @Inject
    AtlasGraph atlasGraph;

    @Inject
    public MigrationImportTest(ImportService importService) {
        this.importService = importService;
    }

    @Test
    public void simpleImport() throws IOException, AtlasBaseException {
        InputStream inputStream = ZipFileResourceTestUtils.getFileInputStream("zip-direct-2.zip");

        AtlasImportRequest importRequest = new AtlasImportRequest();
        importRequest.setOption("migration", "true");

        AtlasImportResult result = importService.run(inputStream, importRequest, null, null, null);
        assertNotNull(result);
    }
}
