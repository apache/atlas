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


import com.google.common.collect.ImmutableList;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

import static org.apache.atlas.model.impexp.AtlasExportRequest.OPTION_KEY_REPLICATED_TO;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runExportWithParameters;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithParameters;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ReplicationEntityAttributeTest extends ExportImportTestBase {
    private final String ENTITIES_SUB_DIR = "stocksDB-Entities";
    private final String EXPORT_REQUEST_FILE = "export-replicatedTo";
    private final String IMPORT_REQUEST_FILE = "import-replicatedFrom";

    private final String DB_GUID = "1637a33e-6512-447b-ade7-249c8cb5344b";
    private final String TABLE_GUID = "df122fc3-5555-40f8-a30f-3090b8a622f8";

    private  String REPLICATED_TO_CLUSTER_NAME = "";
    private  String REPLICATED_FROM_CLUSTER_NAME = "";

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private EntityGraphMapper graphMapper;

    @Inject
    ExportService exportService;

    @Inject
    ImportService importService;

    @Inject
    AtlasServerService atlasServerService;

    private AtlasEntityStoreV2 entityStore;

    private ZipSource zipSource;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        basicSetup(typeDefStore, typeRegistry);
        entityStore = new AtlasEntityStoreV2(deleteHandler, typeRegistry, mockChangeNotifier, graphMapper);
        createEntities(entityStore, ENTITIES_SUB_DIR, new String[]{"db", "table-columns"});
        AtlasType refType = typeRegistry.getType("Referenceable");
        AtlasEntityDef entityDef = (AtlasEntityDef) typeDefStore.getByName(refType.getTypeName());
        assertNotNull(entityDef);
    }

    @BeforeMethod
    public void setupTest() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER,null);
    }

    @Test
    public void exportWithReplicationToOption_AddsClusterObjectIdToReplicatedFromAttribute() throws AtlasBaseException {
        final int expectedEntityCount = 2;

        AtlasExportRequest request = getUpdateMetaInfoUpdateRequest();
        zipSource = runExportWithParameters(exportService, request);

        assertNotNull(zipSource);
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), expectedEntityCount);

        assertCluster(REPLICATED_TO_CLUSTER_NAME, null);
        assertReplicationAttribute(Constants.ATTR_NAME_REPLICATED_TO);
    }

    @Test(dependsOnMethods = "exportWithReplicationToOption_AddsClusterObjectIdToReplicatedFromAttribute")
    public void importWithReplicationFromOption_AddsClusterObjectIdToReplicatedFromAttribute() throws AtlasBaseException, IOException {
        AtlasImportRequest request = getImportRequestWithReplicationOption();
        AtlasImportResult importResult = runImportWithParameters(importService, request, zipSource);

        assertCluster(REPLICATED_FROM_CLUSTER_NAME, importResult);
        assertReplicationAttribute(Constants.ATTR_NAME_REPLICATED_FROM);
    }

    private void assertReplicationAttribute(String attrNameReplication) throws AtlasBaseException {
        pauseForIndexCreation();
        AtlasEntity.AtlasEntitiesWithExtInfo entities = entityStore.getByIds(ImmutableList.of(DB_GUID, TABLE_GUID));
        for (AtlasEntity e : entities.getEntities()) {
            Object ex = e.getAttribute(attrNameReplication);
            assertNotNull(ex);

            List<String> clusterNameSyncType = (List) ex;
            assertEquals(clusterNameSyncType.size(), 1);
        }
    }

    private void assertCluster(String name, AtlasImportResult importResult) throws AtlasBaseException {
        AtlasServer actual = atlasServerService.get(new AtlasServer(name, name));

        assertNotNull(actual);
        assertEquals(actual.getName(), name);

        if(importResult != null) {
            assertClusterAdditionalInfo(actual, importResult);
        }
    }

    private void assertClusterAdditionalInfo(AtlasServer cluster, AtlasImportResult importResult) throws AtlasBaseException {
        AtlasExportRequest request = importResult.getExportResult().getRequest();
        AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(request.getItemsToExport().get(0).getTypeName());
        AtlasEntity.AtlasEntityWithExtInfo entity = entityStore.getByUniqueAttributes(type, request.getItemsToExport().get(0).getUniqueAttributes());
        long actualLastModifiedTimestamp = (long) cluster.getAdditionalInfoRepl(entity.getEntity().getGuid());

        assertTrue(cluster.getAdditionalInfo().size() > 0);
        assertEquals(actualLastModifiedTimestamp, importResult.getExportResult().getLastModifiedTimestamp());
    }

    private AtlasExportRequest getUpdateMetaInfoUpdateRequest() {
        AtlasExportRequest request = getExportRequestWithReplicationOption();
        request.getOptions().put(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, REPLICATED_TO_CLUSTER_NAME);

        return request;
    }

    private AtlasExportRequest getExportRequestWithReplicationOption() {
        try {
            AtlasExportRequest request = TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, EXPORT_REQUEST_FILE, AtlasExportRequest.class);
            REPLICATED_TO_CLUSTER_NAME = (String) request.getOptions().get(OPTION_KEY_REPLICATED_TO);
            return request;
        } catch (IOException e) {
            throw new SkipException(String.format("getExportRequestWithReplicationOption: '%s' could not be laoded.", EXPORT_REQUEST_FILE));
        }
    }

    private AtlasImportRequest getImportRequestWithReplicationOption() {
        try {
            AtlasImportRequest request = TestResourceFileUtils.readObjectFromJson(ENTITIES_SUB_DIR, IMPORT_REQUEST_FILE, AtlasImportRequest.class);
            REPLICATED_FROM_CLUSTER_NAME = request.getOptions().get(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM);
            return request;
        } catch (IOException e) {
            throw new SkipException(String.format("getExportRequestWithReplicationOption: '%s' could not be laoded.", IMPORT_REQUEST_FILE));
        }
    }
}
