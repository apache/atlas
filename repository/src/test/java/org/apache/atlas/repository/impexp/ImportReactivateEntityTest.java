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
package org.apache.atlas.repository.impexp;

import com.google.inject.Inject;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;

import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.getInputStreamFrom;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.getDefaultImportRequest;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadFsModel;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadHiveModel;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.runImportWithParameters;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ImportReactivateEntityTest extends ExportImportTestBase {
    private static final String ENTITY_TYPE_COL        = "hive_column";
    private static final String COLUMNS_ATTR_NAME      = "columns";
    private static final String STORAGE_DESC_ATTR_NAME = "sd";

    private static final String ENTITY_GUID_TABLE_WITH_REL_ATTRS    = "e19e5683-d9ae-436a-af1e-0873582d0f1e";
    private static final String ENTITY_GUID_TABLE_WITHOUT_REL_ATTRS = "027a987e-867a-4c98-ac1e-c5ded41130d3";

    private static final String REPL_FROM = "cl1";

    @Inject
    private ImportService importService;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStore entityStore;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @BeforeTest
    public void setup() throws IOException, AtlasBaseException {
        RequestContextV1.clear();
        RequestContextV1.get().setUser(TestUtilsV2.TEST_USER);
        basicSetup(typeDefStore, typeRegistry);
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testWithRelationshipAttr() throws AtlasBaseException, IOException {
        testReactivation(ENTITY_GUID_TABLE_WITH_REL_ATTRS, 2);
    }

    @Test
    public void testWithoutRelationshipAttr() throws AtlasBaseException, IOException {
        testReactivation(ENTITY_GUID_TABLE_WITHOUT_REL_ATTRS, 7);
    }

    private void testReactivation(String tableEntityGuid, int columnCount) throws AtlasBaseException, IOException {
        importSeedData();

        String newColumnGuid = addColumnToTable(tableEntityGuid);
        columnCount++;

        entityStore.deleteById(tableEntityGuid);
        AtlasEntity.AtlasEntityWithExtInfo entity = entityStore.getById(tableEntityGuid);
        assertEquals(entity.getEntity().getStatus(), DELETED);

        importSeedData();

        assertActivatedEntities(tableEntityGuid, newColumnGuid, columnCount);
    }

    private void importSeedData() throws AtlasBaseException, IOException {
        loadFsModel(typeDefStore, typeRegistry);
        loadHiveModel(typeDefStore, typeRegistry);
        AtlasImportRequest atlasImportRequest = getDefaultImportRequest();

        runImportWithParameters(importService, atlasImportRequest, getDataWithoutRelationshipAttrs());
        runImportWithParameters(importService, atlasImportRequest, getDataWithRelationshipAttrs());
    }

    private InputStream getDataWithoutRelationshipAttrs() {
        return getInputStreamFrom("stocks.zip");
    }

    private InputStream getDataWithRelationshipAttrs() {
        return getInputStreamFrom("repl_exp_1.zip");
    }

    private String addColumnToTable(String tableEntityGuid)  throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entity = entityStore.getById(tableEntityGuid);
        AtlasEntity newColumn = createColumn(entity.getEntity());
        String newColumnGuid = newColumn.getGuid();
        assertNotNull(newColumnGuid);
        return newColumnGuid;
    }

    private AtlasEntity createColumn(AtlasEntity tableEntity) throws AtlasBaseException {
        AtlasEntity ret = new AtlasEntity(ENTITY_TYPE_COL);
        String name = "new_column";

        ret.setAttribute("name", name);
        ret.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, name + REPL_FROM);
        ret.setAttribute("type", "int");
        ret.setAttribute("comment", name);
        ret.setAttribute("table", AtlasTypeUtil.getAtlasObjectId(tableEntity));

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(ret), false);
        ret.setAttribute(GUID_PROPERTY_KEY,  response.getCreatedEntities().get(0).getGuid());

        return ret;
    }

    private void assertActivatedEntities(String tableEntityGuid, String newColumnGuid,int columnCount) throws AtlasBaseException {
        AtlasEntity atlasEntity = entityStore.getById(tableEntityGuid).getEntity();

        String sdGuid = ((AtlasObjectId) atlasEntity.getAttribute(STORAGE_DESC_ATTR_NAME)).getGuid();
        AtlasEntity sd = entityStore.getById(sdGuid).getEntity();
        assertEquals(sd.getStatus(), ACTIVE);

        assertEquals(atlasEntity.getStatus(), ACTIVE);
        List<AtlasObjectId> columns = (List<AtlasObjectId>) atlasEntity.getAttribute(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), columnCount);

        int activeColumnCount = 0;
        int deletedColumnCount = 0;
        for (AtlasObjectId column : columns) {
            final AtlasEntity.AtlasEntityWithExtInfo byId = entityStore.getById(column.getGuid());
            if (column.getGuid().equals(newColumnGuid)){
                assertEquals(byId.getEntity().getStatus(), DELETED);
                deletedColumnCount++;
            }else{
                assertEquals(byId.getEntity().getStatus(), ACTIVE);
                activeColumnCount++;
            }
        }
        assertEquals(activeColumnCount, --columnCount);
        assertEquals(deletedColumnCount, 1);
    }
}