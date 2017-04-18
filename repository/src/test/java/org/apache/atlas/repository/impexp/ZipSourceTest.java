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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class ZipSourceTest {
    @DataProvider(name = "zipFileStocks")
    public static Object[][] getDataFromZipFile() throws IOException {
        FileInputStream fs = ZipFileResourceTestUtils.getFileInputStream("stocks.zip");

        return new Object[][] {{ new ZipSource(fs) }};
    }

    @Test
    public void improperInit_ReturnsNullCreationOrder() throws IOException, AtlasBaseException {
        byte bytes[] = new byte[10];
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ZipSource zs = new ZipSource(bais);
        List<String> s = zs.getCreationOrder();
        Assert.assertNull(s);
    }

    @Test(dataProvider = "zipFileStocks")
    public void examineContents_BehavesAsExpected(ZipSource zipSource) throws IOException, AtlasBaseException {
        List<String> creationOrder = zipSource.getCreationOrder();

        Assert.assertNotNull(creationOrder);
        Assert.assertEquals(creationOrder.size(), 4);

        AtlasTypesDef typesDef = zipSource.getTypesDef();
        Assert.assertNotNull(typesDef);
        Assert.assertEquals(typesDef.getEntityDefs().size(), 6);

        useCreationOrderToFetchEntitiesWithExtInfo(zipSource, creationOrder);
        useCreationOrderToFetchEntities(zipSource, creationOrder);
        attemptToFetchNonExistentGuid_ReturnsNull(zipSource, "non-existent-guid");
        verifyGuidRemovalOnImportComplete(zipSource, creationOrder.get(0));
    }

    private void useCreationOrderToFetchEntities(ZipSource zipSource, List<String> creationOrder) {
        for (String guid : creationOrder) {
            AtlasEntity e = zipSource.getByGuid(guid);
            Assert.assertNotNull(e);
        }
    }

    private void verifyGuidRemovalOnImportComplete(ZipSource zipSource, String guid) {
        AtlasEntity e = zipSource.getByGuid(guid);
        Assert.assertNotNull(e);

        zipSource.onImportComplete(guid);

        e = zipSource.getByGuid(guid);
        Assert.assertNull(e);
    }

    private void attemptToFetchNonExistentGuid_ReturnsNull(ZipSource zipSource, String guid) {
        AtlasEntity e = zipSource.getByGuid(guid);
        Assert.assertNull(e);
    }

    private void useCreationOrderToFetchEntitiesWithExtInfo(ZipSource zipSource, List<String> creationOrder) throws AtlasBaseException {
        for (String guid : creationOrder) {
            AtlasEntity.AtlasEntityExtInfo e = zipSource.getEntityWithExtInfo(guid);
            Assert.assertNotNull(e);
        }
    }

    @Test(dataProvider = "zipFileStocks")
    public void iteratorBehavor_WorksAsExpected(ZipSource zipSource) throws IOException, AtlasBaseException {
        Assert.assertTrue(zipSource.hasNext());

        List<String> creationOrder = zipSource.getCreationOrder();
        for (int i = 0; i < creationOrder.size(); i++) {
            AtlasEntity e = zipSource.next();

            Assert.assertNotNull(e);
            Assert.assertEquals(e.getGuid(), creationOrder.get(i));
        }

        Assert.assertFalse(zipSource.hasNext());
    }
}
