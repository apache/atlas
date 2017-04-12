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


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipSinkTest {
    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private ZipSink zipSink;
    private List<String> defaultExportOrder = new ArrayList<>(Arrays.asList("a", "b", "c", "d"));
    private AtlasExportResult defaultExportResult;

    private void initZipSinkWithExportOrder() throws AtlasBaseException {
        zipSink = new ZipSink(byteArrayOutputStream);
        zipSink.setExportOrder(defaultExportOrder);
        zipSink.close();
    }

    private AtlasExportResult getDefaultExportResult() {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("hive_db", "qualifiedName", "default"));
        request.setItemsToExport(itemsToExport);

        defaultExportResult = new AtlasExportResult(request, "admin", "1.0.0.0", "root", 100);
        return defaultExportResult;
    }

    private ZipInputStream getZipInputStreamForDefaultExportOrder() throws AtlasBaseException {
        initZipSinkWithExportOrder();

        ByteArrayInputStream bis = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        return new ZipInputStream(bis);
    }

    private String getZipEntryAsStream(ZipInputStream zis) throws IOException {
        byte[] buf = new byte[1024];
        int n = 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((n = zis.read(buf, 0, 1024)) > -1) {
            bos.write(buf, 0, n);
        }

        Assert.assertNotNull(bos);
        return bos.toString();
    }

    @Test
    public void correctInit_succeeds() throws AtlasBaseException {
        initZipSinkWithExportOrder();
        Assert.assertTrue(true);
        Assert.assertNotNull(zipSink);
    }

    @Test
    public void zipWithExactlyOneEntry_succeeds() {

        try {
            ZipInputStream zis = getZipInputStreamForDefaultExportOrder();

            try {
                Assert.assertNotNull(zis.getNextEntry());
                Assert.assertNull(zis.getNextEntry());
            } catch (IOException e) {

                Assert.assertTrue(false);
            }
        } catch (AtlasBaseException e) {

            Assert.assertTrue(false, "No exception should be thrown.");
        }
    }

    @Test
    public void verifyExportOrderEntryName_verifies() throws AtlasBaseException, IOException {

        ZipInputStream zis = getZipInputStreamForDefaultExportOrder();
        ZipEntry ze = zis.getNextEntry();

        Assert.assertEquals(ze.getName().replace(".json", ""), ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString());
    }

    @Test
    public void zipWithExactlyOneEntry_ContentsVerified() throws AtlasBaseException, IOException {

        ZipInputStream zis = getZipInputStreamForDefaultExportOrder();
        zis.getNextEntry();

        Assert.assertEquals(getZipEntryAsStream(zis).replace("\"", "'"), "['a','b','c','d']");
    }

    @Test
    public void zipWithExactlyTwoEntries_ContentsVerified() throws AtlasBaseException, IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        useZipSinkToCreateZipWithTwoEntries(byteOutputStream);

        ByteArrayInputStream bis = new ByteArrayInputStream(byteOutputStream.toByteArray());
        ZipInputStream zipStream = new ZipInputStream(bis);
        ZipEntry entry = zipStream.getNextEntry();

        Assert.assertEquals(getZipEntryAsStream(zipStream), "[\"a\",\"b\",\"c\",\"d\"]");
        Assert.assertEquals(entry.getName().replace(".json", ""), ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString());

        entry = zipStream.getNextEntry();
        Assert.assertEquals(entry.getName().replace(".json", ""), ZipExportFileNames.ATLAS_EXPORT_INFO_NAME.toString());
        Assert.assertTrue(compareJsonWithObject(getZipEntryAsStream(zipStream), defaultExportResult));
    }

    private void useZipSinkToCreateZipWithTwoEntries(ByteArrayOutputStream byteOutputStream) throws AtlasBaseException {
        ZipSink zs = new ZipSink(byteOutputStream);
        zs.setExportOrder(defaultExportOrder);
        zs.setResult(getDefaultExportResult());
        zs.close();
    }

    private boolean compareJsonWithObject(String s, AtlasExportResult defaultExportResult) {
        String json = AtlasType.toJson(defaultExportResult);
        return json.equals(s);
    }
}
