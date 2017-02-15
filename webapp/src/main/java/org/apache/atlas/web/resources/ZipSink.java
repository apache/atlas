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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipSink {
    private static final Logger LOG = LoggerFactory.getLogger(ZipSink.class);

    private ZipOutputStream zipOutputStream;
    private ByteArrayOutputStream byteArrayOutputStream;

    public ZipSink() {
        init();
    }

    private void init() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
    }

    public void add(AtlasEntity entity) throws AtlasBaseException {
        String jsonData = convertToJSON(entity);
        saveToZip(entity.getGuid(), jsonData);
    }

    public void setResult(AtlasExportResult result) throws AtlasBaseException {
        String jsonData = convertToJSON(result);
        saveToZip(ZipExportFileNames.ATLAS_EXPORT_INFO_NAME, jsonData);
    }

    public void setTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        String jsonData = convertToJSON(typesDef);
        saveToZip(ZipExportFileNames.ATLAS_TYPESDEF_NAME, jsonData);
    }

    public void setExportOrder(List<String> result) throws AtlasBaseException {
        String jsonData = convertToJSON(result);
        saveToZip(ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME, jsonData);
    }

    public void writeTo(OutputStream stream) throws IOException {
        byteArrayOutputStream.writeTo(stream);
    }

    public void close() {
        try {
            if(zipOutputStream != null) {
                zipOutputStream.close();
                zipOutputStream = null;
            }
        } catch (IOException e) {
            LOG.error("Error closing Zip file", e);
        }
    }

    private String convertToJSON(Object entity) {
        return AtlasType.toJson(entity);
    }

    private void saveToZip(ZipExportFileNames fileName, String jsonData) throws AtlasBaseException {
        saveToZip(fileName.toString(), jsonData);
    }

    private void saveToZip(String fileName, String jsonData) throws AtlasBaseException {
        try {
            addToZipStream(fileName.toString() + ".json", jsonData);
        } catch (IOException e) {
            throw new AtlasBaseException(String.format("Error writing file %s.", fileName), e);
        }
    }

    private void addToZipStream(String entryName, String payload) throws IOException {

        ZipEntry e = new ZipEntry(entryName);
        zipOutputStream.putNextEntry(e);

        zipOutputStream.write(payload.getBytes());
        zipOutputStream.closeEntry();
    }
}
