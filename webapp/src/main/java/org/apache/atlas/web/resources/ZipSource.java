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
package org.apache.atlas.web.resources;

import org.codehaus.jackson.type.TypeReference;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v1.EntityImportStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.atlas.AtlasErrorCode.JSON_ERROR_OBJECT_MAPPER_NULL_RETURNED;


public class ZipSource implements EntityImportStream {
    private static final Logger LOG = LoggerFactory.getLogger(ZipSource.class);

    private final ByteArrayInputStream          inputStream;
    private List<String>                        creationOrder;
    private Iterator<String>                    iterator;
    private Map<String, String>                 guidEntityJsonMap;

    public ZipSource(ByteArrayInputStream inputStream) throws IOException {
        this.inputStream = inputStream;
        guidEntityJsonMap = new HashMap<>();

        updateGuidZipEntryMap();
        this.setCreationOrder();
    }

    public AtlasTypesDef getTypesDef() throws AtlasBaseException {
        final String fileName = ZipExportFileNames.ATLAS_TYPESDEF_NAME.toString();

        String s = getFromCache(fileName);
        return convertFromJson(AtlasTypesDef.class, s);
    }

    private void setCreationOrder() {
        String fileName = ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString();

        try {
            String s = getFromCache(fileName);
            this.creationOrder = convertFromJson(new TypeReference<List<String>>(){}, s);
            this.iterator = this.creationOrder.iterator();
        } catch (AtlasBaseException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", fileName), e);
        }
    }

    private void updateGuidZipEntryMap() throws IOException {

        inputStream.reset();

        ZipInputStream zipInputStream = new ZipInputStream(inputStream);
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        while (zipEntry != null) {
            String entryName = zipEntry.getName().replace(".json", "");

            if (guidEntityJsonMap.containsKey(entryName)) continue;

            byte[] buf = new byte[1024];

            int n = 0;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            while ((n = zipInputStream.read(buf, 0, 1024)) > -1) {
                bos.write(buf, 0, n);
            }

            guidEntityJsonMap.put(entryName, bos.toString());
            zipEntry = zipInputStream.getNextEntry();

        }

        zipInputStream.close();
    }

    public List<String> getCreationOrder() throws AtlasBaseException {
        return this.creationOrder;
    }

    public AtlasEntity getEntity(String guid) throws AtlasBaseException {
        String s = getFromCache(guid);
        return convertFromJson(AtlasEntity.class, s);
    }

    private <T> T convertFromJson(TypeReference clazz, String jsonData) throws AtlasBaseException {
        try {
            ObjectMapper mapper = new ObjectMapper();

            T ret = mapper.readValue(jsonData, clazz);
            if(ret == null) {
                throw new AtlasBaseException(JSON_ERROR_OBJECT_MAPPER_NULL_RETURNED, clazz.toString());
            }

            return ret;
        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }
    }

    private <T> T convertFromJson(Class<T> clazz, String jsonData) throws AtlasBaseException {
        try {
            ObjectMapper mapper = new ObjectMapper();

            return mapper.readValue(jsonData, clazz);

        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }
    }

    private String getFromCache(String entryName) {
        if(!guidEntityJsonMap.containsKey(entryName)) return "";

        return guidEntityJsonMap.get(entryName).toString();
    }

    public void close() {
        try {
            inputStream.close();
            guidEntityJsonMap.clear();
        }
        catch(IOException ex) {
            LOG.warn("{}: Error closing streams.");
        }
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public AtlasEntity next() {
        try {
            return getEntity(this.iterator.next());
        } catch (AtlasBaseException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            getCreationOrder();
            this.iterator = this.creationOrder.iterator();
        } catch (AtlasBaseException e) {
            LOG.error("reset", e);
        }
    }

    @Override
    public AtlasEntity getByGuid(String guid)  {
        try {
            return getEntity(guid);
        } catch (AtlasBaseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
